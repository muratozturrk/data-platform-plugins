"""Incremental Sessionizer -- PostgreSQL + Airflow, the reference implementation.

Storage spaces:
  input              /data_to_load/date=YYYYMMDD/dataset.csv   (one partition per run)
  dp.pending_sessions  PRIVATE: in-flight sessions + technical columns
                         (expiration_batch_id, execution_time_id)
  dp.sessions          PUBLIC: completed sessions (+ optional partial ones, is_completed=false)

Run steps:
  create_tables (idempotent DDL)
  >> [clean_previous_runs_sessions, clean_previous_runs_pending_sessions]
     DELETE ... WHERE execution_time_id >= ds: this AND later runs, because sessions are
     forward dependent (a rerun of D invalidates D+1, D+2 ...).
  >> generate_sessions (one DB session, so the temporary tables survive between statements)
     1. COPY the partition into a temporary table once (a failing query does not reload it);
     2. shape new visits like pending sessions (GROUP BY visit_id, user_id);
     3. FULL OUTER JOIN with pending sessions of exactly prev_ds;
     4. expiration_batch_id = ds + INACTIVITY_RUNS if the session got data, ds if it hit a
        closing page, unchanged otherwise;
     5. INSERT open sessions into pending_sessions, finished ones (expiration <= ds) into
        sessions; optionally also partial sessions with is_completed=false.
  >> log_run_stats

Backfill of day D: clear D and every later run (`airflow tasks clear -s D -e <now>`);
depends_on_past + max_active_runs=1 replay them in order.

Prereqs: Airflow 2.4+, apache-airflow-providers-common-sql + postgres, connection id
`dp_postgres` (credentials live in the connection). Server-side COPY needs the
pg_read_server_files role; use COPY FROM STDIN via a hook otherwise.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN = "dp_postgres"
PARAMS = {
    "inactivity_runs": 2,              # session closes after 2 runs without visits (ds_add(ds, 2))
    "closing_pages": ["logout"],      # finalization by event type
    "emit_partial_sessions": False,   # True -> freshness, consumers must filter is_completed
}

CREATE_TABLES = """
CREATE SCHEMA IF NOT EXISTS dp;
CREATE TABLE IF NOT EXISTS dp.pending_sessions (
    session_id          TEXT NOT NULL,
    user_id             TEXT,
    start_time          TIMESTAMP NOT NULL,
    last_visit_time     TIMESTAMP NOT NULL,
    pages               TEXT[] NOT NULL,
    expiration_batch_id DATE NOT NULL,
    execution_time_id   DATE NOT NULL,
    PRIMARY KEY (execution_time_id, session_id)      -- a retry cannot duplicate a pending session
);
CREATE TABLE IF NOT EXISTS dp.sessions (
    session_id        TEXT NOT NULL,
    user_id           TEXT,
    start_time        TIMESTAMP NOT NULL,
    end_time          TIMESTAMP NOT NULL,
    pages             TEXT[] NOT NULL,
    duration_seconds  BIGINT NOT NULL,
    is_completed      BOOLEAN NOT NULL,
    execution_time_id DATE NOT NULL,
    PRIMARY KEY (execution_time_id, session_id)
);
"""

GENERATE_SESSIONS = """
CREATE TEMPORARY TABLE visits_{{ ds_nodash }} (
    visit_id TEXT, user_id TEXT, event_time TIMESTAMP, page TEXT) ON COMMIT DROP;
COPY visits_{{ ds_nodash }} FROM '/data_to_load/date={{ ds_nodash }}/dataset.csv' CSV HEADER;

CREATE TEMPORARY TABLE sessions_to_classify ON COMMIT DROP AS
SELECT
    COALESCE(p.session_id, n.session_id)           AS session_id,
    COALESCE(p.user_id, n.user_id)                 AS user_id,
    LEAST(p.start_time, n.start_time)              AS start_time,       -- LEAST/GREATEST ignore NULLs
    GREATEST(p.last_visit_time, n.last_visit_time) AS last_visit_time,
    ARRAY_CAT(p.pages, n.pages)                    AS pages,            -- pending pages are older
    CASE
        WHEN n.session_id IS NULL THEN p.expiration_batch_id            -- restored, no new data
        WHEN n.has_closing_page THEN DATE '{{ ds }}'                    -- closed by event type
        ELSE DATE '{{ macros.ds_add(ds, params.inactivity_runs) }}'     -- new or extended
    END                                            AS expiration_batch_id
FROM (
    SELECT visit_id AS session_id, user_id,
           MIN(event_time) AS start_time, MAX(event_time) AS last_visit_time,
           ARRAY_AGG(page ORDER BY event_time) AS pages,
           BOOL_OR(page = ANY (ARRAY{{ params.closing_pages | tojson | replace('"', "'") }}::TEXT[])) AS has_closing_page
    FROM (SELECT DISTINCT * FROM visits_{{ ds_nodash }}                  -- duplicated input lines
          WHERE visit_id IS NOT NULL AND event_time IS NOT NULL) AS v
    GROUP BY visit_id, user_id
) AS n
FULL OUTER JOIN (
    SELECT * FROM dp.pending_sessions WHERE execution_time_id = DATE '{{ prev_ds }}'
) AS p ON n.session_id = p.session_id;

INSERT INTO dp.pending_sessions
    (session_id, user_id, start_time, last_visit_time, pages, expiration_batch_id, execution_time_id)
SELECT session_id, user_id, start_time, last_visit_time, pages, expiration_batch_id, DATE '{{ ds }}'
FROM sessions_to_classify WHERE expiration_batch_id > DATE '{{ ds }}';

INSERT INTO dp.sessions
    (session_id, user_id, start_time, end_time, pages, duration_seconds, is_completed, execution_time_id)
SELECT session_id, user_id, start_time, last_visit_time, pages,
       EXTRACT(EPOCH FROM last_visit_time - start_time)::BIGINT,
       expiration_batch_id <= DATE '{{ ds }}', DATE '{{ ds }}'
FROM sessions_to_classify
WHERE expiration_batch_id <= DATE '{{ ds }}'           -- <= also closes overdue sessions
   OR {{ 'TRUE' if params.emit_partial_sessions else 'FALSE' }};
"""

RUN_STATS = """
SELECT
    (SELECT COUNT(*) FROM dp.sessions WHERE execution_time_id = DATE '{{ ds }}' AND is_completed)  AS completed,
    (SELECT COUNT(*) FROM dp.sessions WHERE execution_time_id = DATE '{{ ds }}' AND NOT is_completed) AS partial,
    (SELECT COUNT(*) FROM dp.pending_sessions WHERE execution_time_id = DATE '{{ ds }}')           AS pending;
"""


@dag(
    dag_id="incremental_sessionizer_sql",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    params=PARAMS,
    default_args={"owner": "data-engineering", "depends_on_past": True,
                  "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["data-value", "sessionization", "incremental-sessionizer"],
)
def incremental_sessionizer_sql():
    create_tables = SQLExecuteQueryOperator(task_id="create_tables", conn_id=CONN, sql=CREATE_TABLES)
    clean_sessions = SQLExecuteQueryOperator(
        task_id="clean_previous_runs_sessions", conn_id=CONN,
        sql="DELETE FROM dp.sessions WHERE execution_time_id >= DATE '{{ ds }}';")
    clean_pending = SQLExecuteQueryOperator(
        task_id="clean_previous_runs_pending_sessions", conn_id=CONN,
        sql="DELETE FROM dp.pending_sessions WHERE execution_time_id >= DATE '{{ ds }}';")
    # autocommit=False: the whole generation is one transaction -> all-or-nothing writes.
    generate = SQLExecuteQueryOperator(task_id="generate_sessions", conn_id=CONN,
                                       sql=GENERATE_SESSIONS, autocommit=False)
    stats = SQLExecuteQueryOperator(task_id="log_run_stats", conn_id=CONN, sql=RUN_STATS,
                                    show_return_value_in_logs=True)

    create_tables >> [clean_sessions, clean_pending] >> generate >> stats


incremental_sessionizer_sql()
