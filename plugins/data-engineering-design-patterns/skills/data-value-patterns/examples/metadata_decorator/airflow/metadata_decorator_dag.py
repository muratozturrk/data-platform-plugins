"""Metadata Decorator -- metadata-table variant for a warehouse.

Relational stores have no record headers, so the technical context of every load
(execution date, loading time, code version, attempt) goes to its own table,
`dp.visits_context`, and each loaded visit references it through
`visits_context_execution_date_time`. The context is
stored once per run instead of once per row.

Business users read a view that hides the technical column (the hidden-column
alternative); only the technical role can read
`dp.visits_context` and the base weekly tables.

Idempotency: one transaction per run deletes this execution's rows (data first, then
context, because of the FK) and inserts them again. A retry therefore replaces the
rows and records the new `loading_attempt` (= try_number).

Run: put in dags/, create the Postgres connection `dp_warehouse`, load
`dp.visits_staging` upstream (e.g. with a Full/Incremental Loader).
Prereqs: apache-airflow>=2.4, apache-airflow-providers-common-sql, -postgres.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator

CONN_ID = "dp_warehouse"


def get_weekly_table_name(execution_date) -> str:
    """dp.visits_2024_23 -- one physical table per ISO week."""
    year, week, _ = execution_date.isocalendar()
    return f"dp.visits_{year}_{week:02d}"


SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS dp;
CREATE TABLE IF NOT EXISTS dp.visits_context (
    execution_date_time TIMESTAMPTZ NOT NULL,
    loading_time        TIMESTAMPTZ NOT NULL,
    code_version        VARCHAR(15) NOT NULL,
    loading_attempt     SMALLINT    NOT NULL,
    PRIMARY KEY (execution_date_time)
);
{% set weekly_table = get_weekly_table_name(execution_date) %}
CREATE TABLE IF NOT EXISTS {{ weekly_table }} (
    visit_id   CHAR(36)    NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    user_id    TEXT,
    page       TEXT,
    visits_context_execution_date_time TIMESTAMPTZ NOT NULL
        REFERENCES dp.visits_context (execution_date_time),
    PRIMARY KEY (visit_id, event_time)
);
"""

# One transaction (split statements run on one connection, committed at the end).
LOAD_SQL = """
{% set weekly_table = get_weekly_table_name(execution_date) %}
DELETE FROM {{ weekly_table }}
 WHERE visits_context_execution_date_time = '{{ execution_date }}';
DELETE FROM dp.visits_context WHERE execution_date_time = '{{ execution_date }}';

INSERT INTO dp.visits_context
    (execution_date_time, loading_time, code_version, loading_attempt)
VALUES ('{{ execution_date }}', '{{ dag_run.start_date }}',
        '{{ params.code_version }}', {{ task_instance.try_number }});

INSERT INTO {{ weekly_table }}
SELECT s.visit_id, s.event_time, s.user_id, s.page,
       '{{ execution_date }}' AS visits_context_execution_date_time
  FROM dp.visits_staging s
 WHERE s.event_time >= '{{ data_interval_start }}' AND s.event_time < '{{ data_interval_end }}';
"""

# Business-facing view: same rows, no technical column. Grants keep the context hidden.
EXPOSE_SQL = """
{% set weekly_table = get_weekly_table_name(execution_date) %}
CREATE SCHEMA IF NOT EXISTS dp_public;
CREATE OR REPLACE VIEW dp_public.visits_current_week AS
SELECT visit_id, event_time, user_id, page FROM {{ weekly_table }};
GRANT USAGE ON SCHEMA dp_public TO {{ params.business_role }};
GRANT SELECT ON dp_public.visits_current_week TO {{ params.business_role }};
REVOKE ALL ON dp.visits_context FROM {{ params.business_role }};
REVOKE ALL ON {{ weekly_table }} FROM {{ params.business_role }};
"""

# One context row for this run with the deployed code version, and every staged visit
# of the interval loaded and linked to it (SQLCheckOperator fails on any false value).
CHECK_SQL = """
{% set weekly_table = get_weekly_table_name(execution_date) %}
SELECT
  (SELECT COUNT(*) FROM dp.visits_context
    WHERE execution_date_time = '{{ execution_date }}'
      AND code_version = '{{ params.code_version }}') = 1 AS one_context_row,
  (SELECT COUNT(*) FROM {{ weekly_table }}
    WHERE visits_context_execution_date_time = '{{ execution_date }}')
  = (SELECT COUNT(*) FROM dp.visits_staging
    WHERE event_time >= '{{ data_interval_start }}' AND event_time < '{{ data_interval_end }}')
    AS all_rows_linked;
"""

with DAG(
    dag_id="metadata_decorator_context_table",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"get_weekly_table_name": get_weekly_table_name},
    params={"code_version": "v1.0.3", "business_role": "business_readers"},
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["data-value", "metadata-decorator"],
) as dag:
    setup = SQLExecuteQueryOperator(task_id="create_tables", conn_id=CONN_ID,
                                    sql=SETUP_SQL, split_statements=True)
    load = SQLExecuteQueryOperator(task_id="load_visits_with_context", conn_id=CONN_ID,
                                   sql=LOAD_SQL, split_statements=True)
    expose = SQLExecuteQueryOperator(task_id="refresh_public_view", conn_id=CONN_ID,
                                     sql=EXPOSE_SQL, split_statements=True)
    check = SQLCheckOperator(task_id="check_context_recorded", conn_id=CONN_ID, sql=CHECK_SQL)

    setup >> load >> check >> expose
