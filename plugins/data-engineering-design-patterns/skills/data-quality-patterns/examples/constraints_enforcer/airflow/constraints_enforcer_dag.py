"""Constraints Enforcer - PostgreSQL constraints driven by an Airflow 2.x DAG.

The DAG keeps no validation code: the table declares type, NOT NULL, CHECK (value)
and FOREIGN KEY (integrity) constraints, and the load runs as one transaction, so a
single bad row rejects the whole daily load (all-or-nothing). Rejection fails the
task; fix the input and clear the task to retry.

Run: put in dags/. Needs apache-airflow-providers-common-sql + postgres and a
connection `dp_postgres`. Raw rows are expected in dp.visits_raw (TEXT columns).
"""
import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CREATE_TABLES = """
CREATE SCHEMA IF NOT EXISTS dp;
CREATE TABLE IF NOT EXISTS dp.pages (
  page VARCHAR(50) PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS dp.visits (
  visit_id   CHAR(36)    NOT NULL,
  event_time TIMESTAMP   NOT NULL CONSTRAINT event_time_not_in_future CHECK (event_time <= NOW()),
  user_id    TEXT        NOT NULL,
  page       VARCHAR(50) NOT NULL REFERENCES dp.pages(page),
  duration_s INT         CONSTRAINT duration_positive CHECK (duration_s IS NULL OR duration_s >= 0),
  PRIMARY KEY (visit_id, event_time)
);
"""

# One statement block = one transaction: DELETE + INSERT both roll back on any violation.
LOAD_DAY = """
BEGIN;
DELETE FROM dp.visits WHERE event_time::date = '{{ ds }}';
INSERT INTO dp.visits (visit_id, event_time, user_id, page, duration_s)
SELECT visit_id, event_time::TIMESTAMP, user_id, page, duration_s::INT
FROM dp.visits_raw WHERE event_time::date = '{{ ds }}';
COMMIT;
"""


@dag(schedule="@daily", start_date=pendulum.datetime(2024, 1, 1), catchup=False,
     max_active_runs=1, tags=["data-quality", "constraints"])
def constraints_enforcer_visits():
    create = SQLExecuteQueryOperator(task_id="create_constrained_tables", conn_id="dp_postgres",
                                     sql=CREATE_TABLES)
    load = SQLExecuteQueryOperator(task_id="load_visits", conn_id="dp_postgres", sql=LOAD_DAY,
                                   retries=0)  # constraint violations are not transient
    create >> load


constraints_enforcer_visits()
