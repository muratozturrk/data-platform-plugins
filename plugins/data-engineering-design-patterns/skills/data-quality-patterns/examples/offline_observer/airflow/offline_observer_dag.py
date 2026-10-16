"""Offline Observer - separate observation DAG on its own schedule, Airflow 2.x.

Runs independently of the pipeline that fills dp.visits_output and never blocks it:
  wait_for_new_data            : SqlSensor, new rows since the last observed id?
  record_new_observation_state : persist first/last row id of this run's scope
                                 (idempotent: a rerun observes exactly the same rows)
  insert_new_observations      : per-column invalid counts into dp.visits_monitoring

Run: put in dags/. Needs apache-airflow-providers-common-sql/postgres, connection
`dp_postgres`; tables are created by the first task.
"""
import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

CONN = "dp_postgres"
LAST_OBSERVED_ID = """COALESCE((SELECT MAX(last_row_id) FROM dp.visits_monitoring_state
                                 WHERE execution_time < '{{ logical_date }}'::TIMESTAMP), 0)"""

CREATE_MONITORING_TABLES = """
CREATE TABLE IF NOT EXISTS dp.visits_monitoring_state (
  execution_time TIMESTAMP PRIMARY KEY, first_row_id BIGINT, last_row_id BIGINT);
CREATE TABLE IF NOT EXISTS dp.visits_monitoring (
  execution_time TIMESTAMP PRIMARY KEY, all_rows BIGINT, invalid_event_time BIGINT,
  invalid_user_id BIGINT, invalid_page BIGINT, invalid_context BIGINT,
  distinct_users BIGINT, observed_at TIMESTAMP DEFAULT NOW());
"""

RECORD_STATE = f"""
DELETE FROM dp.visits_monitoring_state WHERE execution_time = '{{{{ logical_date }}}}';
INSERT INTO dp.visits_monitoring_state (execution_time, first_row_id, last_row_id)
SELECT '{{{{ logical_date }}}}', MIN(id), MAX(id) FROM dp.visits_output WHERE id > {LAST_OBSERVED_ID};
"""

INSERT_OBSERVATIONS = """
DELETE FROM dp.visits_monitoring WHERE execution_time = '{{ logical_date }}';
INSERT INTO dp.visits_monitoring (execution_time, all_rows, invalid_event_time, invalid_user_id,
                                    invalid_page, invalid_context, distinct_users)
SELECT '{{ logical_date }}', COUNT(*),
  SUM(CASE WHEN event_time IS NULL OR event_time > NOW() THEN 1 ELSE 0 END),
  SUM(CASE WHEN user_id IS NULL OR user_id = '' THEN 1 ELSE 0 END),
  SUM(CASE WHEN page IS NULL THEN 1 ELSE 0 END),
  SUM(CASE WHEN context IS NULL THEN 1 ELSE 0 END),
  COUNT(DISTINCT user_id)
FROM dp.visits_output v
JOIN dp.visits_monitoring_state s ON s.execution_time = '{{ logical_date }}'
WHERE v.id BETWEEN s.first_row_id AND s.last_row_id;
"""


# Nightly: away from the producers' daytime load (decoupled schedule).
@dag(schedule="0 2 * * *", start_date=pendulum.datetime(2024, 1, 1), catchup=False,
     max_active_runs=1, tags=["data-quality", "observer"])
def offline_observer_visits():
    create = SQLExecuteQueryOperator(task_id="create_monitoring_tables", conn_id=CONN,
                                     sql=CREATE_MONITORING_TABLES)
    wait_for_new_data = SqlSensor(task_id="wait_for_new_data", conn_id=CONN, mode="reschedule",
                                  poke_interval=300, timeout=6 * 3600,
                                  sql=f"SELECT COUNT(*) FROM dp.visits_output WHERE id > {LAST_OBSERVED_ID}")
    record_state = SQLExecuteQueryOperator(task_id="record_new_observation_state", conn_id=CONN, sql=RECORD_STATE)
    insert_obs = SQLExecuteQueryOperator(task_id="insert_new_observations", conn_id=CONN, sql=INSERT_OBSERVATIONS)
    create >> wait_for_new_data >> record_state >> insert_obs


offline_observer_visits()
