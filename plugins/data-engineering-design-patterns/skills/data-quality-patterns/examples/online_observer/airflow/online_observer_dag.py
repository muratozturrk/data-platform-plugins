"""Online Observer - observation inside the data generation DAG, Airflow 2.x.

Local Sequencer placement: the observation runs right after the data is copied, so it
observes exactly what consumers see. Observation failures must not fail the pipeline:
the final `pipeline_done` task uses trigger_rule=all_done and is the only leaf (the DAG
run state comes from leaf tasks), so it alone decides success from copy_new_visits.

  wait_for_new_data >> record_new_synchronization_state >> clean_previously_added_visits
    >> copy_new_visits >> record_new_observation_state >> insert_new_observations
    >> pipeline_done (all_done)

Run: put in dags/. Needs apache-airflow-providers-common-sql/postgres, connection
`dp_postgres`; reuses dp.visits_monitoring* tables from the offline_observer example.
"""
import pendulum
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

CONN = "dp_postgres"
SCOPE = "(SELECT first_row_id FROM dp.visits_sync_state WHERE execution_time = '{{ logical_date }}') " \
        "AND (SELECT last_row_id FROM dp.visits_sync_state WHERE execution_time = '{{ logical_date }}')"


@dag(schedule="@hourly", start_date=pendulum.datetime(2024, 1, 1), catchup=False,
     max_active_runs=1, tags=["data-quality", "observer"])
def online_observer_visits():
    wait_for_new_data = SqlSensor(task_id="wait_for_new_data", conn_id=CONN, mode="reschedule",
                                  poke_interval=120, timeout=3600, sql="""
        SELECT COUNT(*) FROM dp.visits_input WHERE id > COALESCE(
          (SELECT MAX(last_row_id) FROM dp.visits_sync_state
           WHERE execution_time < '{{ logical_date }}'::TIMESTAMP), 0)""")
    record_sync_state = SQLExecuteQueryOperator(task_id="record_new_synchronization_state", conn_id=CONN, sql="""
        CREATE TABLE IF NOT EXISTS dp.visits_sync_state (
          execution_time TIMESTAMP PRIMARY KEY, first_row_id BIGINT, last_row_id BIGINT);
        DELETE FROM dp.visits_sync_state WHERE execution_time = '{{ logical_date }}';
        INSERT INTO dp.visits_sync_state
        SELECT '{{ logical_date }}', MIN(id), MAX(id) FROM dp.visits_input WHERE id > COALESCE(
          (SELECT MAX(last_row_id) FROM dp.visits_sync_state
           WHERE execution_time < '{{ logical_date }}'::TIMESTAMP), 0);""")
    clean_previous = SQLExecuteQueryOperator(task_id="clean_previously_added_visits", conn_id=CONN,
                                             sql=f"DELETE FROM dp.visits_output WHERE id BETWEEN {SCOPE};")
    copy_new_visits = SQLExecuteQueryOperator(task_id="copy_new_visits", conn_id=CONN, sql=f"""
        INSERT INTO dp.visits_output (id, visit_id, event_time, user_id, page, context)
        SELECT id, visit_id, event_time, user_id, page, context FROM dp.visits_input
        WHERE id BETWEEN {SCOPE};""")

    # --- observation: same SQL as the Offline Observer, now inline -------------------
    record_obs_state = SQLExecuteQueryOperator(task_id="record_new_observation_state", conn_id=CONN, sql="""
        DELETE FROM dp.visits_monitoring_state WHERE execution_time = '{{ logical_date }}';
        INSERT INTO dp.visits_monitoring_state
        SELECT execution_time, first_row_id, last_row_id FROM dp.visits_sync_state
        WHERE execution_time = '{{ logical_date }}';""")
    insert_obs = SQLExecuteQueryOperator(task_id="insert_new_observations", conn_id=CONN, sql=f"""
        DELETE FROM dp.visits_monitoring WHERE execution_time = '{{{{ logical_date }}}}';
        INSERT INTO dp.visits_monitoring (execution_time, all_rows, invalid_event_time,
          invalid_user_id, invalid_page, invalid_context, distinct_users)
        SELECT '{{{{ logical_date }}}}', COUNT(*),
          SUM(CASE WHEN event_time IS NULL OR event_time > NOW() THEN 1 ELSE 0 END),
          SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN page IS NULL THEN 1 ELSE 0 END),
          SUM(CASE WHEN context IS NULL THEN 1 ELSE 0 END), COUNT(DISTINCT user_id)
        FROM dp.visits_output WHERE id BETWEEN {SCOPE};""")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def pipeline_done(ti=None):
        """Sole leaf: run succeeds once data is copied, even if observation failed."""
        copy_state = ti.get_dagrun().get_task_instance("copy_new_visits").state
        if copy_state != "success":
            raise RuntimeError(f"Data generation failed (copy_new_visits={copy_state})")

    (wait_for_new_data >> record_sync_state >> clean_previous >> copy_new_visits
     >> record_obs_state >> insert_obs >> pipeline_done())


online_observer_visits()
