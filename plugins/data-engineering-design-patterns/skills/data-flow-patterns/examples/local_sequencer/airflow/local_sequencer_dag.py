"""Local Sequencer -- Apache Airflow (data orchestration layer).

One pipeline decomposed into small, individually restartable tasks chained with `>>`:
    wait for input (Readiness Marker) >> load into internal table >> expose table to users

Each task is a restart boundary: if `expose_new_table` fails, a retry/clear does NOT rerun
the sensor or the (expensive) load. Airflow runs a task only when its upstream succeeded
(default trigger_rule=all_success), so partial data is never exposed.

Run: drop into $AIRFLOW_HOME/dags, set env INPUT_DIR and the `dp_postgres` connection,
then `airflow dags test local_sequencer 2024-01-01`.
Requires: apache-airflow>=2.6, apache-airflow-providers-common-sql, -postgres.
"""
import os

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor

INPUT_DIR = os.environ.get("INPUT_DIR", "/tmp/dp/devices")


@dag(
    dag_id="local_sequencer",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
)
def local_sequencer():
    # Step 1: Readiness Marker -- cheap, isolated; never re-run because a later step failed.
    input_data_sensor = FileSensor(
        task_id="input_data_sensor",
        filepath=INPUT_DIR + "/{{ ds_nodash }}/_SUCCESS",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",  # free the worker slot while waiting
    )

    # Step 2: Full Loader -- load + its state update stay together (transaction boundary).
    load_data_to_table = SQLExecuteQueryOperator(
        task_id="load_data_to_table",
        conn_id="dp_postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS dp.devices_{{ ds_nodash }} (LIKE dp.devices_template);
            TRUNCATE dp.devices_{{ ds_nodash }};
            COPY dp.devices_{{ ds_nodash }} FROM '""" + INPUT_DIR + """/{{ ds_nodash }}/dataset.csv'
                CSV HEADER;
        """,
    )

    # Step 3: expose -- cheap view swap; can be retried alone.
    expose_new_table = SQLExecuteQueryOperator(
        task_id="expose_new_table",
        conn_id="dp_postgres",
        sql="CREATE OR REPLACE VIEW dp.devices AS SELECT * FROM dp.devices_{{ ds_nodash }};",
    )

    input_data_sensor >> load_data_to_table >> expose_new_table


local_sequencer()
