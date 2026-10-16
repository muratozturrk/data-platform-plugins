"""Skew Detector - window-to-window volume comparison in an Airflow 2.x DAG.

Volume comparison before processing, made safer against the fatality loop: the current file is
compared with the file of the last SUCCESSFUL run (`prev_data_interval_start_success`),
not blindly with the previous schedule. If yesterday failed because of a half-empty
file, today's valid file is compared with the day before yesterday.

Flow: FileSensor (Readiness Marker) >> compare_volumes (Audit) >> transform >> load

Run:  copy to $AIRFLOW_HOME/dags; files at {INPUT_DIR}/date=YYYY-MM-DD/visits.json
Deps: apache-airflow>=2.6, apache-airflow-providers-postgres
"""
import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

INPUT_DIR = os.environ.get("VISITS_INPUT_DIR", "/opt/airflow/data/visits")
MIN_RATIO, MAX_RATIO = 0.5, 1.5


def file_path(date: pendulum.DateTime) -> str:
    return f"{INPUT_DIR}/date={date.to_date_string()}/visits.json"


@dag(schedule="@daily", start_date=pendulum.datetime(2024, 6, 1, tz="UTC"), catchup=True,
     max_active_runs=1, tags=["observability"])
def visits_loader_with_skew_detector():

    next_partition_sensor = FileSensor(task_id="wait_for_partition", fs_conn_id="fs_default",
                                       filepath=INPUT_DIR + "/date={{ ds }}/visits.json",
                                       mode="reschedule", poke_interval=60)

    @task
    def compare_volumes() -> float | None:
        context = get_current_context()
        baseline_date = context["prev_data_interval_start_success"]
        if baseline_date is None:  # first run: nothing to compare with
            return None
        current_size = os.path.getsize(file_path(context["data_interval_start"]))
        baseline_size = os.path.getsize(file_path(baseline_date))
        ratio = current_size / baseline_size
        if not MIN_RATIO <= ratio <= MAX_RATIO:
            raise AirflowFailException(
                f"Unexpected size: {current_size}B vs {baseline_size}B on {baseline_date} (ratio {ratio:.2f})")
        return ratio

    @task
    def transform_file() -> str:
        ctx = get_current_context()
        # ... flatten JSON into a CSV ready for COPY ...
        return file_path(ctx["data_interval_start"]).replace(".json", ".csv")

    load = PostgresOperator(task_id="load_flattened_visits", postgres_conn_id="dp_postgres",
                            sql="CALL dp.load_visits('{{ ds }}')")

    next_partition_sensor >> compare_volumes() >> transform_file() >> load


visits_loader_with_skew_detector()
