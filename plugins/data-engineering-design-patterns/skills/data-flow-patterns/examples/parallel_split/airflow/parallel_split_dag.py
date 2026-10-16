"""Parallel Split -- Apache Airflow.

One common parent (the input sensor) feeds N isolated branches that run in parallel.
Migration scenario: the same dataset is written by the new job in two formats
(Delta Lake for the new consumers, CSV for the legacy ones) until consumers switch.

Unlike the fan-in examples there is no common child: the only shared point is the sensor.
Blocked-execution gotcha: with depends_on_past/max_active_runs=1 the slowest branch gates
the next run -- move a slow branch to its own DAG (Isolated Sequencer) if that hurts.

To stay runnable anywhere this example uses
SparkSubmitOperator instead of SparkKubernetesOperator + Sensor (swap for SparkKubernetesOperator/EmrAddStepsOperator in production).

Run: set env INPUT_DIR and JOB_PATH (path to ../spark/parallel_split_job.py) and a
`spark_default` connection, then `airflow dags test devices_parallel_split 2024-01-01`.
Requires: apache-airflow>=2.6, apache-airflow-providers-apache-spark.
"""
import os

import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

INPUT_DIR = os.environ.get("INPUT_DIR", "/tmp/dp/devices_input")
JOB_PATH = os.environ.get("JOB_PATH", "/opt/jobs/parallel_split_job.py")


@dag(
    dag_id="devices_parallel_split",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def devices_parallel_split():
    file_sensor = FileSensor(
        task_id="input_dataset_waiter",
        filepath=INPUT_DIR + "/{{ ds_nodash }}/dataset.json",
        mode="reschedule",
        timeout=3600,
    )

    for output_format in ["delta", "csv"]:
        load_job = SparkSubmitOperator(
            task_id=f"load_job_{output_format}",
            application=JOB_PATH,
            conn_id="spark_default",
            # each branch can get its own resources (e.g. a memory-heavy CSV coalesce)
            executor_memory="2g" if output_format == "csv" else "1g",
            application_args=[
                "--input", INPUT_DIR + "/{{ ds_nodash }}/dataset.json",
                "--output", f"/tmp/dp/devices_{output_format}",
                "--format", output_format,
                "--batch-id", "{{ ds_nodash }}",
            ],
        )
        file_sensor >> load_job


devices_parallel_split()
