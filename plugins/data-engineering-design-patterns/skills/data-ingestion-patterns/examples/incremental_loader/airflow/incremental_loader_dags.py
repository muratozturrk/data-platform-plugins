"""Incremental Loader: one DAG per implementation.

visits_incremental_partition (partition-based)
    FileSensor waits for the NEXT partition (``data_interval_end | ds``): the producer has moved on, so the
    current one is complete (convention-based readiness, see Readiness Marker). Then the Spark job copies
    ``date={{ ds }}``. Immutable template values make backfills (``airflow dags backfill``) safe.

visits_incremental_delta_column (delta column, hourly)
    No sensor: the job filters ``ingestion_time`` within [data_interval_start, data_interval_end). Each run
    has a fixed volume, and ``max_active_runs`` lets several backfill windows run in parallel.

Airflow 2.4+ with apache-airflow-providers-apache-spark. SparkKubernetesOperator + Sensor or any other
operator that passes the same immutable arguments works. Paths come from Variables.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

JOB = os.path.join(os.path.dirname(__file__), "..", "spark", "incremental_loader_jobs.py")
INPUT_BASE = Variable.get("visits_input_base", default_var="/data_for_demo/input")
OUTPUT_BASE = Variable.get("visits_output_base", default_var="/data_for_demo/output")
DEFAULTS = {"retries": 3, "retry_delay": timedelta(minutes=5), "depends_on_past": False}

with DAG(
    dag_id="visits_incremental_partition",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,                       # every past partition is a self-contained, replayable unit
    max_active_runs=3,
    default_args=DEFAULTS,
    tags=["ingestion", "incremental", "partition"],
) as partition_dag:
    next_partition_sensor = FileSensor(
        task_id="input_partition_sensor",
        filepath=INPUT_BASE + "/date={{ data_interval_end | ds }}",
        mode="reschedule",              # free the worker slot between pokes
        poke_interval=600,
        timeout=12 * 3600,
    )
    load_job = SparkSubmitOperator(
        task_id="load_partition",
        application=JOB,
        application_args=["--mode", "partition", "--input", INPUT_BASE, "--output", OUTPUT_BASE,
                          "--ds", "{{ ds }}"],
        conn_id="spark_default",
        name="visits-partition-{{ ds_nodash }}",
    )
    next_partition_sensor >> load_job

with DAG(
    dag_id="visits_incremental_delta_column",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=4,                  # concurrent backfill windows, if the source can take the load
    default_args=DEFAULTS,
    tags=["ingestion", "incremental", "delta-column"],
) as delta_column_dag:
    SparkSubmitOperator(
        task_id="load_window",
        application=JOB,
        application_args=["--mode", "delta-column", "--input", INPUT_BASE + "/visits_table",
                          "--output", OUTPUT_BASE + "/visits",
                          "--date-from", "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S') }}",
                          "--date-to", "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S') }}"],
        conn_id="spark_default",
        conf={"spark.sql.session.timeZone": "UTC"},   # data_interval_* are UTC, bounds are passed naive
        name="visits-window-{{ ts_nodash }}",
    )
