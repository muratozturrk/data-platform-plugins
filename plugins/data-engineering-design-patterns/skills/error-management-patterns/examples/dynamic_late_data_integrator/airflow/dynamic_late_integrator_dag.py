"""Dynamic Late Data Integrator - concurrent Airflow pipeline.

Two DAGs:

``visits_dynamic_late_data_integrator`` (daily, up to 5 concurrent runs)
    mark_partition_as_being_processed   depends_on_past=True
    process_partition
    mark_partition_as_processed         sets last processed version, is_processed=false
    get_late_partitions_and_mark_them_as_being_processed   depends_on_past=True
    generate_backfilling_arguments      reads this run's plan file
    trigger_late_data_integration       TriggerDagRunOperator mapped per partition

``visits_late_data_integration`` (no schedule)
    The backfill pipeline for one past partition: process + mark processed.
    reset_dag_run=True re-runs an existing run for the same date instead of
    failing with DagRunAlreadyExists.

The two depends_on_past tasks are the only sequential parts: they guarantee a
single planner at a time, so no partition is backfilled twice concurrently.
If the planner fails, every following planner waits - the pipeline is stuck
until someone fixes and clears that task.

Requirements: apache-airflow>=2.6,<3 (TriggerDagRunOperator ``execution_date``;
use ``logical_date`` on Airflow 3), apache-airflow-providers-apache-spark.
Env: DP_DYNAMIC_LATE_BASE_DIR, DP_JOBS_DIR.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

BASE_DIR = os.environ.get("DP_DYNAMIC_LATE_BASE_DIR", "/tmp/dp/dynamic-late-data-integrator")
JOBS_DIR = os.environ.get("DP_JOBS_DIR", "/opt/jobs")
SPARK_JOB = f"{JOBS_DIR}/spark_dynamic_late_integrator.py"
MAX_LOOKBACK_DAYS = 60          # cap for stateful consumers (very late data)
BACKFILL_DAG_ID = "visits_late_data_integration"
PLAN_FILE = BASE_DIR + "/plans/{{ ds }}/{{ run_id | replace(':', '_') | replace('+', '_') }}.json"


def spark_step(task_id: str, command: str, partition: str, *extra: str, **kwargs) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id, application=SPARK_JOB,
        application_args=[command, "--partition", partition, *extra],
        packages="io.delta:delta-spark_2.12:3.2.0", conn_id="spark_default",
        env_vars={"DP_DYNAMIC_LATE_BASE_DIR": BASE_DIR}, **kwargs)


with DAG(
    dag_id="visits_dynamic_late_data_integrator",
    schedule="@daily",
    start_date=dt.datetime(2024, 12, 1),
    catchup=True,
    max_active_runs=5,
    default_args={"depends_on_past": False, "retries": 2, "retry_delay": dt.timedelta(minutes=5)},
    tags=["error-management", "dynamic-late-data-integrator"],
) as main_dag:
    processing_marker = spark_step("mark_partition_as_being_processed", "mark-processing",
                                   "{{ ds }}", depends_on_past=True)
    process = spark_step("process_partition", "process", "{{ ds }}")
    processed_marker = spark_step("mark_partition_as_processed", "mark-processed", "{{ ds }}")
    backfill_creation_job = spark_step(
        "get_late_partitions_and_mark_them_as_being_processed", "plan", "{{ ds }}",
        "--output-file", PLAN_FILE, "--max-lookback-days", str(MAX_LOOKBACK_DAYS),
        depends_on_past=True)

    @task
    def generate_backfilling_arguments() -> list:
        context = get_current_context()
        current_partition_date = context["ds"]
        dag_run = context["dag_run"]
        dag_run_start_time = dag_run.start_date.isoformat()
        plan_file = context["task"].render_template(PLAN_FILE, context)
        configuration = json.loads(Path(plan_file).read_text())

        def _run_id_for_event_time(event_time: str) -> str:
            return f"backfill_{event_time}_from_{current_partition_date}_{dag_run_start_time}"

        return [{"execution_date": p["event_time"],
                 "trigger_run_id": _run_id_for_event_time(p["event_time"])}
                for p in configuration["partitions"]]

    trigger = TriggerDagRunOperator.partial(
        task_id="trigger_late_data_integration",
        trigger_dag_id=BACKFILL_DAG_ID,
        reset_dag_run=True,
        wait_for_completion=False,
    ).expand_kwargs(generate_backfilling_arguments())

    processing_marker >> process >> processed_marker >> backfill_creation_job
    backfill_creation_job >> trigger


with DAG(
    dag_id=BACKFILL_DAG_ID,
    schedule=None,
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=5,
    default_args={"retries": 2, "retry_delay": dt.timedelta(minutes=5)},
    tags=["error-management", "dynamic-late-data-integrator"],
) as backfill_dag:
    # The planner already set is_processed=true for this partition.
    (spark_step("process_partition", "process", "{{ ds }}")
     >> spark_step("mark_partition_as_processed", "mark-processed", "{{ ds }}"))
