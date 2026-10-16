"""Incremental Sessionizer -- Airflow orchestration of the Spark job.

Same task graph as the PostgreSQL version, with Spark/Delta storage:

  wait_for_input >> [clean_previous_runs_sessions, clean_previous_runs_pending_sessions]
                 >> generate_sessions

- execution_time_id = data_interval_start formatted '%Y-%m-%dT%H:00'. It is immutable
  per run, so retries and backfills address the same partitions (never wall-clock time).
- The runs form a chain (pending sessions of run H feed run H+1):
  catchup=True, depends_on_past=True, max_active_runs=1.
- Backfilling hour H is forward dependent: clear H AND every later run, e.g.
    airflow tasks clear incremental_sessionizer_spark -s 2024-01-15T09:00 -e <now> -y
  The clean tasks delete execution_time_id >= H, so the replay starts from a clean state.
- The input sensor waits for the Readiness Marker (`_SUCCESS`) of the hourly partition.
  An hour without visits must still run (pending sessions expire), so the producer
  writes the marker even for an empty partition.

Prereqs: Airflow 2.4+, apache-airflow-providers-apache-spark, a `spark_default`
connection. Paths come from Airflow Variables (no credentials in code).
"""
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

JOB = "/opt/jobs/incremental_sessionizer/spark_incremental_sessionizer.py"
EXECUTION_ID = "{{ data_interval_start.strftime('%Y-%m-%dT%H:00') }}"
INPUT_PATH = "{{ var.value.get('sessionizer_input_path', '/data/bronze/visits') }}"
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"


def spark_step(task_id: str, action: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=JOB,
        application_args=["--action", action, "--execution-time", EXECUTION_ID],
        conn_id="spark_default",
        packages=DELTA_PACKAGE,
        env_vars={
            "INPUT_PATH": INPUT_PATH,
            "PENDING_SESSIONS_PATH": "{{ var.value.get('sessionizer_pending_path', '/data/internal/pending_sessions') }}",
            "SESSIONS_PATH": "{{ var.value.get('sessionizer_sessions_path', '/data/silver/sessions') }}",
            "INACTIVITY_PARTITIONS": "{{ var.value.get('sessionizer_inactivity_partitions', '2') }}",
            "EMIT_PARTIAL_SESSIONS": "{{ var.value.get('sessionizer_emit_partial', 'false') }}",
        },
        name=f"incremental-sessionizer-{action}",
    )


@dag(
    dag_id="incremental_sessionizer_spark",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": True,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["data-value", "sessionization", "incremental-sessionizer"],
)
def incremental_sessionizer_spark():
    wait_for_input = FileSensor(
        task_id="wait_for_input",
        filepath=INPUT_PATH + "/date={{ data_interval_start.strftime('%Y-%m-%d') }}"
                              "/hour={{ data_interval_start.strftime('%H') }}/_SUCCESS",
        fs_conn_id="fs_default",
        mode="reschedule",
        poke_interval=120,
        timeout=6 * 3600,
    )
    clean_sessions = spark_step("clean_previous_runs_sessions", "clean-sessions")
    clean_pending = spark_step("clean_previous_runs_pending_sessions", "clean-pending")
    generate_sessions = spark_step("generate_sessions", "generate")

    wait_for_input >> [clean_sessions, clean_pending] >> generate_sessions


incremental_sessionizer_spark()
