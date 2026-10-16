"""Stateful Merger -- Airflow orchestration of restore -> merge -> update state.

  wait_for_increment
        |
  restore_if_required   spark: --step restore       (TRUNCATE / RESTORE / nothing)
        |
  merge_increment       spark: --step merge         (MERGE with soft deletes)
        |
  update_state_table    spark: --step update-state  (versions: execution_time -> version)

All three tasks call ../spark/delta_version_restore_merger.py. Execution times come from
the schedule, not from wall-clock: `ds` and `ds - 1 day` for a daily DAG, so a backfill
of 2024-10-07 always compares with 2024-10-06 whatever the run order is. Using
`prev_data_interval_start_success` would be wrong here: it skips failed days.

Why the DAG settings matter:
  * max_active_runs=1    -- two executions interleaving restore/merge would corrupt the
                            state table (each restore assumes it is the only writer);
  * depends_on_past=True -- a backfill of day N must finish before day N+1 is replayed,
                            which is what makes day N+1 fall back into the normal run;
  * catchup / `airflow dags backfill` just re-run the same three tasks: the restore step
                            discovers the backfill from the state table on its own.

Requirements: apache-airflow>=2.6, apache-airflow-providers-apache-spark; connection
`spark_default`. The Delta tables (default.devices, default.versions) live in the
metastore configured for that Spark connection.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

JOB = "/opt/jobs/stateful_merger/delta_version_restore_merger.py"
INPUT = "/landing/devices/{{ ds }}"
DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.2.0"
COMMON_ARGS = [
    "--execution-time", "{{ ds }}",
    "--previous-execution-time", "{{ macros.ds_add(ds, -1) }}",
    "--target-table", "default.devices",
    "--state-table", "default.versions",
]
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def spark_step(task_id: str, step: str, extra: list[str] | None = None) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=JOB,
        conn_id="spark_default",
        packages=DELTA_PACKAGES,
        conf=SPARK_CONF,
        application_args=[*COMMON_ARGS, "--step", step, *(extra or [])],
        name=f"stateful-merger-{step}-{{{{ ds_nodash }}}}",
    )


with DAG(
    dag_id="devices_stateful_merger",
    start_date=datetime(2024, 10, 5),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": True,
        "retries": 2,                         # each step is safe to retry on its own
        "retry_delay": timedelta(minutes=10),
    },
    tags=["idempotency", "stateful-merger"],
) as dag:
    wait_for_increment = FileSensor(
        task_id="wait_for_increment",
        filepath=INPUT + "/_SUCCESS",
        poke_interval=300,
        timeout=6 * 3600,
        mode="reschedule",
    )
    restore_if_required = spark_step("restore_if_required", "restore")
    merge_increment = spark_step("merge_increment", "merge", ["--input", INPUT])
    update_state_table = spark_step("update_state_table", "update-state")

    wait_for_increment >> restore_if_required >> merge_increment >> update_state_table
