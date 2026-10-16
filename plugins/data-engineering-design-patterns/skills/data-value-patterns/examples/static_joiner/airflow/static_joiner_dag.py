"""Static Joiner -- Airflow 2.x DAG for the daily SCD2 enrichment.

Flow:
  wait_for_visits      readiness marker of the primary partition (Readiness Marker)
  wait_for_reference   the reference must be loaded for the same logical date,
                       otherwise late reference data turns into join misses
  check_freshness      warn when the reference Delta table has not committed for N days
  run_static_joiner    spark-submit --mode scd2 --as-of execution_date: the join uses
                       {{ ds }}, an immutable property of the run, never NOW()
  validate_output      reads the audit JSON written by the job: row count preserved,
                       miss rate under threshold

Paths are local/mounted (e.g. /dbfs, NFS); for S3 swap the readers for S3Hook.
Run: drop into dags/, set Variables static_joiner_reference_path / _audit_path.
Prereqs: apache-airflow>=2.4, apache-airflow-providers-apache-spark.
"""
import glob
import json
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

REFERENCE_MAX_STALENESS_DAYS = 7
MAX_MISS_RATE = 0.05


def delta_last_commit(table_path: str) -> dict:
    """Reads the newest commit of a Delta table straight from _delta_log (no Spark needed)."""
    commits = sorted(glob.glob(os.path.join(table_path, "_delta_log", "*.json")))
    if not commits:
        raise FileNotFoundError(f"{table_path} is not a Delta table")
    with open(commits[-1]) as f:
        for line in f:
            action = json.loads(line)
            if "commitInfo" in action:
                return {"version": int(os.path.basename(commits[-1]).split(".")[0]),
                        "timestamp_ms": action["commitInfo"]["timestamp"]}
    raise ValueError(f"no commitInfo in {commits[-1]}")


@dag(
    dag_id="static_joiner_visits_users",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["data-value", "enrichment", "static-joiner"],
)
def static_joiner():
    wait_for_visits = FileSensor(
        task_id="wait_for_visits", fs_conn_id="fs_default", poke_interval=300, timeout=6 * 3600,
        mode="reschedule", filepath="/data/silver/visits/event_date={{ ds }}/_SUCCESS")
    wait_for_reference = FileSensor(
        task_id="wait_for_reference", fs_conn_id="fs_default", poke_interval=300, timeout=6 * 3600,
        mode="reschedule", filepath="/data/markers/dim_users/{{ ds }}/_SUCCESS")

    @task
    def check_freshness() -> int:
        commit = delta_last_commit(Variable.get("static_joiner_reference_path", "/data/dim/users"))
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(commit["timestamp_ms"] / 1000, timezone.utc)
        if age.days > REFERENCE_MAX_STALENESS_DAYS:
            # warn, don't fail: stale enrichment is flagged, the business decides
            print(f"WARNING reference v{commit['version']} is {age.days} days old")
        return commit["version"]

    run_join = SparkSubmitOperator(
        task_id="run_static_joiner",
        application="/opt/jobs/static_joiner/spark_static_joiner_batch.py",
        application_args=["--processing-date", "{{ ds }}", "--mode", "scd2", "--as-of", "execution_date"],
        conn_id="spark_default",
        packages="io.delta:delta-spark_2.12:3.1.0",
    )

    @task
    def validate_output(reference_version: int, ds=None) -> dict:
        audit_dir = Variable.get("static_joiner_audit_path", "/data/audit/static_joiner")
        files = glob.glob(f"{audit_dir}/processing_date={ds}/part-*.json")
        if not files:
            raise ValueError(f"no audit record for {ds}: the Spark job did not finish")
        with open(files[0]) as f:
            audit = json.loads(f.readline())
        if audit["output_rows"] != audit["primary_rows"]:
            raise ValueError(f"row count changed by the join: {audit}")
        if audit["output_rows"] == 0:
            raise ValueError(f"empty output for {ds}")
        if audit["miss_rate"] > MAX_MISS_RATE:
            raise ValueError(f"miss rate {audit['miss_rate']:.2%} > {MAX_MISS_RATE:.0%}: "
                             "reference late or join keys misaligned")
        if audit["reference_version"] < reference_version:
            print(f"NOTE job joined reference v{audit['reference_version']}, newer v{reference_version} exists")
        return audit

    version = check_freshness()
    [wait_for_visits, wait_for_reference] >> version >> run_join
    run_join >> validate_output(version)


static_joiner()
