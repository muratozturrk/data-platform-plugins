"""Wrapper -- daily Airflow 2.x DAG running the Spark envelope job and proving raw stays raw.

1. wait for the Bronze partition (Readiness Marker)
2. spark-submit spark/spark_record_envelope_wrapper.py for {{ ds }}:
     job_version   <- Variable `wrapper_job_version` (set by your deployment)
     batch_version <- "{{ run_id }}/try-{{ ti.try_number }}" (immutable per attempt)
   both land in the processing_context struct of every row
3. validate the metrics JSON the job wrote: the raw part re-read from the
   output must have the same row count and checksum as the input. A mismatch
   means a transformation leaked into the raw part, which defeats the pattern.

The Spark job overwrites only the {{ ds }} partition (replaceWhere), so a
retry or a backfill is idempotent.

Run: drop into dags/. Variables: wrapper_job_version, wrapper_app (path to the
.py), optional wrapper_layout (default raw_nested). Connection: spark_default.
Prereqs: apache-airflow>=2.4, apache-airflow-providers-apache-spark.
"""
import json
from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

BRONZE = "/data/bronze/visits"
SILVER = "/data/silver/visits_wrapped"
METRICS = "/data/metrics/wrapper"


def compare_fingerprints(metrics: Dict) -> None:
    """Raise if the raw part of the output differs from the input. Pure logic."""
    src, out = metrics["input"], metrics["raw_output"]
    if src["rows"] == 0:
        raise AirflowException("input partition is empty")
    if src["rows"] != out["rows"]:
        raise AirflowException(f"row count changed: input={src['rows']} raw_output={out['rows']}")
    if src["checksum"] != out["checksum"]:
        raise AirflowException("raw part was modified: checksum mismatch between input and output")


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=3,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["data-value", "decoration"],
)
def visits_wrapper():
    wait_for_bronze = FileSensor(
        task_id="wait_for_bronze",
        filepath=f"{BRONZE}/date={{{{ ds }}}}/_SUCCESS",
        poke_interval=300, timeout=6 * 3600, mode="reschedule")

    wrap_visits = SparkSubmitOperator(
        task_id="wrap_visits",
        conn_id="spark_default",
        application="{{ var.value.wrapper_app }}",
        packages="io.delta:delta-spark_2.12:3.1.0",
        application_args=[
            "--input", f"{BRONZE}/date={{{{ ds }}}}",
            "--output", SILVER,
            "--event-date", "{{ ds }}",
            "--layout", "{{ var.value.get('wrapper_layout', 'raw_nested') }}",
            "--job-version", "{{ var.value.wrapper_job_version }}",
            "--batch-version", "{{ run_id }}/try-{{ ti.try_number }}",
            "--metrics-file", f"{METRICS}/{{{{ ds }}}}.json",
        ])

    @task
    def validate_raw_untouched(ds=None) -> Dict:
        with open(f"{METRICS}/{ds}.json", encoding="utf-8") as f:
            metrics = json.load(f)
        compare_fingerprints(metrics)
        return {"rows": metrics["input"]["rows"], "layout": metrics["layout"],
                "job_version": metrics["job_version"]}

    wait_for_bronze >> wrap_visits >> validate_raw_untouched()


visits_wrapper()


if __name__ == "__main__":
    ok = {"input": {"rows": 3, "checksum": "12"}, "raw_output": {"rows": 3, "checksum": "12"}}
    compare_fingerprints(ok)
    for bad in ({"input": {"rows": 3, "checksum": "12"}, "raw_output": {"rows": 2, "checksum": "12"}},
                {"input": {"rows": 3, "checksum": "12"}, "raw_output": {"rows": 3, "checksum": "13"}}):
        try:
            compare_fingerprints(bad)
            raise AssertionError("mismatch not detected")
        except AirflowException:
            pass
    print("self-test OK")
