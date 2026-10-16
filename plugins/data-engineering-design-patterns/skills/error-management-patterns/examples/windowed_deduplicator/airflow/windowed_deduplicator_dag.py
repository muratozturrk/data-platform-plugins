"""Windowed Deduplicator - Airflow DAG for the batch flavour.

Problem: a batch job exposes visits synchronized from the streaming
layer to business users and must process each distinct record once, although
producers' automatic retries create duplicates. Per daily partition the DAG:
  1. waits for the partition synchronized by the streaming layer;
  2. runs spark_windowed_deduplicator_batch.py (scope = this partition, plus an
     optional lookback over already-published partitions);
  3. reads the job's metrics JSON and fails when the duplicate ratio jumps -
     a sign that producers' retry behaviour changed upstream.

Task retries rerun the Spark job; its replaceWhere overwrite makes that safe.

Requirements: apache-airflow>=2.6, apache-airflow-providers-apache-spark.
Env: DP_DEDUP_BASE_DIR, DP_JOBS_DIR.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

log = logging.getLogger(__name__)

BASE_DIR = os.environ.get("DP_DEDUP_BASE_DIR", "/tmp/dp/dedup")
JOBS_DIR = os.environ.get("DP_JOBS_DIR", "/opt/jobs")


@dag(
    dag_id="visits_windowed_deduplicator",
    schedule="@daily",
    start_date=datetime(2024, 12, 1),
    catchup=True,
    max_active_runs=1,   # lookback scope reads previous outputs: keep runs ordered
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "lookback_days": Param(0, type="integer", minimum=0, maximum=7),
        "max_duplicate_ratio": Param(0.2, type="number"),
    },
    tags=["error-management", "windowed-deduplicator"],
)
def visits_windowed_deduplicator():
    wait_for_partition = FileSensor(
        task_id="wait_for_partition",
        filepath=f"{BASE_DIR}/input/{{{{ ds }}}}/_SUCCESS",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=6 * 3600,
        mode="reschedule",
    )

    deduplicate = SparkSubmitOperator(
        task_id="deduplicate_partition",
        application=f"{JOBS_DIR}/spark_windowed_deduplicator_batch.py",
        application_args=[
            "--base-dir", BASE_DIR, "--partition-date", "{{ ds }}",
            "--keys", "visit_id,event_time", "--method", "row_number",
            "--order-by", "ingested_at",
            "--schema", "visit_id STRING, event_time TIMESTAMP, page STRING, ingested_at TIMESTAMP",
            "--lookback-days", "{{ params.lookback_days }}",
        ],
        packages="io.delta:delta-spark_2.12:3.2.0",
        conn_id="spark_default",
    )

    @task
    def check_duplicate_ratio(ds: str = None, params: dict = None) -> float:
        metrics = json.loads((Path(BASE_DIR) / "_metrics" / f"{ds}.json").read_text())
        ratio = metrics["duplicate_ratio"]
        log.info("partition %s: %s -> %s rows (duplicates %.2f%%)", ds, metrics["input_rows"],
                 metrics["output_rows"], ratio * 100)
        if ratio > params["max_duplicate_ratio"]:
            raise AirflowFailException(
                f"duplicate ratio {ratio:.2%} above {params['max_duplicate_ratio']:.0%}: "
                "check producer retries before exposing the data")
        return ratio

    wait_for_partition >> deduplicate >> check_duplicate_ratio()


visits_windowed_deduplicator()
