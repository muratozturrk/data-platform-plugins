"""Filter Interceptor - regression check after each run.

Answers the question "does the 15% -> 90% filtered spike come from the
data or from a software regression?" automatically:
  1. run spark_filter_interceptor.py for the partition (writes per-filter stats);
  2. compare the per-filter ratios with the median of the previous runs;
  3. fail the run, naming the responsible filter(s), when the overall filtered
     ratio jumps - before the partial dataset is exposed to end users.

A filter whose share grows while the input looks the same points at code
(new release); several filters growing together points at the data.

Requirements: apache-airflow>=2.6, apache-airflow-providers-apache-spark.
Env: DP_FILTER_BASE_DIR, DP_JOBS_DIR.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import statistics
from pathlib import Path
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

log = logging.getLogger(__name__)

BASE_DIR = os.environ.get("DP_FILTER_BASE_DIR", "/tmp/dp/filter-interceptor")
JOBS_DIR = os.environ.get("DP_JOBS_DIR", "/opt/jobs")
HISTORY_RUNS = 14


def suspicious_filters(current: dict, history: List[dict], factor: float, min_share: float) -> Dict[str, str]:
    """Filters whose share of the input grew more than `factor` x their median."""
    findings = {}
    for name, count in current["per_filter"].items():
        share = count / current["input_rows"] if current["input_rows"] else 0.0
        past = [h["per_filter"].get(name, 0) / h["input_rows"] for h in history if h["input_rows"]]
        baseline = statistics.median(past) if past else 0.0
        if share >= min_share and share > factor * max(baseline, 1e-9):
            findings[name] = f"{share:.1%} of input (median {baseline:.1%})"
    return findings


@dag(
    dag_id="devices_filter_interceptor",
    schedule="@daily",
    start_date=dt.datetime(2024, 12, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=5)},
    params={"max_filtered_ratio_jump": Param(2.0, type="number"),
            "min_filter_share": Param(0.05, type="number")},
    tags=["error-management", "filter-interceptor"],
)
def devices_filter_interceptor():
    load = SparkSubmitOperator(
        task_id="filter_devices",
        application=f"{JOBS_DIR}/spark_filter_interceptor.py",
        application_args=["--mode", "programmatic", "--partition-date", "{{ ds }}",
                          "--base-dir", BASE_DIR],
        packages="io.delta:delta-spark_2.12:3.2.0",
        conn_id="spark_default",
        conf={"spark.speculation": "false"},
    )

    @task
    def check_filters(ds: str = None, params: dict = None) -> dict:
        stats_dir = Path(BASE_DIR) / "_filter_stats"
        current = json.loads((stats_dir / f"{ds}.json").read_text())
        history = [json.loads(p.read_text()) for p in sorted(stats_dir.glob("*.json"))
                   if p.stem < ds][-HISTORY_RUNS:]
        for name, count in current["per_filter"].items():
            log.info("%-15s %8s rows", name, count)
        if not history:
            return current
        baseline = statistics.median(h["filtered_ratio"] for h in history)
        findings = suspicious_filters(current, history, params["max_filtered_ratio_jump"],
                                      params["min_filter_share"])
        if current["filtered_ratio"] > params["max_filtered_ratio_jump"] * max(baseline, 1e-9) \
                and current["filtered_ratio"] >= params["min_filter_share"]:
            raise AirflowFailException(
                f"filtered ratio {current['filtered_ratio']:.1%} vs median {baseline:.1%}; "
                f"responsible filters: {findings or 'spread across all filters (data change?)'}")
        if findings:
            log.warning("filters growing faster than usual: %s", findings)
        return current

    load >> check_filters()


devices_filter_interceptor()
