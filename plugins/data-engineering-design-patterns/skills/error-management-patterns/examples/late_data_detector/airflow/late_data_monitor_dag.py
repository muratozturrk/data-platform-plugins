"""Late Data Detector - monitoring DAG.

The mitigation for event skew under the MAX strategy is "appropriate
late events monitoring and the possibility of reintegrating late records".
This hourly DAG reads the per-micro-batch state files written by
spark_late_data_detector.py (``--mode capture``) for the last interval and:
  * computes the late-record ratio;
  * computes the event skew = fastest - slowest partition MAX(event time);
  * detects a stuck watermark (no progress during the interval);
  * raises an alert (error log, hook your alerting on it) above thresholds and,
    when late data piles up, triggers the Dynamic Late Data Integrator DAG
    instead of losing those records. A missing job fails the run.

Requirements: apache-airflow>=2.6. Env: DP_LATE_DETECTOR_BASE_DIR.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import List

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

BASE_DIR = os.environ.get("DP_LATE_DETECTOR_BASE_DIR", "/tmp/dp/late-data-detector")
INTEGRATOR_DAG_ID = "visits_dynamic_late_data_integrator"


def summarize(states: List[dict]) -> dict:
    """Aggregate batch state documents (sorted by batch id) into interval metrics."""
    late = sum(s["late"] for s in states)
    total = sum(s["total"] for s in states)
    last = states[-1]
    times = [dt.datetime.fromisoformat(t) for t in last["partition_max"].values()]
    skew = (max(times) - min(times)).total_seconds() / 60 if times else 0.0
    return {"batches": len(states), "late": late, "total": total,
            "late_ratio": late / total if total else 0.0, "skew_minutes": skew,
            "watermark_start": states[0]["input_watermark"], "watermark_end": last["watermark"]}


@dag(
    dag_id="visits_late_data_monitor",
    schedule="@hourly",
    start_date=dt.datetime(2024, 12, 1),
    catchup=False,
    params={
        "max_late_ratio": Param(0.05, type="number"),
        "max_skew_minutes": Param(40, type="integer"),
        "reintegrate_above_late_records": Param(1000, type="integer"),
    },
    tags=["error-management", "late-data-detector"],
)
def visits_late_data_monitor():
    @task
    def collect_metrics(data_interval_start=None, data_interval_end=None) -> dict:
        states = []
        for path in (Path(BASE_DIR) / "watermark-state").glob("*.json"):
            doc = json.loads(path.read_text())
            computed = dt.datetime.fromisoformat(doc["computed_at"])
            if data_interval_start <= computed < data_interval_end:
                states.append(doc)
        if not states:
            raise AirflowFailException("no micro-batch committed in the interval: is the job down?")
        metrics = summarize(sorted(states, key=lambda s: s["batch_id"]))
        log.info("late data metrics: %s", metrics)
        return metrics

    @task
    def evaluate(metrics: dict, params: dict = None) -> bool:
        problems = []
        if metrics["late_ratio"] > params["max_late_ratio"]:
            problems.append(f"late ratio {metrics['late_ratio']:.2%}")
        if metrics["skew_minutes"] > params["max_skew_minutes"]:
            problems.append(f"event skew {metrics['skew_minutes']:.0f} min between partitions")
        if metrics["batches"] > 1 and metrics["watermark_start"] == metrics["watermark_end"]:
            problems.append("watermark did not move (stuck in the past?)")
        if problems:
            log.error("late data alert: %s", "; ".join(problems))
        return metrics["late"] >= params["reintegrate_above_late_records"]

    @task.short_circuit
    def needs_reintegration(flag: bool) -> bool:
        return flag

    reintegrate = TriggerDagRunOperator(
        task_id="trigger_late_data_integration",
        trigger_dag_id=INTEGRATOR_DAG_ID,
        conf={"reason": "late data detected by visits_late_data_monitor"},
        wait_for_completion=False,
    )

    needs_reintegration(evaluate(collect_metrics())) >> reintegrate


visits_late_data_monitor()
