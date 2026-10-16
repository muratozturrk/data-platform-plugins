"""Dead-Letter pattern - Airflow orchestration of the batch flavour.

Wires the four Dead-Letter components for a daily batch job:
  error handling  -> ``load`` Spark job (spark_error_safe_dead_letter.py) splits
                     valid / dead-lettered rows and writes a metrics JSON;
  dead-letter store -> Delta dead-letter table (written by the job);
  monitoring      -> ``evaluate_dead_letters`` reads the metrics, fails the run
                     when the error ratio means "failure, not error", and
                     compares against the previous runs to spot trends;
  replay pipeline -> optional ``replay`` job (Param ``replay_dead_letters``),
                     followed by a notification so downstream consumers can
                     decide whether to backfill (snowball backfilling effect).

Requirements: apache-airflow>=2.6, apache-airflow-providers-apache-spark.
Env / Variables: DP_DEAD_LETTER_BASE_DIR, DP_JOBS_DIR.
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
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

log = logging.getLogger(__name__)

BASE_DIR = os.environ.get("DP_DEAD_LETTER_BASE_DIR", "/tmp/dp/dead-letter/batch")
JOBS_DIR = os.environ.get("DP_JOBS_DIR", "/opt/jobs")
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"
TREND_RUNS = 7          # previous runs used for the trend check
TREND_FACTOR = 3.0      # today's ratio > 3x the average = sudden degradation


def _spark_job(task_id: str, command: str, **kwargs) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{JOBS_DIR}/spark_error_safe_dead_letter.py",
        application_args=[command, "--base-dir", BASE_DIR, "--partition-date", "{{ ds }}"],
        packages=DELTA_PACKAGE,
        conn_id="spark_default",
        **kwargs,
    )


@dag(
    dag_id="devices_loader_with_dead_letter",
    schedule="@daily",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={"replay_dead_letters": Param(False, type="boolean")},
    tags=["error-management", "dead-letter"],
)
def devices_loader_with_dead_letter():
    # The job itself fails above max_error_ratio; trigger_rule lets us still
    # inspect the metrics and alert with context.
    load = _spark_job("load_devices", "load")

    @task(trigger_rule="all_done")
    def evaluate_dead_letters(ds: str = None) -> dict:
        metrics_dir = Path(BASE_DIR) / "_metrics"
        current_file = metrics_dir / f"{ds}.json"
        if not current_file.exists():
            raise AirflowFailException(f"load did not produce {current_file}")
        current = json.loads(current_file.read_text())
        if current["status"] == "FAILED":
            raise AirflowFailException(
                f"{current['dead_lettered']} dead-lettered rows ({current['error_ratio']:.2%}) - "
                "too many to be occasional errors; consumers stay blocked")

        history = sorted(p for p in metrics_dir.glob("*.json") if p.stem < ds)[-TREND_RUNS:]
        ratios = [json.loads(p.read_text())["error_ratio"] for p in history]
        average = sum(ratios) / len(ratios) if ratios else 0.0
        if average and current["error_ratio"] > TREND_FACTOR * average:
            log.warning("dead-letter ratio %.4f is %.1fx the %s-run average %.4f",
                        current["error_ratio"], current["error_ratio"] / average,
                        len(ratios), average)
        return current

    @task.branch
    def choose_replay(metrics: dict, params: dict = None) -> str:
        if params["replay_dead_letters"] and metrics["dead_lettered"] > 0:
            return "replay_dead_letters"
        return "skip_replay"

    replay = _spark_job("replay_dead_letters", "replay")
    skip = EmptyOperator(task_id="skip_replay")

    @task
    def notify_consumers(ds: str = None) -> None:
        # Replayed rows land in an already-published partition: tell direct
        # consumers so they decide whether to backfill (they own that choice).
        log.info("partition %s received replayed rows flagged was_dead_lettered=true", ds)

    metrics = evaluate_dead_letters()
    load >> metrics
    branch = choose_replay(metrics)
    branch >> [replay, skip]
    replay >> notify_consumers()


devices_loader_with_dead_letter()
