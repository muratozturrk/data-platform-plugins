"""Static Late Data Integrator - Airflow Dynamic Task Mapping.

Each daily run processes its own partition and then re-integrates the static
lookback window: ``generate_backfilling_runs``
returns the N previous dates and ``integrate_late_data`` is expanded once per
date. Integration happens INSIDE the main pipeline, never as separate DAG runs,
otherwise runs would overlap.

``LATE_DATA_STRATEGY`` picks one of the three placements of late partitions:
  late_first     late partitions, then the current one (stateful pipelines);
  current_first  current partition first, late ones afterwards (default);
  parallel       both at the same time (stateless pipelines).

Waste-of-resources control: ``has_late_data`` keeps only the lookback dates
whose raw partition changed after it was last integrated (marker files), so
the mapped tasks run only where late data exists.

Backfilling: restart only the latest run of the range - its lookback already
re-processes the previous N days. See
``spark_static_late_integrator.py plan-backfill``.

Requirements: apache-airflow>=2.6, apache-airflow-providers-apache-spark.
Env: DP_STATIC_LATE_BASE_DIR, DP_JOBS_DIR.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path
from typing import List

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

log = logging.getLogger(__name__)

BASE_DIR = os.environ.get("DP_STATIC_LATE_BASE_DIR", "/tmp/dp/static-late-data-integrator")
JOBS_DIR = os.environ.get("DP_JOBS_DIR", "/opt/jobs")
LOOKBACK_DAYS = 14          # static: never changes between runs
LATE_DATA_STRATEGY = "current_first"
SPARK_JOB = f"{JOBS_DIR}/spark_static_late_integrator.py"
SPARK_KWARGS = dict(packages="io.delta:delta-spark_2.12:3.2.0", conn_id="spark_default")


def _raw_partition(date: str) -> Path:
    return Path(BASE_DIR) / "raw" / "visits" / f"event_date={date}"


def _marker(date: str) -> Path:
    return Path(BASE_DIR) / "_integrated" / f"{date}.marker"


def _last_modification(path: Path) -> float:
    files = [p.stat().st_mtime for p in path.rglob("*") if p.is_file()]
    return max(files, default=0.0)


@dag(
    dag_id="referral_stats_static_late_integrator",
    schedule="@daily",
    start_date=dt.datetime(2024, 12, 1),
    catchup=True,
    # One run at a time: two runs would integrate overlapping lookback windows.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": dt.timedelta(minutes=5),
                  "depends_on_past": LATE_DATA_STRATEGY == "late_first"},
    tags=["error-management", "static-late-data-integrator"],
)
def referral_stats_static_late_integrator():
    file_to_load_sensor = FileSensor(
        task_id="wait_for_current_partition", fs_conn_id="fs_default",
        filepath=f"{BASE_DIR}/raw/visits/event_date={{{{ ds }}}}/_SUCCESS",
        poke_interval=120, timeout=6 * 3600, mode="reschedule")

    load_current_partition = SparkSubmitOperator(
        task_id="load_current_partition", application=SPARK_JOB,
        application_args=["--base-dir", BASE_DIR, "--partition-date", "{{ ds }}"], **SPARK_KWARGS)

    @task
    def generate_backfilling_runs() -> List[str]:
        """Backfilling run list, with the lookback size as a constant."""
        logical_date = get_current_context()["logical_date"]
        start = logical_date - dt.timedelta(days=LOOKBACK_DAYS)
        return [(start + dt.timedelta(days=d)).date().isoformat() for d in range(LOOKBACK_DAYS)]

    @task
    def has_late_data(dates: List[str]) -> List[List[str]]:
        """Keep the dates whose raw partition changed since the last integration."""
        changed = []
        for date in dates:
            marker = _marker(date)
            integrated_at = marker.stat().st_mtime if marker.exists() else 0.0
            if _last_modification(_raw_partition(date)) > integrated_at:
                changed.append(date)
        log.info("late data found for %s of %s lookback dates: %s", len(changed), len(dates), changed)
        return [["--base-dir", BASE_DIR, "--partition-date", d] for d in changed]

    backfilling_dates = generate_backfilling_runs()
    integrate_late_data = SparkSubmitOperator.partial(
        task_id="integrate_late_data", application=SPARK_JOB, **SPARK_KWARGS,
    ).expand(application_args=has_late_data(backfilling_dates))

    @task(trigger_rule="none_failed")
    def mark_integrated(dates: List[str], ds: str = None) -> None:
        # ponytail: a late file landing between the Spark read and this touch is
        # missed until the partition changes again; store the read snapshot time
        # (or a Delta version) instead of mtime if that window matters.
        for date in [ds] + list(dates):
            marker = _marker(date)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()

    marked = mark_integrated(backfilling_dates)
    if LATE_DATA_STRATEGY == "current_first":
        file_to_load_sensor >> load_current_partition >> integrate_late_data >> marked
    elif LATE_DATA_STRATEGY == "late_first":
        file_to_load_sensor >> integrate_late_data >> load_current_partition >> marked
    else:  # parallel
        file_to_load_sensor >> [load_current_partition, integrate_late_data] >> marked


referral_stats_static_late_integrator()
