"""Static Late Data Integrator - Spark partition (re)processor.

Problem: a daily job builds referral statistics; results stay approximate
for a fixed number of days because late data may still arrive for them. Each run
therefore processes the current day AND re-processes a static lookback window
of previous days. Late records are appended by the ingestion to the raw
event-time partitions (``event_date=YYYY-MM-DD``); re-processing a partition
simply recomputes it from everything present now.

This job is the unit of work:
  * ``--partition-date D`` (repeatable) recomputes the stats of D and
    overwrites output partition D with ``replaceWhere`` - idempotent, so the
    same date can be integrated by several runs without double counting;
  * ``--execution-date E --lookback-days N`` processes E plus E-N..E-1 in one
    Spark application, in the order given by ``--order``:
      late_first    stateful jobs - the history must be valid before today;
      current_first stateless jobs that want fresh data delivered first.
The Airflow DAG instead fans the lookback dates out as mapped tasks.

``plan-backfill`` (pure python) prints the minimal set of runs to restart for
a manual backfill: thanks to the lookback, restarting the latest run already
covers the previous N days.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_static_late_integrator.py \
      --execution-date 2024-12-31 --lookback-days 14 --order late_first
  python spark_static_late_integrator.py plan-backfill --dates 2024-10-10,2024-10-11,2024-10-12 \
      --lookback-days 4 --latest-run 2024-10-12
  python spark_static_late_integrator.py --self-test
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import time
from typing import List, Optional

log = logging.getLogger("static_late_integrator")


def lookback_dates(execution_date: str, lookback_days: int) -> List[str]:
    """Past partitions re-processed by one run, oldest first."""
    current = dt.date.fromisoformat(execution_date)
    return [(current - dt.timedelta(days=d)).isoformat() for d in range(lookback_days, 0, -1)]


def processing_order(execution_date: str, lookback_days: int, order: str) -> List[str]:
    late = lookback_dates(execution_date, lookback_days)
    return late + [execution_date] if order == "late_first" else [execution_date] + late


def runs_to_restart(dates_to_fix: List[str], lookback_days: int, latest_run: str) -> List[str]:
    """Minimal executions to restart so every date is reprocessed once.

    A run on E re-processes [E - lookback, E]. Restarting every run would
    process the same dates many times (overlapping executions).
    """
    latest = dt.date.fromisoformat(latest_run)
    pending = sorted(dt.date.fromisoformat(d) for d in dates_to_fix)
    runs: List[str] = []
    while pending:
        run = min(pending[0] + dt.timedelta(days=lookback_days), latest)
        if run < pending[0]:
            raise ValueError(f"{pending[0]} is after the latest run {latest_run}")
        runs.append(run.isoformat())
        window_start = run - dt.timedelta(days=lookback_days)
        pending = [d for d in pending if not (window_start <= d <= run)]
    return runs


def process_partition(spark, base_dir: str, partition_date: str) -> int:
    """Recompute one event-time partition from raw data (on-time + late)."""
    from pyspark.sql import functions as F

    raw = (spark.read.format("delta").load(f"{base_dir}/raw/visits")
           .filter(F.col("event_date") == partition_date))
    stats = (raw.groupBy("event_date", "referral")
             .agg(F.count("*").alias("visits"), F.countDistinct("user_id").alias("unique_users"))
             .withColumn("computed_at", F.current_timestamp()))
    (stats.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"event_date = '{partition_date}'")
     .partitionBy("event_date")
     .save(f"{base_dir}/output/referral-stats"))
    rows = spark.read.format("delta").load(f"{base_dir}/output/referral-stats") \
        .filter(F.col("event_date") == partition_date).count()
    return rows


def run(spark, args: argparse.Namespace) -> None:
    dates = args.partition_date or processing_order(args.execution_date, args.lookback_days, args.order)
    for date in dates:
        started = time.monotonic()
        rows = process_partition(spark, args.base_dir, date)
        log.info("partition %s integrated: %s rows in %.1fs", date, rows, time.monotonic() - started)


def _self_check() -> None:
    dates = lookback_dates("2024-12-31", 14)
    assert dates[0] == "2024-12-17" and dates[-1] == "2024-12-30" and len(dates) == 14
    assert processing_order("2024-12-31", 2, "late_first") == ["2024-12-29", "2024-12-30", "2024-12-31"]
    assert processing_order("2024-12-31", 2, "current_first")[0] == "2024-12-31"
    # Overlapping executions: three executions with a 4-day lookback -> restart only the last one.
    assert runs_to_restart(["2024-10-10", "2024-10-11", "2024-10-12"], 4, "2024-10-12") == ["2024-10-12"]
    assert runs_to_restart(["2024-10-01", "2024-10-12"], 4, "2024-10-12") == ["2024-10-05", "2024-10-12"]
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    if argv and argv[0] == "plan-backfill":
        p = argparse.ArgumentParser(prog="plan-backfill")
        p.add_argument("--dates", required=True)
        p.add_argument("--lookback-days", type=int, required=True)
        p.add_argument("--latest-run", required=True)
        a = p.parse_args(argv[1:])
        print("\n".join(runs_to_restart(a.dates.split(","), a.lookback_days, a.latest_run)))
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--base-dir", default="/tmp/dp/static-late-data-integrator")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--partition-date", action="append")
    group.add_argument("--execution-date")
    p.add_argument("--lookback-days", type=int, default=2)
    p.add_argument("--order", choices=["late_first", "current_first"], default="late_first")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName("static-late-data-integrator")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    run(spark, args)


if __name__ == "__main__":
    main()
