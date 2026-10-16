"""Incremental Loader: the two implementations (partition and delta column) as one Spark job.

Problem: legacy producers still write immutable visit events to a transactional store; the
Bronze layer must receive only what was added since the previous run.

--mode partition   (the ``EventsLoader``)
    Input is time-partitioned; the job copies exactly one partition ``date=<ds>`` to the same partition of the
    output. No state: the partition is resolved from the immutable execution date, so retries and backfills
    always rewrite the same, complete unit of data. Readiness is checked by the orchestrator (FileSensor).

--mode delta-column
    Input is not partitioned; each JSON line carries an ``ingestion_time`` delta column. The job keeps only
    rows with ``date_from <= ingestion_time < date_to`` where both bounds come from the orchestrator's data
    interval. The bounded window gives a constant data volume per run, so a backfill is N small jobs (that
    can run concurrently) instead of one giant full load. A half-open interval avoids loading a row on the exact boundary twice.

Both modes copy the raw line (``value``) untouched and overwrite only their own output directory, so a rerun
is idempotent. Using ``event_time`` as the delta column would miss late events; use the time the
row was written.

Run:
  spark-submit incremental_loader_jobs.py --mode partition --input /data/input --output /data/output --ds 2024-01-01
  spark-submit incremental_loader_jobs.py --mode delta-column --input /data/visits --output /data/bronze/visits \
      --date-from "2024-01-01T10:00:00" --date-to "2024-01-01T11:00:00"
  python incremental_loader_jobs.py --self-test
"""
import argparse
import logging
import sys
from datetime import datetime

log = logging.getLogger("incremental_loader")


# ── Pure logic ─────────────────────────────────────────────────────────────────────────────────────

def partition_paths(input_base: str, output_base: str, ds: str):
    datetime.strptime(ds, "%Y-%m-%d")                           # reject malformed execution dates early
    return f"{input_base.rstrip('/')}/date={ds}", f"{output_base.rstrip('/')}/date={ds}"


def window_output(output_base: str, date_from: str, date_to: str) -> str:
    start, end = datetime.fromisoformat(date_from), datetime.fromisoformat(date_to)
    if end <= start:
        raise ValueError(f"empty or inverted window [{date_from}, {date_to})")
    return f"{output_base.rstrip('/')}/window_start={start.strftime('%Y%m%dT%H%M%S')}"


def _self_check() -> None:
    assert partition_paths("/in/", "/out", "2024-01-01") == ("/in/date=2024-01-01", "/out/date=2024-01-01")
    assert window_output("/o", "2024-01-01T10:00:00", "2024-01-01T11:00:00") == "/o/window_start=20240101T100000"
    for bad in [lambda: partition_paths("/i", "/o", "2024-13-01"),
                lambda: window_output("/o", "2024-01-01T11:00:00", "2024-01-01T10:00:00")]:
        try:
            bad()
            raise AssertionError("must fail")
        except ValueError:
            pass
    print("self-test OK")


# ── Jobs ───────────────────────────────────────────────────────────────────────────────────────────

def load_partition(spark, args) -> int:
    source, target = partition_paths(args.input, args.output, args.ds)
    lines = spark.read.text(source)
    count = lines.count()
    if count == 0 and not args.allow_empty:
        raise RuntimeError(f"{source} is empty; refusing to publish an empty partition")
    lines.write.mode("overwrite").text(target)                  # only this partition is replaced
    return count


def load_delta_column(spark, args) -> int:
    from pyspark.sql import functions as F
    target = window_output(args.output, args.date_from, args.date_to)
    in_data = (spark.read.text(args.input)
               .select("value", F.from_json(F.col("value"), "ingestion_time TIMESTAMP").alias("j")))
    unparsable = in_data.where(F.col("j.ingestion_time").isNull())
    input_to_write = in_data.where(
        (F.col("j.ingestion_time") >= F.to_timestamp(F.lit(args.date_from))) &
        (F.col("j.ingestion_time") < F.to_timestamp(F.lit(args.date_to)))).select("value")
    input_to_write.write.mode("overwrite").text(target)
    bad = unparsable.count()
    if bad:
        # rows without a usable delta column are never picked by any window: make it loud
        log.warning("%d rows have no parsable ingestion_time and are skipped by every window", bad)
    return spark.read.text(target).count()


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["partition", "delta-column"])
    p.add_argument("--input")
    p.add_argument("--output")
    p.add_argument("--ds", help="execution date YYYY-MM-DD (partition mode)")
    p.add_argument("--date-from", help="ISO timestamp, inclusive (delta-column mode)")
    p.add_argument("--date-to", help="ISO timestamp, exclusive (delta-column mode)")
    p.add_argument("--allow-empty", action="store_true")
    p.add_argument("--self-test", action="store_true")
    args = p.parse_args(argv)
    if args.self_test:
        _self_check()
        return 0
    if not (args.mode and args.input and args.output):
        p.error("--mode, --input and --output are required")
    if args.mode == "partition" and not args.ds:
        p.error("--ds is required in partition mode")
    if args.mode == "delta-column" and not (args.date_from and args.date_to):
        p.error("--date-from and --date-to are required in delta-column mode")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"incremental-loader-{args.mode}").getOrCreate()
    try:
        count = load_partition(spark, args) if args.mode == "partition" else load_delta_column(spark, args)
        log.info("Loaded %d rows (%s)", count, args.mode)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
