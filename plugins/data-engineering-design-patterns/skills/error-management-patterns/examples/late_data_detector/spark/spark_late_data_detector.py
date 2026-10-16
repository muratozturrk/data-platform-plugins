"""Late Data Detector - Spark Structured Streaming.

Two modes:

``--mode native``
    ``withWatermark('event_time', allowed_lateness)`` + 10-minute window count.
    Spark tracks MAX(event time) and drops late rows from stateful operators,
    but offers no API to capture them.

``--mode capture``
    The detector written by hand in ``foreachBatch`` so late records can be
    captured (the PySpark counterpart of a Flink side output):
      1. per input partition, track MAX(event_time) - monotonic by design;
      2. aggregate partitions into one event time with MIN (follow the slowest
         partition) or MAX (follow the fastest) - ``--global-strategy``;
      3. watermark candidate = global event time - allowed lateness; the
         watermark only moves forward (candidate < current => ignored);
      4. rows with event_time < input watermark go to the late-data table,
         the rest to the on-time table.
    The watermark of micro-batch N is computed from the state saved by N-1, so
    a retried batch reproduces the same split; Delta writes use txnVersion.
    Per-batch stats (late count, per-partition event times = skew) are written
    for the monitoring DAG.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
      spark_late_data_detector.py --mode capture --allowed-lateness-minutes 30
  python spark_late_data_detector.py --self-test
Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

log = logging.getLogger("late_data_detector")

EPOCH = dt.datetime(1970, 1, 1)


class WatermarkTracker:
    """Partition-level MAX, global MIN/MAX, allowed lateness, monotonic watermark."""

    def __init__(self, allowed_lateness: dt.timedelta, global_strategy: str = "min",
                 partition_max: Optional[Dict[str, dt.datetime]] = None,
                 watermark: dt.datetime = EPOCH):
        if global_strategy not in ("min", "max"):
            raise ValueError("global_strategy must be 'min' or 'max'")
        self.allowed_lateness = allowed_lateness
        self.global_strategy = global_strategy
        self.partition_max: Dict[str, dt.datetime] = dict(partition_max or {})
        self.watermark = watermark

    def is_late(self, event_time: dt.datetime) -> bool:
        return event_time < self.watermark

    def advance(self, observations: Iterable[Tuple[str, dt.datetime]]) -> dt.datetime:
        """Fold (partition, max event time) pairs seen in a batch into the state."""
        for partition, event_time in observations:
            previous = self.partition_max.get(partition)
            # MAX per partition: event time never goes back (no stuck-in-the-past).
            if previous is None or event_time > previous:
                self.partition_max[partition] = event_time
        if self.partition_max:
            agg = min if self.global_strategy == "min" else max
            candidate = agg(self.partition_max.values()) - self.allowed_lateness
            self.watermark = max(self.watermark, candidate)  # never moves backwards
        return self.watermark

    def to_json(self) -> dict:
        return {"watermark": self.watermark.isoformat(),
                "partition_max": {p: t.isoformat() for p, t in self.partition_max.items()}}

    @classmethod
    def from_json(cls, doc: dict, allowed_lateness: dt.timedelta, strategy: str) -> "WatermarkTracker":
        return cls(allowed_lateness, strategy,
                   {p: dt.datetime.fromisoformat(t) for p, t in doc["partition_max"].items()},
                   dt.datetime.fromisoformat(doc["watermark"]))


class WatermarkStateStore:
    """One JSON file per committed micro-batch; batch N reads the newest file < N."""

    def __init__(self, directory: str, keep: int = 100):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.keep = keep

    def load_before(self, batch_id: int) -> Optional[dict]:
        candidates = sorted((int(p.stem), p) for p in self.dir.glob("*.json") if int(p.stem) < batch_id)
        return json.loads(candidates[-1][1].read_text()) if candidates else None

    def save(self, batch_id: int, doc: dict) -> None:
        target = self.dir / f"{batch_id}.json"
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc))
        os.replace(tmp, target)
        for old in sorted(self.dir.glob("*.json"), key=lambda p: int(p.stem))[:-self.keep]:
            old.unlink()


def parse_visits(spark, args):
    from pyspark.sql import functions as F

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                                                                "localhost:9092"))
              .option("subscribe", args.input_topic)
              .option("startingOffsets", "earliest")
              .load())
    return (source
            .select(F.col("partition").cast("string").alias("source_partition"),
                    F.from_json(F.col("value").cast("string"),
                                "visit_id STRING, event_time TIMESTAMP, page STRING").alias("visit"))
            .select("source_partition", "visit.*")
            .filter("event_time IS NOT NULL"))


def run_native(spark, args):
    from pyspark.sql import functions as F

    counts = (parse_visits(spark, args)
              .withWatermark("event_time", f"{args.allowed_lateness_minutes} minutes")
              .groupBy(F.window(F.col("event_time"), "10 minutes")).count())
    return (counts.writeStream.outputMode("append").format("delta")
            .option("checkpointLocation", f"{args.base_dir}/checkpoint-native")
            .start(f"{args.base_dir}/output/visit-counts"))


class LateDataSplitter:
    def __init__(self, args):
        self.args = args
        self.lateness = dt.timedelta(minutes=args.allowed_lateness_minutes)
        self.store = WatermarkStateStore(f"{args.base_dir}/watermark-state")

    def __call__(self, batch_df, batch_id: int) -> None:
        from pyspark.sql import functions as F

        previous = self.store.load_before(batch_id)
        tracker = (WatermarkTracker.from_json(previous, self.lateness, self.args.global_strategy)
                   if previous else WatermarkTracker(self.lateness, self.args.global_strategy))
        input_watermark = tracker.watermark
        flagged = batch_df.withColumn("is_late", F.col("event_time") < F.lit(input_watermark))
        flagged.persist()
        try:
            for is_late, table in ((False, "visits-on-time"), (True, "visits-late")):
                (flagged.filter(F.col("is_late") == is_late).drop("is_late")
                 .withColumn("detection_watermark", F.lit(input_watermark))
                 .write.format("delta").mode("append")
                 .option("txnAppId", f"late-data-detector-{table}").option("txnVersion", batch_id)
                 .save(f"{self.args.base_dir}/output/{table}"))
            stats = flagged.groupBy("source_partition").agg(
                F.max("event_time").alias("max_event_time"),
                F.sum(F.col("is_late").cast("int")).alias("late"),
                F.count("*").alias("total")).collect()
        finally:
            flagged.unpersist()

        tracker.advance((r["source_partition"], r["max_event_time"]) for r in stats)
        doc = tracker.to_json()
        doc.update({"batch_id": batch_id, "input_watermark": input_watermark.isoformat(),
                    "late": sum(r["late"] for r in stats), "total": sum(r["total"] for r in stats),
                    "computed_at": dt.datetime.now(dt.timezone.utc).isoformat()})
        self.store.save(batch_id, doc)  # also the monitoring input
        if doc["late"]:
            log.warning("batch %s: %s/%s late records (watermark %s)", batch_id, doc["late"],
                        doc["total"], input_watermark)


def run_capture(spark, args):
    # Spark exposes datetimes to the driver in the session time zone: pin it to UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return (parse_visits(spark, args).writeStream
            .option("checkpointLocation", f"{args.base_dir}/checkpoint-capture")
            .trigger(processingTime=args.trigger)
            .foreachBatch(LateDataSplitter(args))
            .start())


def _self_check() -> None:
    t = dt.datetime
    # Watermark progression table: allowed lateness 30 minutes, single partition.
    tracker = WatermarkTracker(dt.timedelta(minutes=30))
    assert tracker.watermark == EPOCH
    tracker.advance([("0", t(2024, 1, 1, 10, 6))])            # 10:00, 10:05, 10:06
    assert tracker.watermark == t(2024, 1, 1, 9, 36)
    late = [e for e in (t(2024, 1, 1, 9, 20), t(2024, 1, 1, 9, 31), t(2024, 1, 1, 10, 7))
            if tracker.is_late(e)]
    assert late == [t(2024, 1, 1, 9, 20), t(2024, 1, 1, 9, 31)]
    tracker.advance([("0", t(2024, 1, 1, 10, 7))])
    assert tracker.watermark == t(2024, 1, 1, 9, 37)
    # Monotonic: an older candidate never moves the watermark back.
    tracker.advance([("1", t(2024, 1, 1, 8, 0))])            # new slow partition, MIN strategy
    assert tracker.watermark == t(2024, 1, 1, 9, 37)
    # MAX strategy follows the fastest partition (aggressive with event skew).
    fast = WatermarkTracker(dt.timedelta(minutes=30), "max")
    fast.advance([("a", t(2024, 1, 1, 11, 0)), ("b", t(2024, 1, 1, 10, 20))])
    assert fast.watermark == t(2024, 1, 1, 10, 30) and fast.is_late(t(2024, 1, 1, 10, 20))
    slow = WatermarkTracker(dt.timedelta(minutes=30), "min")
    slow.advance([("a", t(2024, 1, 1, 11, 0)), ("b", t(2024, 1, 1, 10, 20))])
    assert slow.watermark == t(2024, 1, 1, 9, 50) and not slow.is_late(t(2024, 1, 1, 10, 20))
    restored = WatermarkTracker.from_json(slow.to_json(), dt.timedelta(minutes=30), "min")
    assert restored.watermark == slow.watermark and restored.partition_max == slow.partition_max
    print("self-test OK")


def main(argv: Optional[list] = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argv = sys.argv[1:] if argv is None else argv
    if "--self-test" in argv:
        _self_check()
        return
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mode", choices=["native", "capture"], default="capture")
    p.add_argument("--allowed-lateness-minutes", type=int, default=60)
    p.add_argument("--global-strategy", choices=["min", "max"], default="min")
    p.add_argument("--input-topic", default="visits")
    p.add_argument("--base-dir", default="/tmp/dp/late-data-detector")
    p.add_argument("--trigger", default="30 seconds")
    args = p.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(f"late-data-detector-{args.mode}")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    query = run_native(spark, args) if args.mode == "native" else run_capture(spark, args)
    query.awaitTermination()


if __name__ == "__main__":
    main()
