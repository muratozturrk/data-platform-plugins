"""Windowed Deduplicator - Spark Structured Streaming.

A streaming job has no completed dataset, so the pattern simulates one with a
time-based window kept in the state store. The watermark plays two
roles: it rejects records older than the late-data boundary and it
expires remembered keys, which keeps the state from growing forever.

Modes (``--mode``):
  drop_duplicates   Default: ``withWatermark(visit_time) + dropDuplicates(
                    [visit_id, visit_time])``. The event-time column MUST be part
                    of the keys, otherwise Spark can never evict the state.
                    Catches retries that carry the same event time.
  within_watermark  Spark 3.5+: ``dropDuplicatesWithinWatermark([visit_id])``.
                    Deduplicates on the id only, for duplicates whose event time
                    differs (e.g. producer re-stamps on retry). A key is kept
                    until watermark > its first event time + watermark delay.

State store: RocksDB provider (local with fault tolerance - in memory/disk on
executors, snapshotted to the checkpoint location at every micro-batch).
Exactly-once processing here does not mean exactly-once delivery: a restarted
micro-batch rewrites its output, so the Delta sink uses its built-in
idempotent streaming commit.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
      spark_streaming_deduplicator.py --mode drop_duplicates --window "10 minutes"
Env: KAFKA_BOOTSTRAP_SERVERS.
"""
from __future__ import annotations

import argparse
import logging
import os

log = logging.getLogger("streaming_deduplicator")


def build_deduplicated_stream(spark, args: argparse.Namespace):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    event_schema = StructType([StructField("visit_id", StringType()),
                               StructField("visit_time", TimestampType())])
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                                                                "localhost:9092"))
              .option("subscribe", args.input_topic)
              .option("startingOffsets", "earliest")
              .load()
              .selectExpr("CAST(value AS STRING) AS value"))

    # Extract the deduplication attributes, keep the raw payload.
    prepared = (source
                .select(F.from_json("value", event_schema).alias("value_struct"), "value")
                .select("value_struct.visit_time", "value_struct.visit_id", "value")
                .filter("visit_id IS NOT NULL AND visit_time IS NOT NULL"))

    # The watermark bounds both late data and the dedup state.
    watermarked = prepared.withWatermark("visit_time", args.window)
    if args.mode == "drop_duplicates":
        deduplicated = watermarked.dropDuplicates(["visit_id", "visit_time"])
    else:
        deduplicated = watermarked.dropDuplicatesWithinWatermark(["visit_id"])
    return deduplicated.drop("visit_time", "visit_id")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mode", choices=["drop_duplicates", "within_watermark"], default="drop_duplicates")
    p.add_argument("--window", default="10 minutes",
                   help="watermark delay = how long keys are remembered (space vs time trade-off)")
    p.add_argument("--input-topic", default="visits")
    p.add_argument("--base-dir", default="/tmp/dp/dedup/streaming")
    p.add_argument("--trigger", default="30 seconds")
    args = p.parse_args()

    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(f"windowed-deduplicator-{args.mode}")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
             .getOrCreate())

    query = (build_deduplicated_stream(spark, args).writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", f"{args.base_dir}/checkpoint-{args.mode}")
             .trigger(processingTime=args.trigger)
             .start(f"{args.base_dir}/output/visits-{args.mode}"))
    log.info("query %s started", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
