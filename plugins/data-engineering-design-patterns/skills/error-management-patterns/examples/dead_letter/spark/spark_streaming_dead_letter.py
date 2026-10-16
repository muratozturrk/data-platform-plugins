"""Dead-Letter pattern - Spark Structured Streaming.

Streaming flavour of the pattern: the job keeps running when a single record
cannot be processed. It mirrors a Flink side output (try/except inside
the mapping function + side output), adapted to PySpark:

  1. A try/except mapping function (``mapInPandas``) converts each raw Kafka
     value into a reduced visit. On failure it does not raise; it returns the
     raw payload decorated with the error (Metadata Decorator).
  2. ``foreachBatch`` persists the micro-batch once, then splits it into the
     valid output (Delta) and the dead-letter output (Kafka topic + Delta).
  3. Monitoring: per-batch counts are logged and an error-ratio guard stops the
     query when too many records fail.

Writes are idempotent on micro-batch retries thanks to Delta's
``txnAppId``/``txnVersion`` options (the batch id is the version).

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
      spark_streaming_dead_letter.py --base-dir /tmp/dp/dead-letter
  python spark_streaming_dead_letter.py --self-test      # pure-python checks, no Spark needed

Env: KAFKA_BOOTSTRAP_SERVERS (default localhost:9092).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
from typing import Iterator, Optional

log = logging.getLogger("streaming_dead_letter")

JOB_NAME = "visits-streaming-dead-letter"
JOB_VERSION = "1.0.0"
OUTPUT_COLUMNS = ["visit_id", "event_time", "page", "error", "raw_value"]


def map_visit(raw_value: str) -> dict:
    """The 'likely fail spot': parse + reduce one visit. Never raises.

    Returns either a valid row (error=None) or a dead-letter row that keeps the
    original payload and the failure message for the post-analysis stage.
    """
    try:
        evt = json.loads(raw_value)
        event_time = dt.datetime.fromisoformat(evt["event_time"])
        if not evt["visit_id"]:
            raise ValueError("empty visit_id")
        return {"visit_id": str(evt["visit_id"]), "event_time": event_time,
                "page": evt["page"], "error": None, "raw_value": None}
    except Exception as error:  # noqa: BLE001 - this IS the dead-letter boundary
        return {"visit_id": None, "event_time": None, "page": None,
                "error": f"{type(error).__name__}: {error}"[:1000], "raw_value": raw_value}


def map_partition(batches: Iterator["pd.DataFrame"]) -> Iterator["pd.DataFrame"]:
    """mapInPandas wrapper applying map_visit row by row."""
    import pandas as pd

    for pdf in batches:
        rows = [map_visit(v) for v in pdf["raw_value"].tolist()]
        yield pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def should_stop(valid: int, failed: int, max_error_ratio: float, min_records: int) -> bool:
    """Too many dead-lettered records means a failure, not an occasional error."""
    total = valid + failed
    return total >= min_records and failed / total > max_error_ratio


class DeadLetterBatchWriter:
    """foreachBatch callable: valid -> Delta, failed -> Kafka DLQ + Delta DLQ."""

    def __init__(self, args: argparse.Namespace, bootstrap: str):
        self.args = args
        self.bootstrap = bootstrap

    def __call__(self, batch_df, batch_id: int) -> None:
        from pyspark.sql import functions as F

        batch_df.persist()  # the split below reads the batch 3 times
        try:
            valid = batch_df.filter("error IS NULL").drop("error", "raw_value")
            failed = (batch_df.filter("error IS NOT NULL")
                      .select("raw_value", "error")
                      .withColumn("job_name", F.lit(JOB_NAME))
                      .withColumn("job_version", F.lit(JOB_VERSION))
                      .withColumn("batch_id", F.lit(batch_id))
                      .withColumn("dead_lettered_at", F.current_timestamp()))
            valid_count, failed_count = valid.count(), failed.count()

            # Idempotent Delta writes: a retried batch_id is skipped by Delta.
            (valid.write.format("delta").mode("append")
             .option("txnAppId", f"{JOB_NAME}-valid").option("txnVersion", batch_id)
             .save(f"{self.args.base_dir}/output/visits"))
            if failed_count:
                (failed.write.format("delta").mode("append")
                 .option("txnAppId", f"{JOB_NAME}-dlq").option("txnVersion", batch_id)
                 .save(f"{self.args.base_dir}/output/visits-dead-letter"))
                # Streaming broker as the replayable dead-letter store. Not
                # transactional: a retried batch may publish twice -> dedup on replay.
                (failed.select(F.to_json(F.struct("*")).alias("value"))
                 .write.format("kafka")
                 .option("kafka.bootstrap.servers", self.bootstrap)
                 .option("topic", self.args.dlq_topic).save())

            log.info("batch=%s valid=%s dead_lettered=%s", batch_id, valid_count, failed_count)
            if should_stop(valid_count, failed_count, self.args.max_error_ratio,
                           self.args.min_records_for_ratio):
                raise RuntimeError(
                    f"batch {batch_id}: {failed_count}/{valid_count + failed_count} records "
                    f"dead-lettered (> {self.args.max_error_ratio:.0%}); stopping the job")
        finally:
            batch_df.unpersist()


def build_query(spark, args: argparse.Namespace, bootstrap: str):
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    out_schema = StructType([
        StructField("visit_id", StringType()), StructField("event_time", TimestampType()),
        StructField("page", StringType()), StructField("error", StringType()),
        StructField("raw_value", StringType()),
    ])
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribe", args.input_topic)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
              .load()
              .selectExpr("CAST(value AS STRING) AS raw_value"))
    mapped = source.mapInPandas(map_partition, out_schema)
    return (mapped.writeStream
            .option("checkpointLocation", f"{args.base_dir}/checkpoint")
            .trigger(processingTime=args.trigger)
            .foreachBatch(DeadLetterBatchWriter(args, bootstrap))
            .start())


def _self_check() -> None:
    ok = map_visit('{"visit_id": "v1", "event_time": "2024-01-01T10:00:00", "page": "home"}')
    assert ok["error"] is None and ok["visit_id"] == "v1"
    bad_json = map_visit("{not json")
    assert bad_json["error"].startswith("JSONDecodeError") and bad_json["raw_value"] == "{not json"
    bad_time = map_visit('{"visit_id": "v2", "event_time": "yesterday", "page": "home"}')
    assert bad_time["error"].startswith("ValueError")
    missing = map_visit('{"visit_id": "v3"}')
    assert missing["error"].startswith("KeyError")
    assert not should_stop(valid=99, failed=1, max_error_ratio=0.05, min_records=100)
    assert should_stop(valid=90, failed=10, max_error_ratio=0.05, min_records=100)
    assert not should_stop(valid=0, failed=5, max_error_ratio=0.05, min_records=100)
    print("self-test OK")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--base-dir", default="/tmp/dp/dead-letter/streaming")
    p.add_argument("--input-topic", default="visits")
    p.add_argument("--dlq-topic", default="visits_dead_letter")
    p.add_argument("--trigger", default="15 seconds")
    p.add_argument("--max-offsets-per-trigger", type=int, default=10_000)
    p.add_argument("--max-error-ratio", type=float, default=0.05)
    p.add_argument("--min-records-for-ratio", type=int, default=100)
    p.add_argument("--self-test", action="store_true")
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    if args.self_test:
        _self_check()
        return
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(JOB_NAME)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    build_query(spark, args, bootstrap).awaitTermination()


if __name__ == "__main__":
    main()
