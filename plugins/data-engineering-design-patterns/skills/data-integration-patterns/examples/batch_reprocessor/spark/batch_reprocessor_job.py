"""Batch Reprocessor - reprocess from the lakehouse copy when Kafka retention is gone (PySpark).

Modes:
  stateless : read the raw Delta copy of the topic for an offset (or time) range and run
              the SAME logic as the real-time job.
  sessions  : arbitrary stateful logic (30-min inactivity sessions). The requested time
              range does not contain whole sessions, so first collect the users of the
              range, then extend backwards/forwards partition by partition while events
              still belong to the same session, bounded by
              max_iterations.

Output is decorated with origin=reprocessing so consumers can tell it apart.

Run:  spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        --py-files visits_logic.py batch_reprocessor_job.py stateless --start 1200 --end 5400
      ... batch_reprocessor_job.py sessions --start "2024-06-01 10:00:00" --end "2024-06-01 11:00:00"
Env:  KAFKA_BOOTSTRAP. Raw copy table `visits_raw` columns: value, offset, partition, event_time
"""
import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from visits_logic import extract_columns, to_kafka_records

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
RAW_TABLE = "visits_raw"
SESSION_GAP = timedelta(minutes=30)


def write_to_kafka(visits: DataFrame) -> None:
    (to_kafka_records(visits, origin="reprocessing").write.format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", "visits-cleaned").save())


def reprocess_stateless(spark: SparkSession, start_offset: int, end_offset: int) -> None:
    raw = spark.read.table(RAW_TABLE).filter(F.col("offset").between(start_offset, end_offset))
    write_to_kafka(extract_columns(raw))


def extend_sessions(spark: SparkSession, visits_in_range: DataFrame, start: datetime, end: datetime,
                    max_iterations: int = 100) -> DataFrame:
    """Chain backwards/forwards: each hop adds the events at most SESSION_GAP away from the
    current session edge of each user; stops when no user's session continues."""
    collected = visits_in_range
    gap = int(SESSION_GAP.total_seconds())
    for direction in ("past", "future"):
        edge = visits_in_range.groupBy("user_id").agg(F.min("event_time").alias("lo"), F.max("event_time").alias("hi"))
        for _ in range(max_iterations):
            # partition pruning: only partitions reachable by one more hop from any edge
            if direction == "past":
                lo = edge.agg(F.min("lo")).first()[0] - SESSION_GAP
                time_filter = F.col("event_time").between(lo, start)
            else:
                hi = edge.agg(F.max("hi")).first()[0] + SESSION_GAP
                time_filter = F.col("event_time").between(end, hi)
            candidates = (extract_columns(spark.read.table(RAW_TABLE).filter(time_filter))
                          .alias("v").join(edge.alias("e"), F.col("v.user_id") == F.col("e.user_id")))
            if direction == "past":
                delta = F.unix_timestamp("e.lo") - F.unix_timestamp("v.event_time")
            else:
                delta = F.unix_timestamp("v.event_time") - F.unix_timestamp("e.hi")
            extension = candidates.filter((delta > 0) & (delta <= gap)).select("v.*").cache()
            if extension.isEmpty():
                break
            collected = collected.unionByName(extension)
            edge = extension.groupBy("user_id").agg(F.min("event_time").alias("lo"), F.max("event_time").alias("hi"))
    return collected.dropDuplicates(["visit_id"])


def reprocess_sessions(spark: SparkSession, start: datetime, end: datetime) -> None:
    in_range = extract_columns(spark.read.table(RAW_TABLE).filter(F.col("event_time").between(start, end)))
    complete_input = extend_sessions(spark, in_range, start, end)
    # ... apply the SAME sessionization logic as the streaming job on complete_input ...
    write_to_kafka(complete_input)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["stateless", "sessions"])
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    session = (SparkSession.builder.appName("visits_batch_reprocessor")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .getOrCreate())
    if args.mode == "stateless":
        reprocess_stateless(session, int(args.start), int(args.end))
    else:
        fmt = "%Y-%m-%d %H:%M:%S"
        reprocess_sessions(session, datetime.strptime(args.start, fmt), datetime.strptime(args.end, fmt))
