"""API Gateway - replay pipeline for the gateway's fallback (dead-letter) storage.

A Structured Streaming job with the availableNow trigger:
  * reads only NEW fallback files thanks to the checkpoint;
  * deletes processed files (cleanSource=delete);
  * sorts records per key and event time so replayed events keep their order per key;
  * marks them with header src=replay to distinguish them from real-time records.

Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 fallback_replay_job.py
      (scheduled by ../airflow/fallback_replay_dag.py)
Env:  KAFKA_BOOTSTRAP, FALLBACK_DIR, CHECKPOINT_DIR
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
FALLBACK_DIR = os.environ.get("FALLBACK_DIR", "/tmp/dp/fallback/topic=visits")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/dp/checkpoints/fallback_replay")
SCHEMA = "key STRING, referral STRING, payload STRUCT<visit_id STRING, page STRING, event_time STRING>"


def write_sorted_data_to_kafka(df: DataFrame, batch_nr: int) -> None:
    sorted_events = df.sortWithinPartitions("key", "payload.event_time")
    with_headers = sorted_events.select(
        "key", F.to_json("payload").alias("value"),
        F.array(F.struct(F.lit("source").alias("key"), F.lit("replay").cast("binary").alias("value")),
                F.struct(F.lit("referral").alias("key"), F.encode("referral", "UTF-8").alias("value"))
                ).alias("headers"))
    (with_headers.write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
     .option("topic", "visits").save())


def main() -> None:
    spark = SparkSession.builder.appName("gateway_fallback_replay").getOrCreate()
    input_df = (spark.readStream.schema(SCHEMA).option("cleanSource", "delete").json(FALLBACK_DIR))
    (input_df.writeStream.option("checkpointLocation", CHECKPOINT_DIR)
     .trigger(availableNow=True).foreachBatch(write_sorted_data_to_kafka)
     .start().awaitTermination())


if __name__ == "__main__":
    main()
