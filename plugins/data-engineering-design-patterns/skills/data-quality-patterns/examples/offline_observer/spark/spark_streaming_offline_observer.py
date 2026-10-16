"""Offline Observer - independent Structured Streaming observation job, PySpark.

Reads the same `visits` topic as the data generation job, but with its OWN checkpoint,
so it never interferes with it. Per micro-batch it:
  * computes data quality metrics (rows, NULLs per column) and appends them to a
    Delta monitoring table,
  * optionally writes a ydata-profiling HTML report (PROFILE=1),
  * computes the producer job's lag: latest topic offsets vs the last offsets the
    producer committed in its checkpoint (`<checkpoint>/offsets/<n>`).

Run: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
     spark_streaming_offline_observer.py
Env: KAFKA_BOOTSTRAP, BASE_DIR, PRODUCER_CHECKPOINT (checkpoint dir of the observed job), PROFILE.
Needs confluent-kafka on the driver for latest offsets; ydata-profiling if PROFILE=1.
"""
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
BASE_DIR = os.environ.get("BASE_DIR", "/tmp/dp/offline_observer")
PRODUCER_CHECKPOINT = os.environ.get("PRODUCER_CHECKPOINT", "/tmp/dp/visits_job/checkpoint")
TOPIC = "visits"
visit_schema = StructType([
    StructField("visit_id", StringType()), StructField("event_time", TimestampType()),
    StructField("user_id", StringType()), StructField("page", StringType()),
    StructField("context", StructType([StructField("referral", StringType())])),
])


def read_last_processed_offsets() -> dict:
    """Last offsets committed by the producer job (Spark checkpoint `offsets/<batch>` file)."""
    offsets_dir = Path(PRODUCER_CHECKPOINT) / "offsets"
    last = max((f for f in offsets_dir.iterdir() if f.name.isdigit()), key=lambda f: int(f.name))
    source_offsets = json.loads(last.read_text().strip().splitlines()[-1])  # last line = source offsets
    return {int(p): o for p, o in source_offsets[TOPIC].items()}


def read_last_available_offsets(partitions) -> dict:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "offline-observer-lag"})
    try:
        return {p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1] for p in partitions}
    finally:
        consumer.close()


def get_offsets_lag() -> dict:
    processed = read_last_processed_offsets()
    available = read_last_available_offsets(processed.keys())
    return {str(p): available[p] - processed[p] for p in processed}


def generate_and_write_observations(visits: DataFrame, batch_id: int) -> None:
    observation = visits.agg(
        F.count("*").alias("all_rows"),
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}")
          for c in ("visit_id", "event_time", "user_id", "page", "referral")],
        F.countDistinct("user_id").alias("distinct_users"),
    ).withColumn("batch_id", F.lit(batch_id)).withColumn("observed_at", F.current_timestamp())
    try:
        observation = observation.withColumn("producer_lag", F.lit(json.dumps(get_offsets_lag())))
    except (FileNotFoundError, ValueError, KeyError) as error:  # producer not started yet
        observation = observation.withColumn("producer_lag", F.lit(f"unknown: {error}"))
    observation.write.format("delta").mode("append").save(f"{BASE_DIR}/visits_observations")

    if os.environ.get("PROFILE") == "1":
        from ydata_profiling import ProfileReport

        sample = visits.sample(fraction=0.1, seed=batch_id).toPandas()  # profile a sample only
        ProfileReport(sample, minimal=True).to_file(f"{BASE_DIR}/profile_{batch_id}.html")


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("offline-observer")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    visits_to_observe = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
                         .option("subscribe", TOPIC).option("startingOffsets", "EARLIEST").load()
                         .select(F.from_json(F.col("value").cast("string"), visit_schema).alias("visit"))
                         .selectExpr("visit.*")
                         .select("visit_id", "event_time", "user_id", "page", "context.referral"))
    query = (visits_to_observe.writeStream.foreachBatch(generate_and_write_observations)
             .trigger(processingTime="10 minutes")  # observer cadence independent of the producer
             .option("checkpointLocation", f"{BASE_DIR}/checkpoint").start())
    query.awaitTermination()
