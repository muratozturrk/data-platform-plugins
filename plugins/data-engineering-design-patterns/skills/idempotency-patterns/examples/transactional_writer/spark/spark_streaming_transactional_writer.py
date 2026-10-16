# spark_streaming_transactional_writer.py
#
# Transactional Writer -- Spark Structured Streaming
#
# What is guaranteed, and what is not:
#   - Each Delta write below is ONE transaction (one _delta_log commit):
#     readers see the whole micro-batch result for that table or nothing.
#   - foreachBatch is at-least-once: if the query fails after a write but
#     before the checkpoint records the batch, the SAME batch_id runs again.
#     The transaction does not cover that replay (idempotency
#     scope), so each write is made idempotent on its own:
#       orders  -> MERGE on (order_id, event_type): replay = same upsert
#       metrics -> append with txnAppId/txnVersion=batch_id: Delta skips a
#                  replayed batch_id it has already committed (Delta 2.0+)
#   - Two tables = two commits. There is NO cross-table atomicity: between
#     the two commits a reader can see new orders and old metrics.
#
# Run:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
#                io.delta:delta-spark_2.12:3.2.0 \
#     spark_streaming_transactional_writer.py
#
# Requirements: delta-spark, pyspark >= 3.4

import json
import os
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, LongType,
)
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("StreamingTransactionalWriter") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -- Config ------------------------------------------------------------------
KAFKA_BROKERS       = os.environ.get("KAFKA_BROKERS", "localhost:9092")
SOURCE_TOPIC        = "silver.orders"
ORDERS_OUTPUT_PATH  = "/data/gold/orders"
METRICS_OUTPUT_PATH = "/data/gold/order_metrics"
CHECKPOINT_PATH     = "/data/checkpoints/streaming_transactional_writer"
MERGE_KEY           = "order_id"
# Stable per query: Delta stores the last committed txnVersion per app id.
METRICS_TXN_APP_ID  = "streaming_transactional_writer.metrics"

EVENT_SCHEMA = StructType([
    StructField("order_id",    StringType(),    True),
    StructField("customer_id", StringType(),    True),
    StructField("event_type",  StringType(),    True),
    StructField("amount",      LongType(),      True),
    StructField("event_time",  TimestampType(), True),
])


# -- foreachBatch: atomic multi-table write ----------------------------------
def write_batch_transactionally(batch_df, batch_id: int):
    """
    Writes one micro-batch to two Delta tables, one transaction each.
      - If orders commits and metrics fails, Spark re-runs the same
        batch_id: the orders MERGE is a no-op upsert, metrics get written.
      - If both committed but the checkpoint did not, the replay MERGEs
        orders again and Delta skips the metrics append (same txnVersion).
    Each table is atomic; the pair is only eventually consistent. Use a
    single-table design or a database transaction when the pair must flip
    together.
    """
    if batch_df.isEmpty():
        log.info("Batch %d: empty -- skipping.", batch_id)
        return

    # -- Deduplicate within micro-batch -------------------------------------
    # Kafka may deliver the same offset twice within a batch on restart
    batch_df = batch_df.dropDuplicates([MERGE_KEY, "event_type"])

    count = batch_df.count()
    log.info("Batch %d: %d rows.", batch_id, count)

    # -- Write 1: orders table (atomic Delta MERGE) -------------------------
    try:
        if DeltaTable.isDeltaTable(spark, ORDERS_OUTPUT_PATH):
            (
                DeltaTable.forPath(spark, ORDERS_OUTPUT_PATH).alias("t")
                .merge(
                    batch_df.alias("s"),
                    f"t.{MERGE_KEY} = s.{MERGE_KEY} "
                    "AND t.event_type = s.event_type"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            batch_df.write.format("delta") \
                    .mode("overwrite").save(ORDERS_OUTPUT_PATH)

        orders_version = (
            DeltaTable.forPath(spark, ORDERS_OUTPUT_PATH)
            .history(1).collect()[0]["version"]
        )
        log.info(
            "Batch %d: orders written | delta_v=%d",
            batch_id, orders_version,
        )

    except Exception as e:
        log.error(
            "Batch %d: orders write FAILED: %s. "
            "Batch will be retried by Spark checkpoint.",
            batch_id, str(e),
        )
        raise   # re-raise: Spark retries the micro-batch

    # -- Write 2: metrics table (aggregated per batch) ----------------------
    metrics_df = (
        batch_df
        .groupBy(
            F.date_trunc("hour", F.col("event_time")).alias("metric_hour")
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("total_amount"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("batch_id",     F.lit(batch_id))
        .withColumn("_written_at",  F.current_timestamp())
    )

    try:
        # Append is not idempotent by itself; the txn options make it so.
        (
            metrics_df.write.format("delta")
            .mode("append")
            .option("txnAppId", METRICS_TXN_APP_ID)
            .option("txnVersion", batch_id)
            .save(METRICS_OUTPUT_PATH)
        )

        metrics_version = (
            DeltaTable.forPath(spark, METRICS_OUTPUT_PATH)
            .history(1).collect()[0]["version"]
        )
        log.info(
            "Batch %d: metrics written | delta_v=%d",
            batch_id, metrics_version,
        )

    except Exception as e:
        log.error(
            "Batch %d: metrics write FAILED: %s. "
            "Batch will be retried; the orders MERGE is replay-safe.",
            batch_id, str(e),
        )
        raise


# -- Read stream -------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_stream
    .select(
        F.from_json(
            F.col("value").cast("string"),
            EVENT_SCHEMA,
        ).alias("e")
    )
    .select("e.*")
    .filter(F.col("order_id").isNotNull())
)

# -- Write with transactional foreachBatch -----------------------------------
query = (
    parsed.writeStream
    .foreachBatch(write_batch_transactionally)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

log.info("Streaming transactional writer started.")
query.awaitTermination()