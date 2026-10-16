# spark_cdc_streaming_consumer.py
#
# Consumes Debezium CDC events from Kafka using Spark Structured Streaming.
# Applies INSERT / UPDATE / DELETE operations to a Delta Lake target table.
#
# Requirements:
#   - Spark 3.4+
#   - Delta Lake 2.4+
#   - Kafka broker with Debezium publishing to topic: dbserver1.public.orders
#
# Run:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
#                io.delta:delta-core_2.12:2.4.0 \
#     spark_cdc_streaming_consumer.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("CDC_Streaming_Consumer") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Schema definitions ────────────────────────────────────────────────────────

# The row schema of the source table (orders)
ORDER_SCHEMA = StructType([
    StructField("id",          IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
    StructField("status",      StringType(),  True),
    StructField("updated_at",  StringType(),  True),
])

# Debezium wraps every event in this envelope
DEBEZIUM_SCHEMA = StructType([
    StructField("op",     StringType(), True),   # c=insert, u=update, d=delete, r=read
    StructField("before", ORDER_SCHEMA,  True),  # row state before the change
    StructField("after",  ORDER_SCHEMA,  True),  # row state after the change
    StructField("source", StructType([
        StructField("ts_ms", LongType(),   True),  # event timestamp (epoch ms)
        StructField("lsn",   LongType(),   True),  # WAL log sequence number
        StructField("db",    StringType(), True),
        StructField("table", StringType(), True),
    ]), True),
])

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_BROKERS   = "localhost:9092"
CDC_TOPIC       = "dbserver1.public.orders"   # Debezium topic: server.schema.table
DELTA_TABLE     = "/data/warehouse/orders"
CHECKPOINT_PATH = "/data/checkpoints/cdc_orders"


# ── Read CDC stream from Kafka ─────────────────────────────────────────────────

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", CDC_TOPIC)
    # Start from earliest on first run; resume from checkpoint on restart
    .option("startingOffsets", "earliest")
    # Don't fail if Kafka topic has more data than Spark can buffer
    .option("failOnDataLoss", "false")
    .load()
)

# Kafka delivers messages as binary — deserialise the value column
parsed_stream = (
    raw_stream
    .select(
        # Kafka offset = reliable ordering watermark (equivalent to LSN order)
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        # Parse the JSON payload using the Debezium schema
        F.from_json(
            F.col("value").cast("string"),
            DEBEZIUM_SCHEMA
        ).alias("cdc")
    )
    .select(
        "kafka_offset",
        "kafka_timestamp",
        F.col("cdc.op").alias("op"),
        F.col("cdc.source.lsn").alias("lsn"),
        F.col("cdc.source.ts_ms").alias("source_ts_ms"),
        F.col("cdc.before").alias("before"),
        F.col("cdc.after").alias("after"),
    )
)


# ── Apply CDC operations to Delta Lake ────────────────────────────────────────

def apply_cdc_batch(batch_df, batch_id: int):
    """
    Called by Spark for each micro-batch.
    Applies the CDC events in LSN order to the Delta target table.

    Strategy per operation:
      c / r  →  INSERT (or UPDATE if id already exists — idempotency)
      u      →  UPDATE the row using 'after' values
      d      →  DELETE the row by primary key
    """
    if batch_df.isEmpty():
        return

    from delta.tables import DeltaTable

    # ── Deduplicate within the batch ──────────────────────────────────
    # The same id may appear multiple times in one micro-batch
    # (e.g. UPDATE then DELETE). Keep only the last event per id
    # ordered by LSN — the most recent operation wins.
    from pyspark.sql.window import Window

    window = Window.partitionBy("after.id").orderBy(F.desc("lsn"))

    # For DELETE events, 'after' is null — use 'before.id' as the key
    batch_with_key = batch_df.withColumn(
        "record_id",
        F.coalesce(F.col("after.id"), F.col("before.id"))
    )

    window_keyed = Window.partitionBy("record_id").orderBy(F.desc("lsn"))

    deduped = (
        batch_with_key
        .withColumn("row_num", F.row_number().over(window_keyed))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # ── Separate inserts/updates from deletes ─────────────────────────
    upserts_df = (
        deduped
        .filter(F.col("op").isin("c", "u", "r"))
        .select(
            F.col("after.id").alias("id"),
            F.col("after.customer_id").alias("customer_id"),
            F.col("after.amount").alias("amount"),
            F.col("after.status").alias("status"),
            F.col("after.updated_at").alias("updated_at"),
            F.col("source_ts_ms"),
            F.current_timestamp().alias("loaded_at"),
        )
    )

    deletes_df = (
        deduped
        .filter(F.col("op") == "d")
        .select(F.col("before.id").alias("id"))
    )

    # ── Apply to Delta table ───────────────────────────────────────────
    delta_table = DeltaTable.forPath(spark, DELTA_TABLE)

    # UPSERT: merge incoming upserts into Delta
    if upserts_df.count() > 0:
        (
            delta_table.alias("target")
            .merge(
                upserts_df.alias("source"),
                "target.id = source.id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # DELETE: remove rows whose id appears in deletes
    if deletes_df.count() > 0:
        delete_ids = [row.id for row in deletes_df.collect()]
        delta_table.delete(F.col("id").isin(delete_ids))

    print(f"Batch {batch_id}: "
          f"upserted={upserts_df.count()}, "
          f"deleted={deletes_df.count()}")


# ── Start the streaming query ──────────────────────────────────────────────────

query = (
    parsed_stream.writeStream
    .format("delta")
    .foreachBatch(apply_cdc_batch)
    # Checkpoint stores Kafka offsets — this is what enables resume-on-restart
    .option("checkpointLocation", CHECKPOINT_PATH)
    # Process one micro-batch every 10 seconds
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Streaming query started: {query.id}")
query.awaitTermination()