# spark_cdc_batch_replay.py
#
# Batch CDC replay: reads a bounded window of Debezium events from Kafka
# and replays them in LSN order onto a Delta Lake target.
# Use for: backfill, reconciliation, recovery after target corruption.
#
# Run:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
#                io.delta:delta-core_2.12:2.4.0 \
#     spark_cdc_batch_replay.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, DoubleType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("CDC_Batch_Replay") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_BROKERS   = "localhost:9092"
CDC_TOPIC       = "dbserver1.public.orders"
DELTA_TABLE     = "/data/warehouse/orders"

# Replay window: Kafka offsets to replay (inclusive)
# Get these from: kafka-consumer-groups.sh or by inspecting topic metadata
REPLAY_FROM_OFFSET = 0       # start of topic = full replay
REPLAY_TO_OFFSET   = 50000   # replay up to this offset

# ── Schema (same as streaming version) ────────────────────────────────────────

ORDER_SCHEMA = StructType([
    StructField("id",          IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount",      DoubleType(),  True),
    StructField("status",      StringType(),  True),
    StructField("updated_at",  StringType(),  True),
])

DEBEZIUM_SCHEMA = StructType([
    StructField("op",     StringType(), True),
    StructField("before", ORDER_SCHEMA, True),
    StructField("after",  ORDER_SCHEMA, True),
    StructField("source", StructType([
        StructField("ts_ms", LongType(),   True),
        StructField("lsn",   LongType(),   True),
    ]), True),
])

# ── Read bounded batch from Kafka ──────────────────────────────────────────────
# For a batch read, Spark reads Kafka as a static DataFrame between
# startingOffsets and endingOffsets — no streaming, no checkpoint needed.

starting = f'{{"{CDC_TOPIC}":{{"0":{REPLAY_FROM_OFFSET}}}}}'
ending   = f'{{"{CDC_TOPIC}":{{"0":{REPLAY_TO_OFFSET}}}}}'

raw_df = (
    spark.read                          # .read not .readStream = batch mode
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", CDC_TOPIC)
    .option("startingOffsets", starting)
    .option("endingOffsets",   ending)
    .load()
)

print(f"Raw CDC events read: {raw_df.count()}")

# ── Parse Debezium envelope ────────────────────────────────────────────────────

parsed_df = (
    raw_df
    .select(
        F.col("offset").alias("kafka_offset"),
        F.from_json(
            F.col("value").cast("string"),
            DEBEZIUM_SCHEMA
        ).alias("cdc")
    )
    .select(
        "kafka_offset",
        F.col("cdc.op").alias("op"),
        F.col("cdc.source.lsn").alias("lsn"),
        F.col("cdc.before").alias("before"),
        F.col("cdc.after").alias("after"),
    )
    # Sort the entire batch by LSN before processing —
    # this guarantees we replay events in exact commit order
    .orderBy("lsn")
)

# ── Deduplicate: keep last operation per id across the entire replay window ───

parsed_with_key = parsed_df.withColumn(
    "record_id",
    F.coalesce(F.col("after.id"), F.col("before.id"))
)

window = Window.partitionBy("record_id").orderBy(F.desc("lsn"))

deduped_df = (
    parsed_with_key
    .withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# ── Separate upserts and deletes ───────────────────────────────────────────────

upserts_df = (
    deduped_df
    .filter(F.col("op").isin("c", "u", "r"))
    .select(
        F.col("after.id").alias("id"),
        F.col("after.customer_id").alias("customer_id"),
        F.col("after.amount").alias("amount"),
        F.col("after.status").alias("status"),
        F.col("after.updated_at").alias("updated_at"),
        F.current_timestamp().alias("loaded_at"),
    )
)

deletes_df = (
    deduped_df
    .filter(F.col("op") == "d")
    .select(F.col("before.id").alias("id"))
)

upsert_count = upserts_df.count()
delete_count = deletes_df.count()
print(f"Events to replay — upserts: {upsert_count}, deletes: {delete_count}")

# ── Apply to Delta table ───────────────────────────────────────────────────────

delta_table = DeltaTable.forPath(spark, DELTA_TABLE)

if upsert_count > 0:
    (
        delta_table.alias("target")
        .merge(upserts_df.alias("source"), "target.id = source.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Upserted {upsert_count} rows")

if delete_count > 0:
    delete_ids = [row.id for row in deletes_df.collect()]
    delta_table.delete(F.col("id").isin(delete_ids))
    print(f"Deleted {delete_count} rows")

print("CDC batch replay complete.")
spark.stop()