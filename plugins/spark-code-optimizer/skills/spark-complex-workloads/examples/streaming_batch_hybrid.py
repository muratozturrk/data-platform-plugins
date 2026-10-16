"""Structured Streaming x static lookup (manual section 7.4; sparkopt case: hybrid_checkpoint).

Persistence plan (one strategy per component)
  static lookup     -> persist MEMORY_AND_DISK + count(), F.broadcast() in the join.
                       Every micro-batch re-plans the join; the cached relation is substituted
                       each time, so the source is read once instead of once per batch.
                       Trade-off: the snapshot never refreshes (see cached_broadcast.py).
  streaming state   -> writeStream checkpointLocation on durable storage. This is NOT
                       df.checkpoint(): it stores offsets + state store for exactly-once
                       recovery. Never put it on executor-local /tmp.
  intermediate join -> nothing. localCheckpoint() is impossible on a streaming DataFrame;
                       only inside foreachBatch on the batch DataFrame, and only if that batch
                       feeds two or more sinks (then persist/unpersist is usually enough).
  sink              -> Delta (or parquet) table: the durable result.

Corrections vs. the sparkopt template / manual table: MEMORY_ONLY_SER -> MEMORY_AND_DISK;
"localCheckpoint the intermediate join" dropped (not supported on streaming plans);
`schema` defined.

Run: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
       streaming_batch_hybrid.py --brokers kafka:9092 --topic orders \
       --lookup s3a://bucket/products/ --out s3a://bucket/orders_enriched/ \
       --checkpoint s3a://bucket/_chk/orders_enriched/
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

EVENT_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("qty", DoubleType()),
    StructField("event_time", TimestampType()),
])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--brokers", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--lookup", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--checkpoint", required=True, help="durable path: HDFS/S3/ABFS")
    a = p.parse_args()

    spark = SparkSession.builder.appName("stream-static-enrich").getOrCreate()

    products = (
        spark.read.parquet(a.lookup)
        .select("product_id", "category", "unit_price")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    products.count()  # materialize before the stream starts

    orders = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", a.brokers)
        .option("subscribe", a.topic)
        .option("startingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        # Stateful dedup: its state lives in the checkpoint; the watermark bounds it.
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["order_id", "event_time"])
    )

    enriched = (
        orders.join(F.broadcast(products), "product_id", "left")
        .withColumn("amount", F.col("qty") * F.col("unit_price"))
    )

    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", a.checkpoint)
        .trigger(processingTime="30 seconds")
        .start(a.out)
    )
    try:
        query.awaitTermination()
    finally:
        products.unpersist()


if __name__ == "__main__":
    main()
