"""Batch, micro-batch and streaming ingestion into Iceberg.

Batch: a daily job that copies one day from a landing table into a curated table.
Micro-batch / streaming: Spark Structured Streaming from Kafka with different triggers.
Docs: https://iceberg.apache.org/docs/latest/spark-writes/ (overwritePartitions),
https://spark.apache.org/docs/3.5.5/structured-streaming-programming-guide.html#triggers
"""
import time
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession, functions as F

from common.kafka_io import produce
from common.lakehouse import read_kafka

ORDER_DDL = "order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_ts TIMESTAMP"


# --- Batch -------------------------------------------------------------------------------
def create_order_tables(spark: SparkSession, landing: str, curated: str) -> None:
    for t in (landing, curated):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {t} ({ORDER_DDL}, order_date DATE) USING iceberg "
                  f"PARTITIONED BY (order_date)")


def land_orders(spark, landing, day: date, n: int, start_id: int = 0) -> int:
    """Simulate the source landing n orders for a day; returns the new landing snapshot id."""
    rows = [(i, i % 50, float(i % 300), datetime(day.year, day.month, day.day) + timedelta(seconds=i), day)
            for i in range(start_id, start_id + n)]
    spark.createDataFrame(rows, f"{ORDER_DDL}, order_date DATE").writeTo(landing).append()
    return spark.sql(f"SELECT snapshot_id FROM {landing}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]


def daily_batch(spark, landing, curated, day: date, mode: str = "overwrite",
                source_snapshot_id: int | None = None) -> None:
    """Load one day. mode='overwrite' replaces only that day's partition (idempotent rerun);
    mode='append' adds rows again on every rerun. Pinning the source snapshot bounds the input,
    so a retry reads exactly what the first attempt read."""
    reader = spark.read
    if source_snapshot_id is not None:
        reader = reader.option("snapshot-id", source_snapshot_id)
    df = reader.table(landing).where(F.col("order_date") == F.lit(day))
    writer = df.writeTo(curated)
    if mode == "overwrite":
        writer.overwritePartitions()
    elif mode == "append":
        writer.append()
    else:
        raise ValueError(mode)


# --- Micro-batch / streaming ---------------------------------------------------------------
def create_click_table(spark, table) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (click_id BIGINT, user_id BIGINT, "
              f"page STRING, produced_at TIMESTAMP) USING iceberg")


def clicks(n, start_id=0):
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    return [{"click_id": i, "user_id": i % 1000, "page": f"/p/{i % 20}", "produced_at": now}
            for i in range(start_id, start_id + n)]


def click_stream(spark, topic, **kafka_opts):
    return (read_kafka(spark, topic, **kafka_opts)
            .select(F.from_json(F.col("value").cast("string"),
                                "click_id BIGINT, user_id BIGINT, page STRING, produced_at TIMESTAMP").alias("c"))
            .select("c.*"))


def start_click_ingest(spark, topic, table, checkpoint, trigger: dict, **kafka_opts):
    """trigger: {'availableNow': True} for scheduled micro-batch runs,
    {'processingTime': '2 seconds'} for a continuously running stream."""
    return (click_stream(spark, topic, **kafka_opts).writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(**trigger)
            .toTable(table))


def commit_stats(spark, table) -> dict:
    s = spark.sql(f"SELECT count(*) n FROM {table}.snapshots").first().n
    f = spark.sql(f"SELECT count(*) n, coalesce(avg(record_count), 0) avg_rows FROM {table}.files").first()
    return {"snapshots": s, "data_files": f.n, "avg_rows_per_file": round(f.avg_rows, 1)}


def measure_freshness(spark, topic, table, batches: int = 3, batch_size: int = 200,
                      timeout_s: float = 60.0) -> list[float]:
    """Produce a batch, poll the table until it is visible; return seconds per batch."""
    lags = []
    expected = 0
    for b in range(batches):
        t0 = time.monotonic()
        produce(topic, clicks(batch_size, start_id=b * batch_size))
        expected += batch_size
        while spark.table(table).count() < expected:
            if time.monotonic() - t0 > timeout_s:
                raise TimeoutError(f"batch {b} not visible after {timeout_s}s")
            time.sleep(0.25)
        lags.append(round(time.monotonic() - t0, 2))
    return lags
