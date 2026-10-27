"""Synthetic e-commerce orders used by the labs (deterministic, Spark-generated)."""
import calendar
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F

COUNTRIES = ["US", "DE", "TR", "GB", "FR", "BR", "IN", "JP"]
STATUSES = ["placed", "paid", "shipped", "returned"]

DDL = """
CREATE TABLE {table} (
    order_id    BIGINT,
    customer_id BIGINT,
    country     STRING,
    status      STRING,
    amount      DOUBLE,
    order_ts    TIMESTAMP
)
USING iceberg
{partitioning}
TBLPROPERTIES ('format-version'='2', 'write.distribution-mode'='hash')
"""


def create_orders_table(spark: SparkSession, table: str, partitioning: str = "PARTITIONED BY (days(order_ts))") -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(DDL.format(table=table, partitioning=partitioning))


def orders(spark: SparkSession, n: int, start_id: int, day: datetime, days: int = 1) -> DataFrame:
    """n orders spread evenly over `days` consecutive days starting at `day` (UTC)."""
    start = calendar.timegm(datetime(day.year, day.month, day.day).timetuple())
    span = 86400 * days
    countries = F.array(*[F.lit(c) for c in COUNTRIES])
    statuses = F.array(*[F.lit(s) for s in STATUSES])
    return (
        spark.range(start_id, start_id + n)
        .withColumnRenamed("id", "order_id")
        .withColumn("customer_id", (F.col("order_id") * 7919) % 1000)
        .withColumn("country", countries[(F.col("order_id") % len(COUNTRIES)).cast("int")])
        .withColumn("status", statuses[(F.col("order_id") % len(STATUSES)).cast("int")])
        .withColumn("amount", F.round((F.col("order_id") * 37) % 50000 / 100.0, 2))
        .withColumn("order_ts", F.timestamp_seconds(
            F.lit(start) + ((F.col("order_id") - start_id) * span / n).cast("long")))
    )


def arrow_orders(n: int, start_id: int, day: datetime):
    """Same shape as `orders`, built with pyarrow for non-Spark writers (PyIceberg, plain files)."""
    import pyarrow as pa
    start = datetime(day.year, day.month, day.day)
    ids = list(range(start_id, start_id + n))
    return pa.table({
        "order_id": pa.array(ids, pa.int64()),
        "customer_id": pa.array([(i * 7919) % 1000 for i in ids], pa.int64()),
        "country": pa.array([COUNTRIES[i % len(COUNTRIES)] for i in ids], pa.string()),
        "status": pa.array([STATUSES[i % len(STATUSES)] for i in ids], pa.string()),
        "amount": pa.array([round((i * 37) % 50000 / 100.0, 2) for i in ids], pa.float64()),
        "order_ts": pa.array([calendar.timegm(start.timetuple()) + (i - start_id) * 86400 // n for i in ids],
                             pa.float64()).cast(pa.int64()).cast(pa.timestamp("s")).cast(pa.timestamp("us", tz="UTC")),
    })
