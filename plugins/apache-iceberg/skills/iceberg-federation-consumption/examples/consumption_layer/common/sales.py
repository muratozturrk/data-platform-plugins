"""Synthetic gold-layer sales table shared by every lab (deterministic, Spark-generated).

One row per order line of a multi-merchant marketplace: the kind of curated table that
BI dashboards, notebooks, ML jobs and embedded merchant analytics all consume.
"""
import calendar
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F

MERCHANTS = ["m01", "m02", "m03", "m04", "m05"]
REGIONS = ["NA", "EU", "APAC", "LATAM"]
CATEGORIES = ["books", "electronics", "home", "toys", "fashion", "garden"]
STATUSES = ["completed"] * 7 + ["refunded", "cancelled", "completed"]   # 80% / 10% / 10%
START = datetime(2025, 3, 1)

DDL = """
CREATE TABLE {table} (
    order_id       BIGINT,
    merchant_id    STRING,
    customer_id    BIGINT,
    customer_email STRING,
    region         STRING,
    category       STRING,
    quantity       INT,
    unit_price     DOUBLE,
    discount       DOUBLE,
    status         STRING,
    order_ts       TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(order_ts))
TBLPROPERTIES ('format-version'='2', 'write.distribution-mode'='hash')
"""


def create_sales_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(DDL.format(table=table))


def _pick(values: list[str], idx_col) -> F.Column:
    return F.array(*[F.lit(v) for v in values])[(idx_col % len(values)).cast("int")]


def sales(spark: SparkSession, n: int, start_id: int = 0, start: datetime = START, days: int = 30) -> DataFrame:
    """n order lines spread evenly over `days` days starting at `start` (UTC)."""
    t0 = calendar.timegm(start.timetuple())
    span = 86400 * days
    oid = F.col("order_id")
    return (
        spark.range(start_id, start_id + n).withColumnRenamed("id", "order_id")
        .withColumn("merchant_id", _pick(MERCHANTS, (oid * 31 / 7).cast("long")))
        .withColumn("customer_id", (oid * 7919) % 20_000)
        .withColumn("customer_email", F.concat(F.lit("c"), F.col("customer_id"), F.lit("@example.com")))
        .withColumn("region", _pick(REGIONS, (oid * 13 / 3).cast("long")))
        .withColumn("category", _pick(CATEGORIES, (oid * 17 / 5).cast("long")))
        .withColumn("quantity", ((oid * 7) % 5 + 1).cast("int"))
        .withColumn("unit_price", F.round(((oid * 37) % 20_000) / 100.0 + 1.0, 2))
        .withColumn("discount", F.when(oid % 4 == 0, F.lit(0.1)).otherwise(F.lit(0.0)))
        .withColumn("status", _pick(STATUSES, oid * 11))
        .withColumn("order_ts", F.timestamp_seconds(F.lit(t0) + ((oid - start_id) * span / n).cast("long")))
    )


def load_sales(spark: SparkSession, table: str, n: int = 300_000, days: int = 30) -> None:
    """Create the table and write `n` rows (one file per daily partition)."""
    create_sales_table(spark, table)
    sales(spark, n, days=days).writeTo(table).append()


def day(offset: int) -> datetime:
    return START + timedelta(days=offset)
