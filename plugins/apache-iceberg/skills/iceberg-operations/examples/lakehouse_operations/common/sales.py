"""Synthetic sales orders used by every test module.

Deterministic per seed so tests are reproducible.
"""
import calendar
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F

from common.lakehouse import drop_table

REGIONS = ["US", "EU", "APAC", "LATAM"]

DDL = """
CREATE TABLE {table} (
    order_id     BIGINT,
    customer_id  BIGINT,
    region       STRING,
    amount       DOUBLE,
    order_ts     TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(order_ts))
TBLPROPERTIES ({props})
"""


def create_sales_table(spark: SparkSession, table: str, **props: str) -> None:
    base = {"format-version": "2", "write.distribution-mode": "none"}
    base.update(props)
    drop_table(spark, table)
    spark.sql(DDL.format(table=table, props=", ".join(f"'{k}'='{v}'" for k, v in base.items())))


def orders(spark: SparkSession, n: int, start_id: int, day: datetime, seed: int = 0,
           n_customers: int = 1_000) -> DataFrame:
    """n orders spread over one day."""
    day_start = calendar.timegm(datetime(day.year, day.month, day.day).timetuple())
    return (
        spark.range(start_id, start_id + n)
        .withColumnRenamed("id", "order_id")
        .withColumn("customer_id", (F.rand(seed) * n_customers).cast("bigint") + 1)
        .withColumn("region", F.element_at(F.array(*map(F.lit, REGIONS)),
                                           (F.rand(seed + 1) * len(REGIONS)).cast("int") + 1))
        .withColumn("amount", F.round(F.rand(seed + 2) * 500 + 1, 2))
        .withColumn("order_ts", F.timestamp_seconds(F.lit(day_start) + (F.rand(seed + 3) * 86399).cast("int")))
    )


def append_batches(spark: SparkSession, table: str, batches: int, rows_per_batch: int, day: datetime,
                   files_per_batch: int = 4, start_id: int = 0, seed: int = 0) -> int:
    """One commit per batch, like a micro-batch / CDC writer. Returns the next free order_id."""
    next_id = start_id
    for b in range(batches):
        (orders(spark, rows_per_batch, next_id, day, seed=seed + b)
         .repartition(files_per_batch).writeTo(table).append())
        next_id += rows_per_batch
    return next_id


def fingerprint(spark: SparkSession, table: str) -> tuple:
    """(row count, rounded amount sum, order-id hash sum): must not change on maintenance/recovery."""
    r = spark.sql(f"SELECT count(*) c, round(sum(amount), 2) s, sum(hash(order_id, amount)) h FROM {table}").first()
    return r.c, r.s, r.h
