"""Synthetic clickstream events used by every lab.

Shape mirrors a typical web analytics table (user_id, session, event type, country,
event time). Deterministic per seed so tests are reproducible.
"""
import calendar
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F

EVENT_TYPES = ["page_view", "add_to_cart", "checkout", "search", "login"]
COUNTRIES = ["US", "DE", "TR", "GB", "FR", "BR", "IN", "JP"]

DDL = """
CREATE TABLE {table} (
    event_id    BIGINT,
    user_id     BIGINT,
    session_id  STRING,
    event_type  STRING,
    country     STRING,
    amount      DOUBLE,
    event_ts    TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ({props})
"""


def create_events_table(spark: SparkSession, table: str, **props: str) -> None:
    base = {"format-version": "2", "write.distribution-mode": "none"}
    base.update(props)
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(DDL.format(table=table, props=", ".join(f"'{k}'='{v}'" for k, v in base.items())))


def events(spark: SparkSession, n: int, start_id: int, day: datetime,
           n_users: int = 500, seed: int = 0) -> DataFrame:
    """n events spread over one day, user ids in a random (unsorted) order."""
    day_start = datetime(day.year, day.month, day.day)
    return (
        spark.range(start_id, start_id + n)
        .withColumnRenamed("id", "event_id")
        .withColumn("r", F.rand(seed))
        .withColumn("user_id", (F.rand(seed + 1) * n_users).cast("bigint") + 1)
        .withColumn("session_id", F.concat(F.lit("s-"), (F.col("event_id") / 20).cast("bigint")))
        .withColumn("event_type", F.element_at(F.array(*map(F.lit, EVENT_TYPES)),
                                               (F.col("r") * len(EVENT_TYPES)).cast("int") + 1))
        .withColumn("country", F.element_at(F.array(*map(F.lit, COUNTRIES)),
                                            (F.rand(seed + 2) * len(COUNTRIES)).cast("int") + 1))
        .withColumn("amount", F.when(F.col("event_type") == "checkout",
                                     F.round(F.rand(seed + 3) * 200, 2)))
        .withColumn("event_ts", F.timestamp_seconds(
            F.lit(calendar.timegm(day_start.timetuple())) + (F.rand(seed + 4) * 86399).cast("int")))
        .drop("r")
    )


def micro_batch_ingest(spark: SparkSession, table: str, batches: int, rows_per_batch: int,
                       day: datetime, files_per_batch: int = 4, seed: int = 0, start_id: int = 0) -> None:
    """Simulates a streaming job: one small commit per micro-batch (one snapshot each)."""
    for b in range(batches):
        (events(spark, rows_per_batch, start_id + b * rows_per_batch, day, seed=seed + b)
         .repartition(files_per_batch)
         .writeTo(table).append())


def day(offset: int) -> datetime:
    """A fixed reference day (2025-01-10) shifted by offset days; keeps tests deterministic."""
    return datetime(2025, 1, 10) + timedelta(days=offset)
