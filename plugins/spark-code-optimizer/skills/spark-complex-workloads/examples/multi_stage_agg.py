"""Rollup + window + pivot over ONE cached base (sparkopt case: multi_stage_agg).

Persistence plan
  orders (raw)  -> no persist: scanned once, column-pruned and pre-aggregated
  daily         -> MEMORY_AND_DISK + count(): the shared base for 3 consumers; it is the
                   daily grain (category, region, event_date), far smaller than raw rows
  rollup / window / pivot results -> no persist: each is written exactly once

Use the daily pre-aggregate only when every consumer is additive (sum, count, min, max).
If a branch needs row-level data (rank of individual orders, exact distinct counts),
cache the column-pruned raw `base` instead.

Corrections vs. the sparkopt template: rollup subtotal rows marked with grouping_id() instead
of fillna("ALL") (fillna silently skips int columns); 30-day window uses rangeBetween on a day
number, not rowsBetween(-29, 0) (= 30 rows); the daily pre-aggregate is cached instead of the raw
base. Original sparkopt also used MEMORY_ONLY_SER, joined rollup with pivot on a missing
`category` column and unpersisted before the first action; the bundled copy fixes those three
(writes before unpersist).

Run: spark-submit multi_stage_agg.py --input s3a://bucket/orders/ --out s3a://bucket/marts/
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main(input_path: str, out: str) -> None:
    spark = SparkSession.builder.appName("multi-stage-agg").getOrCreate()

    base = (
        spark.read.parquet(input_path)
        .where(F.col("status") == "COMPLETE")
        .select("order_id", "category", "region", "event_date", "revenue")  # prune first
    )
    daily = (
        base.groupBy("category", "region", "event_date")
        .agg(F.sum("revenue").alias("revenue"), F.count("*").alias("orders"))
        .withColumn("year", F.year("event_date"))
        .withColumn("quarter", F.quarter("event_date"))
        .withColumn("month", F.month("event_date"))
    )
    daily.persist(StorageLevel.MEMORY_AND_DISK)
    daily.count()  # materialize once; all three branches below read the cached blocks

    # Branch 1: hierarchical rollup. grouping_id() tells subtotal rows from real NULLs.
    rollup_df = (
        daily.rollup("year", "quarter", "month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("orders").alias("order_count"),
            (F.sum("revenue") / F.sum("orders")).alias("avg_order_value"),
            F.grouping_id().alias("level"),  # 0=month, 1=quarter, 3=year, 7=grand total
        )
    )

    # Branch 2: 30-calendar-day rolling average + running total per category.
    cat_daily = daily.groupBy("category", "event_date").agg(F.sum("revenue").alias("revenue"))
    day_no = F.datediff("event_date", F.lit("1970-01-01"))
    w30 = Window.partitionBy("category").orderBy(day_no).rangeBetween(-29, 0)
    w_all = (Window.partitionBy("category").orderBy("event_date")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    w_rank = Window.partitionBy("event_date").orderBy(F.desc("revenue"))
    windowed_df = cat_daily.select(
        "category", "event_date", "revenue",
        F.avg("revenue").over(w30).alias("rolling_30d_avg"),
        F.sum("revenue").over(w_all).alias("cumulative_revenue"),
        F.rank().over(w_rank).alias("daily_category_rank"),
    )

    # Branch 3: pivot. Listing the values skips the extra job that collects distinct regions.
    pivot_df = (
        daily.groupBy("category", "year")
        .pivot("region", ["EU", "US", "APAC"])
        .agg(F.sum("revenue"))
    )

    rollup_df.write.mode("overwrite").parquet(f"{out}/revenue_rollup")
    windowed_df.write.mode("overwrite").parquet(f"{out}/category_daily_window")
    pivot_df.write.mode("overwrite").parquet(f"{out}/category_region_pivot")

    daily.unpersist()  # only after the last action
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()
    main(a.input, a.out)
