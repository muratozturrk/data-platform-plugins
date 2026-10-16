"""Aligned Fan-In -- PySpark (data processing layer).

Two flavors of merging branches into a single dataset:
  * vertical alignment  -> UNION: same columns, more rows. Use `unionByName`, NOT the
    position-based `union`, otherwise `SELECT a,b,c` + `SELECT c,b,a` silently mixes columns.
  * horizontal alignment -> JOIN: rows combined, more columns.
The job is "aligned": it fails if ANY of the expected hourly partial aggregates is missing,
mirroring all_success at the orchestration layer.

Run: spark-submit aligned_fan_in_job.py --partials /tmp/dp/hourly_aggs --devices /tmp/dp/devices \
         --date 2024-01-01 --output /tmp/dp/daily_trends
     (add --demo to generate local sample inputs first; the missing-hour check uses the
     local FS -- swap os.path.exists for your object-store listing)
Requires: pyspark>=3.3.
"""
import argparse
import functools
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

HOURS = [f"{h:02d}" for h in range(24)]


def write_demo_inputs(spark: SparkSession, partials: str, devices: str, date: str) -> None:
    for hour in HOURS:
        # columns deliberately in a different order per hour to show why unionByName matters
        df = spark.createDataFrame([("home", "dev-1", 3), ("cart", "dev-2", 1)],
                                   "page STRING, device_id STRING, visits INT")
        cols = ["visits", "page", "device_id"] if int(hour) % 2 else ["page", "device_id", "visits"]
        df.select(*cols).write.mode("overwrite").parquet(f"{partials}/date={date}/hour={hour}")
    spark.createDataFrame([("dev-1", "android"), ("dev-2", "ios")], "device_id STRING, type STRING") \
        .write.mode("overwrite").parquet(devices)


def read_all_hours(spark: SparkSession, partials: str, date: str) -> DataFrame:
    paths = [f"{partials}/date={date}/hour={hour}" for hour in HOURS]
    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        # Aligned: all parents must succeed; a partial day is an error, not an output.
        raise RuntimeError(f"{len(missing)} hourly partials missing, e.g. {missing[0]}")
    hourly = [spark.read.parquet(p) for p in paths]
    return functools.reduce(lambda left, right: left.unionByName(right), hourly)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partials", required=True)
    parser.add_argument("--devices", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("aligned-fan-in").getOrCreate()
    if args.demo:
        write_demo_inputs(spark, args.partials, args.devices, args.date)

    day_visits = read_all_hours(spark, args.partials, args.date)        # vertical fan-in
    devices = spark.read.parquet(args.devices)
    enriched = day_visits.join(devices, "device_id", "left")            # horizontal fan-in

    trends = enriched.groupBy("page", "type").agg(F.sum("visits").alias("visits"))
    trends.withColumn("day", F.lit(args.date)).write.mode("overwrite").parquet(
        f"{args.output}/date={args.date}")
    spark.stop()


if __name__ == "__main__":
    main()
