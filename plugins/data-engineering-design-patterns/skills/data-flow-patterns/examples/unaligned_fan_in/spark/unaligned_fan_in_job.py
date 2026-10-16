"""Unaligned Fan-In -- PySpark (data processing layer, "less declarative" flavor).

The job explicitly evaluates which parents (hourly partitions) are available instead of
requiring all of them. It merges whatever exists, computes a completeness ratio and:
  * marks every output row with `is_approximate` and writes a completeness companion file;
  * optionally (`--private-if-partial`) keeps a partial result in a PRIVATE location so it is
    not exposed to consumers until the gaps are filled.
If no parent succeeded at all it fails -> the orchestrator's failure path takes over.

Run: spark-submit unaligned_fan_in_job.py --partials /tmp/dp/hourly_aggs --date 2024-01-01 \
         --output /tmp/dp/daily_cube --private /tmp/dp/_private/daily_cube [--private-if-partial]
Uses local-FS existence checks; swap os.path.exists for your object-store listing.
Requires: pyspark>=3.3.
"""
import argparse
import functools
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EXPECTED_HOURS = [f"{h:02d}" for h in range(24)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partials", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--private", required=True)
    parser.add_argument("--private-if-partial", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("unaligned-fan-in").getOrCreate()

    available = [h for h in EXPECTED_HOURS
                 if os.path.exists(f"{args.partials}/date={args.date}/hour={h}")]
    if not available:
        raise RuntimeError("No parent succeeded -- let the fallback/error task handle it")

    completeness = len(available) / len(EXPECTED_HOURS)
    hourly = [spark.read.parquet(f"{args.partials}/date={args.date}/hour={h}") for h in available]
    day = functools.reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), hourly)

    cube = (day.cube("page", "device_type").agg(F.sum("visits").alias("visits"))
            .withColumn("is_approximate", F.lit(completeness < 1.0)))

    is_partial = completeness < 1.0
    target_root = args.private if (is_partial and args.private_if_partial) else args.output
    target = f"{target_root}/date={args.date}"
    cube.write.mode("overwrite").parquet(target)

    # Companion completeness metadata (could also be object tags / a table row).
    with open(os.path.join(target, "_COMPLETENESS.json"), "w") as f:
        json.dump({"date": args.date, "completeness": completeness,
                   "missing_hours": sorted(set(EXPECTED_HOURS) - set(available))}, f)
    print(f"wrote {target} completeness={completeness:.0%}")
    spark.stop()


if __name__ == "__main__":
    main()
