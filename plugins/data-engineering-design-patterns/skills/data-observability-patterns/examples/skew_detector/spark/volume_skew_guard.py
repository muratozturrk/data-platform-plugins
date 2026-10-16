"""Skew Detector - window-to-window volume guard + partition skew ratio (PySpark).

Used as the first "Audit" step of an AWAP pipeline: the job refuses to process a
partition whose volume differs too much from the LAST SUCCESSFUL run (not simply
the previous day - this avoids the "fatality loop" where a valid day is flagged
because it is compared to a broken one).

Baseline volumes live in a small Delta stats table; a run appends its count only
after it passes the guard, so failed runs never become a baseline.

Also computes the storage skew ratio STDDEV(x)/AVG(x)*100 over a partitioning key.

Run:  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 volume_skew_guard.py 2024-06-02
Env:  INPUT_DIR (json, partitioned by date=YYYY-MM-DD), STATS_TABLE_PATH
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_DIR = os.environ.get("INPUT_DIR", "/tmp/dp/input/visits")
STATS_TABLE = os.environ.get("STATS_TABLE_PATH", "/tmp/dp/stats/visits_volumes")
TOLERANCE = 0.5            # accept 50% less or 50% more data than the baseline
MAX_PARTITION_SKEW_PCT = 40.0
SEASONAL_EXCEPTIONS = {"2024-11-29"}  # e.g. Black Friday: business-approved exceptions


class SkewDetected(Exception):
    pass


def last_successful_volume(spark: SparkSession) -> int | None:
    try:
        row = (spark.read.format("delta").load(STATS_TABLE)
               .orderBy(F.col("partition_date").desc()).limit(1).collect())
    except Exception:  # first run: no stats table yet
        return None
    return row[0].row_count if row else None


def partition_skew_pct(df, key: str) -> float:
    """Ratio of a standard deviation, in percent, of row counts per key."""
    stats = (df.groupBy(key).count()
             .agg((F.stddev_pop("count") / F.avg("count") * 100).alias("skew")).collect()[0])
    return stats.skew or 0.0


def main(partition_date: str) -> None:
    spark = (SparkSession.builder.appName("visits_skew_guard")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    visits = spark.read.json(f"{INPUT_DIR}/date={partition_date}")
    current = visits.count()

    baseline = last_successful_volume(spark)
    if baseline and partition_date not in SEASONAL_EXCEPTIONS:
        ratio = current / baseline
        if ratio > 1 + TOLERANCE or ratio < 1 - TOLERANCE:
            raise SkewDetected(f"{partition_date}: {current} rows vs baseline {baseline} (ratio {ratio:.2f})")

    skew = partition_skew_pct(visits, "page")
    if skew > MAX_PARTITION_SKEW_PCT:
        raise SkewDetected(f"{partition_date}: key skew {skew:.1f}% > {MAX_PARTITION_SKEW_PCT}%")

    # ... Write / Audit / Publish steps of the pipeline go here ...

    (spark.createDataFrame([(partition_date, current)], "partition_date STRING, row_count LONG")
     .write.format("delta").mode("append").save(STATS_TABLE))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2024-06-02")
