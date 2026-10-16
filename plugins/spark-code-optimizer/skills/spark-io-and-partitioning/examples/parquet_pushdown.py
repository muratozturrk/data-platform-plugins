"""parquet_pushdown: column pruning + predicate pushdown on a Parquet scan.

Writes a small Parquet dataset to a temp dir, then compares a UDF filter (not pushed)
with a native filter (pushed). Check the FileScan line: PushedFilters / ReadSchema.

Run: python parquet_pushdown.py   (PySpark 3.3+)
"""
import datetime as dt
import tempfile

from pyspark.sql import SparkSession, functions as F, types as T


def read_pruned(spark, path):
    """Only the needed columns, predicate as a plain comparison, so it is pushed down."""
    return (
        spark.read.parquet(path)
        .select("user_id", "amount", "event_date")
        .filter(F.col("event_date") >= F.to_date(F.lit("2024-01-01")))
    )


def scan_line(df):
    """Return the FileScan line of the physical plan."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    return next(line.strip() for line in plan.splitlines() if "FileScan" in line)


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[2]").appName("parquet_pushdown").getOrCreate()
    path = tempfile.mkdtemp() + "/events"
    (spark.range(10_000)
        .withColumn("user_id", F.col("id") % 100)
        .withColumn("amount", F.rand(1) * 100)
        .withColumn("event_date", F.date_add(F.lit(dt.date(2023, 6, 1)), (F.col("id") % 400).cast("int")))
        .withColumn("payload", F.repeat(F.lit("x"), 50))
        .write.mode("overwrite").parquet(path))

    is_recent = F.udf(lambda d: d is not None and d.year >= 2024, T.BooleanType())
    bad = spark.read.parquet(path).filter(is_recent("event_date"))
    good = read_pruned(spark, path)

    bad_scan, good_scan = scan_line(bad), scan_line(good)
    print("UDF   :", bad_scan)
    print("native:", good_scan)
    assert "PushedFilters: []" in bad_scan
    assert "GreaterThanOrEqual(event_date" in good_scan
    assert "payload" not in good_scan.split("ReadSchema:")[1]
    spark.stop()
