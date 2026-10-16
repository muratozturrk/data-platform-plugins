"""coalesce: merge too-many/too-small partitions; fix it at the read first.

sparkopt fires when partitions > 2000 and avg rows/partition < 10_000 (heuristic) and emits a
fixed coalesce(200). Prefer: bigger scan splits (maxPartitionBytes / openCostInBytes) and AQE
post-shuffle coalescing; then coalesce(n) as a cheap last step.

Run: python coalesce.py   (PySpark 3.3+)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


def pack_small_files(spark: SparkSession, max_partition_mb: int = 256, open_cost_mb: int = 8) -> None:
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(max_partition_mb * 1024**2))
    spark.conf.set("spark.sql.files.openCostInBytes", str(open_cost_mb * 1024**2))


def shrink(df: DataFrame, n: int | None = None) -> DataFrame:
    """coalesce to n (default: cluster cores). Never increases the partition count."""
    n = n or df.sparkSession.sparkContext.defaultParallelism
    return df.coalesce(n) if df.rdd.getNumPartitions() > n else df


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[4]").appName("coalesce").getOrCreate()
    pack_small_files(spark)
    df = spark.range(0, 30_000, numPartitions=3000).filter(F.col("id") % 1000 == 0)

    merged = shrink(df)
    print("before:", df.rdd.getNumPartitions(), "after:", merged.rdd.getNumPartitions())
    assert merged.rdd.getNumPartitions() == 4
    assert shrink(merged, 100).rdd.getNumPartitions() == 4  # coalesce cannot increase
    assert "Exchange" not in merged._jdf.queryExecution().executedPlan().toString()
    spark.stop()
