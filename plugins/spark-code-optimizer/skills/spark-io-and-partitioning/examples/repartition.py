"""repartition: raise parallelism for under-partitioned data, sized by cores AND bytes.

sparkopt fires when partitions < 8 and generates repartition(max(count*4, 200*2)) = 400,
regardless of data size. This version skips small data and sizes by ~128 MB per partition.

Run: python repartition.py   (PySpark 3.3+)
"""
from __future__ import annotations

import math

from pyspark.sql import DataFrame, SparkSession

TARGET_PARTITION_BYTES = 128 * 1024**2
SMALL_DATA_BYTES = 256 * 1024**2  # below this, a few partitions are fine


def target_partitions(est_bytes: int, cores: int) -> int:
    return max(cores * 2, math.ceil(est_bytes / TARGET_PARTITION_BYTES))


def ensure_parallelism(df: DataFrame, est_bytes: int, key: str | None = None) -> DataFrame:
    spark = df.sparkSession
    cores = spark.sparkContext.defaultParallelism
    current = df.rdd.getNumPartitions()
    if est_bytes < SMALL_DATA_BYTES or current >= cores:
        return df
    n = target_partitions(est_bytes, cores)
    return df.repartition(n, key) if key else df.repartition(n)


if __name__ == "__main__":
    assert target_partitions(30 * 1024**3, 16) == 240
    assert target_partitions(10 * 1024**2, 16) == 32
    spark = SparkSession.builder.master("local[4]").appName("repartition").getOrCreate()
    one = spark.range(0, 100_000, numPartitions=1)

    small = ensure_parallelism(one, est_bytes=1024**2)
    big = ensure_parallelism(one, est_bytes=2 * 1024**3, key="id")
    print("small:", small.rdd.getNumPartitions(), "big:", big.rdd.getNumPartitions())
    assert small.rdd.getNumPartitions() == 1
    assert big.rdd.getNumPartitions() == 16
    big.explain()  # Exchange hashpartitioning(id#0L, 16), REPARTITION_BY_NUM
    spark.stop()
