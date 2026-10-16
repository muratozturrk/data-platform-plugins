"""persistence_strategy: apply sparkopt's PersistenceAdvisor decision tree to a DataFrame.

The tree (first match wins; thresholds are sparkopt heuristics, not Spark rules):
  1. streaming and reuse >= 2      -> HYBRID (handled by hybrid_checkpoint.py)
  2. iterative loop                -> ITERATIVE_LOCAL_CHECKPOINT (see iterative_loop.py)
  3. depth > 20                    -> RELIABLE_CHECKPOINT
  4. depth > 10 and reuse >= 2     -> LOCAL_CHECKPOINT
  5. reuse >= 2                    -> CACHE with MemoryProfile level
  6. otherwise                     -> NO_PERSIST

Run: spark-submit persistence_strategy.py   (or python persistence_strategy.py with pyspark installed)
"""
import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def recommended_level(df_size_mb, executor_memory_mb=4096, executor_count=10):
    """MemoryProfile.recommended_storage_level with PySpark-valid names."""
    usable = executor_memory_mb * executor_count * 0.6 * 0.5
    if df_size_mb < usable * 0.8:
        return StorageLevel.MEMORY_ONLY          # original sparkopt: MEMORY_ONLY_SER
    if df_size_mb / 3.0 < usable * 0.8:
        return StorageLevel.MEMORY_AND_DISK      # original sparkopt: MEMORY_AND_DISK_SER
    return StorageLevel.DISK_ONLY


def decide(depth, reuse, iterative=False, streaming=False):
    if streaming and reuse >= 2:
        return "HYBRID"
    if iterative:
        return "ITERATIVE_LOCAL_CHECKPOINT"
    if depth > 20:
        return "RELIABLE_CHECKPOINT"
    if depth > 10 and reuse >= 2:
        return "LOCAL_CHECKPOINT"
    if reuse >= 2:
        return "CACHE"
    return "NO_PERSIST"


def apply_decision(spark, df: DataFrame, strategy, df_size_mb=0.0) -> DataFrame:
    """Return the DataFrame to use downstream. Caller unpersists CACHE results after the last action."""
    if strategy == "RELIABLE_CHECKPOINT":
        # Shared FS on a cluster (hdfs://, s3a://, abfss://). Local path is fine only in local mode.
        spark.sparkContext.setCheckpointDir(
            os.environ.get("SPARK_CHECKPOINT_DIR", "file:///tmp/spark-ckpt"))
        return df.checkpoint(eager=True)
    if strategy in ("LOCAL_CHECKPOINT", "ITERATIVE_LOCAL_CHECKPOINT"):
        return df.localCheckpoint(eager=True)    # no setCheckpointDir needed
    if strategy == "CACHE":
        df = df.persist(recommended_level(df_size_mb))
        df.count()                               # eager materialization of all partitions
        return df
    return df


def main():
    assert decide(25, 3) == "RELIABLE_CHECKPOINT"
    assert decide(15, 2) == "LOCAL_CHECKPOINT"
    assert decide(15, 1) == "NO_PERSIST"
    assert decide(5, 2) == "CACHE"
    assert decide(30, 2, iterative=True) == "ITERATIVE_LOCAL_CHECKPOINT"
    assert decide(30, 2, streaming=True) == "HYBRID"
    assert recommended_level(100) is StorageLevel.MEMORY_ONLY
    assert recommended_level(20_000) is StorageLevel.MEMORY_AND_DISK
    assert recommended_level(50_000) is StorageLevel.DISK_ONLY

    spark = SparkSession.builder.master("local[2]").appName("persistence_strategy").getOrCreate()
    orders = spark.range(1_000_000).withColumn("customer_id", F.col("id") % 1000)
    customers = spark.range(1000).withColumnRenamed("id", "customer_id") \
        .withColumn("country", F.concat(F.lit("C"), (F.col("customer_id") % 7).cast("string")))
    enriched = orders.join(customers, "customer_id")

    strategy = decide(depth=4, reuse=2)
    enriched = apply_decision(spark, enriched, strategy, df_size_mb=40)
    try:
        enriched.groupBy("country").count().show()
        print(enriched.agg(F.sum("id")).first()[0])
    finally:
        if strategy == "CACHE":
            enriched.unpersist()                 # after the last action
    spark.stop()


if __name__ == "__main__":
    main()
