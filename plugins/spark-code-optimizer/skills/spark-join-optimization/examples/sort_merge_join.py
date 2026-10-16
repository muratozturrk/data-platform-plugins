"""sort_merge_join: large x large joins with AQE sizing; pre-partition only for reuse.

sparkopt fires when estimated_bytes >= 10 GB (sparkopt heuristic, large_table_threshold_bytes).
Correction vs. the library snippet: Spark already inserts Exchange + Sort for SortMergeJoin, so a
manual repartition(200).sortWithinPartitions() before a single join only hardcodes 200 partitions.
Partition once only when the same frame feeds several operators on the same key.

Run: python sort_merge_join.py   (needs pyspark>=3.3)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def configure_smj(spark, max_partitions=400):
    """High initial partition count; AQE coalesces to ~advisory size per partition."""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
    spark.conf.set("spark.sql.shuffle.partitions", str(max_partitions))


def main():
    spark = (SparkSession.builder.master("local[2]").appName("sort_merge_join")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # simulate two big sides
             .getOrCreate())
    configure_smj(spark)

    left = spark.range(1_000_000).select((F.col("id") % 100_000).alias("k"),
                                         F.col("id").alias("lv"))
    right = spark.range(1_000_000).select((F.col("id") % 100_000).alias("k"),
                                          (F.col("id") * 2).alias("rv"))

    # Single join: just join. Spark plans SortMergeJoin with its own Exchange/Sort.
    single = left.join(right, "k")
    assert "SortMergeJoin" in single._jdf.queryExecution().executedPlan().toString()

    # Reuse: partition `left` once by k; the aggregation reuses that partitioning in the same plan
    # (only the repartition's Exchange remains). Note: through .cache() + AQE (Spark 3.5) the
    # cached plan's partitioning may be hidden and a new Exchange added -- check explain().
    left_p = left.repartition(400, "k")
    joined = left_p.join(right, "k")
    per_key = left_p.groupBy("k").agg(F.count("*").alias("n"))
    agg_plan = per_key._jdf.queryExecution().executedPlan().toString()
    assert agg_plan.count("Exchange") == 1, agg_plan

    print("joined:", joined.count(), "keys:", per_key.count())
    spark.stop()


if __name__ == "__main__":
    main()
