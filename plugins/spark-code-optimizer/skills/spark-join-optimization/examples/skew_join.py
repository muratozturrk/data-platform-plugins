"""skew_join: AQE skew join first, targeted salting as fallback (corrected sparkopt output).

sparkopt fires when partition skew_ratio (max_rows / avg_rows) > 3.0 (sparkopt heuristic).
Corrections vs. the library snippet:
  * prefer spark.sql.adaptive.skewJoin.enabled (Spark 3.x) before any code change;
  * salt only hot keys, and join on [key, _salt] so no ambiguous duplicate key column remains.

Run: python skew_join.py   (needs pyspark>=3.3)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def enable_aqe_skew(spark, threshold="256m", factor="5"):
    """Let AQE split skewed sort-merge-join partitions at runtime."""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", threshold)
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", factor)


def hot_keys(df, key, min_rows):
    """Keys with more than min_rows rows (collected to the driver: keep the list small)."""
    rows = df.groupBy(key).count().filter(F.col("count") > min_rows).select(key).collect()
    return [r[0] for r in rows]


def salted_join(left, right, key, hot, n=16, how="inner"):
    """Equi-join where only `hot` keys are salted into n buckets on the left side.

    Right rows for hot keys are replicated n times (one per salt); other rows get salt 0.
    Valid for inner and left joins (left = the skewed, preserved side).
    """
    is_hot = F.col(key).isin(hot)
    left_s = left.withColumn(
        "_salt", F.when(is_hot, (F.rand(seed=42) * n).cast("int")).otherwise(F.lit(0)))
    salts = left.sparkSession.range(n).select(F.col("id").cast("int").alias("_salt"))
    right_s = (right.filter(is_hot).crossJoin(salts)
               .unionByName(right.filter(~is_hot).withColumn("_salt", F.lit(0))))
    return left_s.join(right_s, [key, "_salt"], how).drop("_salt")


def main():
    spark = SparkSession.builder.master("local[2]").appName("skew_join").getOrCreate()

    # 80% of events belong to user 0: classic hot key.
    events = spark.range(500_000).select(
        F.when(F.col("id") % 10 < 8, F.lit(0)).otherwise(F.col("id") % 5000).alias("user_id"),
        F.col("id").alias("event_id"),
    )
    users = spark.range(5000).select(F.col("id").alias("user_id"),
                                     (F.col("id") % 7).alias("segment"))

    # Step 1: AQE (no code change). Broadcast disabled to force a SortMergeJoin for the demo.
    enable_aqe_skew(spark, threshold="1m", factor="2")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    aqe_result = events.join(users, "user_id")
    expected = aqe_result.count()

    # Step 2: fallback salting (non-equi joins, AQE not splitting, RDD-style pipelines).
    hot = hot_keys(events, "user_id", min_rows=50_000)
    salted = salted_join(events, users, "user_id", hot, n=16)
    assert salted.count() == expected, "salting must not change the join result"
    assert salted.columns.count("user_id") == 1
    print("hot keys:", hot, "rows:", expected)
    spark.stop()


if __name__ == "__main__":
    main()
