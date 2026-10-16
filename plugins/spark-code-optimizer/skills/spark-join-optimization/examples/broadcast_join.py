"""broadcast_join: explicit broadcast of a small dimension table (corrected sparkopt output).

sparkopt fires when the small side is <= 200 MB (a sparkopt heuristic for explicit hints).
Spark's own auto threshold, spark.sql.autoBroadcastJoinThreshold, stays at its 10MB default:
hint the one join instead of raising the global threshold.

Run: python broadcast_join.py   (needs pyspark>=3.3)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def broadcast_join(large_df, small_df, keys, how="inner"):
    """Join large_df with small_df broadcast to every executor (no shuffle of large_df)."""
    return large_df.join(F.broadcast(small_df), keys, how)


def main():
    spark = (SparkSession.builder.master("local[2]").appName("broadcast_join")
             # Disable auto broadcast so the example shows the hint doing the work.
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .getOrCreate())

    orders = spark.range(1_000_000).select(
        F.col("id").alias("order_id"),
        (F.col("id") % 1000).alias("customer_id"),
        (F.rand(seed=1) * 100).alias("amount"),
    )
    customers = spark.range(1000).select(
        F.col("id").alias("customer_id"),
        F.concat(F.lit("cust_"), F.col("id").cast("string")).alias("name"),
    )

    # Prune the build side before broadcasting: only the columns you need.
    result = broadcast_join(orders, customers.select("customer_id", "name"), "customer_id")

    plan = result._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan, plan
    result.explain("formatted")
    print("rows:", result.count())

    # Hint form, same effect:
    orders.join(customers.hint("broadcast"), "customer_id").explain()
    spark.stop()


if __name__ == "__main__":
    main()
