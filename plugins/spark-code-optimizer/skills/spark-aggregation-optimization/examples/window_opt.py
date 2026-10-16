"""window_opt: partitioned windows, one shared WindowSpec, groupBy instead of windows.

sparkopt fires when row_count > 500K (sparkopt heuristic).
Corrections vs. the library snippet: a running unboundedPreceding frame is cheap (incremental);
WindowExec buffers the whole window *partition* for any frame. The real risks are missing
partitionBy, hot partition keys and whole-partition collect_list. A 7-day rangeBetween changes
the result and belongs only where a rolling window is the requirement.

Run: python window_opt.py   (needs pyspark>=3.3 for F.max_by)
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def add_user_features(df):
    """Several window columns sharing one partitionBy/orderBy: one Exchange + one Sort."""
    w = Window.partitionBy("user_id").orderBy("ts")
    running = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    last_7d = (Window.partitionBy("user_id").orderBy(F.col("ts").cast("long"))
                     .rangeBetween(-7 * 86400, 0))
    return (df.withColumn("rn", F.row_number().over(w))
              .withColumn("prev_value", F.lag("value").over(w))
              .withColumn("running_sum", F.sum("value").over(running))
              .withColumn("avg_7d", F.avg("value").over(last_7d)))


def latest_per_user(df):
    """Latest row per key without a window: partial aggregation, no sort."""
    return df.groupBy("user_id").agg(F.max("ts").alias("ts"),
                                     F.max_by("value", "ts").alias("value"))


def main():
    spark = SparkSession.builder.master("local[2]").appName("window_opt").getOrCreate()
    df = spark.range(600_000).select(
        (F.col("id") % 1000).alias("user_id"),
        F.timestamp_seconds(F.lit(1_700_000_000) + F.col("id") * 60).alias("ts"),
        (F.col("id") % 13).cast("double").alias("value"),
    )

    feats = add_user_features(df)
    feats.explain()
    print("feature rows:", feats.count())

    latest = latest_per_user(df)
    via_window = (df.withColumn("rn", F.row_number().over(
                      Window.partitionBy("user_id").orderBy(F.desc("ts"))))
                    .filter("rn = 1").select("user_id", "ts", "value"))
    assert sorted(latest.collect()) == sorted(via_window.collect())
    spark.stop()


if __name__ == "__main__":
    main()
