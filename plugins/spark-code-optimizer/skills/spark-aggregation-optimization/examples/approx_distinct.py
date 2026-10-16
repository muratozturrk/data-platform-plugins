"""approx_distinct: approx_count_distinct instead of countDistinct (corrected sparkopt output).

sparkopt fires when row_count > 5M (sparkopt heuristic).
Correction: Spark's default is rsd=0.05 (5% relative standard deviation). The library's
"~2% error" only holds because its snippet passes rsd=0.02, which needs ~8x larger sketches.

Run: python approx_distinct.py   (needs pyspark>=3.3; the sketch part needs 3.5+)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def distinct_users(df, key, col, rsd=None):
    """Exact when rsd is None, otherwise HyperLogLog++ with the given rsd."""
    if rsd is None:
        agg = F.countDistinct(col)
    else:
        agg = F.approx_count_distinct(col, rsd=rsd)
    return df.groupBy(key).agg(agg.alias("users"))


def main():
    spark = SparkSession.builder.master("local[2]").appName("approx_distinct").getOrCreate()
    events = spark.range(3_000_000).select(
        (F.col("id") % 7).alias("day"),
        (F.col("id") % 400_000).alias("user_id"),
    )

    exact = {r.day: r.users for r in distinct_users(events, "day", "user_id").collect()}
    for rsd in (0.05, 0.02):  # 0.05 is what F.approx_count_distinct(col) uses by default
        approx = {r.day: r.users for r in distinct_users(events, "day", "user_id", rsd).collect()}
        worst = max(abs(approx[d] - exact[d]) / exact[d] for d in exact)
        print(f"rsd={rsd}: worst relative error {worst:.3%}")
        assert worst < 4 * rsd, "estimate far outside the expected error band"

    # Re-aggregatable sketches (Spark 3.5+): daily sketches -> weekly distinct users.
    if hasattr(F, "hll_sketch_agg"):
        daily = events.groupBy("day").agg(F.hll_sketch_agg("user_id").alias("sk"))
        weekly = daily.agg(F.hll_sketch_estimate(F.hll_union_agg("sk")).alias("wau"))
        print("weekly approx users:", weekly.first().wau)
    spark.stop()


if __name__ == "__main__":
    main()
