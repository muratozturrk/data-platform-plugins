"""partial_agg: keep Spark's map-side partial aggregation (corrected sparkopt output).

sparkopt fires when row_count > 1M (sparkopt heuristic).
Original sparkopt's snippet put repartition("group_col") before groupBy (it shuffles raw rows and
pushes partial_sum after the shuffle) and set the fake spark.sql.aggregate.fastHashMap.enabled and
the default useObjectHashAggregateExec; the bundled copy fixes all three. This demo shows why:
the "bad" plan uses the original repartition-first style.

Run: python partial_agg.py   (needs pyspark>=3.3)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def aggregate(df, key, value):
    """Built-in aggregates only: Spark plans partial_* before the Exchange."""
    return df.groupBy(key).agg(F.sum(value).alias("total"), F.count("*").alias("cnt"))


def partial_before_shuffle(df):
    """True if a partial_ aggregate runs below (before) the first Exchange."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    # Tree string lists the root first; text after the first Exchange is the map side.
    _, _, map_side = plan.partition("Exchange")
    return "partial_" in map_side


def main():
    spark = (SparkSession.builder.master("local[2]").appName("partial_agg")
             .config("spark.sql.adaptive.enabled", "false")  # static plan for the check
             .getOrCreate())
    df = spark.range(2_000_000).select((F.col("id") % 100).alias("store_id"),
                                       (F.col("id") % 17).alias("amount"))

    good = aggregate(df, "store_id", "amount")
    bad = aggregate(df.repartition("store_id"), "store_id", "amount")  # original sparkopt style

    assert partial_before_shuffle(good), "plain groupBy should combine map-side"
    assert not partial_before_shuffle(bad), "repartition first shuffles raw rows"
    assert sorted(good.collect()) == sorted(bad.collect())
    good.explain()
    bad.explain()

    # RDD equivalent: reduceByKey combines map-side; groupByKey does not.
    totals = df.rdd.map(lambda r: (r.store_id, r.amount)).reduceByKey(lambda a, b: a + b)
    print("stores:", totals.count())
    spark.stop()


if __name__ == "__main__":
    main()
