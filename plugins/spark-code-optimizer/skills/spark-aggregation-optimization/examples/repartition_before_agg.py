"""repartition_before_agg: drop the pre-agg repartition; salt hot keys in two phases.

sparkopt fires when partition skew_ratio > 3.0 or row_count > 10M (sparkopt heuristics).
Corrections vs. the library snippet: groupBy already hash-partitions by its keys, so a
repartition on the same keys adds a raw-row shuffle and cannot fix key skew (a hot key still
lands in one partition). Hot keys need a two-phase salted aggregation; AQE skew handling only
covers joins.

Run: python repartition_before_agg.py   (needs pyspark>=3.3)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def salted_sum_count(df, key, value, n=32, seed=7):
    """Two-phase aggregation for decomposable aggregates (sum, count -> avg)."""
    phase1 = (df.withColumn("_salt", (F.rand(seed=seed) * n).cast("int"))
                .groupBy(key, "_salt")
                .agg(F.sum(value).alias("s"), F.count("*").alias("c")))
    return (phase1.groupBy(key)
                  .agg(F.sum("s").alias("total"), F.sum("c").alias("cnt"))
                  .withColumn("avg", F.col("total") / F.col("cnt")))


def main():
    spark = SparkSession.builder.master("local[2]").appName("repartition_before_agg").getOrCreate()
    # 90% of rows have country 'US': hot group key.
    df = spark.range(1_000_000).select(
        F.when(F.col("id") % 10 != 0, F.lit("US"))
         .otherwise(F.concat(F.lit("C"), (F.col("id") % 50).cast("string"))).alias("country"),
        (F.col("id") % 100).alias("amount"),
    )

    plain = df.groupBy("country").agg(F.sum("amount").alias("total"),
                                      F.count("*").alias("cnt"))
    salted = salted_sum_count(df, "country", "amount")

    expected = {r.country: (r.total, r.cnt) for r in plain.collect()}
    got = {r.country: (r.total, r.cnt) for r in salted.collect()}
    assert got == expected, "salting must not change the totals"

    # Reuse case: one repartition shared by several aggregations on the same key.
    by_country = df.repartition("country").cache()
    maxes = by_country.groupBy("country").agg(F.max("amount"))
    mins = by_country.groupBy("country").agg(F.min("amount"))
    print("groups:", maxes.count(), mins.count())
    by_country.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
