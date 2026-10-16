"""adaptive_qe: AQE settings for Spark 3.x, including the parallelismFirst fix.

sparkopt sets enabled / coalescePartitions.enabled / skewJoin.enabled = true and an advisory size of
128m. On 3.2+ the three flags are already defaults, and coalescePartitions.parallelismFirst=true
(default) makes coalescing IGNORE the advisory size. Set it to false for the target to apply.

Run: python adaptive_qe.py   (PySpark 3.3+)
"""
from pyspark.sql import SparkSession, functions as F

AQE_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    "spark.sql.adaptive.skewJoin.enabled": "true",
}


def enable_aqe(spark: SparkSession, initial_partitions: int = 2000) -> None:
    """All runtime SQL confs, so spark.conf.set works (no restart needed)."""
    for key, value in AQE_CONF.items():
        spark.conf.set(key, value)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", str(initial_partitions))


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[2]").appName("adaptive_qe").getOrCreate()
    enable_aqe(spark, initial_partitions=400)
    df = spark.range(200_000).groupBy((F.col("id") % 50).alias("k")).count()

    print(df._jdf.queryExecution().executedPlan().toString().splitlines()[0])  # isFinalPlan=false
    df.collect()  # run it so AQE can re-plan from shuffle stats
    final = df._jdf.queryExecution().executedPlan().toString()
    print(final)
    assert "isFinalPlan=true" in final
    assert df.rdd.getNumPartitions() < 400  # coalesced from the 400 initial partitions
    spark.stop()
