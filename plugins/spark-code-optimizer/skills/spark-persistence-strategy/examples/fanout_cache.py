"""fanout_cache: one expensive base DataFrame, several actions, one materialization.

Discipline: select only needed columns, persist, count() to materialize, run every action,
unpersist() in finally after the LAST action (not after defining lazy transformations).

Run: spark-submit fanout_cache.py [output_dir]
"""
import sys
import tempfile

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(out_dir):
    spark = SparkSession.builder.master("local[2]").appName("fanout_cache").getOrCreate()
    raw = spark.range(2_000_000).select(
        "id",
        (F.col("id") % 100).alias("key"),
        (F.rand(seed=1) * 100).alias("revenue"),
        (F.col("id") % 3 != 0).alias("is_valid"),
    )
    dim = spark.range(100).select(
        F.col("id").alias("key"),
        F.element_at(F.array(F.lit("EU"), F.lit("US"), F.lit("APAC")), (F.col("id") % 3 + 1).cast("int"))
        .alias("region"),
        F.when(F.col("id") % 2 == 0, "electronics").otherwise("other").alias("category"),
    )

    base = (raw.join(dim, "key", "left")
            .filter("is_valid")
            .select("id", "region", "category", "revenue")      # cache only what consumers need
            .persist(StorageLevel.MEMORY_AND_DISK))              # original sparkopt: MEMORY_ONLY_SER
    base.count()                                                 # materialize every partition once
    try:
        base.groupBy("region").agg(F.sum("revenue").alias("revenue")) \
            .write.mode("overwrite").parquet(f"{out_dir}/report_a")
        n_electronics = base.filter(F.col("category") == "electronics").count()
        base.select("id", "revenue").write.mode("overwrite").parquet(f"{out_dir}/ml_input")
        print("electronics rows:", n_electronics, "| storage level:", base.storageLevel)
        base.groupBy("category").count().explain()               # shows InMemoryTableScan
    finally:
        base.unpersist()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="fanout_"))
