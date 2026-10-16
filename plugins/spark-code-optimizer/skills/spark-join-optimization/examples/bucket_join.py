"""bucket_join: bucket BOTH sides on the join key, read back as tables (corrected sparkopt output).

sparkopt fires when row_count > 10M and the table is not small (> 200 MB), both sparkopt heuristics.
Corrections vs. the library snippet: bucket both tables with the same count and key, join the
bucketed *tables* (spark.table), drop the no-op bucketing.enabled conf (default true).
Bucketing needs saveAsTable (catalog tables); Delta Lake does not support bucketBy.

Run: python bucket_join.py   (needs pyspark>=3.3; writes to ./spark-warehouse;
on local Windows, table writes need HADOOP_HOME with winutils.exe)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NUM_BUCKETS = 8
KEY = "order_id"


def write_bucketed(df, table, key=KEY, n=NUM_BUCKETS):
    """One shuffle at write time; repartition(n, key) gives one file per bucket."""
    (df.repartition(n, key)
       .write.mode("overwrite")
       .bucketBy(n, key).sortBy(key)
       .format("parquet")
       .saveAsTable(table))


def main():
    spark = (SparkSession.builder.master("local[2]").appName("bucket_join")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .getOrCreate())

    orders = spark.range(200_000).select(F.col("id").alias(KEY),
                                         (F.col("id") % 97).alias("store"))
    items = spark.range(600_000).select((F.col("id") % 200_000).alias(KEY),
                                        (F.rand(seed=3) * 50).alias("price"))

    write_bucketed(orders, "orders_b")
    write_bucketed(items, "order_items_b")

    joined = spark.table("orders_b").join(spark.table("order_items_b"), KEY)
    plan = joined._jdf.queryExecution().executedPlan().toString()
    assert "Exchange" not in plan, "bucketed join should not shuffle:\n" + plan
    joined.explain("formatted")
    print("rows:", joined.count())

    spark.sql("DROP TABLE IF EXISTS orders_b")
    spark.sql("DROP TABLE IF EXISTS order_items_b")
    spark.stop()


if __name__ == "__main__":
    main()
