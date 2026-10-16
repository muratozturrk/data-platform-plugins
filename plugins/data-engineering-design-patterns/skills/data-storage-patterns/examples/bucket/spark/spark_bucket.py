"""Bucket - PySpark bucketed tables, bucket pruning and shuffle-free joins.

Two tables bucketed identically on the high-cardinality `user_id`
(hash(user_id) % 8). Then:
  * a point filter on user_id reads only one bucket (bucket pruning),
  * a join on user_id has no Exchange (shuffle) in the physical plan.
bucketBy only works with saveAsTable (metastore-backed tables).

Run:
    pip install pyspark
    python spark_bucket.py [--buckets 8]   (bucket_dag.py passes --buckets 50 --date <ds>)
"""
import argparse

from pyspark.sql import SparkSession, functions as F


def main(buckets: int) -> None:
    spark = (SparkSession.builder.master("local[*]").appName("bucket")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # force sort-merge join for the demo
             .config("spark.sql.sources.bucketing.enabled", "true")
             .getOrCreate())

    visits = (spark.range(0, 200_000)
              .withColumn("user_id", (F.col("id") % 50_000).cast("string"))
              .withColumn("page", F.concat(F.lit("page_"), (F.col("id") % 20).cast("string"))))
    users = (spark.range(0, 50_000)
             .withColumn("user_id", F.col("id").cast("string"))
             .withColumn("country", F.element_at(F.array(F.lit("FR"), F.lit("PL"), F.lit("US")),
                                                 (F.col("id") % 3 + 1).cast("int"))))

    # Same bucket column + same bucket count on both sides => co-located buckets.
    (visits.write.mode("overwrite").bucketBy(buckets, "user_id").sortBy("user_id")
     .saveAsTable("visits_bucketed"))
    (users.write.mode("overwrite").bucketBy(buckets, "user_id").sortBy("user_id")
     .saveAsTable("users_bucketed"))

    # Bucket pruning: look for "SelectedBucketsCount: 1 out of <buckets>" in the scan node.
    spark.table("visits_bucketed").filter("user_id = '42'").explain()

    # Shuffle elimination: no Exchange hashpartitioning(user_id) above the scans.
    joined = spark.table("visits_bucketed").join(spark.table("users_bucketed"), "user_id")
    joined.explain()
    plan = joined._jdf.queryExecution().executedPlan().toString()
    print("shuffle present:", "Exchange hashpartitioning" in plan)
    joined.groupBy("country").count().show()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--buckets", type=int, default=8)  # must match the Athena DDL / both join sides
    parser.add_argument("--date")  # run date from Airflow; the demo data ignores it
    main(parser.parse_args().buckets)
