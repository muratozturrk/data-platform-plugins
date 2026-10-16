"""Dataset Materializer - PySpark + Delta Lake: full and incremental materialization.

* full_refresh  - recompute an expensive UNION + aggregation once and store it
                  as a table (readers no longer re-run the query like a view).
* incremental   - Incremental Loader + Merger: only rows with insertion_time >
                  last processed watermark are aggregated and MERGEd into the
                  counter table. Works for insert-only sources.
The watermark is stored next to the table (a small Delta table) so reruns are
consistent.

Run:
    pip install pyspark delta-spark
    python spark_dataset_materializer.py /tmp/dp/materializer
"""
import sys

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("dataset-materializer")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def full_refresh(spark, weekly_tables: list[str], target: str) -> None:
    union = None
    for path in weekly_tables:
        df = spark.read.format("delta").load(path)
        union = df if union is None else union.unionByName(df)
    (union.groupBy("user_id").agg(F.count("*").alias("visits"))
     .write.mode("overwrite").format("delta").save(target))


def incremental(spark, source: str, target: str, watermark_path: str) -> None:
    last = (spark.read.format("delta").load(watermark_path).first()[0]
            if DeltaTable.isDeltaTable(spark, watermark_path) else "1970-01-01 00:00:00")
    new_rows = spark.read.format("delta").load(source).filter(F.col("insertion_time") > F.lit(last))
    new_max = new_rows.agg(F.max("insertion_time")).first()[0]
    if new_max is None:
        print("nothing new since", last)
        return
    increment = new_rows.groupBy("user_id").agg(F.count("*").alias("visits"))
    (DeltaTable.forPath(spark, target).alias("target")
     .merge(increment.alias("input"), "target.user_id = input.user_id")
     .whenMatchedUpdate(set={"visits": "target.visits + input.visits"})
     .whenNotMatchedInsertAll()
     .execute())
    # ponytail: watermark written after the MERGE; a crash in between double-counts. Use one
    # transaction (or track the source table version) if exactly-once matters.
    spark.createDataFrame([(str(new_max),)], "last STRING").write.mode("overwrite") \
        .format("delta").save(watermark_path)


def main(base: str) -> None:
    spark = build_spark()
    weeks = []
    for week in (1, 2, 3):
        path = f"{base}/visits_week_{week}"
        (spark.range(0, 1000).withColumn("user_id", (F.col("id") % 10).cast("string"))
         .withColumn("insertion_time", F.lit(f"2024-11-0{week} 00:00:00").cast("timestamp"))
         .write.mode("overwrite").format("delta").save(path))
        weeks.append(path)
    full_refresh(spark, weeks, f"{base}/visits_counter")

    (spark.range(0, 50).withColumn("user_id", (F.col("id") % 10).cast("string"))
     .withColumn("insertion_time", F.lit("2024-11-09 03:27:33").cast("timestamp"))
     .write.mode("append").format("delta").save(weeks[-1]))
    spark.createDataFrame([("2024-11-08 00:00:00",)], "last STRING").write.mode("overwrite") \
        .format("delta").save(f"{base}/_watermark")
    incremental(spark, weeks[-1], f"{base}/visits_counter", f"{base}/_watermark")
    spark.read.format("delta").load(f"{base}/visits_counter").orderBy("user_id").show()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dp/materializer")
