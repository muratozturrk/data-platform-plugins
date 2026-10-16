"""Horizontal Partitioner - PySpark + Delta Lake.

Writes a visits dataset partitioned by event-time (year/month/day/hour) so that
incremental readers (e.g. a 4-day rolling aggregate) prune whole directories
instead of filtering every row.

Run:
    pip install pyspark delta-spark
    python spark_horizontal_partitioner.py /tmp/dp/visits_partitioned
"""
import sys
from datetime import datetime, timedelta

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("horizontal-partitioner")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main(output_dir: str) -> None:
    spark = build_spark()
    start = datetime(2024, 12, 25)
    rows = [(f"v{i}", f"u{i % 50}", start + timedelta(hours=i), f"page_{i % 7}")
            for i in range(24 * 7)]
    visits = spark.createDataFrame(rows, "visit_id STRING, user_id STRING, "
                                         "event_time TIMESTAMP, page STRING")

    # Partition columns derived from the dataset (event time), not the job run date.
    # Low cardinality: one partition per hour, many records per partition.
    partitioned = (visits
                   .withColumn("year", F.year("event_time"))
                   .withColumn("month", F.month("event_time"))
                   .withColumn("day", F.dayofmonth("event_time"))
                   .withColumn("hour", F.hour("event_time")))

    (partitioned.write.mode("overwrite").format("delta")
     .partitionBy("year", "month", "day", "hour").save(output_dir))

    # Reader: the predicate on partition columns triggers partition pruning.
    last_4_days = (spark.read.format("delta").load(output_dir)
                   .filter("year = 2024 AND month = 12 AND day BETWEEN 28 AND 31"))
    last_4_days.groupBy("page").count().show()
    last_4_days.explain()  # look for PartitionFilters in the scan node

    # Partition-level replace (pairs with Fast Metadata Cleaner / idempotent backfills).
    one_day = partitioned.filter("day = 31")
    (one_day.write.format("delta").mode("overwrite")
     .option("replaceWhere", "year = 2024 AND month = 12 AND day = 31")
     .save(output_dir))
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dp/visits_partitioned")
