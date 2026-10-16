"""Sorter - PySpark: lexicographic sort on write vs Delta Lake Z-order.

1. Writes a Delta table sorted by event_time inside each file
   (sortWithinPartitions) so per-file min/max stats become narrow ranges and
   time predicates skip most files.
2. Compacts the table with Z-order on (visit_id, page): a curved layout that
   helps queries filtering on EITHER column, unlike a composite lexicographic
   key that only helps when the leading column is filtered. Z-order rewrites the
   files, so the event_time order from step 1 is gone afterwards.

Run:
    pip install pyspark delta-spark
    python spark_sorter.py /tmp/dp/visits_sorted            # write + zorder
    python spark_sorter.py /tmp/dp/visits_sorted --optimize-only   # scheduled re-sort
"""
import sys

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("sorter")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_sorted(spark: SparkSession, output_dir: str) -> None:
    visits = (spark.range(0, 100_000)
              .withColumn("visit_id", F.sha1(F.col("id").cast("string")))
              .withColumn("event_time", F.expr("timestamp'2024-07-01' + make_interval(0,0,0,0,0,0,id)"))
              .withColumn("page", F.concat(F.lit("page_"), (F.col("id") % 100).cast("string"))))
    (visits.repartition(10, "page")           # 10 files...
     .sortWithinPartitions("event_time")      # ...each internally sorted
     .write.mode("overwrite").format("delta").save(output_dir))


def zorder(spark: SparkSession, output_dir: str) -> None:
    # Re-sorting outside the writer handles the "unsorted segments" left by appends.
    DeltaTable.forPath(spark, output_dir).optimize().executeZOrderBy(["visit_id", "page"])


def main(output_dir: str, optimize_only: bool) -> None:
    spark = build_spark()
    table = lambda: spark.read.format("delta").load(output_dir)  # noqa: E731
    if not optimize_only:
        write_sorted(spark, output_dir)
        # Lexicographic layout: event_time ranges per file are narrow -> file skipping.
        table().filter("event_time BETWEEN '2024-07-01 01:00:00' AND '2024-07-01 02:00:00'").show(3)
    # Z-order REWRITES the files: the event_time order above is replaced by the curve on
    # (visit_id, page). Pick one layout per table, driven by the dominant predicates.
    zorder(spark, output_dir)
    table().filter("page = 'page_7'").count()  # either Z-ordered column alone skips files
    table().filter(F.col("visit_id") == F.sha1(F.lit("7"))).show(1, truncate=False)
    spark.stop()


if __name__ == "__main__":
    args = sys.argv[1:]
    main(next((a for a in args if not a.startswith("--")), "/tmp/dp/visits_sorted"),
         "--optimize-only" in args)
