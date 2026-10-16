"""Normalizer - PySpark + Delta Lake: from a flat visits feed to a snowflake schema.

Builds:
  dim_page_category <- dim_page   (page described by a sub-dimension)
  dim_date_month / dim_date_quarter <- dim_date
  fact_visit (visit_id, dim_page_id, dim_date_id, event_time)
Each fact of a page or date is stored ONCE, so updating a category name is a
single-row change. The price: reading the full visit needs a chain of joins
(shown at the end, small dimensions broadcast to avoid shuffles).

Run:
    pip install pyspark delta-spark
    python spark_normalizer.py /tmp/dp/snowflake
"""
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("normalizer")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main(base: str) -> None:
    spark = build_spark()
    flat = spark.createDataFrame([
        ("v1", "2024-07-01 09:00:00", "home.html", "landing"),
        ("v2", "2024-07-01 10:00:00", "post-1.html", "blog"),
        ("v3", "2024-08-15 11:00:00", "post-1.html", "blog"),
    ], "visit_id STRING, event_time STRING, page STRING, category STRING") \
        .withColumn("event_time", F.to_timestamp("event_time"))

    def save(df, name):
        df.write.mode("overwrite").format("delta").save(f"{base}/{name}")

    # Sub-dimensions first (surrogate keys via HASH for determinism across reruns).
    save(flat.selectExpr("HASH(category) AS page_category_id", "category").distinct(), "dim_page_category")
    save(flat.selectExpr("HASH(page) AS page_id", "page", "HASH(category) AS dim_page_category_id")
         .distinct(), "dim_page")
    dates = flat.select(F.to_date("event_time").alias("d")).distinct()
    save(dates.selectExpr("month(d) AS month_id", "date_format(d, 'MMMM') AS month_name").distinct(),
         "dim_date_month")
    save(dates.selectExpr("quarter(d) AS quarter_id", "concat('Q', quarter(d)) AS quarter_name").distinct(),
         "dim_date_quarter")
    save(dates.selectExpr("HASH(d) AS date_id", "d AS date", "month(d) AS dim_month_id",
                          "quarter(d) AS dim_quarter_id"), "dim_date")
    save(flat.selectExpr("visit_id", "HASH(page) AS dim_page_id", "HASH(TO_DATE(event_time)) AS dim_date_id",
                         "event_time"), "fact_visit")

    # Reading the full picture = join chain (the Normalizer's query cost).
    t = {n: spark.read.format("delta").load(f"{base}/{n}") for n in
         ("fact_visit", "dim_page", "dim_page_category", "dim_date", "dim_date_month", "dim_date_quarter")}
    page_w_category = t["dim_page"].join(
        F.broadcast(t["dim_page_category"]),
        t["dim_page"].dim_page_category_id == t["dim_page_category"].page_category_id, "left_outer")
    date_w_month_quarter = (t["dim_date"]
                            .join(F.broadcast(t["dim_date_month"]),
                                  t["dim_date"].dim_month_id == t["dim_date_month"].month_id, "left_outer")
                            .join(F.broadcast(t["dim_date_quarter"]),
                                  t["dim_date"].dim_quarter_id == t["dim_date_quarter"].quarter_id, "left_outer"))
    full_visit = (t["fact_visit"]
                  .join(page_w_category, t["fact_visit"].dim_page_id == page_w_category.page_id, "left_outer")
                  .join(date_w_month_quarter, t["fact_visit"].dim_date_id == date_w_month_quarter.date_id,
                        "left_outer"))
    full_visit.select("visit_id", "page", "category", "date", "month_name", "quarter_name").show()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dp/snowflake")
