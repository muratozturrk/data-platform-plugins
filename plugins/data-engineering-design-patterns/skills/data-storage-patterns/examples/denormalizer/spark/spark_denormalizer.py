"""Denormalizer - PySpark + Delta Lake: One Big Table and star schema.

Starts from a small snowflake model (built inline) and writes:
  * one_big_table     - every dimension attribute flattened next to the fact;
                        costly write once, join-free reads.
  * nested variant    - same data with dimensions kept as STRUCT columns
                        (access as page.category, date.quarter_name).
  * star schema       - sub-dimensions folded into one level of dimensions;
                        reads need only fact -> dim joins.

Run:
    pip install pyspark delta-spark
    python spark_denormalizer.py /tmp/dp/denormalized
"""
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("denormalizer")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main(base: str) -> None:
    spark = build_spark()
    fact_visit = spark.createDataFrame(
        [("v1", 1, 20240701, "09:00:00"), ("v2", 2, 20240701, "10:00:00"), ("v3", 2, 20240815, "11:00:00")],
        "visit_id STRING, dim_page_id INT, dim_date_id INT, event_time STRING")
    dim_page = spark.createDataFrame([(1, "home.html", 10), (2, "post-1.html", 20)],
                                     "page_id INT, page STRING, dim_page_category_id INT")
    dim_page_category = spark.createDataFrame([(10, "landing"), (20, "blog")],
                                              "page_category_id INT, category STRING")
    dim_date = spark.createDataFrame([(20240701, "2024-07-01", 7, 3), (20240815, "2024-08-15", 8, 3)],
                                     "date_id INT, date STRING, dim_month_id INT, dim_quarter_id INT")
    dim_date_month = spark.createDataFrame([(7, "July"), (8, "August")], "month_id INT, month_name STRING")
    dim_date_quarter = spark.createDataFrame([(3, "Q3")], "quarter_id INT, quarter_name STRING")

    # Fold sub-dimensions into their parent (this alone is the star schema's dimension level).
    page_w_category = (dim_page.join(dim_page_category,
                                     dim_page.dim_page_category_id == dim_page_category.page_category_id,
                                     "left_outer").drop("dim_page_category_id", "page_category_id"))
    date_w_month_quarter = (dim_date
                            .join(dim_date_month, dim_date.dim_month_id == dim_date_month.month_id, "left_outer")
                            .join(dim_date_quarter, dim_date.dim_quarter_id == dim_date_quarter.quarter_id,
                                  "left_outer")
                            .drop("dim_month_id", "dim_quarter_id", "month_id", "quarter_id"))

    # 1) One Big Table: flattened top-level columns.
    obt = (fact_visit
           .join(page_w_category, fact_visit.dim_page_id == page_w_category.page_id, "left_outer")
           .join(date_w_month_quarter, fact_visit.dim_date_id == date_w_month_quarter.date_id, "left_outer")
           .drop("dim_page_id", "dim_date_id", "page_id", "date_id"))
    obt.write.mode("overwrite").format("delta").save(f"{base}/one_big_table")

    # 2) Nested variant: dimensions as STRUCTs.
    nested = obt.select("visit_id", "event_time",
                        F.struct("page", "category").alias("page"),
                        F.struct("date", "month_name", "quarter_name").alias("date"))
    nested.write.mode("overwrite").format("delta").save(f"{base}/one_big_table_nested")

    # 3) Star schema: fact + one level of dimensions.
    fact_visit.write.mode("overwrite").format("delta").save(f"{base}/star/fact_visit")
    page_w_category.write.mode("overwrite").format("delta").save(f"{base}/star/dim_page")
    date_w_month_quarter.write.mode("overwrite").format("delta").save(f"{base}/star/dim_date")

    # Reads: OBT is join-free; star needs fact -> dim joins only.
    spark.read.format("delta").load(f"{base}/one_big_table").groupBy("category").count().show()
    spark.read.format("delta").load(f"{base}/one_big_table_nested").select("visit_id", "page.category").show()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dp/denormalized")
