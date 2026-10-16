"""Metadata Enhancer - Parquet footers and Delta commit-log statistics.

1. Writes Parquet with Spark (stats are created under the hood) and prints the
   per-row-group min/max/null_count from the footer with pyarrow.
2. Writes the same data as Delta and prints the `stats` JSON stored in the
   `add` actions of _delta_log (numRecords, minValues, maxValues, nullCount).
3. Runs a selective query: `PushedFilters` in the plan + file skipping use those stats.

Run:
    pip install pyspark delta-spark pyarrow
    python spark_metadata_enhancer.py /tmp/dp/metadata
"""
import glob
import json
import sys

import pyarrow.parquet as pq
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("metadata-enhancer")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def print_parquet_footer_stats(parquet_dir: str) -> None:
    for path in sorted(glob.glob(f"{parquet_dir}/*.parquet"))[:2]:
        meta = pq.ParquetFile(path).metadata
        print(f"{path}: rows={meta.num_rows} row_groups={meta.num_row_groups}")
        for col_idx in range(meta.num_columns):
            col = meta.row_group(0).column(col_idx)
            s = col.statistics
            print(f"  {col.path_in_schema}: min={s.min} max={s.max} nulls={s.null_count}")


def print_delta_log_stats(delta_dir: str) -> None:
    for log_file in sorted(glob.glob(f"{delta_dir}/_delta_log/*.json")):
        with open(log_file, encoding="utf-8") as f:
            for line in f:
                action = json.loads(line)
                if "add" in action:
                    print(action["add"]["path"], json.loads(action["add"]["stats"]))


def main(base_dir: str) -> None:
    spark = build_spark()
    devices = (spark.range(0, 6_100)
               .withColumn("type", F.element_at(F.array(*[F.lit(t) for t in ("galaxy", "iphone", "mac")]),
                                                (F.col("id") % 3 + 1).cast("int")))
               .withColumn("age", (F.col("id") % 90).cast("int"))
               .withColumn("version", F.when(F.col("id") % 10 == 0, None)
                           .otherwise(F.concat(F.lit("v"), F.col("id").cast("string")))))
    # Sorting by age before writing keeps per-file [min, max] ranges disjoint -> more skipping.
    devices = devices.repartitionByRange(4, "age")

    devices.write.mode("overwrite").parquet(f"{base_dir}/parquet")
    print_parquet_footer_stats(f"{base_dir}/parquet")

    devices.write.mode("overwrite").format("delta").save(f"{base_dir}/delta")
    print_delta_log_stats(f"{base_dir}/delta")

    query = spark.read.format("delta").load(f"{base_dir}/delta").filter("age > 80")
    query.explain()  # PushedFilters: [IsNotNull(age), GreaterThan(age,80)]
    print("rows:", query.count())
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dp/metadata")
