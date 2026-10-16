"""Manifest - PySpark: Delta symlink manifest + a hand-written manifest for readers.

1. Delta: the _delta_log already IS a manifest (readers never list data files).
   `generate('symlink_format_manifest')` exports it for engines that don't speak
   Delta (BigQuery external tables, Athena/Presto).
2. Plain Parquet: the producer records the files it just wrote into
   `_manifest.json` (one listing, done once by the writer). Readers load exactly
   those paths - no object-store listing on each read.

Run:
    pip install pyspark delta-spark
    python spark_manifest.py /tmp/dp/manifest
"""
import glob
import json
import os
import sys

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[*]").appName("manifest")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_with_manifest(df, output_dir: str, run_id: str) -> str:
    run_dir = f"{output_dir}/run={run_id}"
    df.write.mode("overwrite").parquet(run_dir)
    # ponytail: local glob as the single listing; on S3/GCS use the committer output or one list call.
    files = sorted(os.path.abspath(p) for p in glob.glob(f"{run_dir}/*.parquet"))
    manifest_path = f"{output_dir}/_manifests/{run_id}.json"
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "entries": [{"url": p, "mandatory": True} for p in files]}, f)
    return manifest_path


def read_from_manifest(spark, manifest_path: str):
    with open(manifest_path, encoding="utf-8") as f:
        paths = [e["url"] for e in json.load(f)["entries"]]
    return spark.read.parquet(*paths)


def main(base: str) -> None:
    spark = build_spark()
    devices = (spark.range(0, 1000).withColumn("type", F.lit("galaxy"))
               .withColumn("version", F.concat(F.lit("v"), F.col("id").cast("string"))))

    devices.write.mode("overwrite").format("delta").save(f"{base}/devices_delta")
    DeltaTable.forPath(spark, f"{base}/devices_delta").generate("symlink_format_manifest")
    print(open(glob.glob(f"{base}/devices_delta/_symlink_format_manifest/manifest")[0]).read())

    manifest = write_with_manifest(devices.repartition(4), f"{base}/devices_parquet", "20250601_1031")
    print("rows via manifest:", read_from_manifest(spark, manifest).count())
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/dp/manifest")
