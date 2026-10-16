"""Parallel Split -- PySpark (data processing layer).

Mode `split` (default): read the input ONCE, `persist()` it, then write two independent
branches (raw Delta table + enriched Delta table). Without persist each write would re-read
and re-parse the source. Each branch write uses Delta's idempotent-write options
(`txnAppId` + `txnVersion`): replaying the job with the same batch id is a no-op instead of
appending duplicates.

Mode `single` (used by the Airflow DAG): writes only one output in the given --format,
so the orchestration-layer split can give each branch its own hardware.

Run:
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 parallel_split_job.py \
        --input /tmp/dp/devices.json --output /tmp/dp/devices --batch-id 20240101
Requires: pyspark>=3.3, delta-spark.
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

APP_ID = "devices-loader-v1"  # must stay stable across retries, change it only with the job version


def build_session() -> SparkSession:
    return (SparkSession.builder.appName("parallel-split")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def idempotent_append(df: DataFrame, path: str, batch_id: int) -> None:
    (df.write.mode("append").format("delta")
     .option("txnVersion", batch_id).option("txnAppId", APP_ID)
     .save(path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--format", choices=["delta", "csv"])
    args = parser.parse_args()

    spark = build_session()
    input_dataset = (spark.read.format("json")
                     .schema("type STRING, full_name STRING, version STRING")
                     .load(args.input))

    if args.format == "csv":  # single-branch mode for the legacy consumers
        input_dataset.coalesce(1).write.mode("overwrite").option("header", True) \
            .csv(f"{args.output}/batch={args.batch_id}")
    elif args.format == "delta":
        idempotent_append(input_dataset, args.output, args.batch_id)
    else:  # split in the processing layer: common read computed once
        input_dataset.persist(StorageLevel.MEMORY_AND_DISK)
        idempotent_append(input_dataset, f"{args.output}/devices", args.batch_id)
        enriched = (input_dataset.withColumn("loading_time", F.current_timestamp())
                    .withColumn("full_name", F.concat_ws(" ", "full_name", "version")))
        idempotent_append(enriched, f"{args.output}/devices_enriched", args.batch_id)
        input_dataset.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
