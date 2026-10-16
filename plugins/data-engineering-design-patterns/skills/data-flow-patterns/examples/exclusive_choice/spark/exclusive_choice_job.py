"""Exclusive Choice -- PySpark (data processing layer).

Two flavors of choosing exactly one path inside a job:
  1. External parameter: `--output_type delta|csv` + a small Factory hiding the per-type
     SparkSession and writer (no if/else repeated in the client code).
  2. Dataset characteristics: route by the input SCHEMA (metadata, cheap) instead of the data
     (`--route-by-schema`): >= 3 fields -> new location, else legacy location.

Prefer pushing the choice to the orchestrator (different entry points) when branches write to
different stores -- in-job branching hides possible outcomes from the pipeline graph.

Run:
    spark-submit exclusive_choice_job.py --input /tmp/dp/devices.json --output_dir /tmp/dp/out \
        --output_type csv [--route-by-schema]
Requires: pyspark>=3.3 (+ delta-spark for --output_type delta).
"""
import argparse
from enum import Enum

from pyspark.sql import DataFrame, SparkSession


class OutputType(str, Enum):
    delta_lake = "delta"
    csv = "csv"


class DeltaWriter:
    def spark_session(self) -> SparkSession:
        return (SparkSession.builder.appName("exclusive-choice-delta")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())

    def write(self, df: DataFrame, location: str) -> None:
        df.write.mode("overwrite").format("delta").save(location)


class CsvWriter:
    def spark_session(self) -> SparkSession:
        return SparkSession.builder.appName("exclusive-choice-csv").getOrCreate()

    def write(self, df: DataFrame, location: str) -> None:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(location)


def output_generation_factory(output_type: OutputType):
    # Dedicated classes instead of if/else in every method (cleaner than repeated branching).
    return {OutputType.delta_lake: DeltaWriter, OutputType.csv: CsvWriter}[output_type]()


def route_by_schema(df: DataFrame, output_dir: str) -> str:
    # Metadata-based condition: no data scan needed.
    if len(df.schema.fields) >= 3:
        return f"{output_dir}/devices_schema_changed"
    return f"{output_dir}/devices_legacy"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--output_type", required=True, type=OutputType)
    parser.add_argument("--route-by-schema", action="store_true")
    args = parser.parse_args()

    writer = output_generation_factory(args.output_type)
    spark = writer.spark_session()
    raw_data = spark.read.json(args.input)

    location = (route_by_schema(raw_data, args.output_dir) if args.route_by_schema
                else f"{args.output_dir}/devices")
    writer.write(raw_data, location)
    print(f"wrote {args.output_type.value} to {location}")
    spark.stop()


if __name__ == "__main__":
    main()
