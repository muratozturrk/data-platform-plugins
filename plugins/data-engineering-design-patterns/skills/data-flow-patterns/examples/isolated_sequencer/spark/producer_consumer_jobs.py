"""Isolated Sequencer -- data-based contract between two independent Spark jobs.

`produce` (team A) writes a partition and THEN a readiness marker with a small manifest
(row count, schema version). `consume` (team B) refuses to start until the marker exists
and validates the manifest -- the only coupling between the two jobs is this contract,
so either side can be renamed, rescheduled or rewritten independently.

Run:
    spark-submit producer_consumer_jobs.py produce --base /tmp/dp/devices --date 2024-01-01
    spark-submit producer_consumer_jobs.py consume --base /tmp/dp/devices --date 2024-01-01
Local file system paths are used for the marker; swap for your object store client.
Requires: pyspark>=3.3.
"""
import argparse
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SCHEMA_VERSION = 1
MARKER = "_READY.json"


def produce(spark: SparkSession, base: str, date: str) -> None:
    out = f"{base}/date={date}"
    devices = spark.range(100).select(
        F.concat(F.lit("dev-"), F.col("id")).alias("device_id"),
        F.when(F.col("id") % 2 == 0, "android").otherwise("ios").alias("type"),
    )
    devices.write.mode("overwrite").parquet(out)
    # Marker is the commit point of the contract -> written after the data.
    manifest = {"rows": spark.read.parquet(out).count(), "schema_version": SCHEMA_VERSION}
    with open(os.path.join(out, MARKER), "w") as f:
        json.dump(manifest, f)


def consume(spark: SparkSession, base: str, date: str) -> None:
    src = f"{base}/date={date}"
    marker = os.path.join(src, MARKER)
    if not os.path.exists(marker):
        # Let the orchestrator retry/sensor; never process a partition without its marker.
        sys.exit(f"Readiness marker {marker} missing -- provider has not delivered yet")
    with open(marker) as f:
        manifest = json.load(f)
    if manifest["schema_version"] != SCHEMA_VERSION:
        sys.exit(f"Contract broken: schema_version={manifest['schema_version']}")

    devices = spark.read.parquet(src)
    if devices.count() != manifest["rows"]:
        sys.exit("Row count mismatch between dataset and manifest")
    devices.groupBy("type").count().show()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["produce", "consume"])
    parser.add_argument("--base", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"isolated-sequencer-{args.role}").getOrCreate()
    (produce if args.role == "produce" else consume)(spark, args.base, args.date)
    spark.stop()


if __name__ == "__main__":
    main()
