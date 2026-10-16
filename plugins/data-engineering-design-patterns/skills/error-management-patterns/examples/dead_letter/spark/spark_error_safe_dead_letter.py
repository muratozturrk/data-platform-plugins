"""Dead-Letter pattern - Spark batch with error-safe functions.

Error-safe SQL functions (CONCAT, CAST, TRY_* ...) never throw; they return NULL.
The dead-letter "safety control" is therefore an if-else on the output, not a
try/except: *input present but output NULL* => unprocessable record.

Subcommands
  load    Transform one partition (``--partition-date``). Rows are flagged with
          ``is_valid``; the flagged dataset is persisted once and written twice:
          valid rows to the devices table, invalid rows (decorated with failure
          metadata) to the dead-letter table. Both writes use ``replaceWhere`` on the
          partition, so a rerun is idempotent. A metrics JSON is written for the
          monitoring layer and the job fails if the error ratio is too high.
  replay  Optional replay pipeline: re-runs the (fixed) transformation on the
          dead-lettered rows of a partition and MERGEs them into the devices table
          with ``was_dead_lettered = true`` plus replay metadata, so downstream
          consumers can tell replayed rows apart.

Run:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark_error_safe_dead_letter.py \
      load --base-dir /tmp/dp/dead-letter/batch --partition-date 2024-12-31
  python spark_error_safe_dead_letter.py --self-test
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

log = logging.getLogger("error_safe_dead_letter")

JOB_NAME = "devices-loader"
JOB_VERSION = "1.0.0"
KEY_COLUMNS = ["type", "full_name", "version"]

# The subquery applies the error-safe function, the top-level
# query validates it. NULL output with a non-NULL input = processing error.
VALIDATION_QUERY = """
SELECT type, full_name, version, name_with_version, partition_date,
  CASE
    WHEN (full_name IS NOT NULL OR version IS NOT NULL) AND name_with_version IS NULL
      THEN 'CONCAT(full_name, version) returned NULL'
    WHEN version IS NOT NULL AND version_number IS NULL
      THEN 'CAST(version AS DOUBLE) returned NULL'
    ELSE NULL
  END AS failure_reason
FROM (
  SELECT type, full_name, version, partition_date,
         CONCAT(full_name, version) AS name_with_version,
         TRY_CAST(version AS DOUBLE) AS version_number
  FROM devices_to_load
)
"""


def error_ratio(valid: int, invalid: int) -> float:
    total = valid + invalid
    return invalid / total if total else 0.0


def metrics_document(partition_date: str, valid: int, invalid: int, max_ratio: float) -> dict:
    ratio = error_ratio(valid, invalid)
    return {"job": JOB_NAME, "version": JOB_VERSION, "partition_date": partition_date,
            "valid": valid, "dead_lettered": invalid, "error_ratio": round(ratio, 6),
            "max_error_ratio": max_ratio, "status": "FAILED" if ratio > max_ratio else "OK",
            "written_at": dt.datetime.now(dt.timezone.utc).isoformat()}


def write_metrics(base_dir: str, doc: dict) -> None:
    """Local/shared FS for the sample; use an object-store client in production."""
    target = Path(base_dir) / "_metrics" / f"{doc['partition_date']}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2))
    os.replace(tmp, target)  # atomic: the monitor never reads half a file


def get_spark():
    from pyspark.sql import SparkSession

    return (SparkSession.builder.appName(JOB_NAME)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def run_load(spark, args: argparse.Namespace) -> dict:
    from pyspark.sql import functions as F

    partition_filter = f"partition_date = '{args.partition_date}'"
    (spark.read.schema("type STRING, full_name STRING, version STRING")
     .json(f"{args.base_dir}/input/{args.partition_date}")
     .withColumn("partition_date", F.lit(args.partition_date).cast("date"))
     .createOrReplaceTempView("devices_to_load"))

    flagged = spark.sql(VALIDATION_QUERY)
    flagged.persist()  # prevents executing the query twice
    try:
        valid = flagged.filter("failure_reason IS NULL").drop("failure_reason") \
            .withColumn("was_dead_lettered", F.lit(False))
        invalid = (flagged.filter("failure_reason IS NOT NULL")
                   .withColumn("job_name", F.lit(JOB_NAME))
                   .withColumn("job_version", F.lit(JOB_VERSION))
                   .withColumn("dead_lettered_at", F.current_timestamp()))
        valid_count, invalid_count = valid.count(), invalid.count()

        for df, table in ((valid, "devices-table"), (invalid, "devices-dead-letter-table")):
            (df.write.format("delta").mode("overwrite")
             .option("replaceWhere", partition_filter)
             .partitionBy("partition_date")
             .save(f"{args.base_dir}/output/{table}"))
    finally:
        flagged.unpersist()

    doc = metrics_document(args.partition_date, valid_count, invalid_count, args.max_error_ratio)
    write_metrics(args.base_dir, doc)
    log.info("load finished: %s", doc)
    if doc["status"] == "FAILED":
        # The data is written, but the run fails so the orchestrator blocks consumers.
        raise RuntimeError(f"error ratio {doc['error_ratio']:.2%} > {args.max_error_ratio:.2%}")
    return doc


def run_replay(spark, args: argparse.Namespace) -> int:
    """Replay pipeline: fixed transformation + MERGE (rerunnable, no duplicates)."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    dead_letters = (spark.read.format("delta")
                    .load(f"{args.base_dir}/output/devices-dead-letter-table")
                    .filter(F.col("partition_date") == args.partition_date))
    # The "fix": default missing parts instead of letting CONCAT return NULL.
    fixed = (dead_letters
             .withColumn("name_with_version",
                         F.concat(F.coalesce("full_name", F.lit("unknown")),
                                  F.coalesce("version", F.lit("0"))))
             .select(*KEY_COLUMNS, "name_with_version", "partition_date")
             .withColumn("was_dead_lettered", F.lit(True))
             .withColumn("replay_job", F.lit(f"{JOB_NAME}-replay:{JOB_VERSION}"))
             .withColumn("replayed_at", F.current_timestamp()))
    count = fixed.count()
    if not count:
        log.info("nothing to replay for %s", args.partition_date)
        return 0
    # replay_job / replayed_at are new columns for the main table -> schema evolution.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    condition = " AND ".join(f"t.{c} <=> s.{c}" for c in KEY_COLUMNS + ["partition_date"])
    (DeltaTable.forPath(spark, f"{args.base_dir}/output/devices-table").alias("t")
     .merge(fixed.alias("s"), condition)
     .whenNotMatchedInsertAll()
     .execute())
    log.info("replayed %s rows into %s - notify downstream consumers to backfill",
             count, args.partition_date)
    return count


def _self_check() -> None:
    assert error_ratio(0, 0) == 0.0
    assert error_ratio(90, 10) == 0.1
    ok = metrics_document("2024-12-31", 99, 1, 0.05)
    assert ok["status"] == "OK" and ok["dead_lettered"] == 1
    assert metrics_document("2024-12-31", 50, 50, 0.05)["status"] == "FAILED"
    assert "CONCAT(full_name, version)" in VALIDATION_QUERY
    print("self-test OK")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    if argv is None:
        argv = sys.argv[1:]
    if "--self-test" in argv:
        return argparse.Namespace(self_test=True)
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("command", choices=["load", "replay"])
    p.add_argument("--base-dir", default="/tmp/dp/dead-letter/batch")
    p.add_argument("--partition-date", required=True, type=lambda s: dt.date.fromisoformat(s).isoformat())
    p.add_argument("--max-error-ratio", type=float, default=0.05)
    ns = p.parse_args(argv)
    ns.self_test = False
    return ns


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    if args.self_test:
        _self_check()
        return
    spark = get_spark()
    if args.command == "load":
        run_load(spark, args)
    else:
        run_replay(spark, args)


if __name__ == "__main__":
    main()
