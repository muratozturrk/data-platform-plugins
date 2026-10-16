"""In-Place Overwriter (data removal) - PySpark.

Two flavours:
  * `json`  : raw files with no native DELETE. Read as text (keep lines byte-identical),
              extract only the filter attribute, write the survivors to a STAGING dir,
              then promote staging -> output. Never overwrite the input in place: a
              failed/retried distributed write would lose data.
  * `delta` : table format with native DELETE; follow it with VACUUM so time travel
              can't resurrect the removed rows.

Run:
  export INPUT_DIR=/tmp/dp/visits_json STAGING_DIR=/tmp/dp/visits_staging DELTA_TABLE=/tmp/dp/visits_delta
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 in_place_overwriter_job.py json|delta <user_id> [<user_id>...]
"""
import os
import shutil
import sys

from pyspark.sql import SparkSession, functions as F

INPUT_DIR = os.environ.get("INPUT_DIR", "/tmp/dp/visits_json")
STAGING_DIR = os.environ.get("STAGING_DIR", "/tmp/dp/visits_staging")
DELTA_TABLE = os.environ.get("DELTA_TABLE", "/tmp/dp/visits_delta")


def build_session() -> SparkSession:
    from delta import configure_spark_with_delta_pip
    return configure_spark_with_delta_pip(
        SparkSession.builder.appName("in-place-overwriter")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ).getOrCreate()


def remove_from_json(spark: SparkSession, user_ids: list) -> None:
    raw = spark.read.text(INPUT_DIR)
    with_user = raw.withColumn("user", F.from_json("value", "user_id STRING"))
    survivors = with_user.filter(~F.col("user.user_id").isin(user_ids)).select("value")
    survivors.write.mode("overwrite").format("text").save(STAGING_DIR)

    # Sanity check before promotion: nothing of the removed users left, not empty by accident.
    staged = spark.read.text(STAGING_DIR).withColumn("user", F.from_json("value", "user_id STRING"))
    leaked = staged.filter(F.col("user.user_id").isin(user_ids)).count()
    if leaked:
        raise RuntimeError(f"{leaked} rows of removed users still in staging")
    if os.environ.get("PROMOTE", "true") == "true":  # orchestrator may promote instead (see airflow DAG)
        promote(STAGING_DIR, INPUT_DIR)


def promote(staging: str, output: str) -> None:
    """Local-FS promotion. On S3 use `aws s3 rm output --recursive && aws s3 mv staging output --recursive`
    (not atomic on object stores - keep staging until the copy succeeds; enable bucket versioning
    if you need a rollback, since the original dataset is gone after promotion)."""
    backup = output.rstrip("/") + "_previous"
    shutil.rmtree(backup, ignore_errors=True)
    os.rename(output, backup)
    os.rename(staging, output)
    shutil.rmtree(backup)  # ponytail: drop immediately; keep it if you need replay of a buggy removal


def remove_from_delta(spark: SparkSession, user_ids: list) -> None:
    from delta import DeltaTable
    table = DeltaTable.forPath(spark, DELTA_TABLE)
    table.delete(F.col("user_id").isin(user_ids))
    # Parquet stats let the engine skip files without matching user_ids, reducing I/O.
    table.vacuum(float(os.environ.get("VACUUM_RETENTION_HOURS", "168")))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    mode, ids = sys.argv[1], sys.argv[2:]
    session = build_session()
    if mode == "json":
        remove_from_json(session, ids)
    elif mode == "delta":
        remove_from_delta(session, ids)
    else:
        raise SystemExit(f"unknown mode {mode}")
