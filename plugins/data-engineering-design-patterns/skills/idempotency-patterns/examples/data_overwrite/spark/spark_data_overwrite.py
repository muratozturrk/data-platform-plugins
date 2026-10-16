# spark_data_overwrite.py
#
# Data Overwrite: replaces specific Silver partitions with corrected data.
# Uses Delta replaceWhere for atomic partition-level replacement.
# Validates source completeness before overwrite.
# Captures before/after row counts and Delta version in audit log.
# Inserts Gold invalidation trigger for affected dates.
#
# Rely on the writer's native replace semantics. The
# replaceWhere predicate MUST use the same column the input is read by
# (here _ingestion_date). Reading by _ingestion_date but replacing by
# DATE(updated_at) deletes rows that were not re-read and makes Delta
# reject rows that do not match the predicate.
#
# Run:
#   spark-submit \
#     --packages io.delta:delta-spark_2.12:3.2.0 \
#     spark_data_overwrite.py \
#     --dates 2024-01-10,2024-01-11 \
#     --reason "bug_fix_amount_cast" \
#     --dry-run
#
# Requirements: delta-spark, pyspark, psycopg2-binary

import argparse
import hashlib
import json
import logging
import os
import psycopg2
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


@dataclass
class OverwriteConfig:
    bronze_path:     str
    silver_path:     str
    audit_path:      str
    partition_col:   str       = "_ingestion_date"
    business_key:    str       = "id"
    event_time_col:  str       = "updated_at"
    reason:          str       = ""           # human-readable correction reason
    dry_run:         bool      = False
    db_conn:         Optional[dict] = None


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DataOverwrite")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # No partitionOverwriteMode=dynamic here: replaceWhere already scopes
        # the overwrite, and Delta rejects mixing the two in writer options.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ── Pre-overwrite validation ───────────────────────────────────────────────────

def validate_source_partition(
    spark:           SparkSession,
    bronze_path:     str,
    partition_date:  str,
    partition_col:   str,
) -> int:
    """
    Confirms the Bronze source partition exists and is non-empty.
    Returns row count.
    Raises if empty -- overwriting Silver with zero rows is never correct.
    """
    df    = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col(partition_col) == partition_date)
    )
    count = df.count()
    if count == 0:
        raise ValueError(
            f"Bronze partition {partition_date} is empty. "
            f"Aborting overwrite -- writing zero rows to Silver is not valid."
        )
    log.info(
        "Source validated: partition=%s rows=%d",
        partition_date, count,
    )
    return count


def get_silver_before_count(
    spark:          SparkSession,
    silver_path:    str,
    partition_date: str,
    partition_col: str,
) -> int:
    """
    Returns the current row count in the Silver partition before overwrite.
    Used for before/after audit comparison.
    Returns 0 if the partition does not exist yet (first write).
    """
    if not DeltaTable.isDeltaTable(spark, silver_path):
        return 0
    return (
        spark.read.format("delta").load(silver_path)
        .filter(F.col(partition_col) == partition_date)
        .count()
    )


# ── Transformation ─────────────────────────────────────────────────────────────

def transform_partition(
    spark:          SparkSession,
    bronze_path:    str,
    partition_date: str,
    partition_col:  str,
    overwrite_id:   str,
) -> DataFrame:
    """
    Reads the Bronze partition and applies Silver transformation rules.
    Identical to the normal transformation pipeline -- overwrite does not
    use different transformation logic, only different write behaviour.
    """
    df = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col(partition_col) == partition_date)
        .withColumn("id",          F.col("id").cast(IntegerType()))
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
        .withColumn("amount_usd",
            (F.col("ord_amt").cast(DoubleType()) / 100)
            .cast(DoubleType()))
        .withColumn("updated_at",
            F.to_utc_timestamp(
                F.to_timestamp(
                    (F.col("upd_ts").cast("long") / 1000)
                    .cast(TimestampType())
                ),
                "UTC"
            ))
        .withColumn("_overwrite_id",  F.lit(overwrite_id))
        .withColumn("_overwritten_at", F.current_timestamp())
    )
    return df


# ── Atomic partition overwrite ─────────────────────────────────────────────────

def overwrite_partition(
    spark:          SparkSession,
    df:             DataFrame,
    silver_path:    str,
    partition_date: str,
    partition_col: str,
    dry_run:        bool,
) -> dict:
    """
    Atomically overwrites the Silver partition for partition_date.

    Uses Delta replaceWhere to scope the overwrite to the specific date.
    All other partitions in the Silver table are untouched.

    replaceWhere semantics:
      - Files containing rows matching the predicate are replaced
      - Files containing no matching rows are untouched
      - The operation is committed atomically in the Delta transaction log

    Returns Delta version and row count after write.
    """
    after_count = df.count()

    if dry_run:
        log.info(
            "DRY RUN: would overwrite Silver partition %s "
            "with %d rows",
            partition_date, after_count,
        )
        return {
            "after_count":   after_count,
            "delta_version": -1,
            "dry_run":       True,
        }

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"{partition_col} = '{partition_date}'"
        )
        .option("overwriteSchema", "false")   # fail if schema changed
        .partitionBy(partition_col)           # keeps each overwrite small
        .save(silver_path)
    )

    # Read Delta version written
    history     = DeltaTable.forPath(spark, silver_path).history(1).collect()[0]
    delta_ver   = history["version"]
    op_metrics  = history["operationMetrics"] or {}

    result = {
        "after_count":         after_count,
        "delta_version":       delta_ver,
        "files_added":         int(op_metrics.get("numFiles",           0)),
        "files_removed":       int(op_metrics.get("numRemovedFiles",    0)),
        "bytes_written":       int(op_metrics.get("numOutputBytes",     0)),
    }
    log.info(
        "Partition overwrite complete | date=%s after_rows=%d "
        "delta_v=%d files_added=%d files_removed=%d",
        partition_date,
        result["after_count"],
        result["delta_version"],
        result["files_added"],
        result["files_removed"],
    )
    return result


# ── Rollback utility ───────────────────────────────────────────────────────────

def rollback_partition(
    spark:          SparkSession,
    silver_path:    str,
    partition_date: str,
    partition_col: str,
    to_version:     int,
    dry_run:        bool,
):
    """
    Rolls back a Silver partition to its state at `to_version`
    by reading the Delta time-travel snapshot and re-overwriting.

    Use when an overwrite produced incorrect output and the previous
    version is known to be correct.

    Caveat: requires Delta log history for `to_version` to still exist.
    Run VACUUM before this function only with caution -- VACUUM may
    have removed files needed for time travel.
    """
    log.info(
        "Rolling back partition %s to Delta version %d",
        partition_date, to_version,
    )

    snapshot_df = (
        spark.read
        .format("delta")
        .option("versionAsOf", to_version)
        .load(silver_path)
        .filter(F.col(partition_col) == partition_date)
        .withColumn("_rolled_back_at", F.current_timestamp())
        .withColumn("_rolled_back_to_version", F.lit(to_version))
    )

    count = snapshot_df.count()
    log.info("Rollback snapshot rows: %d", count)

    if dry_run:
        log.info("DRY RUN: rollback preview only -- no write.")
        return

    (
        snapshot_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"{partition_col} = '{partition_date}'"
        )
        .save(silver_path)
    )

    new_version = (
        DeltaTable.forPath(spark, silver_path)
        .history(1).collect()[0]["version"]
    )
    log.info(
        "Rollback complete | partition=%s | rolled_back_to_version=%d "
        "| new_delta_version=%d | rows=%d",
        partition_date, to_version, new_version, count,
    )


# ── Gold invalidation ──────────────────────────────────────────────────────────

def invalidate_gold(conn, partition_dates: list[str], cfg: OverwriteConfig):
    """
    Inserts Gold re-aggregation triggers for all overwritten dates.
    Identical to the trigger insertion used by other correction patterns.
    """
    if not conn:
        log.warning("No db_conn -- Gold invalidation skipped.")
        return
    with conn.cursor() as cur:
        for d in partition_dates:
            cur.execute("""
                INSERT INTO trigger_queue
                    (trigger_id, event_type, source_system, payload, status)
                VALUES (
                    gen_random_uuid()::text,
                    'partition_stale',
                    'data_overwrite',
                    %s,
                    'pending'
                )
                ON CONFLICT DO NOTHING
            """, (json.dumps({
                "partition_date": d,
                "silver_path":    cfg.silver_path,
                "reason":         cfg.reason,
            }),))
    conn.commit()
    log.info("Gold invalidation triggers inserted for: %s", partition_dates)


# ── Audit ──────────────────────────────────────────────────────────────────────

def write_audit(
    spark:          SparkSession,
    audit_path:     str,
    partition_date: str,
    before_count:   int,
    overwrite_id:   str,
    reason:         str,
    result:         dict,
):
    audit_df = spark.createDataFrame([{
        "partition_date":  partition_date,
        "overwrite_id":    overwrite_id,
        "reason":          reason,
        "before_count":    before_count,
        "after_count":     result.get("after_count",   0),
        "delta_version":   result.get("delta_version", -1),
        "files_added":     result.get("files_added",    0),
        "files_removed":   result.get("files_removed",  0),
        "bytes_written":   result.get("bytes_written",  0),
        "dry_run":         result.get("dry_run", False),
        "overwritten_at":  datetime.now(tz=timezone.utc).isoformat(),
    }])

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(audit_path)
    )
    log.info(
        "Audit written | partition=%s before=%d after=%d delta_v=%d",
        partition_date,
        before_count,
        result.get("after_count", 0),
        result.get("delta_version", -1),
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def run(
    dates:  list[str],
    cfg:    OverwriteConfig,
):
    spark       = get_spark()
    conn        = psycopg2.connect(**cfg.db_conn) if cfg.db_conn else None
    overwrite_id = hashlib.md5(
        ("|".join(sorted(dates)) + cfg.reason).encode()
    ).hexdigest()[:12]

    log.info(
        "Data Overwrite | overwrite_id=%s | dates=%s | reason=%s | "
        "dry_run=%s",
        overwrite_id, dates, cfg.reason, cfg.dry_run,
    )

    all_results = {}

    for partition_date in sorted(dates):
        log.info("--- Partition: %s ---", partition_date)

        # Validate
        validate_source_partition(
            spark, cfg.bronze_path, partition_date, cfg.partition_col
        )
        before = get_silver_before_count(
            spark, cfg.silver_path, partition_date, cfg.partition_col
        )
        log.info("Silver before count: %d rows", before)

        # Transform
        df = transform_partition(
            spark, cfg.bronze_path, partition_date,
            cfg.partition_col, overwrite_id,
        )

        # Overwrite
        result = overwrite_partition(
            spark, df, cfg.silver_path, partition_date,
            cfg.partition_col, cfg.dry_run,
        )
        all_results[partition_date] = result

        # Audit
        write_audit(
            spark, cfg.audit_path, partition_date,
            before, overwrite_id, cfg.reason, result,
        )

    # Invalidate Gold for all affected dates (after all partitions succeed)
    if not cfg.dry_run and conn:
        invalidate_gold(conn, dates, cfg)

    if conn:
        conn.close()

    # Summary
    log.info(
        "Overwrite complete | overwrite_id=%s | partitions=%d | "
        "results=%s",
        overwrite_id, len(all_results),
        {d: {"after":  r.get("after_count"),
             "delta_v": r.get("delta_version")}
         for d, r in all_results.items()},
    )

    spark.stop()
    return all_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dates", required=True,
        help="Comma-separated list of partition dates: 2024-01-10,2024-01-11",
    )
    parser.add_argument("--reason",    required=True)
    parser.add_argument("--dry-run",   action="store_true")
    args = parser.parse_args()

    cfg = OverwriteConfig(
        bronze_path    = "/data/raw/orders",
        silver_path    = "/data/silver/orders",
        audit_path     = "/data/audit/overwrites",
        partition_col  = "_ingestion_date",
        business_key   = "id",
        event_time_col = "updated_at",
        reason         = args.reason,
        dry_run        = args.dry_run,
        db_conn        = dict(
            host="state-db", dbname="pipeline_state",
            user="pipeline", password=os.environ["PG_PASSWORD"],
        ),
    )

    dates = [d.strip() for d in args.dates.split(",")]
    run(dates, cfg)