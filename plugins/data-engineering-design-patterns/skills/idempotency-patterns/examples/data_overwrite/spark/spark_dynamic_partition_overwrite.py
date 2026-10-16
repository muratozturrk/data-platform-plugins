# spark_dynamic_partition_overwrite.py
#
# Dynamic Partition Overwrite: rewrites all partitions present in
# the output DataFrame, leaving unaffected partitions untouched.
# Used for backfill jobs where the set of affected dates is determined
# by the data itself, not by a caller-supplied list.
#
# partitionOverwriteMode=dynamic is the key setting:
#   - Spark detects which partition values exist in the output DataFrame
#   - Only those partitions are replaced in the target table
#   - Partitions absent from the output are left intact
#
# The partition column MUST be derived from the column the input is read
# by (_ingestion_date). Partitioning by DATE(updated_at) while reading by
# _ingestion_date would replace a whole updated_at day with only the rows
# that happened to be ingested inside the range -- silent data loss.
#
# Run:
#   spark-submit \
#     --packages io.delta:delta-spark_2.12:3.2.0 \
#     spark_dynamic_partition_overwrite.py \
#     --start-date 2024-01-01 \
#     --end-date   2024-01-07 \
#     --reason     "backfill_v2_transformation"
#
# Requirements: delta-spark, pyspark, psycopg2-binary

import argparse
import hashlib
import json
import logging
import os
import psycopg2
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


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DynamicPartitionOverwrite")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # dynamic: only overwrite partitions present in the output DataFrame
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def read_and_transform(
    spark:       SparkSession,
    bronze_path: str,
    start_date:  str,
    end_date:    str,
    overwrite_id: str,
) -> DataFrame:
    """
    Reads the Bronze date range and applies Silver transformations.
    Adds _overwrite_date partition column = the input partition
    (_ingestion_date), so every replaced partition is fully re-read.
    """
    df = (
        spark.read.format("delta").load(bronze_path)
        .filter(
            (F.col("_ingestion_date") >= start_date) &
            (F.col("_ingestion_date") <= end_date)
        )
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
                "UTC",
            ))
        # Partition column for dynamic overwrite scoping
        .withColumn(
            "_overwrite_date",
            F.col("_ingestion_date").cast("string"),
        )
        .withColumn("_overwrite_id",    F.lit(overwrite_id))
        .withColumn("_overwritten_at",  F.current_timestamp())
    )
    return df


def get_before_counts(
    spark:       SparkSession,
    silver_path: str,
    start_date:  str,
    end_date:    str,
) -> dict[str, int]:
    """
    Reads current Silver row counts per date in the target range.
    Returns {date_str: count}. Dates with no Silver rows return 0.
    """
    if not DeltaTable.isDeltaTable(spark, silver_path):
        return {}

    rows = (
        spark.read.format("delta").load(silver_path)
        .filter(
            (F.col("_overwrite_date") >= start_date) &
            (F.col("_overwrite_date") <= end_date)
        )
        .groupBy(F.col("_overwrite_date").alias("d"))
        .count()
        .collect()
    )
    return {r["d"]: r["count"] for r in rows}


def run(
    start_date:  str,
    end_date:    str,
    reason:      str,
    bronze_path: str,
    silver_path: str,
    audit_path:  str,
    dry_run:     bool = False,
    db_conn:     Optional[dict] = None,
):
    spark = get_spark()

    overwrite_id = hashlib.md5(
        f"{start_date}_{end_date}_{reason}".encode()
    ).hexdigest()[:12]

    log.info(
        "Dynamic partition overwrite | id=%s | range=%s to %s | "
        "reason=%s | dry_run=%s",
        overwrite_id, start_date, end_date, reason, dry_run,
    )

    # ── Before counts ──────────────────────────────────────────────────
    before_counts = get_before_counts(spark, silver_path, start_date, end_date)
    log.info("Silver before counts: %s", before_counts)

    # ── Transform ──────────────────────────────────────────────────────
    output_df = read_and_transform(
        spark, bronze_path, start_date, end_date, overwrite_id
    )

    # Collect affected dates from the output itself
    # This is what makes it "dynamic" -- the data determines scope
    affected_dates = [
        row[0] for row in
        output_df
        .select("_overwrite_date")
        .distinct()
        .orderBy("_overwrite_date")
        .collect()
        if row[0] is not None
    ]
    log.info("Affected dates in output: %s", affected_dates)

    total_rows = output_df.count()
    log.info("Total output rows: %d", total_rows)

    if total_rows == 0:
        raise ValueError(
            f"Output DataFrame is empty for range {start_date} to {end_date}. "
            "Aborting -- writing zero rows would wipe affected partitions."
        )

    if dry_run:
        log.info(
            "DRY RUN: would overwrite %d date partition(s) "
            "with %d total rows.",
            len(affected_dates), total_rows,
        )
        spark.stop()
        return {"dry_run": True, "affected_dates": affected_dates,
                "total_rows": total_rows}

    # ── Dynamic overwrite ──────────────────────────────────────────────
    # Delta detects which _overwrite_date values are present in output_df
    # and replaces exactly those partitions.
    (
        output_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_overwrite_date")
        .option("overwriteSchema", "false")
        .save(silver_path)
    )

    history    = DeltaTable.forPath(spark, silver_path).history(1).collect()[0]
    delta_ver  = history["version"]
    op_metrics = history["operationMetrics"] or {}

    log.info(
        "Dynamic overwrite complete | delta_v=%d files_added=%s "
        "files_removed=%s",
        delta_ver,
        op_metrics.get("numFiles", "N/A"),
        op_metrics.get("numRemovedFiles", "N/A"),
    )

    # ── After counts ───────────────────────────────────────────────────
    after_counts = get_before_counts(spark, silver_path, start_date, end_date)

    # ── Audit ──────────────────────────────────────────────────────────
    audit_rows = []
    for d in affected_dates:
        audit_rows.append({
            "partition_date":  d,
            "overwrite_id":    overwrite_id,
            "reason":          reason,
            "before_count":    before_counts.get(d, 0),
            "after_count":     after_counts.get(d, 0),
            "delta_version":   delta_ver,
            "overwritten_at":  datetime.now(tz=timezone.utc).isoformat(),
        })

    if audit_rows:
        (
            spark.createDataFrame(audit_rows)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(audit_path)
        )
        log.info("Audit written: %d partition records.", len(audit_rows))

    # ── Gold invalidation ──────────────────────────────────────────────
    if db_conn:
        conn = psycopg2.connect(**db_conn)
        try:
            with conn.cursor() as cur:
                for d in affected_dates:
                    cur.execute("""
                        INSERT INTO trigger_queue
                            (trigger_id, event_type, source_system,
                             payload, status)
                        VALUES (
                            gen_random_uuid()::text,
                            'partition_stale',
                            'data_overwrite',
                            %s, 'pending'
                        )
                        ON CONFLICT DO NOTHING
                    """, (json.dumps({
                        "partition_date": d,
                        "silver_path":    silver_path,
                        "reason":         reason,
                        "overwrite_id":   overwrite_id,
                    }),))
            conn.commit()
            log.info(
                "Gold triggers inserted for %d dates.", len(affected_dates)
            )
        finally:
            conn.close()

    spark.stop()
    return {
        "overwrite_id":    overwrite_id,
        "affected_dates":  affected_dates,
        "total_rows":      total_rows,
        "delta_version":   delta_ver,
        "before_counts":   before_counts,
        "after_counts":    after_counts,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date",  required=True)
    parser.add_argument("--end-date",    required=True)
    parser.add_argument("--reason",      required=True)
    parser.add_argument("--dry-run",     action="store_true")
    args = parser.parse_args()

    run(
        start_date  = args.start_date,
        end_date    = args.end_date,
        reason      = args.reason,
        bronze_path = "/data/raw/orders",
        silver_path = "/data/silver/orders",
        audit_path  = "/data/audit/overwrites",
        dry_run     = args.dry_run,
        db_conn     = dict(
            host="state-db", dbname="pipeline_state",
            user="pipeline", password=os.environ["PG_PASSWORD"],
        ),
    )