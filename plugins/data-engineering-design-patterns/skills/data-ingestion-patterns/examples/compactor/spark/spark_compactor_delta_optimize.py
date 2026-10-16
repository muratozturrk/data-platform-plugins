# spark_compactor_delta_optimize.py
#
# Delta Lake Compactor: runs OPTIMIZE on Delta tables to consolidate
# small files into optimally-sized files (target: 1 GB per file).
# Optionally applies Z-ORDER to co-locate rows for faster query skipping.
# Safe to run while readers and writers are active — Delta ACID guarantees
# no reader sees an inconsistent state during compaction.
#
# Run:
#   spark-submit \
#     --packages io.delta:delta-core_2.12:2.4.0 \
#     spark_compactor_delta_optimize.py
#
# Requirements: delta-spark, pyspark

import argparse
import time
import logging
from dataclasses import dataclass, field
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config dataclass ───────────────────────────────────────────────────────────

@dataclass
class CompactionTarget:
    """Defines a single table to compact."""
    name:            str             # human-readable name for logging
    path:            str             # Delta table path
    z_order_cols:    list[str]       = field(default_factory=list)
    # If set, only compact partitions matching this SQL predicate
    partition_filter: Optional[str] = None


# Define all tables to compact in one place
COMPACTION_TARGETS = [
    CompactionTarget(
        name            = "orders_bronze",
        path            = "/data/raw/orders",
        # Bronze: no Z-ORDER — queries always read full partitions
        z_order_cols    = [],
        # Only compact recent partitions — older ones are already healthy
        partition_filter = "_ingestion_date >= '2024-01-01'",
    ),
    CompactionTarget(
        name            = "orders_silver",
        path            = "/data/silver/orders",
        # Silver: Z-ORDER on the most common query filter columns
        # This enables data skipping when analysts filter by customer or status
        z_order_cols    = ["customer_id", "status"],
        partition_filter = None,   # compact all partitions
    ),
    CompactionTarget(
        name            = "customers_silver",
        path            = "/data/silver/customers",
        z_order_cols    = ["customer_id"],
        partition_filter = None,
    ),
]


# ── Spark session ──────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DeltaCompactor")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Target file size for OPTIMIZE: 1 GB
        # Delta will try to produce files of approximately this size
        .config("spark.databricks.delta.optimize.maxFileSize",
                str(1024 * 1024 * 1024))
        # Minimum file size below which a file is a candidate for compaction
        .config("spark.databricks.delta.optimize.minFileSize",
                str(128 * 1024 * 1024))   # 128 MB
        .getOrCreate()
    )


# ── Compaction logic ───────────────────────────────────────────────────────────

def get_file_stats(spark: SparkSession, path: str) -> dict:
    """
    Returns file count and total size for a Delta table.
    Used to measure compaction effectiveness before and after.
    """
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    return {
        "num_files":   detail["numFiles"],
        "size_bytes":  detail["sizeInBytes"],
        "avg_file_mb": (detail["sizeInBytes"] / detail["numFiles"] / 1024 / 1024)
                        if detail["numFiles"] > 0 else 0,
    }


def compact_table(spark: SparkSession, target: CompactionTarget) -> dict:
    """
    Runs OPTIMIZE (and optionally Z-ORDER) on a single Delta table.
    Returns a dict with before/after statistics.
    """
    log.info("Starting compaction: %s | path=%s", target.name, target.path)

    # ── Collect before stats ───────────────────────────────────────────
    before = get_file_stats(spark, target.path)
    log.info(
        "Before | files=%d | size=%.1f GB | avg_file=%.1f MB",
        before["num_files"],
        before["size_bytes"] / 1024**3,
        before["avg_file_mb"],
    )

    start_ts = time.time()

    # ── Build and run OPTIMIZE SQL ─────────────────────────────────────
    # OPTIMIZE rewrites small files into target-size files.
    # WHERE clause restricts to specific partitions if set.
    # ZORDER BY physically co-locates rows with similar column values.

    optimize_sql = f"OPTIMIZE delta.`{target.path}`"

    if target.partition_filter:
        optimize_sql += f" WHERE {target.partition_filter}"

    if target.z_order_cols:
        z_cols = ", ".join(target.z_order_cols)
        optimize_sql += f" ZORDER BY ({z_cols})"

    log.info("Running: %s", optimize_sql)
    metrics_df = spark.sql(optimize_sql)

    # OPTIMIZE returns metrics about files added/removed
    metrics = metrics_df.collect()[0]
    elapsed = time.time() - start_ts

    # ── Collect after stats ────────────────────────────────────────────
    after = get_file_stats(spark, target.path)
    log.info(
        "After  | files=%d | size=%.1f GB | avg_file=%.1f MB | elapsed=%.1fs",
        after["num_files"],
        after["size_bytes"] / 1024**3,
        after["avg_file_mb"],
        elapsed,
    )

    files_removed = before["num_files"] - after["num_files"]
    log.info(
        "Result | files_removed=%d | reduction=%.1f%%",
        files_removed,
        (files_removed / before["num_files"] * 100) if before["num_files"] > 0 else 0,
    )

    return {
        "table":         target.name,
        "before_files":  before["num_files"],
        "after_files":   after["num_files"],
        "before_avg_mb": before["avg_file_mb"],
        "after_avg_mb":  after["avg_file_mb"],
        "elapsed_s":     elapsed,
    }


def run_vacuum(spark: SparkSession, path: str, retention_hours: int = 168):
    """
    Runs VACUUM to physically delete old files that were replaced by OPTIMIZE.
    Default retention: 168 hours (7 days) — the Delta Lake default.
    WARNING: never set retention below 7 days if concurrent readers exist.
             Doing so can cause readers to fail with FileNotFoundException.
    """
    log.info("Running VACUUM on %s (retain=%dh)", path, retention_hours)
    spark.sql(
        f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS"
    )
    log.info("VACUUM complete.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--vacuum", action="store_true",
        help="Also run VACUUM after OPTIMIZE to reclaim storage"
    )
    parser.add_argument(
        "--vacuum-retention-hours", type=int, default=168,
        help="VACUUM retention in hours (default: 168 = 7 days)"
    )
    parser.add_argument(
        "--table", default=None,
        help="Compact only this table name (default: all tables)"
    )
    args = parser.parse_args()

    spark   = get_spark()
    results = []

    targets = COMPACTION_TARGETS
    if args.table:
        targets = [t for t in COMPACTION_TARGETS if t.name == args.table]
        if not targets:
            raise ValueError(f"Unknown table: {args.table}")

    for target in targets:
        try:
            result = compact_table(spark, target)
            results.append(result)

            if args.vacuum:
                run_vacuum(spark, target.path, args.vacuum_retention_hours)

        except Exception as e:
            log.error("Compaction failed for %s: %s", target.name, e,
                      exc_info=True)
            results.append({"table": target.name, "error": str(e)})

    # ── Summary ────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("COMPACTION SUMMARY")
    log.info("=" * 60)
    for r in results:
        if "error" in r:
            log.error("  %s: FAILED — %s", r["table"], r["error"])
        else:
            log.info(
                "  %s: %d → %d files (%.1f MB → %.1f MB avg) in %.1fs",
                r["table"], r["before_files"], r["after_files"],
                r["before_avg_mb"], r["after_avg_mb"], r["elapsed_s"],
            )

    spark.stop()


if __name__ == "__main__":
    main()