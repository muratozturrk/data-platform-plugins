# spark_compactor_binpack.py
#
# Selective bin-packing compactor for plain Parquet tables.
# Scans all partitions, identifies those with average file size below
# the threshold, and rewrites only those partitions — skipping healthy ones.
# Writes to a staging location first, then atomically renames to target
# (same Stage-then-Swap safety as Full Loader production pattern).
#
# Run:
#   spark-submit \
#     spark_compactor_binpack.py --path /data/raw/orders \
#                                --target-file-mb 512 \
#                                --min-file-mb 64
#
# Requirements: pyspark

import os
import argparse
import logging
import time
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ── Spark session ──────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BinPackCompactor")
        # Coalesce output files to approach target size
        .config("spark.sql.files.maxPartitionBytes",
                str(512 * 1024 * 1024))  # 512 MB read partition
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# ── Partition file size analysis ───────────────────────────────────────────────

def get_partition_file_stats(spark: SparkSession, base_path: str) -> list[dict]:
    """
    Lists all partitions under base_path and returns file size stats
    for each partition directory.
    Returns a list of dicts: {partition_path, file_count, total_bytes, avg_mb}
    """
    hadoop_conf = spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI.create(base_path),
        hadoop_conf
    )
    path_obj = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(base_path)

    stats = []

    def scan_directory(current_path, depth=0):
        """Recursively scan directories to find leaf partition directories."""
        items = list(fs.listStatus(current_path))
        subdirs = [i for i in items if i.isDirectory()
                   and not i.getPath().getName().startswith("_")]
        files   = [i for i in items if not i.isDirectory()
                   and i.getPath().getName().endswith(".parquet")]

        if files:
            # This is a leaf partition directory containing parquet files
            total_bytes = sum(f.getLen() for f in files)
            file_count  = len(files)
            avg_mb = total_bytes / file_count / 1024 / 1024 if file_count > 0 else 0
            stats.append({
                "partition_path": current_path.toString(),
                "file_count":     file_count,
                "total_bytes":    total_bytes,
                "avg_mb":         avg_mb,
            })
        elif subdirs and depth < 5:
            for subdir in subdirs:
                scan_directory(subdir.getPath(), depth + 1)

    scan_directory(path_obj)
    return stats


# ── Partition compaction ───────────────────────────────────────────────────────

def compact_partition(
    spark: SparkSession,
    partition_path: str,
    target_file_mb: int,
    staging_suffix: str = "_compacting",
) -> dict:
    """
    Reads a single partition, coalesces to target file count,
    writes to staging path, then atomically renames to replace the original.

    Returns before/after stats dict.
    """
    staging_path = partition_path + staging_suffix

    log.info("Compacting partition: %s", partition_path)

    # ── Read ────────────────────────────────────────────────────────────
    df = spark.read.parquet(partition_path)
    total_rows = df.count()

    # Estimate number of output files needed
    # Read the partition size from stats already collected
    hadoop = spark.sparkContext._jvm.org.apache.hadoop.fs
    conf   = spark.sparkContext._jvm.org.apache.hadoop.conf.Configuration()
    fs     = hadoop.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI.create(partition_path), conf
    )
    content_summary = fs.getContentSummary(
        hadoop.Path(partition_path)
    )
    total_mb  = content_summary.getLength() / 1024 / 1024
    num_files = max(1, int(total_mb / target_file_mb))

    log.info("  rows=%d, size=%.1f MB → target %d file(s)", total_rows,
             total_mb, num_files)

    # ── Write to staging ───────────────────────────────────────────────
    # coalesce(n) reduces shuffle — only use repartition if data is skewed
    (
        df.coalesce(num_files)
        .write
        .mode("overwrite")
        .parquet(staging_path)
    )

    # ── Atomic swap ────────────────────────────────────────────────────
    # Delete original, rename staging to original path
    # This is near-atomic at the filesystem level
    original_path_obj = hadoop.Path(partition_path)
    staging_path_obj  = spark.sparkContext._jvm.hadoop.fs.Path(staging_path)

    fs.delete(original_path_obj, True)          # delete original recursively
    fs.rename(staging_path_obj, original_path_obj)  # rename staging → original

    # ── After stats ────────────────────────────────────────────────────
    after_items = [
        i for i in fs.listStatus(original_path_obj)
        if i.getPath().getName().endswith(".parquet")
    ]
    after_count = len(after_items)
    log.info("  compacted to %d file(s)", after_count)

    return {
        "partition":    partition_path,
        "before_files": None,   # caller has this from the scan
        "after_files":  after_count,
        "total_rows":   total_rows,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path",           required=True,
                        help="Base path of the Parquet table")
    parser.add_argument("--target-file-mb", type=int, default=512,
                        help="Target file size in MB (default: 512)")
    parser.add_argument("--min-file-mb",    type=int, default=64,
                        help="Min avg file size below which partition is compacted")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Print which partitions would be compacted, no writes")
    args = parser.parse_args()

    spark = get_spark()

    log.info("Scanning partitions: %s", args.path)
    all_partitions = get_partition_file_stats(spark, args.path)
    log.info("Found %d partitions total", len(all_partitions))

    # Select only unhealthy partitions (avg file size below threshold)
    candidates = [
        p for p in all_partitions
        if p["avg_mb"] < args.min_file_mb
    ]

    log.info(
        "Compaction candidates: %d/%d partitions "
        "(avg_file_mb < %d MB)",
        len(candidates), len(all_partitions), args.min_file_mb,
    )

    if args.dry_run:
        for c in candidates:
            log.info("  DRY RUN | %s | files=%d | avg=%.1f MB",
                     c["partition_path"], c["file_count"], c["avg_mb"])
        spark.stop()
        return

    results = []
    for partition in candidates:
        start = time.time()
        try:
            result = compact_partition(
                spark,
                partition["partition_path"],
                args.target_file_mb,
            )
            result["before_files"] = partition["file_count"]
            result["elapsed_s"]    = time.time() - start
            results.append(result)
        except Exception as e:
            log.error("Failed: %s — %s", partition["partition_path"], e,
                      exc_info=True)

    log.info("=" * 60)
    log.info("BIN-PACK COMPACTION SUMMARY")
    log.info("Total partitions compacted: %d", len(results))
    total_files_before = sum(r["before_files"] for r in results)
    total_files_after  = sum(r["after_files"]  for r in results)
    log.info("Files: %d → %d (reduction: %.1f%%)",
             total_files_before, total_files_after,
             (1 - total_files_after / total_files_before) * 100
             if total_files_before > 0 else 0)

    spark.stop()


if __name__ == "__main__":
    main()