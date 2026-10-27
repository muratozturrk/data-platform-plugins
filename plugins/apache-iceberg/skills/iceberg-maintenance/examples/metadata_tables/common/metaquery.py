"""Metadata query library: reusable SQL over Iceberg metadata tables (Spark, Iceberg 1.8.1).

Every query was checked against the real 1.8.1 schemas (see querying/test_querying.py). Functions
return plain Python values so they can feed alerts, dashboards or a scheduler.

    change_audit              who changed what, when, on which ref
    protected_snapshot_ids    snapshots pinned by tags and branches
    file_lifecycle            when a file was added and removed
    file_size_distribution    data files per size bucket
    delete_file_accumulation  live vs reachable delete files per partition
    partition_skew            partition sizes relative to the average
    snapshot_growth           per-snapshot totals from summaries
    compaction_trigger        small-file ratio trigger, restricted to data files
    snapshots_since           snapshots committed in a recent window
"""
from dataclasses import dataclass
from datetime import datetime, timedelta

MiB = 1024 * 1024

# Spark-only metadata tables available in Iceberg 1.8.1 (DESCRIBE-verified in querying/test_querying.py).
METADATA_TABLES = [
    "history", "metadata_log_entries", "snapshots", "refs",
    "manifests", "all_manifests", "partitions",
    "entries", "all_entries", "files", "data_files", "delete_files",
    "all_files", "all_data_files", "all_delete_files", "position_deletes",
]


def _rows(spark, sql: str) -> list[dict]:
    return [r.asDict(recursive=True) for r in spark.sql(sql).collect()]


# --- history and references -------------------------------------------------------

CHANGE_AUDIT_SQL = """
SELECT s.committed_at, s.snapshot_id, s.parent_id, s.operation,
       h.snapshot_id IS NOT NULL                 AS in_main_history,
       coalesce(h.is_current_ancestor, false)    AS is_current_ancestor,
       coalesce(r.refs, array())                 AS refs,
       s.summary['spark.app.id']                 AS app_id,
       CAST(coalesce(s.summary['added-records'], '0') AS BIGINT)          AS added_records,
       CAST(coalesce(s.summary['deleted-records'], '0') AS BIGINT)        AS deleted_records,
       CAST(coalesce(s.summary['added-position-deletes'], '0') AS BIGINT) AS added_position_deletes,
       CAST(s.summary['total-records'] AS BIGINT)                         AS total_records
FROM {t}.snapshots s
LEFT JOIN {t}.history h ON s.snapshot_id = h.snapshot_id
LEFT JOIN (SELECT snapshot_id, sort_array(collect_list(name)) AS refs
           FROM {t}.refs GROUP BY snapshot_id) r ON s.snapshot_id = r.snapshot_id
ORDER BY s.committed_at
"""


def change_audit(spark, table: str) -> list[dict]:
    """One row per retained snapshot: operation, ref names, job id, record deltas.

    Starts from `snapshots` (all refs) and LEFT JOINs `history` (main only), so branch
    commits are not lost; `in_main_history` tells them apart.
    """
    return _rows(spark, CHANGE_AUDIT_SQL.format(t=table))


def protected_snapshot_ids(spark, table: str) -> set[int]:
    """Snapshots that a ref points to: expire_snapshots never removes them."""
    return {r["snapshot_id"] for r in _rows(spark, f"SELECT snapshot_id FROM {table}.refs")}


FILE_LIFECYCLE_SQL = """
SELECT e.snapshot_id, e.status, e.sequence_number, s.committed_at, s.operation
FROM {t}.all_entries e
LEFT JOIN {t}.snapshots s ON e.snapshot_id = s.snapshot_id   -- LEFT: keep expired snapshots
WHERE e.data_file.file_path = '{path}'
ORDER BY e.sequence_number, e.status
"""


def file_lifecycle(spark, table: str, file_path: str) -> list[dict]:
    """Distinct (snapshot, status) events for one file. status 1 = added, 2 = deleted.

    Status 0 rows are repeats of the same file in rewritten manifests and are dropped.
    `committed_at` is NULL when the snapshot that made the change has been expired.
    """
    rows = _rows(spark, FILE_LIFECYCLE_SQL.format(t=table, path=file_path))
    seen, events = set(), []
    for r in rows:
        key = (r["snapshot_id"], r["status"])
        if r["status"] != 0 and key not in seen:
            seen.add(key)
            events.append(r)
    return events


# --- files and delete files ---------------------------------------------------------

DEFAULT_BUCKETS = [("< 1 MiB", 1 * MiB), ("1-32 MiB", 32 * MiB), ("32-128 MiB", 128 * MiB),
                   ("128-512 MiB", 512 * MiB), (">= 512 MiB", None)]


def file_size_distribution(spark, table: str, buckets=DEFAULT_BUCKETS) -> dict[str, dict]:
    """Current data files (content = 0) per size bucket: {label: {files, bytes}}."""
    cases = " ".join(f"WHEN file_size_in_bytes < {upper} THEN '{label}'"
                     for label, upper in buckets if upper is not None)
    last = buckets[-1][0]
    rows = _rows(spark, f"""
        SELECT CASE {cases} ELSE '{last}' END AS bucket,
               count(*) AS files, sum(file_size_in_bytes) AS bytes
        FROM {table}.data_files GROUP BY 1""")
    found = {r["bucket"]: {"files": r["files"], "bytes": r["bytes"]} for r in rows}
    return {label: found.get(label, {"files": 0, "bytes": 0}) for label, _ in buckets}


DELETE_ACCUMULATION_SQL = """
WITH live AS (
  SELECT partition,
         count_if(content = 0)                                         AS data_files,
         count(DISTINCT IF(content > 0, file_path, NULL))              AS live_delete_files,
         count_if(content > 0)                                         AS live_delete_entries,
         coalesce(sum(IF(content > 0, record_count, 0)), 0)            AS live_deleted_rows
  FROM {t}.files GROUP BY partition),
reachable AS (
  SELECT partition, count(DISTINCT file_path) AS reachable_delete_files
  FROM {t}.all_delete_files GROUP BY partition)
SELECT coalesce(l.partition, r.partition)          AS partition,
       coalesce(l.data_files, 0)                   AS data_files,
       coalesce(l.live_delete_files, 0)            AS live_delete_files,
       coalesce(l.live_delete_entries, 0)          AS live_delete_entries,
       coalesce(l.live_deleted_rows, 0)            AS live_deleted_rows,
       coalesce(r.reachable_delete_files, 0)       AS reachable_delete_files
FROM live l FULL OUTER JOIN reachable r ON l.partition <=> r.partition
"""


def delete_file_accumulation(spark, table: str) -> list[dict]:
    """Per partition: live delete files (read on every scan) vs delete files still kept
    for time travel (freed only by expire_snapshots).

    Files are counted DISTINCT by path: in v3 several deletion vectors share one Puffin
    file, so `count(*)` over `files` counts vectors, not files.
    """
    rows = _rows(spark, DELETE_ACCUMULATION_SQL.format(t=table))
    return [r for r in rows if r["live_delete_entries"] or r["reachable_delete_files"]]


# --- manifests and partitions -------------------------------------------------------

def partition_skew(spark, table: str) -> list[dict]:
    """Partitions with their share of records and a skew factor (records / average)."""
    return _rows(spark, f"""
        SELECT partition, spec_id, record_count, file_count, total_data_file_size_in_bytes,
               position_delete_file_count + equality_delete_file_count AS delete_file_count,
               round(record_count / avg(record_count) OVER (), 2)      AS skew_factor
        FROM {table}.partitions
        ORDER BY record_count DESC, file_count DESC""")


def manifest_stats(spark, table: str) -> dict:
    """Current manifests: count per content type and files tracked per data manifest."""
    r = spark.sql(f"""
        SELECT count_if(content = 0) AS data_manifests,
               count_if(content = 1) AS delete_manifests,
               coalesce(sum(IF(content = 0, added_data_files_count + existing_data_files_count, 0)), 0)
                   AS tracked_data_files,
               coalesce(sum(IF(content = 1, added_delete_files_count + existing_delete_files_count, 0)), 0)
                   AS tracked_delete_files,
               coalesce(sum(length), 0) AS manifest_bytes
        FROM {table}.manifests""").first().asDict()
    r["avg_data_files_per_manifest"] = (r["tracked_data_files"] / r["data_manifests"]
                                        if r["data_manifests"] else 0.0)
    return r


# --- monitoring ---------------------------------------------------------

def snapshot_growth(spark, table: str) -> list[dict]:
    """Totals after each retained snapshot, read from snapshot summaries."""
    return _rows(spark, f"""
        SELECT committed_at, snapshot_id, operation,
               CAST(summary['total-data-files'] AS INT)   AS total_data_files,
               CAST(summary['total-delete-files'] AS INT) AS total_delete_files,
               CAST(summary['total-records'] AS BIGINT)   AS total_records,
               CAST(summary['total-files-size'] AS BIGINT) AS total_files_size
        FROM {table}.snapshots ORDER BY committed_at""")


@dataclass(frozen=True)
class CompactionSignal:
    total_files: int
    small_files: int
    ratio: float
    trigger: bool


def compaction_trigger(spark, table: str, small_bytes: int = 128 * MiB,
                       max_small_ratio: float = 0.10) -> CompactionSignal:
    """Compact when more than 10% of files are below 128 MB, counting data files only.

    The commonly quoted query reads `files`, which also contains delete files.
    """
    r = spark.sql(f"""
        SELECT COUNT(*) AS total_files,
               SUM(CASE WHEN file_size_in_bytes < {small_bytes} THEN 1 ELSE 0 END) AS small_files
        FROM {table}.files WHERE content = 0""").first()
    total, small = r.total_files, r.small_files or 0
    ratio = small / total if total else 0.0
    return CompactionSignal(total, small, ratio, ratio > max_small_ratio)


def snapshots_since(spark, table: str, since: timedelta = timedelta(days=1),
                    now: datetime | None = None) -> int:
    """Snapshots committed in a recent window (all refs)."""
    if now is None:
        return spark.sql(f"""SELECT COUNT(*) AS n FROM {table}.snapshots
            WHERE committed_at > CURRENT_TIMESTAMP() - INTERVAL {int(since.total_seconds())} SECONDS""").first().n
    cutoff = now - since
    return spark.sql(f"""SELECT COUNT(*) AS n FROM {table}.snapshots
        WHERE committed_at > TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}'""").first().n


def health_report(spark, table: str) -> dict:
    """One dict of gauges for a dashboard or a maintenance scheduler."""
    signal = compaction_trigger(spark, table)
    deletes = delete_file_accumulation(spark, table)
    counts = spark.sql(f"""SELECT
        (SELECT count(*) FROM {table}.snapshots)            AS snapshots,
        (SELECT count(*) FROM {table}.metadata_log_entries) AS metadata_files,
        (SELECT count(*) FROM {table}.refs WHERE type = 'TAG') AS tags,
        (SELECT count(*) FROM {table}.refs WHERE type = 'BRANCH') AS branches""").first().asDict()
    return {
        **counts,
        **{f"manifests_{k}": v for k, v in manifest_stats(spark, table).items()},
        "data_files": signal.total_files,
        "small_data_files": signal.small_files,
        "compaction_needed": signal.trigger,
        "live_delete_files": sum(d["live_delete_files"] for d in deletes),
        "reachable_delete_files": sum(d["reachable_delete_files"] for d in deletes),
        "snapshots_last_24h": snapshots_since(spark, table),
    }
