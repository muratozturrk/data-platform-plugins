"""Common Spark SQL metadata-table queries, with `db.table` replaced by `{t}`.

These are the queries typically quoted in Iceberg guides for auditing and health checks.
`status`:

    as-is   runs unchanged and answers the question it is meant to answer
    fixed   runs unchanged but returns a misleading answer; `fixed` holds the corrected query

`topic` groups the queries by the lab module that tests them.
Dremio `TABLE(table_xxx(...))` variants are not tested (no Dremio in the lab).
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class ReferenceQuery:
    topic: str
    sql: str
    status: str = "as-is"
    fixed: str | None = None
    note: str = ""


REFERENCE_QUERIES = {
    "history-all": ReferenceQuery("history_refs", "SELECT * FROM {t}.history"),
    "snapshots-all": ReferenceQuery("history_refs", "SELECT * FROM {t}.snapshots"),
    "entries-all": ReferenceQuery("history_refs", "SELECT * FROM {t}.entries"),
    "history-join-snapshots": ReferenceQuery("history_refs", """
        SELECT h.made_current_at, s.operation, h.snapshot_id, h.is_current_ancestor,
               s.summary['spark.app.id']
        FROM {t}.history h
        JOIN {t}.snapshots s ON h.snapshot_id = s.snapshot_id
        ORDER BY h.made_current_at""",
        note="history covers main only: branch commits never appear"),
    "delete-snapshots": ReferenceQuery("history_refs", """
        SELECT snapshot_id, operation, committed_at FROM {t}.snapshots
        WHERE operation = 'delete'"""),
    "snapshot-app-ids": ReferenceQuery("history_refs", """
        SELECT committed_at, operation, snapshot_id, summary['spark.app.id']
        FROM {t}.snapshots ORDER BY committed_at DESC"""),
    "metadata-log-join-snapshots": ReferenceQuery("history_refs", """
        SELECT l.timestamp, l.file, s.operation
        FROM {t}.metadata_log_entries l
        LEFT JOIN {t}.snapshots s ON l.latest_snapshot_id = s.snapshot_id""",
        note="operation is the latest snapshot's, not what the metadata commit did; "
             "NULL once that snapshot expired"),
    "refs-all": ReferenceQuery("history_refs", "SELECT * FROM {t}.refs"),
    "largest-manifests": ReferenceQuery("manifests_partitions", """
        SELECT path, length, added_snapshot_id FROM {t}.manifests
        ORDER BY length DESC LIMIT 10"""),
    "top-partitions-by-files": ReferenceQuery("manifests_partitions", """
        SELECT partition, file_count FROM {t}.partitions
        ORDER BY file_count DESC LIMIT 5"""),
    "small-files": ReferenceQuery("files_deletes", """
        SELECT file_path, file_size_in_bytes FROM {t}.files
        WHERE file_size_in_bytes <134217728""",
        status="fixed",
        fixed="""SELECT file_path, file_size_in_bytes FROM {t}.files
                 WHERE content = 0 AND file_size_in_bytes < 134217728""",
        note="files also lists delete files; they are not compaction candidates by size"),
    "delete-files": ReferenceQuery("files_deletes", """
        SELECT file_path, content FROM {t}.files WHERE content != 0"""),
    "readable-metrics": ReferenceQuery("files_deletes", """
        SELECT readable_metrics FROM {t}.files WHERE file_path LIKE '%parquet'""",
        note="v2 position delete files are Parquet too; v3 deletion vectors are Puffin"),
    "count-position-deletes": ReferenceQuery("files_deletes", "SELECT COUNT(*) FROM {t}.position_deletes"),
    "deletes-per-partition": ReferenceQuery("files_deletes", """
        SELECT partition, COUNT(*) AS delete_count FROM {t}.position_deletes
        GROUP BY partition ORDER BY delete_count DESC"""),
    "historic-size-range": ReferenceQuery("files_deletes", """
        SELECT partition, MAX(file_size_in_bytes) AS max_size, MIN(file_size_in_bytes) AS min_size
        FROM {t}.all_data_files GROUP BY partition"""),
    "delete-file-totals": ReferenceQuery("files_deletes", """
        SELECT COUNT(*) AS num_delete_files, SUM(record_count) AS total_deleted_rows
        FROM {t}.all_delete_files""",
        status="fixed",
        fixed="""SELECT COUNT(DISTINCT file_path) AS num_delete_files, SUM(record_count) AS total_deleted_rows
                 FROM {t}.all_delete_files""",
        note="all_* tables repeat a file once per manifest that lists it; v3 deletion "
             "vectors share one Puffin file"),
    "entries-join-snapshots": ReferenceQuery("files_deletes", """
        SELECT e.status, e.snapshot_id, s.committed_at, e.data_file.file_path
        FROM {t}.all_entries e
        JOIN {t}.snapshots s ON e.snapshot_id = s.snapshot_id
        ORDER BY s.committed_at""",
        status="fixed",
        fixed="""SELECT e.status, e.snapshot_id, s.committed_at, e.data_file.file_path
                 FROM {t}.all_entries e
                 LEFT JOIN {t}.snapshots s ON e.snapshot_id = s.snapshot_id
                 ORDER BY e.sequence_number, s.committed_at""",
        note="entries keep the id of the snapshot that added the file, which may be expired: "
             "the inner join silently drops them"),
    "small-file-ratio": ReferenceQuery("health", """
        SELECT COUNT(*) AS total_files,
               SUM(CASE WHEN file_size_in_bytes <134217728 THEN 1 ELSE 0 END) AS small_files
        FROM {t}.files""",
        status="fixed",
        fixed="""SELECT COUNT(*) AS total_files,
                        SUM(CASE WHEN file_size_in_bytes < 134217728 THEN 1 ELSE 0 END) AS small_files
                 FROM {t}.files WHERE content = 0""",
        note="counts delete files as small data files"),
    "snapshots-last-24h": ReferenceQuery("health", """
        SELECT COUNT(*) AS snapshots_last_24h FROM {t}.snapshots
        WHERE committed_at >CURRENT_TIMESTAMP() - INTERVAL 1 DAY"""),
}


def run(spark, key: str, table: str, fixed: bool = False):
    q = REFERENCE_QUERIES[key]
    return spark.sql((q.fixed if fixed else q.sql).format(t=table)).collect()
