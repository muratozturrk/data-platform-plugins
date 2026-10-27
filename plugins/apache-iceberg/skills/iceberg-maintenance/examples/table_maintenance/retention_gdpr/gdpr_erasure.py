"""GDPR / CCPA "right to erasure" on an Iceberg table.

A logical DELETE is not enough: old snapshots, MOR delete files and tags keep the
subject's rows physically on storage. The workflow below follows a
multistep approach and ends with a physical verification on object storage, which
is the evidence an auditor asks for.

  1. find tags/branches whose snapshot still contains the subject (retention anchors)
  2. DELETE the subject's rows
  3. rewrite data files that carry deletes, purge dangling delete files
  4. expire every snapshot except the current one
  5. remove orphan files older than the safety window
  6. scan every physical data file for the subject id
"""
import io
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
from pyspark.sql import SparkSession

from common.lakehouse import list_objects, read_object, table_location
from compaction.compaction import CompactionPolicy, compact_mor_table
from retention_gdpr.retention import drop_tag, expire_snapshots, remove_orphan_files, tags


ORPHAN_SAFETY_WINDOW = timedelta(days=3)


@dataclass
class ErasureReport:
    subject_id: int
    rows_deleted: int
    blocking_refs: list[str] = field(default_factory=list)
    dropped_refs: list[str] = field(default_factory=list)
    data_files_rewritten: int = 0
    expired_data_files: int = 0
    residual_files: list[str] = field(default_factory=list)

    @property
    def compliant(self) -> bool:
        return not self.residual_files and not self.blocking_refs


def physical_files_containing(spark: SparkSession, table: str, column: str, value) -> list[str]:
    """Scan every Parquet data file under the table location, referenced or not."""
    hits = []
    for uri, _ in list_objects(table_location(spark, table), "data/"):
        if not uri.endswith(".parquet"):
            continue                                   # puffin deletion vectors, etc.
        pf = pq.ParquetFile(io.BytesIO(read_object(uri)))
        if column not in pf.schema_arrow.names:
            continue                                   # position delete files: (file_path, pos)
        if value in pf.read(columns=[column]).column(column).to_pylist():
            hits.append(uri)
    return hits


def refs_containing(spark: SparkSession, table: str, column: str, value) -> list[str]:
    return [name for name in tags(spark, table)
            if spark.sql(f"SELECT 1 FROM {table} VERSION AS OF '{name}' "
                         f"WHERE {column} = {value} LIMIT 1").count() > 0]


def erase_subject(spark: SparkSession, table: str, subject_id: int,
                  drop_blocking_tags: bool = False, subject_column: str = "user_id") -> ErasureReport:
    report = ErasureReport(subject_id, rows_deleted=spark.sql(
        f"SELECT count(*) c FROM {table} WHERE {subject_column} = {subject_id}").first().c)

    # 1. Tags pin snapshots forever: they must be handled before expiration.
    for ref in refs_containing(spark, table, subject_column, subject_id):
        if drop_blocking_tags:
            drop_tag(spark, table, ref)
            report.dropped_refs.append(ref)
        else:
            report.blocking_refs.append(ref)

    # 2. Logical delete (COW rewrites files, MOR writes delete files).
    spark.sql(f"DELETE FROM {table} WHERE {subject_column} = {subject_id}")

    # 3. Physically rewrite files that still hold the rows behind delete files.
    result, _ = compact_mor_table(spark, table, CompactionPolicy(delete_file_threshold=1, min_input_files=1))
    report.data_files_rewritten = result.rewritten_data_files

    # 4. Drop history that still references the old files: this physically deletes them.
    now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=1)
    report.expired_data_files = expire_snapshots(spark, table, older_than=now, retain_last=1).deleted_data_files
    # 5. Stray files of failed writes (only those older than the safety window).
    remove_orphan_files(spark, table, older_than=now - ORPHAN_SAFETY_WINDOW)

    # 6. Evidence: no physical data file may still contain the subject.
    report.residual_files = physical_files_containing(spark, table, subject_column, subject_id)
    return report
