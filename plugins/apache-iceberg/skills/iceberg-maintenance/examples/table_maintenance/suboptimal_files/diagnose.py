"""Quantify the four common layout problems from Iceberg metadata tables.

  small files        -> data_files, avg_data_file_bytes, small_file_ratio
  poor colocation    -> files_touched_by_lookup()  (min/max column stats per file)
  metadata sprawl    -> manifests, avg_files_per_manifest, snapshots, metadata_json_versions
  MOR read penalty   -> position/equality delete files, deletes per data file
"""
from dataclasses import dataclass

from pyspark.sql import SparkSession

DEFAULT_SMALL_FILE_BYTES = 32 * 1024 * 1024  # 32 MiB; tune to your target file size


@dataclass(frozen=True)
class TableDiagnosis:
    data_files: int
    data_bytes: int
    avg_data_file_bytes: float
    small_file_ratio: float
    manifests: int
    avg_files_per_manifest: float
    snapshots: int
    metadata_json_versions: int
    position_delete_files: int
    equality_delete_files: int

    @property
    def delete_files_per_data_file(self) -> float:
        deletes = self.position_delete_files + self.equality_delete_files
        return deletes / self.data_files if self.data_files else 0.0


def diagnose(spark: SparkSession, table: str,
             small_file_bytes: int = DEFAULT_SMALL_FILE_BYTES) -> TableDiagnosis:
    files = spark.sql(f"""
        SELECT
          count_if(content = 0)                                   AS data_files,
          coalesce(sum(IF(content = 0, file_size_in_bytes, 0)), 0) AS data_bytes,
          count_if(content = 0 AND file_size_in_bytes < {small_file_bytes}) AS small_files,
          count_if(content = 1)                                   AS pos_deletes,
          count_if(content = 2)                                   AS eq_deletes
        FROM {table}.files
    """).first()
    manifests = spark.sql(f"""
        SELECT count(*) AS n,
               coalesce(sum(added_data_files_count + existing_data_files_count), 0) AS tracked
        FROM {table}.manifests WHERE content = 0
    """).first()
    snapshots = spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").first().n
    versions = spark.sql(f"SELECT count(*) AS n FROM {table}.metadata_log_entries").first().n

    n_files = files.data_files
    return TableDiagnosis(
        data_files=n_files,
        data_bytes=files.data_bytes,
        avg_data_file_bytes=files.data_bytes / n_files if n_files else 0.0,
        small_file_ratio=files.small_files / n_files if n_files else 0.0,
        manifests=manifests.n,
        avg_files_per_manifest=manifests.tracked / manifests.n if manifests.n else 0.0,
        snapshots=snapshots,
        metadata_json_versions=versions,
        position_delete_files=files.pos_deletes,
        equality_delete_files=files.eq_deletes,
    )


def files_touched_by_lookup(spark: SparkSession, table: str, column: str, value) -> tuple[int, int]:
    """(files whose min/max range contains value, total data files).

    This is exactly the pruning decision a query engine makes from manifest stats for
    `WHERE column = value`: files whose [lower_bound, upper_bound] covers the value
    must be opened. Poor colocation -> nearly every file must be opened.
    """
    row = spark.sql(f"""
        SELECT
          count_if(readable_metrics.{column}.lower_bound <= {value!r}
                   AND readable_metrics.{column}.upper_bound >= {value!r}) AS touched,
          count(*) AS total
        FROM {table}.data_files
    """).first()
    return row.touched, row.total


def last_commit_stats(spark: SparkSession, table: str) -> dict[str, int]:
    """Numbers from the latest snapshot summary (what the last write physically did)."""
    summary = spark.sql(
        f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first().summary
    keys = ["added-data-files", "deleted-data-files", "added-files-size", "removed-files-size",
            "added-delete-files", "added-position-deletes", "added-dvs"]
    return {k: int(summary.get(k, 0)) for k in keys}
