"""Policy-driven Iceberg compaction on top of Spark procedures.

A table-level CompactionPolicy is turned into a `rewrite_data_files` call, so the same
code can run bin-pack, sort or z-order compaction, scoped by a `where` filter and
size thresholds. Pattern follows how maintenance services such as Apache Amoro
and AWS Glue table optimizers configure compaction per table.
"""
from dataclasses import dataclass, field
from datetime import datetime

from pyspark.sql import SparkSession

from common.lakehouse import CATALOG

MiB = 1024 * 1024


@dataclass(frozen=True)
class CompactionPolicy:
    strategy: str = "binpack"                 # "binpack" | "sort"
    sort_order: str | None = None             # "user_id ASC NULLS LAST" or "zorder(user_id, country)"
    target_file_size_bytes: int = 128 * MiB
    min_input_files: int = 5                  # skip groups with fewer files (not worth a rewrite)
    where: str | None = None                  # scope: partitions / time window
    delete_file_threshold: int | None = None  # rewrite a data file once it has N delete files
    remove_dangling_deletes: bool = False
    partial_progress: bool = True             # commit file groups incrementally
    max_concurrent_file_group_rewrites: int = 4
    rewrite_job_order: str = "bytes-asc"      # smallest groups first
    extra_options: dict[str, str] = field(default_factory=dict)

    def options(self) -> dict[str, str]:
        opts = {
            "target-file-size-bytes": str(self.target_file_size_bytes),
            "min-input-files": str(self.min_input_files),
            "partial-progress.enabled": str(self.partial_progress).lower(),
            "max-concurrent-file-group-rewrites": str(self.max_concurrent_file_group_rewrites),
            "rewrite-job-order": self.rewrite_job_order,
        }
        if self.delete_file_threshold is not None:
            opts["delete-file-threshold"] = str(self.delete_file_threshold)
        if self.remove_dangling_deletes:
            opts["remove-dangling-deletes"] = "true"
        opts.update(self.extra_options)
        return opts


@dataclass(frozen=True)
class CompactionResult:
    rewritten_data_files: int
    added_data_files: int
    rewritten_bytes: int
    failed_data_files: int
    removed_delete_files: int


def _sql_str(value: str) -> str:
    """Spark SQL string literal. Spark has no '' escape, so values that contain single
    quotes (e.g. where filters) are wrapped in double quotes, as in the Iceberg docs."""
    if "'" not in value:
        return f"'{value}'"
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def rewrite_data_files_sql(table: str, policy: CompactionPolicy, catalog: str = CATALOG) -> str:
    args = [f"table => {_sql_str(table)}", f"strategy => {_sql_str(policy.strategy)}"]
    if policy.strategy == "sort":
        if not policy.sort_order:
            raise ValueError("sort strategy requires sort_order")
        args.append(f"sort_order => {_sql_str(policy.sort_order)}")
    if policy.where:
        args.append(f"where => {_sql_str(policy.where)}")
    opts = ", ".join(f"{_sql_str(k)}, {_sql_str(v)}" for k, v in policy.options().items())
    args.append(f"options => map({opts})")
    return f"CALL {catalog}.system.rewrite_data_files(" + ", ".join(args) + ")"


def compact(spark: SparkSession, table: str, policy: CompactionPolicy) -> CompactionResult:
    row = spark.sql(rewrite_data_files_sql(table, policy)).first().asDict()
    return CompactionResult(
        rewritten_data_files=row["rewritten_data_files_count"],
        added_data_files=row["added_data_files_count"],
        rewritten_bytes=row["rewritten_bytes_count"],
        failed_data_files=row.get("failed_data_files_count", 0),
        removed_delete_files=row.get("removed_delete_files_count", 0),
    )


def remove_dangling_position_deletes(spark: SparkSession, table: str) -> int:
    """Drop position delete files that point at data files no longer in the table.

    After rewrite_data_files has applied deletes into new base files, the old
    position delete files are still tracked in manifests ("dangling"). They cost
    planning time and keep old delete files alive. Returns delete files removed.
    """
    row = spark.sql(f"""
        CALL {CATALOG}.system.rewrite_position_delete_files(
          table => {_sql_str(table)}, options => map('rewrite-all', 'true'))
    """).first()
    return row.rewritten_delete_files_count - row.added_delete_files_count


def compact_mor_table(spark: SparkSession, table: str, policy: CompactionPolicy) -> tuple[CompactionResult, int]:
    """Full MOR clean-up: materialize deletes into base files, then purge dangling deletes."""
    result = compact(spark, table, policy)
    return result, remove_dangling_position_deletes(spark, table)


def rewrite_manifests(spark: SparkSession, table: str) -> tuple[int, int]:
    """Consolidate manifests (metadata compaction). Returns (rewritten, added)."""
    row = spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => {_sql_str(table)})").first()
    return row.rewritten_manifests_count, row.added_manifests_count


def closed_partitions_filter(ts_column: str, now: datetime) -> str:
    """Filter for daily partitions that no longer receive writes (everything before today).

    Compacting only closed time partitions avoids commit conflicts with the
    streaming writer that is still appending to the current day.
    """
    cutoff = datetime(now.year, now.month, now.day)
    return f"{ts_column} < TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}'"


def fingerprint(spark: SparkSession, table: str) -> tuple:
    """Order-independent content checksum: compaction must never change it."""
    row = spark.sql(f"""
        SELECT count(*) AS n, round(sum(amount), 2) AS amount,
               sum(CAST(hash(event_id, user_id, event_type, country, amount, event_ts) AS BIGINT)) AS h
        FROM {table}
    """).first()
    return tuple(row)
