"""Audit trail from Iceberg metadata .

- change_log():     one row per snapshot with record counts and the job that committed it
- files_changed():  drill down from a suspicious snapshot to the exact files it added/removed
- schema_history(): schema changes are metadata versions, not snapshots
- commit_baseline(): a 30-day commit-rate query, used to flag unusual activity
"""
import json
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from common.lakehouse import read_object

JOB_PROPERTY = "audit.job-id"        # custom snapshot summary keys written by every pipeline
RUN_PROPERTY = "audit.run-id"


def write_with_audit(df: DataFrame, table: str, job_id: str, run_id: str, mode: str = "append") -> None:
    """Attach job/run identity to the snapshot itself (Iceberg `snapshot-property.<key>` write option)."""
    writer = (df.writeTo(table)
              .option(f"snapshot-property.{JOB_PROPERTY}", job_id)
              .option(f"snapshot-property.{RUN_PROPERTY}", run_id))
    writer.append() if mode == "append" else writer.overwritePartitions()


@dataclass(frozen=True)
class Change:
    snapshot_id: int
    parent_id: int | None
    committed_at: datetime
    operation: str
    added_records: int
    deleted_records: int
    job_id: str | None
    run_id: str | None

    @property
    def lossy(self) -> bool:
        """An overwrite that removed more rows than it wrote back (e.g. 50,000 removed vs 48,000 written)."""
        return self.operation == "overwrite" and self.deleted_records > self.added_records


def change_log(spark: SparkSession, table: str) -> list[Change]:
    rows = spark.sql(f"""
        SELECT snapshot_id, parent_id, committed_at, operation,
               cast(coalesce(summary['added-records'], '0') AS BIGINT)   AS added,
               cast(coalesce(summary['deleted-records'], '0') AS BIGINT) AS deleted,
               summary['{JOB_PROPERTY}'] AS job_id, summary['{RUN_PROPERTY}'] AS run_id
        FROM {table}.snapshots ORDER BY committed_at""").collect()
    return [Change(r.snapshot_id, r.parent_id, r.committed_at, r.operation, r.added, r.deleted,
                   r.job_id, r.run_id) for r in rows]


def files_changed(spark: SparkSession, table: str, snapshot_id: int) -> dict[str, set[str]]:
    """Files added and removed by one snapshot: diff of the files metadata table at parent vs snapshot."""
    parent = spark.sql(f"SELECT parent_id FROM {table}.snapshots WHERE snapshot_id = {snapshot_id}").first().parent_id

    def files_at(sid):
        if sid is None:
            return set()
        return {r.file_path for r in spark.sql(f"SELECT file_path FROM {table}.files VERSION AS OF {sid}").collect()}

    before, after = files_at(parent), files_at(snapshot_id)
    return {"added": after - before, "removed": before - after}


def schema_history(spark: SparkSession, table: str) -> list[dict]:
    """One entry per metadata.json version where the table schema changed.

    metadata_log_entries.latest_schema_id is the schema of the latest *snapshot*, so a schema change
    without a data commit does not show up there. The metadata.json files themselves are authoritative.
    """
    rows = spark.sql(f"SELECT timestamp, file FROM {table}.metadata_log_entries ORDER BY timestamp").collect()
    out, last = [], None
    for r in rows:
        meta = json.loads(read_object(r.file))
        if meta["current-schema-id"] != last:
            schema = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
            out.append({"at": r.timestamp, "schema_id": meta["current-schema-id"],
                        "snapshot_id": meta.get("current-snapshot-id"),
                        "columns": [f["name"] for f in schema["fields"]], "metadata_file": r.file})
            last = meta["current-schema-id"]
    return out


def commit_baseline(spark: SparkSession, table: str, days: int = 30) -> dict:
    """Commit-rate baseline for detecting unusual activity, with per-operation counts."""
    r = spark.sql(f"""
        SELECT count(*) AS commits, min(committed_at) AS first_commit, max(committed_at) AS last_commit,
               count_if(operation = 'append') AS appends, count_if(operation = 'overwrite') AS overwrites,
               count_if(operation = 'delete') AS deletes
        FROM {table}.snapshots
        WHERE committed_at > current_timestamp() - INTERVAL {days} DAYS""").first()
    return r.asDict()


def unexpected_commits(changes: list[Change], allowed_jobs: set[str]) -> list[Change]:
    """Commits not attributed to a known pipeline: manual fixes, misconfigured jobs or worse."""
    return [c for c in changes if c.job_id not in allowed_jobs]
