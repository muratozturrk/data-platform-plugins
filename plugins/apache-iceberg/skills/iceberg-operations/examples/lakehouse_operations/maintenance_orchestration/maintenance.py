"""Metadata-aware, per-table Iceberg maintenance.

Split into three layers so an orchestrator can call each one as a task:
  collect_metrics()  reads metadata tables only (cheap, no data scan)
  plan()             pure function: metrics + policy -> which procedures to run
  compact()/expire() run the Spark procedures and return what they did
Metrics go to a Prometheus Pushgateway.
"""
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession

from common.lakehouse import CATALOG

MiB = 1024 * 1024
DEFAULT_POLICIES = Path(__file__).with_name("table_policies.json")


@dataclass(frozen=True)
class TablePolicy:
    small_file_threshold_mb: int = 20        # files below 20 MB count as small
    max_small_files: int = 500               # compact when more than 500
    max_delete_files: int = 50
    target_file_size_mb: int = 128
    retain_last: int = 10
    max_snapshot_age_hours: int = 24 * 7     # 7-day snapshot retention
    max_snapshots: int = 100


def load_policies(path: str | os.PathLike | None = None) -> dict[str, TablePolicy]:
    """Central per-table policy file: {"ns.table": {...overrides...}}."""
    path = path or os.getenv("TABLE_POLICIES", DEFAULT_POLICIES)
    return {table: TablePolicy(**cfg) for table, cfg in json.loads(Path(path).read_text()).items()}


def collect_metrics(spark: SparkSession, table: str, policy: TablePolicy) -> dict:
    """Health indicators from the files and snapshots metadata tables (JSON-serializable for XCom)."""
    small = policy.small_file_threshold_mb * MiB
    f = spark.sql(f"""
        SELECT count_if(content = 0)                                   AS data_files,
               count_if(content = 0 AND file_size_in_bytes < {small})  AS small_files,
               count_if(content > 0)                                   AS delete_files,
               coalesce(sum(file_size_in_bytes), 0)                    AS total_bytes
        FROM {table}.files""").first()
    s = spark.sql(f"""
        SELECT count(*) AS snapshots,
               coalesce((unix_timestamp(current_timestamp()) - unix_timestamp(min(committed_at))) / 3600.0, 0)
                 AS oldest_snapshot_age_hours
        FROM {table}.snapshots""").first()
    return {"table": table, "data_files": f.data_files, "small_files": f.small_files,
            "delete_files": f.delete_files, "total_bytes": int(f.total_bytes),
            "snapshots": s.snapshots, "oldest_snapshot_age_hours": float(s.oldest_snapshot_age_hours)}


def plan(metrics: dict, policy: TablePolicy) -> list[str]:
    """Which procedures this table needs now. Order matters: compaction first, then expiration."""
    actions = []
    if metrics["small_files"] > policy.max_small_files or metrics["delete_files"] > policy.max_delete_files:
        actions.append("compact")
    if (metrics["snapshots"] > policy.retain_last
            and (metrics["oldest_snapshot_age_hours"] > policy.max_snapshot_age_hours
                 or metrics["snapshots"] > policy.max_snapshots)):
        actions.append("expire")
    return actions


def compact(spark: SparkSession, table: str, policy: TablePolicy) -> dict:
    """Bin-pack small files and materialize deletes; then drop dangling position deletes (Iceberg 1.8.1)."""
    target = policy.target_file_size_mb * MiB
    r = spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(table => '{table}', options => map(
          'target-file-size-bytes', '{target}', 'min-input-files', '2',
          'delete-file-threshold', '1', 'partial-progress.enabled', 'true'))""").first()
    d = spark.sql(f"""CALL {CATALOG}.system.rewrite_position_delete_files(
          table => '{table}', options => map('rewrite-all', 'true'))""").first()
    return {"rewritten_data_files": r.rewritten_data_files_count, "added_data_files": r.added_data_files_count,
            "removed_delete_files": d.rewritten_delete_files_count}


def expire(spark: SparkSession, table: str, policy: TablePolicy, metrics: dict) -> dict:
    """Expire by age (or everything beyond retain_last when the count limit is breached)."""
    hours = 0 if metrics["snapshots"] > policy.max_snapshots else policy.max_snapshot_age_hours
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours) + timedelta(seconds=1)
    r = spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
          table => '{table}',
          older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}',
          retain_last => {policy.retain_last})""").first()
    return {"deleted_data_files": r.deleted_data_files_count,
            "deleted_manifest_lists": r.deleted_manifest_lists_count}


def push_metrics(metrics: dict, gateway: str | None = None, job: str = "iceberg_table_maintenance",
                 extra: dict[str, float] | None = None) -> None:
    """Push one gauge per indicator, labeled by table (scraped by Prometheus, alerts in Alertmanager)."""
    registry = CollectorRegistry()
    values = {k: v for k, v in metrics.items() if isinstance(v, (int, float))} | (extra or {})
    for name, value in values.items():
        Gauge(f"iceberg_{name}", f"Iceberg table indicator {name}", ["table"],
              registry=registry).labels(table=metrics["table"]).set(value)
    push_to_gateway(gateway or os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091"), job=job,
                    registry=registry, grouping_key={"table": metrics["table"]})


