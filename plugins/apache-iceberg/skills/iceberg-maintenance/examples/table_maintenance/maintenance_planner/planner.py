"""Turn TableHealth into maintenance actions, then execute them.

plan_maintenance() is pure (no Spark), so the decision logic is unit-testable and
can run in a cheap scheduler task; execute() calls the compaction and retention helpers.
Rules follow common operational guidance:
  - compact only closed partitions (no collisions with the active writer)
  - compact a partition when small files or delete files pile up
  - rewrite manifests when many thin manifests track few files
  - expire snapshots past the retention window, keep a minimum history
  - never auto-drop tags: flag old ones for review (they may block GDPR erasure)
"""
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession

from compaction.compaction import CompactionPolicy, compact_mor_table, rewrite_manifests
from retention_gdpr.retention import expire_snapshots
from maintenance_planner.health import TableHealth

MiB = 1024 * 1024


@dataclass(frozen=True)
class Thresholds:
    min_small_files_per_partition: int = 10
    max_delete_files_per_data_file: float = 0.2
    min_manifests_to_rewrite: int = 10
    min_avg_files_per_manifest: float = 20.0
    snapshot_retention: timedelta = timedelta(days=7)
    min_snapshots_to_keep: int = 10
    max_tag_age: timedelta = timedelta(days=90)
    target_file_size_bytes: int = 128 * MiB


@dataclass(frozen=True)
class Action:
    kind: str                      # compact_partition | rewrite_manifests | expire_snapshots | review_tag
    reason: str
    params: dict = field(default_factory=dict)


def _day_filter(ts_column: str, day: date) -> str:
    return (f"{ts_column} >= TIMESTAMP '{day:%Y-%m-%d} 00:00:00' AND "
            f"{ts_column} < TIMESTAMP '{day + timedelta(days=1):%Y-%m-%d} 00:00:00'")


def plan_maintenance(health: TableHealth, t: Thresholds, now: datetime, current_day: date) -> list[Action]:
    actions: list[Action] = []

    for p in health.partitions:
        if p.day >= current_day:
            continue                                   # open partition: the writer is still active
        delete_ratio = p.delete_files / p.data_files if p.data_files else 0.0
        reasons = []
        if p.small_files >= t.min_small_files_per_partition:
            reasons.append(f"{p.small_files} small files")
        if delete_ratio > t.max_delete_files_per_data_file:
            reasons.append(f"{p.delete_files} delete files for {p.data_files} data files")
        if reasons:
            actions.append(Action("compact_partition", f"{p.day}: " + ", ".join(reasons),
                                  {"where": _day_filter(health.ts_column, p.day),
                                   "with_deletes": p.delete_files > 0}))

    if (health.manifests >= t.min_manifests_to_rewrite
            and health.avg_files_per_manifest < t.min_avg_files_per_manifest):
        actions.append(Action("rewrite_manifests",
                              f"{health.manifests} manifests, {health.avg_files_per_manifest:.1f} files each"))

    cutoff = now - t.snapshot_retention
    if (health.oldest_snapshot_at is not None and health.oldest_snapshot_at < cutoff
            and health.snapshots > t.min_snapshots_to_keep):
        actions.append(Action("expire_snapshots",
                              f"{health.snapshots} snapshots, oldest {health.oldest_snapshot_at:%Y-%m-%d %H:%M}",
                              {"older_than": cutoff, "retain_last": t.min_snapshots_to_keep}))

    for name, committed_at in sorted(health.tag_snapshot_at.items()):
        if now - committed_at > t.max_tag_age:
            actions.append(Action("review_tag", f"tag '{name}' pins a snapshot from {committed_at:%Y-%m-%d}"))

    return actions


def execute(spark: SparkSession, health: TableHealth, actions: list[Action], t: Thresholds) -> list[str]:
    """Run actions in a safe order: data compaction, metadata compaction, expiration."""
    order = {"compact_partition": 0, "rewrite_manifests": 1, "expire_snapshots": 2, "review_tag": 3}
    log = []
    for a in sorted(actions, key=lambda a: order[a.kind]):
        if a.kind == "compact_partition":
            policy = CompactionPolicy(where=a.params["where"], min_input_files=2,
                                      target_file_size_bytes=t.target_file_size_bytes,
                                      delete_file_threshold=1 if a.params["with_deletes"] else None)
            result, purged = compact_mor_table(spark, health.table, policy)
            log.append(f"compacted {result.rewritten_data_files} -> {result.added_data_files} files, "
                       f"purged {purged} delete files ({a.reason})")
        elif a.kind == "rewrite_manifests":
            rewritten, added = rewrite_manifests(spark, health.table)
            log.append(f"manifests {rewritten} -> {added}")
        elif a.kind == "expire_snapshots":
            r = expire_snapshots(spark, health.table, a.params["older_than"], a.params["retain_last"])
            log.append(f"expired snapshots, deleted {r.deleted_data_files} data files")
        else:
            log.append(f"NEEDS REVIEW: {a.reason}")
    return log
