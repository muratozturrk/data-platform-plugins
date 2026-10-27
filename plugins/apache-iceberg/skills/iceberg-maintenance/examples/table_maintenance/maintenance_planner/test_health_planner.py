from datetime import date, datetime, timedelta, timezone

from common.clickstream import create_events_table, day, micro_batch_ingest
from suboptimal_files.workloads import apply_refunds, set_row_level_mode
from compaction.compaction import fingerprint
from retention_gdpr.retention import create_tag
from maintenance_planner.health import PartitionHealth, TableHealth, collect_health
from maintenance_planner.planner import Thresholds, execute, plan_maintenance

NOW = datetime(2025, 1, 12, 12, 0)
TODAY = date(2025, 1, 12)


def health(**overrides) -> TableHealth:
    """A healthy table; tests override one dimension at a time."""
    base = dict(
        table="db.events", ts_column="event_ts", data_files=30, small_files=0, delete_files=0,
        manifests=3, avg_files_per_manifest=10.0, snapshots=5,
        oldest_snapshot_at=NOW - timedelta(days=2), tag_snapshot_at={},
        partitions=[PartitionHealth(date(2025, 1, d), 10, 0, 0, 10 * 128 * 2**20) for d in (10, 11, 12)],
    )
    base.update(overrides)
    return TableHealth(**base)


# ---------- planner rules (pure, no Spark) ----------

def test_healthy_table_needs_nothing():
    assert plan_maintenance(health(), Thresholds(), NOW, TODAY) == []


def test_small_files_trigger_compaction_of_closed_partitions_only():
    parts = [PartitionHealth(date(2025, 1, d), 40, 40, 0, 40 * 2**20) for d in (10, 11, 12)]
    actions = plan_maintenance(health(partitions=parts), Thresholds(), NOW, TODAY)

    compactions = [a for a in actions if a.kind == "compact_partition"]
    assert [a.reason.split(":")[0] for a in compactions] == ["2025-01-10", "2025-01-11"]
    assert compactions[0].params["where"] == (
        "event_ts >= TIMESTAMP '2025-01-10 00:00:00' AND event_ts < TIMESTAMP '2025-01-11 00:00:00'")


def test_delete_file_buildup_triggers_compaction_with_delete_handling():
    parts = [PartitionHealth(date(2025, 1, 10), 10, 0, 5, 10 * 128 * 2**20)]
    [action] = plan_maintenance(health(partitions=parts), Thresholds(), NOW, TODAY)
    assert action.kind == "compact_partition" and action.params["with_deletes"] is True


def test_thin_manifests_trigger_metadata_compaction():
    actions = plan_maintenance(health(manifests=50, avg_files_per_manifest=1.5), Thresholds(), NOW, TODAY)
    assert [a.kind for a in actions] == ["rewrite_manifests"]


def test_old_history_is_expired_but_minimum_kept():
    old = health(snapshots=200, oldest_snapshot_at=NOW - timedelta(days=30))
    [action] = plan_maintenance(old, Thresholds(), NOW, TODAY)
    assert action.kind == "expire_snapshots"
    assert action.params == {"older_than": NOW - timedelta(days=7), "retain_last": 10}

    few = health(snapshots=8, oldest_snapshot_at=NOW - timedelta(days=30))
    assert plan_maintenance(few, Thresholds(), NOW, TODAY) == []


def test_old_tags_are_flagged_never_dropped():
    tagged = health(tag_snapshot_at={"fy2023_close": NOW - timedelta(days=400),
                                     "last_week": NOW - timedelta(days=7)})
    [action] = plan_maintenance(tagged, Thresholds(), NOW, TODAY)
    assert action.kind == "review_tag" and "fy2023_close" in action.reason


# ---------- end to end against the lakehouse ----------

def test_monitor_plan_execute_converges_to_healthy(spark, ns):
    table = f"{ns}.events"
    create_events_table(spark, table)
    for d in range(3):                                     # day(2) is "today", still open
        micro_batch_ingest(spark, table, 6, 2_000, day(d), 4, seed=10 * d, start_id=d * 12_000)
    set_row_level_mode(spark, table, "merge-on-read")
    apply_refunds(spark, table, every_nth_checkout=10)
    create_tag(spark, table, "backfill_2024")
    fp = fingerprint(spark, table)

    wall_now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=1)
    t = Thresholds(min_small_files_per_partition=10, min_manifests_to_rewrite=5,
                   snapshot_retention=timedelta(0), min_snapshots_to_keep=3,
                   max_tag_age=timedelta(0))
    before = collect_health(spark, table)
    actions = plan_maintenance(before, t, wall_now, day(2).date())
    kinds = sorted(a.kind for a in actions)

    assert kinds == ["compact_partition", "compact_partition", "expire_snapshots",
                     "review_tag", "rewrite_manifests"]

    log = execute(spark, before, actions, t)
    after = collect_health(spark, table)

    assert any(line.startswith("NEEDS REVIEW: tag 'backfill_2024'") for line in log)
    closed = [p for p in after.partitions if p.day < day(2).date()]
    assert all(p.data_files == 1 and p.delete_files == 0 for p in closed)
    open_day = next(p for p in after.partitions if p.day == day(2).date())
    assert open_day.data_files >= 24                        # untouched: still the writer's partition
    assert after.manifests < before.manifests
    # Everything older than the plan's cutoff is gone except the tagged snapshot.
    # Snapshots created by this run's own compactions survive until the next run
    # (the "sawtooth" pattern of healthy maintenance).
    pre_cutoff = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots "
                           f"WHERE committed_at < TIMESTAMP '{wall_now:%Y-%m-%d %H:%M:%S.%f}'").collect()
    assert len(pre_cutoff) == 1 and after.snapshots < before.snapshots
    assert fingerprint(spark, table) == fp

    # Second run: only the open partition and the tag remain, nothing to compact.
    again = plan_maintenance(after, t, wall_now, day(2).date())
    assert all(a.kind in ("review_tag", "rewrite_manifests", "expire_snapshots") for a in again)
    assert not [a for a in again if a.kind == "compact_partition"]
