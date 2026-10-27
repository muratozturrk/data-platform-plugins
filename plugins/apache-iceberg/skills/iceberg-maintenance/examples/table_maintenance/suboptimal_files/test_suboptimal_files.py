import pytest

from common.clickstream import create_events_table, day
from suboptimal_files.diagnose import diagnose, files_touched_by_lookup, last_commit_stats
from suboptimal_files.workloads import apply_refunds, build_streaming_table, set_row_level_mode

BATCHES, ROWS, FILES_PER_BATCH = 20, 2_000, 4


@pytest.fixture(scope="module")
def streaming_table(spark, ns):
    table = f"{ns}.events_stream"
    build_streaming_table(spark, table, BATCHES, ROWS, day(0), FILES_PER_BATCH)
    return table


def test_micro_batches_create_small_files_and_metadata_sprawl(spark, streaming_table):
    d = diagnose(spark, streaming_table)

    assert d.data_files == BATCHES * FILES_PER_BATCH       # 80 files for 40k rows
    assert d.small_file_ratio == 1.0                        # every file far below 32 MiB
    assert d.snapshots == BATCHES                           # one snapshot per commit
    assert d.manifests >= BATCHES                           # appends do not merge manifests yet
    assert d.avg_files_per_manifest <= FILES_PER_BATCH      # many thin manifests
    assert d.metadata_json_versions >= BATCHES              # metadata.json rewritten per commit


def test_poor_colocation_forces_reading_almost_every_file(spark, ns, streaming_table):
    touched, total = files_touched_by_lookup(spark, streaming_table, "user_id", 42)
    assert touched / total > 0.9, "random arrival order: user 42 may be in any file"

    # Same rows, clustered by user_id: min/max stats now prune most files.
    clustered = f"{ns}.events_clustered"
    create_events_table(spark, clustered)
    (spark.table(streaming_table).repartitionByRange(8, "user_id")
     .sortWithinPartitions("user_id").writeTo(clustered).append())

    touched_c, total_c = files_touched_by_lookup(spark, clustered, "user_id", 42)
    assert total_c == 8
    assert touched_c <= 2


def test_cow_rewrites_files_while_mor_writes_small_delete_files(spark, ns):
    results = {}
    for mode in ("copy-on-write", "merge-on-read"):
        table = f"{ns}.refunds_{mode.replace('-', '_')}"
        build_streaming_table(spark, table, 10, ROWS, day(0), FILES_PER_BATCH)
        set_row_level_mode(spark, table, mode)
        corrected = apply_refunds(spark, table, every_nth_checkout=50)
        results[mode] = (corrected, last_commit_stats(spark, table), diagnose(spark, table))

    (cow_n, cow, cow_d), (mor_n, mor, _) = results["copy-on-write"], results["merge-on-read"]
    assert cow_n == mor_n > 0

    # COW: affected data files rewritten in full, no delete files.
    assert cow["deleted-data-files"] > 0 and cow["added-delete-files"] == 0
    assert cow_d.position_delete_files == 0
    # MOR: only new rows + position delete files, original files kept.
    assert mor["deleted-data-files"] == 0 and mor["added-delete-files"] > 0
    assert mor["added-position-deletes"] == mor_n
    assert mor["added-files-size"] < cow["added-files-size"]

    # Both modes must expose the same logical table.
    totals = [spark.sql(f"SELECT round(sum(amount), 2) s FROM {ns}.refunds_{m}").first().s
              for m in ("copy_on_write", "merge_on_read")]
    assert totals[0] == totals[1]


def _mor_update_history(spark, table, commits=4, **props):
    """Run N refund MERGEs on a MOR table; return (live delete files, removed delete files) per commit."""
    build_streaming_table(spark, table, 5, ROWS, day(0), FILES_PER_BATCH, **props)
    set_row_level_mode(spark, table, "merge-on-read")
    history = []
    for i in range(commits):
        apply_refunds(spark, table, every_nth_checkout=20, refund_tag=i)
        removed = int(spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1")
                      .first().summary.get("removed-delete-files", 0))
        history.append((diagnose(spark, table).position_delete_files, removed))
    return history


def test_v2_partition_scoped_deletes_accumulate_with_every_commit(spark, ns):
    """The classic growth pattern: one more delete file per commit, never removed."""
    history = _mor_update_history(spark, f"{ns}.mor_v2_partition", **{"write.delete.granularity": "partition"})
    live = [h[0] for h in history]
    assert live == [1, 2, 3, 4]
    assert all(removed == 0 for _, removed in history)


def test_v2_file_scoped_deletes_are_replaced_not_stacked(spark, ns):
    """Iceberg 1.8 default (file granularity): each commit replaces the previous delete file
    of a data file, so live delete files stay bounded by the data files they cover."""
    history = _mor_update_history(spark, f"{ns}.mor_v2_file", **{"write.delete.granularity": "file"})
    first_live = history[0][0]
    assert all(live <= first_live + 2 for live, _ in history)    # bounded, not linear
    assert all(removed > 0 for _, removed in history[1:])        # old delete files replaced


def test_v3_deletion_vectors_keep_one_delete_artifact_per_data_file(spark, ns):
    table = f"{ns}.mor_v3"
    build_streaming_table(spark, table, 5, ROWS, day(0), FILES_PER_BATCH, **{"format-version": "3"})
    set_row_level_mode(spark, table, "merge-on-read")

    for i in range(4):
        apply_refunds(spark, table, every_nth_checkout=20, refund_tag=i)

    d = diagnose(spark, table)
    assert last_commit_stats(spark, table)["added-dvs"] > 0
    # Live deletion vectors are bounded by the number of data files they belong to.
    assert d.position_delete_files <= d.data_files
