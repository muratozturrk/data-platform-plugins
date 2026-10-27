"""Monitoring table health with metadata tables."""
from datetime import datetime, timedelta, timezone

from common.reference_queries import run
from common.history import build_history
from common.metaquery import compaction_trigger, health_report, snapshot_growth, snapshots_since


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=1)


def test_naive_small_file_trigger_counts_delete_files(spark, hist):
    naive = run(spark, "small-file-ratio", hist.table)[0]
    fixed = run(spark, "small-file-ratio", hist.table, fixed=True)[0]
    assert (tuple(naive), tuple(fixed)) == ((8, 8), (6, 6))
    sig = compaction_trigger(spark, hist.table)
    assert (sig.total_files, sig.small_files, sig.ratio, sig.trigger) == (6, 6, 1.0, True)
    none_small = compaction_trigger(spark, hist.table, small_bytes=1000)
    assert (none_small.small_files, none_small.trigger) == (0, False)


def test_snapshot_frequency_query(spark, hist):
    assert run(spark, "snapshots-last-24h", hist.table)[0][0] == 4
    assert snapshots_since(spark, hist.table) == 4
    assert snapshots_since(spark, hist.table, now=datetime(2100, 1, 1)) == 0


def test_snapshot_growth_from_summaries(spark, hist):
    growth = [(g["snapshot_id"], g["operation"], g["total_data_files"], g["total_delete_files"],
               g["total_records"]) for g in snapshot_growth(spark, hist.table)]
    # total-records counts rows in data files: position deletes do not lower it.
    assert growth == [(hist.s3, "delete", 5, 2, 50), (hist.s4, "overwrite", 5, 2, 50),
                      (hist.s5, "append", 6, 2, 60), (hist.s6, "append", 7, 2, 65)]


def test_health_report_gauges(spark, hist):
    r = health_report(spark, hist.table)
    assert {k: r[k] for k in ("snapshots", "metadata_files", "tags", "branches", "manifests_data_manifests",
                              "manifests_delete_manifests", "data_files", "small_data_files",
                              "compaction_needed", "live_delete_files", "reachable_delete_files",
                              "snapshots_last_24h")} == {
        "snapshots": 4, "metadata_files": 11, "tags": 1, "branches": 2,
        "manifests_data_manifests": 2, "manifests_delete_manifests": 1,
        "data_files": 6, "small_data_files": 6, "compaction_needed": True,
        "live_delete_files": 2, "reachable_delete_files": 2, "snapshots_last_24h": 4}


def test_gauges_react_to_maintenance_and_refs_block_cleanup(spark, ns):
    h = build_history(spark, f"{ns}.orders_maint")
    t = h.table
    spark.sql(f"""CALL lake.system.rewrite_data_files(table => '{t}',
                  options => map('delete-file-threshold', '1', 'min-input-files', '1'))""")
    spark.sql(f"CALL lake.system.rewrite_position_delete_files(table => '{t}', options => map('rewrite-all', 'true'))")
    spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', "
              f"older_than => TIMESTAMP '{_now():%Y-%m-%d %H:%M:%S}', retain_last => 1)")
    after = health_report(spark, t)
    # Deletes are gone from main, but the tag and branch still pin snapshots that use them.
    assert (after["live_delete_files"], after["reachable_delete_files"]) == (0, 2)
    assert after["snapshots"] == 3                     # main head + audit_s4 (S4) + dev head (S6)

    spark.sql(f"ALTER TABLE {t} DROP TAG audit_s4")
    spark.sql(f"ALTER TABLE {t} DROP BRANCH dev")
    spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', "
              f"older_than => TIMESTAMP '{_now():%Y-%m-%d %H:%M:%S}', retain_last => 1)")
    clean = health_report(spark, t)
    assert (clean["live_delete_files"], clean["reachable_delete_files"], clean["snapshots"],
            clean["tags"], clean["branches"]) == (0, 0, 1, 0, 1)
    assert spark.sql(f"SELECT count(*) AS n FROM {t}").first().n == 57      # 60 rows - 3 deleted
