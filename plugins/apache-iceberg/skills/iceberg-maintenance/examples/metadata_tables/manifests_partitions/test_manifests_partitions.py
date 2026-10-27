"""manifests, all_manifests and partitions."""
from collections import Counter

import pytest

from common.reference_queries import REFERENCE_QUERIES, run
from common.metaquery import manifest_stats, partition_skew

FEB_2025 = (2025 - 1970) * 12 + 1        # months(order_date) is stored as months since 1970-01


def test_current_manifests_show_content_spec_and_origin(spark, hist):
    m = [r.asDict(recursive=True) for r in spark.sql(
        f"SELECT * FROM {hist.table}.manifests ORDER BY content, partition_spec_id").collect()]
    assert [(r["content"], r["partition_spec_id"], r["added_snapshot_id"]) for r in m] == [
        (0, 0, hist.s5), (0, 1, hist.s5), (1, 0, hist.s3)]
    assert [(r["added_data_files_count"], r["existing_data_files_count"],
             r["added_delete_files_count"]) for r in m] == [(0, 5, 0), (1, 0, 0), (0, 0, 2)]
    # Partition summaries are human-readable strings, one struct per partition field.
    assert [(s["lower_bound"], s["upper_bound"]) for s in m[1]["partition_summaries"]] == [
        ("EU", "EU"), ("2025-02", "2025-02")]
    assert [(s["lower_bound"], s["upper_bound"]) for s in m[0]["partition_summaries"]] == [("APAC", "US")]


def test_manifest_stats_library(spark, hist):
    s = manifest_stats(spark, hist.table)
    assert {k: s[k] for k in ("data_manifests", "delete_manifests", "tracked_data_files",
                              "tracked_delete_files", "avg_data_files_per_manifest")} == {
        "data_manifests": 2, "delete_manifests": 1, "tracked_data_files": 6,
        "tracked_delete_files": 2, "avg_data_files_per_manifest": 3.0}
    assert 0 < s["manifest_bytes"] < 3 * 16 * 1024


def test_all_manifests_has_one_row_per_manifest_and_referencing_snapshot(spark, hist):
    rows = spark.sql(f"SELECT * FROM {hist.table}.all_manifests").collect()
    assert len(rows) == 14
    assert len({r.path for r in rows}) == 8
    # reference_snapshot_id = the retained snapshot whose manifest list contains the manifest.
    assert Counter(r.reference_snapshot_id for r in rows) == {hist.s3: 3, hist.s4: 4, hist.s5: 3, hist.s6: 4}
    # Not "the snapshot that created it": manifests added by expired S1/S2 are still listed.
    from_expired = {(r.added_snapshot_id, r.reference_snapshot_id) for r in rows
                    if r.added_snapshot_id in hist.expired}
    assert from_expired == {(hist.s1, hist.s3), (hist.s2, hist.s3), (hist.s2, hist.s4)}


def test_partitions_report_per_spec_stats(spark, hist):
    parts = {(r.partition.region, r.partition.order_date_month, r.spec_id): r for r in
             spark.sql(f"SELECT * FROM {hist.table}.partitions").collect()}
    assert {k: (r.record_count, r.file_count, r.position_delete_record_count,
                r.position_delete_file_count, r.equality_delete_file_count) for k, r in parts.items()} == {
        ("EU", None, 0): (20, 2, 3, 2, 0),
        ("US", None, 0): (20, 2, 0, 0, 0),
        ("APAC", None, 0): (10, 1, 0, 0, 0),
        ("EU", FEB_2025, 1): (10, 1, 0, 0, 0),
    }
    # record_count counts rows in data files: the 3 deleted EU rows are still included.
    live_eu = spark.sql(f"SELECT count(*) AS n FROM {hist.table} WHERE region = 'EU' "
                        "AND order_date < DATE '2025-02-01'").first().n
    assert live_eu == 17
    # last_updated_* is NULL when the snapshot that last touched the partition expired.
    assert {k: r.last_updated_snapshot_id for k, r in parts.items()} == {
        ("EU", None, 0): hist.s3, ("US", None, 0): None,
        ("APAC", None, 0): hist.s4, ("EU", FEB_2025, 1): hist.s5}
    assert parts[("US", None, 0)].last_updated_at is None


def test_partition_skew_library(spark, hist):
    skew = [(r["partition"]["region"], r["spec_id"], r["record_count"], r["delete_file_count"], r["skew_factor"])
            for r in partition_skew(spark, hist.table)]
    assert sorted(skew[:2]) == [("EU", 0, 20, 2, 1.33), ("US", 0, 20, 0, 1.33)]
    assert sorted(skew[2:]) == [("APAC", 0, 10, 0, 0.67), ("EU", 1, 10, 0, 0.67)]


@pytest.mark.parametrize("key", ["largest-manifests", "top-partitions-by-files"])
def test_reference_queries_run_as_printed(spark, hist, key):
    assert REFERENCE_QUERIES[key].status == "as-is"
    result = run(spark, key, hist.table)
    if key == "largest-manifests":
        assert len(result) == 3
        assert [r.length for r in result] == sorted((r.length for r in result), reverse=True)
    else:
        assert len(result) == 4
        assert [r.file_count for r in result] == [2, 2, 1, 1]
