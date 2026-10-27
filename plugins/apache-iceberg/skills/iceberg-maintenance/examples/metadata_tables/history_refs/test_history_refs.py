"""history, snapshots, metadata_log_entries and refs."""
from datetime import datetime, timedelta, timezone

import pytest

from common.reference_queries import REFERENCE_QUERIES, run
from common.history import build_history, current_snapshot, rows
from common.metaquery import change_audit, protected_snapshot_ids


def _history(spark, table):
    return [r.asDict() for r in spark.sql(
        f"SELECT snapshot_id, parent_id, is_current_ancestor FROM {table}.history "
        "ORDER BY made_current_at").collect()]


def test_history_is_the_main_lineage_after_expiration(spark, hist):
    h = _history(spark, hist.table)
    assert [r["snapshot_id"] for r in h] == hist.main_lineage          # S3, S4, S5
    assert all(r["is_current_ancestor"] for r in h)
    assert h[0]["parent_id"] == hist.s2                                # points to an expired snapshot
    assert [r["parent_id"] for r in h[1:]] == [hist.s3, hist.s4]
    assert hist.s6 not in {r["snapshot_id"] for r in h}                # branch commit: not in history


def test_snapshots_record_operation_and_summary(spark, hist):
    snaps = {r.snapshot_id: r for r in spark.sql(f"SELECT * FROM {hist.table}.snapshots").collect()}
    assert {sid: snaps[sid].operation for sid in snaps} == {
        hist.s3: "delete", hist.s4: "overwrite", hist.s5: "append", hist.s6: "append"}
    assert snaps[hist.s6].parent_id == hist.s5                         # branch forks from main
    s3, s4 = snaps[hist.s3].summary, snaps[hist.s4].summary
    assert (s3["added-position-deletes"], s3["added-delete-files"], s3["total-records"]) == ("3", "2", "50")
    assert (s4["replace-partitions"], s4["deleted-data-files"], s4["added-data-files"]) == ("true", "1", "1")
    assert s4["spark.app.id"] == spark.sparkContext.applicationId
    assert all(r.manifest_list.endswith(".avro") for r in snaps.values())


def test_change_audit_covers_all_refs(spark, hist):
    audit = change_audit(spark, hist.table)
    view = [(r["snapshot_id"], r["operation"], r["in_main_history"], r["refs"],
             r["added_records"], r["deleted_records"], r["added_position_deletes"]) for r in audit]
    assert view == [
        (hist.s3, "delete", True, [], 0, 0, 3),
        (hist.s4, "overwrite", True, ["audit_s4"], 10, 10, 0),
        (hist.s5, "append", True, ["main"], 10, 0, 0),
        (hist.s6, "append", False, ["dev"], 5, 0, 0),
    ]
    assert {r["app_id"] for r in audit} == {spark.sparkContext.applicationId}


def test_rollback_leaves_non_ancestor_snapshots_in_history(spark, ns):
    t = f"{ns}.rollback"
    spark.sql(f"CREATE TABLE {t} (id BIGINT, region STRING, amount DOUBLE, order_date DATE) USING iceberg")
    ids = []
    for lo in (1, 11, 21):
        spark.sql(f"INSERT INTO {t} {rows(lo, lo + 9)}")
        ids.append(current_snapshot(spark, t))
    spark.sql(f"CALL lake.system.rollback_to_snapshot('{t}', {ids[0]})")
    h = _history(spark, t)
    assert [(r["snapshot_id"], r["is_current_ancestor"]) for r in h] == [
        (ids[0], True), (ids[1], False), (ids[2], False), (ids[0], True)]
    # The abandoned snapshots are still listed (and time-travelable) until they expire.
    assert spark.sql(f"SELECT count(*) AS n FROM {t}.snapshots").first().n == 3


def test_metadata_log_tracks_every_metadata_file(spark, hist):
    log = [r.asDict() for r in spark.sql(
        f"SELECT * FROM {hist.table}.metadata_log_entries ORDER BY timestamp").collect()]
    assert len(log) == 11
    assert all(r["file"].endswith(".metadata.json") for r in log)
    assert [r["file"].rsplit("/", 1)[1][:5] for r in log] == [f"{i:05d}" for i in range(11)]
    # Create + S1 + S2: NULL, because the snapshots were expired (or did not exist yet).
    assert [r["latest_snapshot_id"] for r in log[:3]] == [None, None, None]
    # Commits without a new main snapshot (tag, spec change, branch, dev commit, expire)
    # repeat main's latest snapshot id.
    assert [r["latest_snapshot_id"] for r in log[3:]] == [hist.s3, hist.s4, hist.s4, hist.s4,
                                                          hist.s5, hist.s5, hist.s5, hist.s5]
    assert [r["latest_sequence_number"] for r in log[3:]] == [3, 4, 4, 4, 5, 5, 5, 5]
    assert {r["latest_schema_id"] for r in log[3:]} == {0}


def test_metadata_log_is_capped_but_history_is_not(spark, ns):
    """The 'most recent 100' limit applies to the metadata log, not to history."""
    t = f"{ns}.capped"
    spark.sql(f"""CREATE TABLE {t} (id BIGINT, region STRING, amount DOUBLE, order_date DATE)
                  USING iceberg TBLPROPERTIES ('write.metadata.previous-versions-max'='3')""")
    for lo in range(1, 61, 10):
        spark.sql(f"INSERT INTO {t} {rows(lo, lo + 9)}")
    n = lambda m: spark.sql(f"SELECT count(*) AS n FROM {t}.{m}").first().n   # noqa: E731
    assert (n("history"), n("snapshots"), n("metadata_log_entries")) == (6, 6, 4)


def test_refs_pin_snapshots_through_expiration(spark, ns):
    h = build_history(spark, f"{ns}.orders_refs")
    t = h.table
    spark.sql(f"ALTER TABLE {t} CREATE TAG eoy_2024 AS OF VERSION {h.s3} RETAIN 7 DAYS")
    # REPLACE BRANCH without AS OF VERSION also moves the branch to main's head.
    spark.sql(f"ALTER TABLE {t} REPLACE BRANCH dev WITH SNAPSHOT RETENTION 2 SNAPSHOTS")
    assert current_snapshot(spark, t, "dev") == h.s5
    spark.sql(f"ALTER TABLE {t} REPLACE BRANCH dev AS OF VERSION {h.s6} WITH SNAPSHOT RETENTION 2 SNAPSHOTS")
    refs = {r.name: r for r in spark.sql(f"SELECT * FROM {t}.refs").collect()}
    assert {n: r.type for n, r in refs.items()} == {
        "main": "BRANCH", "dev": "BRANCH", "audit_s4": "TAG", "eoy_2024": "TAG"}
    assert refs["eoy_2024"].max_reference_age_in_ms == 7 * 24 * 3600 * 1000
    assert refs["dev"].min_snapshots_to_keep == 2
    assert refs["main"].max_reference_age_in_ms is None

    protected = protected_snapshot_ids(spark, t)
    assert protected == {h.s3, h.s4, h.s5, h.s6}
    spark.sql(f"ALTER TABLE {t} DROP TAG eoy_2024")
    protected = protected_snapshot_ids(spark, t)
    now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=1)
    spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', "
              f"older_than => TIMESTAMP '{now:%Y-%m-%d %H:%M:%S}', retain_last => 1)")
    left = {r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {t}.snapshots").collect()}
    # S3 only had the dropped tag; dev keeps 2 snapshots (S6 and its parent S5).
    assert left == protected == {h.s4, h.s5, h.s6}


@pytest.mark.parametrize("key", [k for k, q in REFERENCE_QUERIES.items()
                                 if q.topic == "history_refs"])
def test_reference_queries_run_as_printed(spark, hist, key):
    assert REFERENCE_QUERIES[key].status == "as-is"
    result = run(spark, key, hist.table)
    expected_rows = {"history-all": 3, "snapshots-all": 4, "entries-all": 8,
                     "history-join-snapshots": 3, "delete-snapshots": 1, "snapshot-app-ids": 4,
                     "metadata-log-join-snapshots": 11, "refs-all": 3}
    assert len(result) == expected_rows[key]
    if key == "history-join-snapshots":
        assert [r.operation for r in result] == ["delete", "overwrite", "append"]
    if key == "delete-snapshots":
        assert result[0].snapshot_id == hist.s3
    if key == "metadata-log-join-snapshots":
        assert sum(r.operation is None for r in result) == 3
