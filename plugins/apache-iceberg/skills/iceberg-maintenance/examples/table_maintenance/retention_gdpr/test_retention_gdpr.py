from datetime import datetime, timedelta, timezone

import pytest
from pyspark.errors import PySparkException
from py4j.protocol import Py4JJavaError

from common.clickstream import day
from common.lakehouse import list_objects, s3_client, table_location
from suboptimal_files.workloads import build_streaming_table, set_row_level_mode
from retention_gdpr.gdpr_erasure import erase_subject, physical_files_containing
from retention_gdpr.retention import (
    create_tag, expire_snapshots, remove_orphan_files, set_retention_policy, snapshot_ids, tags,
)

SUBJECT = 42          # the user who filed the erasure request


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=1)


def others_fingerprint(spark, table):
    return tuple(spark.sql(f"""
        SELECT count(*), round(sum(amount), 2) FROM {table} WHERE user_id <> {SUBJECT}
    """).first())


def test_retain_last_keeps_a_minimum_history(spark, ns):
    table = f"{ns}.retain_last"
    build_streaming_table(spark, table, 10, 1_000, day(0))

    result = expire_snapshots(spark, table, older_than=utc_now(), retain_last=3)

    assert len(snapshot_ids(spark, table)) == 3
    assert result.deleted_manifest_lists == 7          # one manifest list per expired snapshot
    assert result.deleted_data_files == 0              # appends: every file is still live


def test_table_properties_drive_default_expiration(spark, ns):
    table = f"{ns}.policy"
    build_streaming_table(spark, table, 6, 1_000, day(0))
    set_retention_policy(spark, table, max_snapshot_age_ms=1, min_snapshots_to_keep=2)

    expire_snapshots(spark, table)                     # no args: table policy applies

    assert len(snapshot_ids(spark, table)) == 2


def test_expiration_ends_time_travel_and_frees_rewritten_files(spark, ns):
    table = f"{ns}.time_travel"
    build_streaming_table(spark, table, 4, 1_000, day(0))
    set_row_level_mode(spark, table, "copy-on-write")
    before_delete = snapshot_ids(spark, table)[-1]
    spark.sql(f"DELETE FROM {table} WHERE user_id = {SUBJECT}")

    # Logically gone, still reachable through time travel and still on storage.
    assert spark.sql(f"SELECT * FROM {table} WHERE user_id = {SUBJECT}").count() == 0
    assert spark.sql(f"SELECT * FROM {table} VERSION AS OF {before_delete} "
                     f"WHERE user_id = {SUBJECT}").count() > 0
    assert physical_files_containing(spark, table, "user_id", SUBJECT)

    result = expire_snapshots(spark, table, older_than=utc_now(), retain_last=1)

    assert result.deleted_data_files > 0               # files replaced by the COW delete
    assert physical_files_containing(spark, table, "user_id", SUBJECT) == []
    with pytest.raises((PySparkException, Py4JJavaError)):
        spark.sql(f"SELECT * FROM {table} VERSION AS OF {before_delete}").count()


def test_tag_pins_a_snapshot_through_expiration(spark, ns):
    table = f"{ns}.tagged"
    build_streaming_table(spark, table, 5, 1_000, day(0))
    audit_snapshot = snapshot_ids(spark, table)[1]
    create_tag(spark, table, "q4_audit", audit_snapshot, retain_days=365)

    expire_snapshots(spark, table, older_than=utc_now(), retain_last=1)

    remaining = snapshot_ids(spark, table)
    assert audit_snapshot in remaining and len(remaining) == 2   # current + tagged
    assert tags(spark, table) == {"q4_audit": audit_snapshot}
    assert spark.sql(f"SELECT * FROM {table} VERSION AS OF 'q4_audit'").count() == 2_000


def test_orphan_cleanup_removes_only_unreferenced_old_files(spark, ns):
    table = f"{ns}.orphans"
    build_streaming_table(spark, table, 3, 1_000, day(0))
    rows_before = spark.table(table).count()

    # A Spark task that died before commit leaves a file no snapshot references.
    location = table_location(spark, table)
    bucket, key = location.removeprefix("s3://").split("/", 1)
    s3_client().put_object(Bucket=bucket, Key=f"{key}/data/aborted-task-00042.parquet", Body=b"PAR1")
    # Inventory view of the bucket: the aborted file is 5 days old, live files are new.
    listing = [(uri, mtime - timedelta(days=5) if "aborted-task" in uri else mtime)
               for uri, mtime in list_objects(location)]

    removed = remove_orphan_files(spark, table, older_than=utc_now() - timedelta(days=3), listing=listing)

    assert [p.rsplit("/", 1)[-1] for p in removed] == ["aborted-task-00042.parquet"]
    assert spark.table(table).count() == rows_before


def test_orphan_cleanup_refuses_a_window_shorter_than_24h(spark, ns):
    table = f"{ns}.orphans_guard"
    build_streaming_table(spark, table, 1, 1_000, day(0))
    with pytest.raises(PySparkException, match="less than 24 hours"):
        remove_orphan_files(spark, table, older_than=utc_now())


@pytest.mark.parametrize("mode", ["merge-on-read", "copy-on-write"])
def test_gdpr_erasure_removes_subject_from_storage(spark, ns, mode):
    table = f"{ns}.gdpr_{mode.replace('-', '_')}"
    build_streaming_table(spark, table, 6, 2_000, day(0))
    set_row_level_mode(spark, table, mode)
    others = others_fingerprint(spark, table)
    assert physical_files_containing(spark, table, "user_id", SUBJECT)

    report = erase_subject(spark, table, SUBJECT)

    assert report.rows_deleted > 0
    assert report.compliant, report
    assert spark.sql(f"SELECT * FROM {table} WHERE user_id = {SUBJECT}").count() == 0
    assert others_fingerprint(spark, table) == others      # nobody else was affected
    assert len(snapshot_ids(spark, table)) == 1


def test_gdpr_erasure_reports_tags_that_block_compliance(spark, ns):
    table = f"{ns}.gdpr_tagged"
    build_streaming_table(spark, table, 4, 2_000, day(0))
    set_row_level_mode(spark, table, "merge-on-read")
    create_tag(spark, table, "month_end_close")

    blocked = erase_subject(spark, table, SUBJECT)

    assert not blocked.compliant
    assert blocked.blocking_refs == ["month_end_close"]
    assert blocked.residual_files                          # the tag keeps old files alive

    # Erasure has precedence over the tag once the DPO approves dropping it.
    approved = erase_subject(spark, table, SUBJECT, drop_blocking_tags=True)
    assert approved.dropped_refs == ["month_end_close"]
    assert approved.compliant, approved
