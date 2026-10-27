import pytest

from iceberg_writes.writes import (
    Writer, accounts_df, race_appends, concurrent_appends, create_accounts, java_error, last_snapshot, upsert,
)


def count(spark, sql):
    return spark.sql(sql).first()[0]


def test_every_write_is_one_atomic_snapshot(spark, ns):
    t = f"{ns}.accounts_cow"
    create_accounts(spark, t)
    accounts_df(spark, range(0, 100), "eu").writeTo(t).append()
    first = last_snapshot(spark, t)
    accounts_df(spark, range(100, 200), "us").writeTo(t).append()
    accounts_df(spark, range(0, 50), "eu", 200.0).writeTo(t).overwritePartitions()
    replaced = last_snapshot(spark, t)
    accounts_df(spark, [10, 20, 300], "us", 7.0).createOrReplaceTempView("staging_cow")
    upsert(spark, t, "staging_cow")
    merged = last_snapshot(spark, t)
    spark.sql(f"DELETE FROM {t} WHERE region = 'eu'")
    deleted = last_snapshot(spark, t)

    ops = [r.operation for r in spark.sql(f"SELECT operation FROM {t}.snapshots ORDER BY committed_at").collect()]
    assert ops == ["append", "append", "overwrite", "overwrite", "delete"]
    assert replaced.summary["deleted-records"] == "100" and replaced.summary["added-records"] == "50"
    assert int(merged.summary["deleted-data-files"]) > 0          # copy-on-write rewrote files
    assert "added-delete-files" not in merged.summary
    assert int(deleted.summary["deleted-data-files"]) > 0 and "added-data-files" not in deleted.summary
    # Old snapshots stay readable: time travel to the first commit.
    assert count(spark, f"SELECT count(*) FROM {t} VERSION AS OF {first.snapshot_id}") == 100
    assert count(spark, f"SELECT count(*) FROM {t}") == 103          # us 100 + moved 10, 20 + new 300


def test_merge_on_read_merge_writes_delete_files_instead_of_rewriting(spark, ns):
    t = f"{ns}.accounts_mor"
    create_accounts(spark, t, merge_mode="merge-on-read")
    accounts_df(spark, range(0, 1000), "eu").writeTo(t).append()
    accounts_df(spark, [1, 2, 3, 5000], "eu", 1.0).createOrReplaceTempView("staging_mor")
    upsert(spark, t, "staging_mor")
    s = last_snapshot(spark, t)
    assert s.operation == "overwrite"
    assert int(s.summary["added-position-delete-files"]) >= 1 and s.summary["added-position-deletes"] == "3"
    assert "deleted-data-files" not in s.summary                   # base file untouched
    assert count(spark, f"SELECT count(*) FROM {t} WHERE balance = 1.0") == 4


def test_failed_write_leaves_no_trace(spark, ns):
    t = f"{ns}.accounts_failed"
    create_accounts(spark, t)
    accounts_df(spark, range(0, 100), "eu").writeTo(t).append()
    before = count(spark, f"SELECT count(*) FROM {t}.snapshots")
    with pytest.raises(Exception, match="bad record 5000"):
        spark.sql(f"""INSERT INTO {t}
                      SELECT id, 'us', CASE WHEN id = 5000 THEN raise_error('bad record 5000') ELSE 1.0 END
                      FROM range(0, 10000)""")
    assert count(spark, f"SELECT count(*) FROM {t}.snapshots") == before
    assert count(spark, f"SELECT count(*) FROM {t}") == 100


def test_concurrent_spark_appends_all_commit(spark, ns):
    t = f"{ns}.accounts_concurrent"
    create_accounts(spark, t)
    concurrent_appends(spark, t, writers=4, rows_per_writer=1000)
    assert count(spark, f"SELECT count(*) FROM {t}.snapshots WHERE operation = 'append'") == 4
    assert count(spark, f"SELECT count(DISTINCT id) FROM {t}") == 4000


@pytest.fixture
def two_writers(spark, ns, request):
    """Table with one snapshot, then two clients that both load it (same base snapshot)."""
    t = f"{ns}.{request.node.name[5:40]}"
    create_accounts(spark, t)
    for prop in getattr(request, "param", {}).items():
        spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('{prop[0]}'='{prop[1]}')")
    w0 = Writer(spark, "w0", t)
    w0.append("eu", "initial")
    w1, w2 = Writer(spark, "w1", t), Writer(spark, "w2", t)
    assert w1.base_snapshot_id() == w2.base_snapshot_id()
    yield t, w1, w2
    for w in (w0, w1, w2):
        w.close()


def test_stale_append_is_retried_and_rebased(spark, two_writers):
    t, w1, w2 = two_writers
    w1.append("eu", "w1")                     # pointer moves from S1 to S2
    w2.append("eu", "w2")                     # w2 still holds S1: refreshed and re-applied on S2
    w1_snap = w1.table.currentSnapshot().snapshotId()
    assert w2.table.currentSnapshot().parentId() == w1_snap      # rebased on top of w1
    assert count(spark, f"SELECT count(*) FROM {t}.files") == 3
    assert count(spark, f"SELECT count(*) FROM {t}.snapshots") == 3


def files_count(spark, t):
    return count(spark, f"SELECT count(*) FROM {t}.files")


def test_racing_appends_with_default_retries(spark, two_writers):
    """Default commit.retry.num-retries=4: some of 8 simultaneous writers can still lose."""
    t, _, _ = two_writers
    errors = race_appends(spark, t, writers=8)
    print(f"\nrace outcomes with default retries: {errors.count(None)} committed, "
          f"{len(errors) - errors.count(None)} failed")
    assert set(errors) <= {None, "CommitFailedException"}
    assert files_count(spark, t) == 1 + errors.count(None)


@pytest.mark.parametrize("two_writers", [{"commit.retry.num-retries": "20"}], indirect=True)
def test_racing_appends_all_commit_when_retries_match_concurrency(spark, two_writers):
    t, _, _ = two_writers
    errors = race_appends(spark, t, writers=8)
    assert errors == [None] * 8 and files_count(spark, t) == 1 + 8


@pytest.mark.parametrize("two_writers", [{"commit.retry.num-retries": "0"}], indirect=True)
def test_racing_appends_without_retries_raise_commit_failed(spark, two_writers):
    t, _, _ = two_writers
    seen, committed = [], 0
    for _ in range(5):                        # a lost race is very likely in one round, retry to be sure
        errors = race_appends(spark, t, writers=8)
        seen += errors
        committed += errors.count(None)
        if "CommitFailedException" in errors:
            break
    assert "CommitFailedException" in seen and set(seen) <= {None, "CommitFailedException"}
    assert files_count(spark, t) == 1 + committed           # losers published nothing
    print(f"\nrace outcomes without retries: {seen.count(None)} committed, "
          f"{seen.count('CommitFailedException')} CommitFailedException")


def test_overlapping_overwrite_fails_validation(spark, two_writers):
    t, w1, w2 = two_writers
    base = w2.base_snapshot_id()
    w1.append("eu", "late_eu_data")           # concurrent write into the partition w2 replaces
    err = java_error(lambda: w2.overwrite_region("eu", "rebuilt_eu", validate_from=base))
    assert err == "ValidationException"
    files = [r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()]
    assert any("late_eu_data" in f for f in files) and not any("rebuilt_eu" in f for f in files)


def test_disjoint_overwrite_succeeds_after_retry(spark, two_writers):
    t, w1, w2 = two_writers
    base = w2.base_snapshot_id()
    w1.append("us", "us_data")                # different partition: no logical conflict
    assert java_error(lambda: w2.overwrite_region("eu", "rebuilt_eu", validate_from=base)) is None
    files = sorted(r.file_path.rsplit("/", 1)[1] for r in spark.sql(f"SELECT file_path FROM {t}.files").collect())
    assert files == ["rebuilt_eu.parquet", "us_data.parquet"]


def test_unvalidated_overwrite_silently_removes_the_concurrent_write(spark, two_writers):
    t, w1, w2 = two_writers
    w1.append("eu", "late_eu_data")
    assert java_error(lambda: w2.overwrite_region("eu", "rebuilt_eu")) is None
    files = [r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()]
    assert not any("late_eu_data" in f for f in files)         # lost update, no error raised
