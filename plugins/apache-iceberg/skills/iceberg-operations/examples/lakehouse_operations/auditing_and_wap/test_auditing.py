"""Audit trail from snapshot metadata, branch/tag governance, Write-Audit-Publish, lineage."""
import itertools
import json
import uuid
from datetime import datetime

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from common.sales import append_batches, create_sales_table, orders
from auditing_and_wap import audit, lineage
from auditing_and_wap.wap import branch_head, write_audit_publish

WAP_IDS = itertools.count()
DAY1, DAY2, DAY3 = datetime(2025, 3, 1), datetime(2025, 3, 2), datetime(2025, 3, 3)


@pytest.fixture(scope="module")
def history(spark, ns):
    """3 audited daily loads, a lossy backfill of day 1, a manual INSERT, then a schema change."""
    t = f"{ns}.sales"
    create_sales_table(spark, t)
    for i, day in enumerate((DAY1, DAY2, DAY3)):
        audit.write_with_audit(orders(spark, 1_000, i * 1_000, day, seed=i).repartition(2), t,
                               "orders_ingest", f"run-{i}")
    day1_files = {r.file_path for r in spark.sql(
        f"SELECT file_path FROM {t}.files WHERE partition.order_ts_day = DATE '2025-03-01'").collect()}
    backfill = spark.table(t).where("order_ts < TIMESTAMP '2025-03-02'").where("order_id % 25 != 0")
    audit.write_with_audit(backfill.localCheckpoint(), t, "orders_backfill", "run-3", mode="overwrite")
    spark.sql(f"INSERT INTO {t} VALUES (99999, 7, 'EU', 12.5, TIMESTAMP '2025-03-03 10:00:00')")  # manual hotfix
    snapshots_before_alter = spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first().c
    spark.sql(f"ALTER TABLE {t} ADD COLUMN channel STRING")
    return {"table": t, "day1_files": day1_files, "snapshots_before_alter": snapshots_before_alter}


def test_change_log_pinpoints_the_lossy_overwrite(spark, history):
    log = audit.change_log(spark, history["table"])
    assert [c.operation for c in log] == ["append", "append", "append", "overwrite", "append"]
    lossy = [c for c in log if c.lossy]
    assert len(lossy) == 1
    assert (lossy[0].deleted_records, lossy[0].added_records, lossy[0].job_id) == (1_000, 960, "orders_backfill")
    assert all(c.parent_id == p.snapshot_id for p, c in zip(log, log[1:]))      # linear lineage


def test_drill_down_to_files_of_the_suspicious_snapshot(spark, history):
    t = history["table"]
    lossy = next(c for c in audit.change_log(spark, t) if c.lossy)
    changed = audit.files_changed(spark, t, lossy.snapshot_id)
    assert changed["removed"] == history["day1_files"]                 # exactly the day-1 files, nothing else
    assert changed["added"] and all("order_ts_day=2025-03-01" in p for p in changed["added"])


def test_every_commit_is_attributed_to_a_job_except_the_manual_fix(spark, history):
    log = audit.change_log(spark, history["table"])
    suspects = audit.unexpected_commits(log, allowed_jobs={"orders_ingest", "orders_backfill"})
    assert len(suspects) == 1 and suspects[0] is log[-1]
    assert suspects[0].job_id is None and suspects[0].added_records == 1
    baseline = audit.commit_baseline(spark, history["table"])
    assert (baseline["commits"], baseline["appends"], baseline["overwrites"]) == (5, 4, 1)


def test_schema_change_is_a_metadata_version_not_a_snapshot(spark, history):
    t = history["table"]
    assert spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first().c == history["snapshots_before_alter"]
    log = spark.sql(f"SELECT latest_schema_id FROM {t}.metadata_log_entries ORDER BY timestamp").collect()
    assert log[-1].latest_schema_id == 0               # the metadata table still reports the snapshot's schema
    changes = audit.schema_history(spark, t)           # read from the metadata.json files themselves
    assert [c["schema_id"] for c in changes] == [0, 1]
    assert changes[-1]["columns"][-1] == "channel"
    current = spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    assert changes[-1]["snapshot_id"] == current.snapshot_id    # same snapshot, new metadata.json


def test_naive_audit_queries_do_not_resolve_on_iceberg_1_8_1(spark, history):
    t = history["table"]
    assert "user_id" not in spark.table(f"{t}.snapshots").columns
    assert spark.table(f"{t}.snapshots").columns == ["committed_at", "snapshot_id", "parent_id", "operation",
                                                    "manifest_list", "summary"]
    assert "ref_name" not in spark.table(f"{t}.refs").columns and "name" in spark.table(f"{t}.refs").columns
    with pytest.raises(AnalysisException, match="operation"):        # naive GDPR deleted-files query
        spark.sql(f"""SELECT f.file_path FROM {t}.files f LEFT JOIN {t}.metadata_log_entries m
                      ON f.file_path = m.file WHERE m.operation = 'DELETE'""").collect()


def test_refs_carry_branch_retention_and_tags_are_read_only(spark, ns):
    t = f"{ns}.refs_demo"
    create_sales_table(spark, t)
    append_batches(spark, t, batches=1, rows_per_batch=100, day=DAY1, files_per_batch=1)
    spark.sql(f"ALTER TABLE {t} CREATE BRANCH dev RETAIN 7 DAYS")
    spark.sql(f"ALTER TABLE {t} CREATE TAG release_2025_03")
    refs = {r.name: r for r in spark.sql(f"SELECT * FROM {t}.refs").collect()}
    assert refs["dev"].type == "BRANCH" and refs["dev"].max_reference_age_in_ms == 604_800_000
    assert refs["release_2025_03"].type == "TAG" and refs["release_2025_03"].max_reference_age_in_ms is None
    append_batches(spark, t, batches=1, rows_per_batch=100, day=DAY2, files_per_batch=1, start_id=100)
    refs_after = {r.name: r.snapshot_id for r in spark.sql(f"SELECT * FROM {t}.refs").collect()}
    assert refs_after["release_2025_03"] == refs["release_2025_03"].snapshot_id != refs_after["main"]
    with pytest.raises(Exception, match="Cannot write to table at a specific snapshot"):
        spark.sql(f"INSERT INTO {t}.tag_release_2025_03 VALUES (1, 1, 'EU', 1.0, TIMESTAMP '2025-03-01')")


@pytest.fixture()
def wap_table(spark, ns):
    t = f"{ns}.wap_{next(WAP_IDS)}"
    create_sales_table(spark, t)
    append_batches(spark, t, batches=1, rows_per_batch=1_000, day=DAY1, files_per_batch=2)
    return t


def test_wap_good_batch_is_published_by_fast_forward_and_tagged(spark, wap_table):
    t = wap_table
    main_before = branch_head(spark, t, "main")
    seen_on_main = []
    result = write_audit_publish(spark, t, orders(spark, 500, 10_000, DAY2), run_id="r1",
                                 on_main_moved=lambda: seen_on_main.append(spark.table(t).count()))
    assert seen_on_main == [1_000]                                       # staged batch invisible on main
    assert result.published and result.method == "fast_forward" and result.failures == []
    assert spark.table(t).count() == 1_500
    head = branch_head(spark, t, "main")
    assert head == result.snapshot_id != main_before                     # main now points at the audited snapshot
    refs = {r.name: r.snapshot_id for r in spark.sql(f"SELECT * FROM {t}.refs").collect()}
    assert refs == {"main": head, "audited_r1": head}                    # restore point tagged, branch gone
    summary = spark.sql(f"SELECT summary FROM {t}.snapshots WHERE snapshot_id = {head}").first().summary
    assert summary["audit.run-id"] == "r1"


def test_wap_bad_batch_never_reaches_main(spark, wap_table):
    t = wap_table
    main_before = branch_head(spark, t, "main")
    bad = (orders(spark, 500, 20_000, DAY2)
           .withColumn("customer_id", F.when(F.col("order_id") % 50 == 0, None).otherwise(F.col("customer_id")))
           .unionByName(orders(spark, 5, 20_000, DAY2)))                 # 5 duplicate order ids
    result = write_audit_publish(spark, t, bad, run_id="r2")
    assert not result.published
    assert result.failures == ["10 orders without customer_id", "5 duplicate order_id values"]
    assert branch_head(spark, t, "main") == main_before
    assert spark.table(t).count() == 1_000
    assert {r.name for r in spark.sql(f"SELECT name FROM {t}.refs").collect()} == {"main"}


def test_wap_cherrypicks_when_main_moved_during_the_audit(spark, wap_table):
    t = wap_table

    def concurrent_writer():                                             # another job appends to main meanwhile
        orders(spark, 200, 50_000, DAY3).writeTo(t).append()

    result = write_audit_publish(spark, t, orders(spark, 300, 40_000, DAY2), run_id="r3",
                                 on_main_moved=concurrent_writer)
    assert result.published and result.method == "cherrypick"
    assert spark.table(t).count() == 1_000 + 200 + 300
    ops = [r.operation for r in spark.sql(f"SELECT operation FROM {t}.snapshots ORDER BY committed_at").collect()]
    assert ops.count("append") >= 3


def test_lineage_event_joins_to_the_iceberg_snapshot(spark, ns, tmp_path):
    t = f"{ns}.lineage_demo"
    create_sales_table(spark, t)
    run_id = str(uuid.uuid4())
    log_file = str(tmp_path / "openlineage.jsonl")
    ol = lineage.client(log_file)
    ol.emit(lineage.run_event(lineage.RunState.START, run_id, "daily_etl_job", t))
    audit.write_with_audit(orders(spark, 100, 0, DAY1), t, "daily_etl_job", run_id)
    snap = next(c for c in audit.change_log(spark, t) if c.run_id == run_id)
    ol.emit(lineage.run_event(lineage.RunState.COMPLETE, run_id, "daily_etl_job", t, snap.snapshot_id,
                              checks={"not_null(customer_id)": True}))
    events = [json.loads(line) for line in open(log_file)]
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
    out = events[1]["outputs"][0]
    assert events[1]["run"]["runId"] == run_id and out["name"] == t
    assert out["facets"]["version"]["datasetVersion"] == str(snap.snapshot_id)
    assert events[1]["job"]["facets"]["sourceCodeLocation"]["url"].startswith("https://github.com/")
    with pytest.raises(ImportError):                                     # pre-facet_v2 import path
        from openlineage.client.facet import SourceCodeLocationRunFacet  # noqa: F401
