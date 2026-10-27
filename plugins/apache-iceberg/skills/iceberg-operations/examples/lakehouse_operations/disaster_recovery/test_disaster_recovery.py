"""Catalog loss, bad writes, missing files and region loss, each recovered and verified."""
import os
from datetime import datetime
from pathlib import Path

import pytest
import requests
from pyspark.errors import AnalysisException
from prometheus_client.parser import text_string_to_metric_families

from common.lakehouse import CATALOG, drop_table, list_objects, s3_client, table_location
from common.sales import append_batches, create_sales_table, fingerprint, orders
from disaster_recovery import catalog_backup as cb
from disaster_recovery import recovery as r
from disaster_recovery.replication import (copy_object, naive_copy, replicate_with_path_rewrite,
                                                    replication_lag)

DAY1, DAY2 = datetime(2025, 9, 1), datetime(2025, 9, 2)
PRIMARY, DR = "s3://warehouse", "s3://warehouse-dr"


def build(spark, table, commits=2, rows=500):
    drop_table(spark, table)
    delete_prefix(f"{PRIMARY}/{table.replace('.', '/')}")       # leftovers of earlier runs (same location)
    create_sales_table(spark, table)
    append_batches(spark, table, batches=commits, rows_per_batch=rows, day=DAY1, files_per_batch=2)


def snapshot_ids(spark, table):
    return [x.snapshot_id for x in spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at").collect()]


def delete_prefix(location):
    for uri, _ in list_objects(location):
        bucket, key = uri[5:].split("/", 1)
        s3_client().delete_object(Bucket=bucket, Key=key)


def full_scan(spark, table):
    """Reads every data file (count(*) alone is answered from manifest stats and reads none)."""
    return spark.sql(f"SELECT sum(amount) FROM {table}").first()[0]


def bad_write(spark, table, start_id):
    """Upstream bug: negative amounts (2 data files)."""
    orders(spark, 300, start_id, DAY2).repartition(2).selectExpr("order_id", "customer_id", "region", "-amount AS amount",
                                                               "order_ts", "CAST(NULL AS STRING) AS channel").writeTo(table).append()


# ---------------------------------------------------------------- catalog loss

@pytest.fixture()
def catalog_ns(spark, ns):
    name = f"{ns}_catalog"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    yield name
    for row in spark.sql(f"SHOW TABLES IN {name}").collect():
        drop_table(spark, f"{name}.{row.tableName}")
    spark.sql(f"DROP NAMESPACE IF EXISTS {name}")


def test_catalog_export_and_reregistration_after_catalog_loss(spark, catalog_ns):
    t1, t2 = f"{catalog_ns}.sales", f"{catalog_ns}.customers_daily"
    build(spark, t1), build(spark, t2, commits=3)
    spark.sql(f"ALTER TABLE {t1} CREATE TAG month_end_2025_08")
    before = {t: (fingerprint(spark, t), snapshot_ids(spark, t)) for t in (t1, t2)}
    backup_uri = cb.save_backup(cb.export_catalog([catalog_ns]))
    assert backup_uri.startswith("s3://warehouse-dr/catalog-backups/")

    for t in (t1, t2):                                             # catalog entries lost, files intact
        spark.sql(f"DROP TABLE {t}")
    assert not spark.catalog.tableExists(t1) and list_objects(f"{PRIMARY}/{catalog_ns}/")

    backup = cb.load_latest_backup()
    assert sorted(cb.restore_catalog(spark, backup)) == sorted([t1, t2])
    for t in (t1, t2):
        assert (fingerprint(spark, t), snapshot_ids(spark, t)) == before[t]
        assert r.current_snapshot_id(spark, t) == backup["tables"][t]["current_snapshot_id"]   # no set_current_snapshot needed
    first = before[t1][1][0]
    assert spark.sql(f"SELECT count(*) c FROM {t1} VERSION AS OF {first}").first().c == 500   # time travel works
    assert "month_end_2025_08" in {x.name for x in spark.sql(f"SELECT name FROM {t1}.refs").collect()}
    assert cb.restore_catalog(spark, backup) == []                 # idempotent
    orders(spark, 10, 90_000, DAY2).writeTo(t1).append()           # the restored table is writable again
    assert spark.table(t1).count() == 1_010


def test_reregistration_without_backup_uses_latest_metadata_file(spark, ns):
    t = f"{ns}.no_backup"
    build(spark, t, commits=3)
    spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('data.owner'='sales-team')")      # metadata-only commit
    before, location = fingerprint(spark, t), table_location(spark, t)
    spark.sql(f"DROP TABLE {t}")
    latest = cb.latest_metadata_file(location)
    assert latest.split("/")[-1].startswith("00004-")              # create + 3 appends + property change
    cb.register(spark, t, latest)
    assert fingerprint(spark, t) == before and len(snapshot_ids(spark, t)) == 3
    props = {x.key: x.value for x in spark.sql(f"SHOW TBLPROPERTIES {t}").collect()}
    assert props["data.owner"] == "sales-team"


def test_split_brain_registration_corrupts_the_primary(spark, ns):
    """Split-brain: the same metadata registered twice and written in both places."""
    primary, secondary = f"{ns}.split_primary", f"{ns}.split_secondary"
    build(spark, primary)
    original_files = {x.file_path for x in spark.sql(f"SELECT file_path FROM {primary}.files").collect()}
    drop_table(spark, secondary)
    cb.register(spark, secondary, cb.latest_metadata_file(table_location(spark, primary)))

    orders(spark, 50, 0, DAY1).writeTo(secondary).overwritePartitions()        # "harmless" test write
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    deleted = spark.sql(f"""CALL {CATALOG}.system.expire_snapshots(table => '{secondary}',
                            older_than => TIMESTAMP '{now}', retain_last => 1)""").first().deleted_data_files_count
    assert deleted == len(original_files)                          # the secondary deletes the primary's files
    missing = r.validate_snapshot(spark, primary)
    assert len(missing["manifest_list"]) == 1                      # even the primary's manifest list is gone
    on_storage = {u for u, _ in list_objects(table_location(spark, primary), "data/")}
    assert not original_files & on_storage                         # and every data file it references
    with pytest.raises(Exception, match="(?i)not ?found|NoSuchKey|404"):
        full_scan(spark, primary)


# ---------------------------------------------------------------- bad write

def good_then_bad(spark, table):
    build(spark, table, commits=1, rows=1_000)
    spark.sql(f"ALTER TABLE {table} CREATE TAG audited_r1")
    good, good_fp = r.current_snapshot_id(spark, table), fingerprint(spark, table)
    spark.sql(f"ALTER TABLE {table} ADD COLUMN channel STRING")
    bad_write(spark, table, 10_000)
    return good, good_fp


def test_rollback_is_metadata_only_and_keeps_newer_schema(spark, ns):
    t = f"{ns}.rollback_demo"
    good, good_fp = good_then_bad(spark, t)
    bad = r.current_snapshot_id(spark, t)
    files_at = lambda v: {x.file_path for x in spark.sql(f"SELECT file_path FROM {t}.files VERSION AS OF {v}").collect()}
    bad_files = files_at(bad) - files_at(good)
    assert r.last_good_snapshot(spark, t) == good
    r.rollback(spark, t, good)
    assert r.current_snapshot_id(spark, t) == good and fingerprint(spark, t) == good_fp
    assert bad in snapshot_ids(spark, t)                           # abandoned snapshot still in history
    on_storage = {u for u, _ in list_objects(table_location(spark, t), "data/")}
    assert bad_files and bad_files <= on_storage                   # and its files still on storage
    assert "channel" in spark.table(t).columns                     # schema change is not rolled back


def test_expire_cutoff_does_not_remove_the_abandoned_snapshot(spark, ns):
    t = f"{ns}.expire_after_rollback"
    good, _ = good_then_bad(spark, t)
    bad = r.current_snapshot_id(spark, t)
    r.rollback(spark, t, good)
    cutoff = spark.sql(f"SELECT committed_at FROM {t}.snapshots WHERE snapshot_id = {bad}").first().committed_at
    spark.sql(f"""CALL {CATALOG}.system.expire_snapshots(table => '{t}',
                  older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}')""")   # cutoff before the bad write
    assert bad in snapshot_ids(spark, t)
    assert r.purge_snapshots(spark, t, [bad]) == 2                 # by id: gone, with its 2 data files
    assert snapshot_ids(spark, t) == [good]


def test_recovery_dag_rolls_back_to_last_audited_snapshot(spark, ns):
    from airflow.models import DagBag
    t = f"{ns}.prod_sales"
    good, good_fp = good_then_bad(spark, t)
    bad_write(spark, t, 20_000)                                    # two bad commits since the restore point
    dag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags" / "iceberg_recovery.py"),
                 include_examples=False).get_dag("iceberg_recovery")
    run = dag.test(run_conf={"table": t})
    assert {ti.task_id: ti.state for ti in run.get_task_instances()} == {
        k: "success" for k in ("find_last_good", "rollback", "purge_abandoned", "validate", "notify")}
    assert r.current_snapshot_id(spark, t) == good and fingerprint(spark, t) == good_fp
    assert snapshot_ids(spark, t) == [good]
    text = requests.get(os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091") + "/metrics", timeout=10).text
    pushed = {(s.name, s.labels.get("table")): s.value for f in text_string_to_metric_families(text) for s in f.samples}
    assert pushed[("iceberg_abandoned_snapshots", t)] == 2 and pushed[("iceberg_recovery_success", t)] == 1


# ---------------------------------------------------------------- missing files

def test_validation_scan_finds_a_lost_file_and_restore_from_backup_copy(spark, ns):
    t = f"{ns}.validated"
    build(spark, t)
    assert r.validate_snapshot(spark, t) == {"manifest_list": [], "manifests": [], "files": []}
    lost = spark.sql(f"SELECT file_path FROM {t}.files LIMIT 1").first().file_path
    backup = lost.replace(PRIMARY, DR + "/object-backup", 1)
    copy_object(lost, backup)                                      # versioned / replicated copy
    bucket, key = lost[5:].split("/", 1)
    s3_client().delete_object(Bucket=bucket, Key=key)              # accidental delete on storage

    assert r.validate_snapshot(spark, t)["files"] == [lost]
    with pytest.raises(Exception):
        full_scan(spark, t)
    copy_object(backup, lost)
    assert r.validate_snapshot(spark, t)["files"] == []
    assert spark.table(t).count() == 1_000
    with pytest.raises(AnalysisException, match="file_path"):      # naive manifests-vs-files query
        spark.sql(f"""SELECT file_path FROM {t}.manifests WHERE NOT EXISTS (
                      SELECT 1 FROM {t}.files WHERE files.file_path = manifests.file_path)""").collect()


# ---------------------------------------------------------------- region loss

@pytest.fixture(scope="module")
def replicas(spark, ns):
    primary = f"{ns}.sales_primary"
    build(spark, primary, commits=3)
    src = table_location(spark, primary)
    dst = src.replace(PRIMARY, DR, 1)
    naive, rewritten = f"{ns}.sales_dr_naive", f"{ns}.sales_dr"
    for t, prefix in ((naive, dst + "_naive"), (rewritten, dst)):
        drop_table(spark, t)
        delete_prefix(prefix)
    naive_copy(spark, primary, src, dst + "_naive")
    cb.register(spark, naive, cb.latest_metadata_file(dst + "_naive"))
    cb.register(spark, rewritten, replicate_with_path_rewrite(spark, primary, src, dst, f"{DR}/staging/{ns}/"))
    return {"primary": primary, "naive": naive, "dr": rewritten, "src": src, "dst": dst,
            "fingerprint": fingerprint(spark, primary)}


def test_naive_bucket_copy_still_points_at_the_primary_region(spark, replicas):
    paths = [x.file_path for x in spark.sql(f"SELECT file_path FROM {replicas['naive']}.files").collect()]
    assert paths and all(p.startswith(PRIMARY + "/") for p in paths)
    assert table_location(spark, replicas["naive"]).startswith(PRIMARY + "/")


def test_rewritten_replica_points_at_the_dr_region(spark, replicas):
    dr = replicas["dr"]
    paths = [x.file_path for x in spark.sql(f"SELECT file_path FROM {dr}.files").collect()]
    assert paths and all(p.startswith(replicas["dst"] + "/") for p in paths)
    assert table_location(spark, dr) == replicas["dst"]
    assert snapshot_ids(spark, dr) == snapshot_ids(spark, replicas["primary"])   # same ids, same history
    assert r.validate_snapshot(spark, dr) == {"manifest_list": [], "manifests": [], "files": []}


def test_replication_lag_is_measurable_before_destructive_maintenance(spark, replicas):
    orders(spark, 100, 50_000, DAY2).writeTo(replicas["primary"]).append()
    lag = replication_lag(spark, replicas["primary"], replicas["dr"])
    assert lag == {r.current_snapshot_id(spark, replicas["primary"])}  # expiring now would outpace the replica
    spark.sql(f"DROP TABLE {replicas['dr']}")                          # catalog pointer only
    cb.register(spark, replicas["dr"], replicate_with_path_rewrite(
        spark, replicas["primary"], replicas["src"], replicas["dst"], f"{DR}/staging/catchup/"))
    assert replication_lag(spark, replicas["primary"], replicas["dr"]) == set()
    replicas["fingerprint"] = fingerprint(spark, replicas["primary"])


def test_only_the_rewritten_replica_survives_region_loss(spark, replicas):
    delete_prefix(replicas["src"])                                     # primary region gone
    assert list_objects(replicas["src"]) == []
    with pytest.raises(Exception):
        full_scan(spark, replicas["naive"])
    assert fingerprint(spark, replicas["dr"]) == replicas["fingerprint"]
