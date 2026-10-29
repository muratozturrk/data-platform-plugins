"""Maintaining streaming tables: three Iceberg procedures run through the Polaris catalog on the
small-file table written by streaming/."""
from conftest import record

T = "polaris.db.stream_events"


def stats(spark):
    return {
        "rows": spark.table(T).count(),
        "data_files": spark.table(f"{T}.files").count(),
        "snapshots": spark.table(f"{T}.snapshots").count(),
        "manifests": spark.table(f"{T}.manifests").count(),
    }


def test_compact_expire_and_rewrite_manifests(spark, checks, measure):
    before = stats(spark)
    assert before["data_files"] > before["snapshots"]          # the small-file problem

    rewritten = spark.sql(f"CALL polaris.system.rewrite_data_files(table => 'db.stream_events', "
                          f"options => map('min-input-files', '2'))").collect()[0]
    after_compaction = stats(spark)
    assert after_compaction["rows"] == before["rows"] and after_compaction["data_files"] < before["data_files"]

    current = spark.sql(f"SELECT max(committed_at) FROM {T}.snapshots").collect()[0][0]
    expired = spark.sql(f"CALL polaris.system.expire_snapshots(table => 'db.stream_events', "
                        f"older_than => TIMESTAMP '{current}', retain_last => 1)").collect()[0]
    after_expire = stats(spark)
    assert after_expire["snapshots"] == 1

    manifests = spark.sql("CALL polaris.system.rewrite_manifests('db.stream_events')").collect()[0]
    after_manifests = stats(spark)
    assert after_manifests["rows"] == before["rows"] and after_manifests["manifests"] <= after_expire["manifests"]

    for name in ("rewrite_data_files (compact small files)", "expire_snapshots", "rewrite_manifests"):
        record(checks, f"Maintenance: {name}", "ok", "claim holds", "CALL polaris.system.<procedure>")
    measure("maintenance", {
        "before": before,
        "rewrite_data_files": rewritten.asDict(),
        "after_compaction": after_compaction,
        "expire_snapshots": expired.asDict(),
        "after_expire": after_expire,
        "rewrite_manifests": manifests.asDict(),
        "after_rewrite_manifests": after_manifests,
    })
