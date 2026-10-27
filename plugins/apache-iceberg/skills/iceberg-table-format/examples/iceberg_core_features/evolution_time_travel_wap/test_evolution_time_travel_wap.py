"""Key features and their benefits, each checked against files and snapshots."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from common.lakehouse import scan_metrics
from common.orders import create_orders_table, orders
from evolution_time_travel_wap.features import (audit, current_snapshot_id, data_file_paths,
                                                publish, rollback_to, wap_branch)

JAN = datetime(2025, 1, 1)


def test_schema_evolution_is_metadata_only(spark, ns):
    t = f"{ns}.orders_schema"
    create_orders_table(spark, t)
    orders(spark, 10_000, 0, JAN, days=3).writeTo(t).append()
    files_before = data_file_paths(spark, t)

    spark.sql(f"ALTER TABLE {t} ADD COLUMN channel STRING")
    spark.sql(f"ALTER TABLE {t} RENAME COLUMN amount TO amount_eur")
    spark.sql(f"ALTER TABLE {t} ALTER COLUMN status FIRST")
    spark.sql(f"ALTER TABLE {t} DROP COLUMN customer_id")

    assert data_file_paths(spark, t) == files_before                 # nothing rewritten
    assert spark.table(t).columns == ["status", "order_id", "country", "amount_eur", "order_ts", "channel"]
    old = spark.table(t).agg(F.sum("amount_eur").alias("s"), F.count("channel").alias("c")).first()
    assert old.c == 0 and old.s > 0                                  # renamed column still reads old files
    spark.sql(f"INSERT INTO {t} VALUES ('paid', 99999, 'TR', 10.0, TIMESTAMP '2025-01-02 10:00:00', 'web')")
    assert spark.sql(f"SELECT count(*) c FROM {t} WHERE channel = 'web'").first().c == 1
    assert spark.sql(f"SELECT count(*) c FROM {t}.metadata_log_entries").first().c >= 6  # schema history


def test_time_travel_reads_the_snapshot_schema_after_a_drop(spark, ns):
    t = f"{ns}.orders_drop"
    create_orders_table(spark, t)
    orders(spark, 1_000, 0, JAN).writeTo(t).append()
    before_drop = current_snapshot_id(spark, t)
    spark.sql(f"ALTER TABLE {t} DROP COLUMN customer_id")
    old = spark.sql(f"SELECT * FROM {t} VERSION AS OF {before_drop}")
    # Iceberg 1.8.1 + Spark 3.5: time travel by snapshot id uses that snapshot's schema ...
    assert "customer_id" in old.columns
    assert old.agg(F.sum("customer_id")).first()[0] == sum(range(1_000))   # values are readable
    # ... but a pushed-down filter on the dropped column is bound to the CURRENT schema and fails.
    with pytest.raises(Exception, match="Cannot find field 'customer_id'"):
        old.where("customer_id IS NOT NULL").count()
    assert "customer_id" not in spark.table(t).columns               # the drop itself is not undone


def test_partition_evolution_keeps_old_files_and_prunes_both_layouts(spark, ns):
    t = f"{ns}.orders_spec"
    create_orders_table(spark, t, "PARTITIONED BY (months(order_ts))")
    orders(spark, 30_000, 0, JAN, days=31).writeTo(t).append()                  # January: monthly
    old_files = data_file_paths(spark, t)
    spark.sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD order_ts_month WITH days(order_ts)")
    orders(spark, 28_000, 30_000, datetime(2025, 2, 1), days=28).writeTo(t).append()  # February: daily

    assert old_files <= data_file_paths(spark, t)                    # January files untouched
    specs = dict(spark.sql(f"SELECT spec_id, count(*) n FROM {t}.files GROUP BY spec_id").collect())
    assert specs[0] == len(old_files) and specs[1] >= 28
    feb = scan_metrics(spark.sql(f"SELECT * FROM {t} WHERE order_ts >= '2025-02-10' AND order_ts < '2025-02-11'"))
    assert feb["resultDataFiles"] == 1                               # one daily file out of 28+
    jan = scan_metrics(spark.sql(f"SELECT * FROM {t} WHERE order_ts >= '2025-01-10' AND order_ts < '2025-01-11'"))
    assert jan["resultDataFiles"] == len(old_files)                  # only the January (month) files
    assert spark.sql(f"SELECT count(*) c FROM {t}").first().c == 58_000


def test_hidden_partitioning_avoids_the_hive_full_scan(spark, ns):
    t = f"{ns}.orders_hidden"
    create_orders_table(spark, t)
    df = orders(spark, 31_000, 0, JAN, days=31)
    df.writeTo(t).append()
    # Plain Hive-style layout (directory per order_date). Local path: the image has no hadoop-aws (s3a).
    hive_path = "file:///tmp/iceberg_core_features/hive_orders"
    (df.withColumn("order_date", F.to_date("order_ts")).repartition("order_date")
       .write.mode("overwrite").partitionBy("order_date").parquet(hive_path))

    query = "order_ts >= '2025-01-10 00:00:00' AND order_ts < '2025-01-11 00:00:00'"
    hive = spark.read.parquet(hive_path)
    hive_naive = scan_metrics(hive.where(query))                     # user forgot order_date
    hive_right = scan_metrics(hive.where(query + " AND order_date = '2025-01-10'"))
    ice = scan_metrics(spark.table(t).where(query))                  # same naive filter
    assert hive_naive["numFiles"] == 31 and hive_right["numFiles"] == 1
    assert ice["resultDataFiles"] == 1 and ice["skippedDataFiles"] == 30
    assert hive.where(query).count() == spark.table(t).where(query).count() == 1_000


def test_time_travel_and_rollback_undo_a_bad_load(spark, ns):
    t = f"{ns}.orders_rollback"
    create_orders_table(spark, t)
    orders(spark, 5_000, 0, JAN).writeTo(t).append()
    good = current_snapshot_id(spark, t)
    good_ts = spark.sql(f"SELECT committed_at FROM {t}.snapshots WHERE snapshot_id = {good}").first().committed_at
    orders(spark, 5_000, 0, JAN).writeTo(t).append()                  # the job ran twice: duplicates
    assert spark.table(t).count() == 10_000

    assert spark.sql(f"SELECT count(*) c FROM {t} VERSION AS OF {good}").first().c == 5_000
    assert spark.sql(f"SELECT count(*) c FROM {t} TIMESTAMP AS OF '{good_ts}'").first().c == 5_000
    rollback_to(spark, t, good)
    assert spark.table(t).count() == 5_000
    assert current_snapshot_id(spark, t) == good
    assert spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first().c == 2  # bad snapshot kept for audit


def test_write_audit_publish_on_a_branch(spark, ns):
    t = f"{ns}.orders_wap"
    create_orders_table(spark, t)
    orders(spark, 5_000, 0, JAN).writeTo(t).append()
    main_before = current_snapshot_id(spark, t)

    with wap_branch(spark, t, "etl_run_1"):
        orders(spark, 1_000, 5_000, JAN).writeTo(t).append()
        orders(spark, 10, 5_000, JAN).writeTo(t).append()            # a buggy retry: duplicates
    assert spark.table(t).count() == 5_000                           # readers of main see nothing
    assert current_snapshot_id(spark, t) == main_before
    assert audit(spark, t, "etl_run_1") == ["duplicate order_id"]    # not published

    with wap_branch(spark, t, "etl_run_2"):
        orders(spark, 1_000, 5_000, JAN).writeTo(t).append()
    assert audit(spark, t, "etl_run_2") == []
    publish(spark, t, "etl_run_2")
    assert spark.table(t).count() == 6_000
    spark.sql(f"ALTER TABLE {t} CREATE TAG published_2025_01_01")
    refs = {r.name: r.type for r in spark.sql(f"SELECT name, type FROM {t}.refs").collect()}
    assert refs == {"main": "BRANCH", "etl_run_1": "BRANCH", "etl_run_2": "BRANCH", "published_2025_01_01": "TAG"}
