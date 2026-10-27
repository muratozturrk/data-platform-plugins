"""A table format is metadata over plain Parquet files, and why that layer is needed."""
import io
from datetime import datetime

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pyiceberg.exceptions import CommitFailedException

from common.lakehouse import delete_prefix, list_objects, read_object, table_location
from common.orders import arrow_orders, create_orders_table, orders
from files_vs_table.file_lake import (WriterCrashed, overwrite_dataset, read_current,
                                            read_dataset, write_batch)
from files_vs_table.iceberg_writes import (append_that_fails, concurrent_appends,
                                                 parquet_footer_stats)

DAY = datetime(2025, 3, 1)
PLAIN = "s3://warehouse/plain_parquet"


def snapshot_count(spark, table):
    return spark.sql(f"SELECT count(*) c FROM {table}.snapshots").first().c


def test_iceberg_data_files_are_plain_parquet(spark, ns):
    t = f"{ns}.orders_parquet"
    create_orders_table(spark, t)
    orders(spark, 20_000, 0, DAY, days=2).writeTo(t).append()

    files = spark.sql(f"""SELECT file_path, file_format, record_count, file_size_in_bytes,
                                 readable_metrics.amount.lower_bound AS lo,
                                 readable_metrics.amount.upper_bound AS hi
                          FROM {t}.files""").collect()
    assert {f.file_format for f in files} == {"PARQUET"}
    for f in files:
        pf = pq.ParquetFile(io.BytesIO(read_object(f.file_path)))     # no Iceberg library involved
        assert pf.metadata.num_rows == f.record_count
        lo, hi, amount_bytes = parquet_footer_stats(f.file_path, "amount")
        assert (lo, hi) == (f.lo, f.hi)                               # Iceberg copies footer stats
        assert amount_bytes < f.file_size_in_bytes / 3                # one of 6 columns: column pruning


def test_the_directory_is_not_the_table(spark, ns):
    t = f"{ns}.orders_dir"
    create_orders_table(spark, t)
    orders(spark, 20_000, 0, DAY, days=2).writeTo(t).append()
    spark.sql(f"DELETE FROM {t} WHERE status = 'returned'")          # copy-on-write by default

    raw = read_dataset(table_location(spark, t) + "/data/")           # "the folder of Parquet files"
    assert spark.table(t).count() == 15_000
    assert raw.num_rows == 20_000 + 15_000                            # old + rewritten files
    assert pc.sum(pc.equal(raw["status"], "returned")).as_py() == 5_000


def test_crashed_plain_writer_leaves_partial_and_duplicate_data():
    prefix = f"{PLAIN}/crash/"
    delete_prefix(prefix)
    batch = arrow_orders(10_000, 0, DAY)
    with pytest.raises(WriterCrashed):
        write_batch(prefix, batch, n_files=5, crash_after=3)
    assert read_dataset(prefix).num_rows == 6_000                     # readers see 3 of 5 files

    write_batch(prefix, batch, n_files=5)                             # the scheduler retries the job
    ds = read_dataset(prefix)
    assert ds.num_rows == 16_000
    assert len(pc.unique(ds["order_id"])) == 10_000                   # 6,000 duplicates


def test_failed_iceberg_job_changes_nothing(spark, ns):
    t = f"{ns}.orders_atomic"
    create_orders_table(spark, t)
    orders(spark, 10_000, 0, DAY).writeTo(t).append()
    before_snapshots, before_files = snapshot_count(spark, t), spark.sql(f"SELECT count(*) c FROM {t}.files").first().c
    location = table_location(spark, t)
    objects_before = len(list_objects(location, "data/"))

    with pytest.raises(Exception, match="bad record"):
        append_that_fails(spark, t, orders(spark, 10_000, 10_000, DAY), poison_order_id=19_999)

    assert spark.table(t).count() == 10_000
    assert snapshot_count(spark, t) == before_snapshots
    assert spark.sql(f"SELECT count(*) c FROM {t}.files").first().c == before_files
    # Files written by the tasks that succeeded are deleted when Spark aborts the write.
    assert len(list_objects(location, "data/")) == objects_before


def test_plain_overwrite_loses_a_concurrent_update():
    prefix = f"{PLAIN}/lost_update/"
    delete_prefix(prefix)
    overwrite_dataset(prefix, arrow_orders(100, 0, DAY))
    seen_by_a, seen_by_b = read_current(prefix), read_current(prefix)  # both jobs start
    overwrite_dataset(prefix, seen_by_a.filter(pc.less(seen_by_a["order_id"], 90)))   # A: drop 10 rows
    overwrite_dataset(prefix, seen_by_b.filter(pc.greater_equal(seen_by_b["order_id"], 50)))  # B: drop 50
    final = read_current(prefix)
    assert final.num_rows == 50                                       # A's change silently lost
    assert pc.max(final["order_id"]).as_py() == 99


def test_stale_iceberg_writer_is_rejected(spark, ns, catalog):
    t = f"{ns}.orders_occ"
    create_orders_table(spark, t)
    writer_a, writer_b = catalog.load_table(t), catalog.load_table(t)  # same base snapshot
    writer_a.append(arrow_orders(100, 0, DAY))
    with pytest.raises(CommitFailedException):
        writer_b.append(arrow_orders(100, 100, DAY))                  # based on a stale view
    writer_b.refresh()
    writer_b.append(arrow_orders(100, 100, DAY))                      # retry on the new state
    assert spark.table(t).count() == 200
    assert snapshot_count(spark, t) == 2


def test_concurrent_spark_appends_all_commit(spark, ns):
    t = f"{ns}.orders_concurrent"
    create_orders_table(spark, t)
    concurrent_appends(spark, t, [orders(spark, 5_000, i * 5_000, DAY) for i in range(4)])
    assert spark.table(t).count() == 20_000
    assert spark.sql(f"SELECT count(DISTINCT order_id) c FROM {t}").first().c == 20_000
    assert snapshot_count(spark, t) == 4
