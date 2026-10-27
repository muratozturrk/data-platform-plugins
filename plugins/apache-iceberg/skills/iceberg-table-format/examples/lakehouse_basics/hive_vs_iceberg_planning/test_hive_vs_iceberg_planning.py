"""The three Hive limitations Iceberg was built to fix:
planning through directory listings, schema tied to file layout, no transactional isolation."""
import threading
from datetime import date

import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyspark.sql import types as T

from common.lakehouse import get_pyiceberg_catalog, s3_client
from common.orders import orders
from hive_vs_iceberg_planning.metadata_planning import (
    CountingFileIO, ListCounter, hive_plan, iceberg_plan, write_hive_layout)

DAYS = 30
WINDOW = And(GreaterThanOrEqual("order_ts", "2025-01-10T00:00:00+00:00"),
             LessThan("order_ts", "2025-01-13T00:00:00+00:00"))


@pytest.fixture(scope="module")
def data():
    return orders(30_000, days=DAYS)


@pytest.fixture(scope="module")
def iceberg_orders(spark, ns, data):
    t = f"{ns}.orders"
    spark.sql(f"""CREATE TABLE {t} (order_id bigint, customer_id bigint, country string, status string,
                  amount double, order_ts timestamp, shipping_address string, user_agent string, notes string)
                  USING iceberg PARTITIONED BY (days(order_ts))""")
    spark.createDataFrame(data.to_pandas()).writeTo(t).append()
    return t


def test_hive_style_planning_lists_directories(data, ns):
    prefix = f"s3://warehouse/hive_style/{ns}/orders"
    assert write_hive_layout(data, prefix) == DAYS
    with ListCounter(s3_client()) as by_partition:
        pruned = hive_plan(prefix, {date(2025, 1, 10), date(2025, 1, 11), date(2025, 1, 12)})
    with ListCounter(s3_client()) as by_timestamp:
        unpruned = hive_plan(prefix, None)           # WHERE order_ts BETWEEN ...: not the partition column
    print(f"\nHive LIST calls: filter on order_date {by_partition.calls}, filter on order_ts {by_timestamp.calls}")
    assert len(pruned) == 3 and by_partition.calls == 1 + 3
    assert len(unpruned) == DAYS and by_timestamp.calls == 1 + DAYS


def test_iceberg_plans_from_metadata_with_hidden_partitioning(spark, iceberg_orders):
    catalog = get_pyiceberg_catalog(**{
        "py-io-impl": "hive_vs_iceberg_planning.metadata_planning.CountingFileIO"})
    table = catalog.load_table(iceberg_orders)
    total_files = spark.sql(f"SELECT count(*) c FROM {iceberg_orders}.data_files").first().c

    CountingFileIO.opened.clear()
    planned = iceberg_plan(table, WINDOW)            # filter on order_ts itself, no derived column
    opened = list(CountingFileIO.opened)
    print(f"\nIceberg: {len(planned)} of {total_files} data files planned, "
          f"{len(opened)} metadata files read, no listing")

    expected = {r.file_path for r in spark.sql(
        f"SELECT file_path FROM {iceberg_orders}.data_files "
        f"WHERE partition.order_ts_day BETWEEN DATE'2025-01-10' AND DATE'2025-01-12'").collect()}
    assert set(planned) == expected and 3 <= len(planned) < total_files / 5
    assert opened and all(p.endswith(".avro") for p in opened)   # manifest list + manifests only
    assert not any("/data/" in p for p in opened)                # no data file touched while planning
    assert len(opened) <= 3                                      # manifest list + manifest(s), not one call per day


def test_rename_column_plain_parquet_loses_data(spark, tmp_path, data):
    path = f"{tmp_path}/orders"
    spark.createDataFrame(data.slice(0, 1_000).to_pandas()).write.parquet(path)
    renamed = T.StructType([T.StructField("order_id", T.LongType()), T.StructField("customer_ref", T.LongType())])
    # Parquet columns are matched by name: the renamed column reads as NULL until every file is rewritten.
    assert spark.read.schema(renamed).parquet(path).where("customer_ref IS NOT NULL").count() == 0


def test_rename_column_in_iceberg_is_metadata_only(spark, ns, data):
    t = f"{ns}.orders_rename"
    spark.createDataFrame(data.slice(0, 1_000).to_pandas()).writeTo(t).create()
    files_before = {r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.data_files").collect()}
    total = spark.sql(f"SELECT sum(customer_id) s FROM {t}").first().s

    spark.sql(f"ALTER TABLE {t} RENAME COLUMN customer_id TO customer_ref")
    spark.sql(f"ALTER TABLE {t} DROP COLUMN notes")

    assert {r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.data_files").collect()} == files_before
    assert spark.sql(f"SELECT sum(customer_ref) s FROM {t}").first().s == total   # columns tracked by ID
    assert "notes" not in spark.table(t).columns


def test_snapshot_isolation_reader_keeps_its_snapshot(spark, ns, data):
    t = f"{ns}.orders_isolation"
    spark.createDataFrame(data.slice(0, 1_000).to_pandas()).writeTo(t).create()
    catalog = get_pyiceberg_catalog()
    reader_view = catalog.load_table(t)              # a long-running reader starts here

    spark.createDataFrame(data.slice(1_000, 500).to_pandas()).writeTo(t).append()
    spark.sql(f"DELETE FROM {t} WHERE country = 'US'")

    assert reader_view.scan().to_arrow().num_rows == 1_000            # unaffected by both commits
    assert catalog.load_table(t).scan().to_arrow().num_rows == spark.table(t).count() < 1_500


def test_concurrent_writers_do_not_lose_commits(spark, ns, data):
    t = f"{ns}.orders_concurrent"
    spark.createDataFrame(data.slice(0, 10).to_pandas()).limit(0).writeTo(t).create()
    before = spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first().c
    batches = [spark.createDataFrame(data.slice(i * 2_000, 2_000).to_pandas()) for i in range(4)]
    errors, barrier = [], threading.Barrier(len(batches))

    def writer(df):
        try:
            barrier.wait()
            df.writeTo(t).append()                   # optimistic concurrency: retry on conflicting commit
        except Exception as e:                       # pragma: no cover - failure is reported below
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(b,)) for b in batches]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors
    assert spark.table(t).count() == 8_000
    assert spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first().c == before + 4   # one commit per writer
    parents = spark.sql(f"SELECT parent_id FROM {t}.snapshots WHERE parent_id IS NOT NULL").collect()
    assert len({r.parent_id for r in parents}) == len(parents)    # linear history: no two commits share a parent
