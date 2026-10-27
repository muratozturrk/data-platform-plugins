"""The lakehouse promise: one open table, many engines, no copies."""
import pytest

from common.lakehouse import get_pyiceberg_catalog, list_keys
from common.orders import orders
from multi_engine_lakehouse.engines import (
    append_from_python_service, files_read_by_pyiceberg, lookup_order, revenue_duckdb, revenue_spark)


@pytest.fixture(scope="module")
def table(spark, ns):
    t = f"{ns}.orders"
    spark.sql(f"""CREATE TABLE {t} (order_id bigint, customer_id bigint, country string, status string,
                  amount double, order_ts timestamp, shipping_address string, user_agent string, notes string)
                  USING iceberg PARTITIONED BY (days(order_ts))""")
    spark.createDataFrame(orders(20_000).to_pandas()).writeTo(t).append()   # the batch ETL job
    return t


def test_spark_and_duckdb_agree_on_the_same_table(spark, table):
    py_table = get_pyiceberg_catalog().load_table(table)
    spark_rows, duck_rows = revenue_spark(spark, table), revenue_duckdb(py_table)
    assert len(spark_rows) == 10
    assert [r[0] for r in duck_rows] == [r[0] for r in spark_rows]
    assert [r[2] for r in duck_rows] == [r[2] for r in spark_rows]
    assert [r[1] for r in duck_rows] == pytest.approx([r[1] for r in spark_rows])


def test_engines_read_the_same_files_and_create_no_copies(spark, table):
    objects_before = len(list_keys("s3://warehouse/"))
    py_table = get_pyiceberg_catalog().load_table(table)

    spark_files = {r.file_path for r in spark.sql(f"SELECT file_path FROM {table}.data_files").collect()}
    assert files_read_by_pyiceberg(py_table) == spark_files            # same physical Parquet files
    revenue_spark(spark, table)
    revenue_duckdb(py_table)
    assert len(list_keys("s3://warehouse/")) == objects_before         # reads add nothing to storage


def test_python_writer_is_visible_to_spark_immediately(spark, table):
    py_table = get_pyiceberg_catalog().load_table(table)
    before = spark.table(table).count()
    snapshot_id = append_from_python_service(py_table, orders(500, seed=99, first_id=1_000_001))

    assert spark.table(table).count() == before + 500
    latest = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    assert latest.snapshot_id == snapshot_id                            # one history, shared by all engines
    assert spark.sql(f"SELECT count(*) c FROM {table} WHERE order_id > 1000000").first().c == 500


def test_spark_change_is_visible_to_python_after_reload(spark, table):
    spark.sql(f"UPDATE {table} SET status = 'returned' WHERE order_id = 42")
    assert lookup_order(get_pyiceberg_catalog().load_table(table), 42)["status"] == "returned"
