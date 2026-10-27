"""One open table, several engines, five modular layers."""
from datetime import datetime

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestamptzType

from common.lakehouse import list_objects, rest_get
from common.orders import arrow_orders, create_orders_table, orders
from open_lakehouse.components import LAYERS, federated_revenue, probe_all

DAY = datetime(2025, 4, 1)
CUSTOMERS = pa.table({"customer_id": pa.array(range(1000), pa.int64()),
                      "segment": pa.array(["enterprise" if i < 100 else "smb" for i in range(1000)])})


def snapshot_ids_spark(spark, t):
    return [r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at").collect()]


def test_spark_write_is_read_by_pyiceberg_and_duckdb(spark, ns, catalog):
    t = f"{ns}.orders_interop"
    create_orders_table(spark, t)
    orders(spark, 20_000, 0, DAY, days=2).writeTo(t).append()
    expected = {r.country: round(r.revenue, 2) for r in spark.sql(
        f"SELECT country, sum(amount) revenue FROM {t} GROUP BY country").collect()}

    table = catalog.load_table(t)
    assert table.scan().to_arrow().num_rows == 20_000
    con = table.scan(selected_fields=("country", "amount")).to_duckdb(table_name="orders")
    got = dict(con.execute("SELECT country, round(sum(amount), 2) FROM orders GROUP BY country").fetchall())
    assert got == expected


def test_pyiceberg_created_table_is_read_by_spark(spark, ns, catalog):
    ident = f"{ns}.orders_from_python"
    schema = Schema(
        NestedField(1, "order_id", LongType()), NestedField(2, "customer_id", LongType()),
        NestedField(3, "country", StringType()), NestedField(4, "status", StringType()),
        NestedField(5, "amount", DoubleType()), NestedField(6, "order_ts", TimestamptzType()))
    table = catalog.create_table(ident, schema=schema)
    for i in range(3):                                               # three micro-batches
        table.append(arrow_orders(1_000, i * 1_000, DAY))
    assert spark.table(ident).count() == 3_000
    assert spark.sql(f"SELECT count(*) c FROM {ident}.snapshots WHERE operation = 'append'").first().c == 3
    fmt = spark.sql(f"SHOW TBLPROPERTIES {ident} ('format-version')").first().value
    assert fmt == "2"


def test_engines_share_one_snapshot_lineage(spark, ns, catalog):
    t = f"{ns}.orders_lineage"
    create_orders_table(spark, t)
    orders(spark, 1_000, 0, DAY).writeTo(t).append()                # Spark
    catalog.load_table(t).append(arrow_orders(1_000, 1_000, DAY))   # PyIceberg
    orders(spark, 1_000, 2_000, DAY).writeTo(t).append()            # Spark again, sees the PyIceberg commit
    py_ids = [s.snapshot_id for s in catalog.load_table(t).snapshots()]
    assert snapshot_ids_spark(spark, t) == py_ids and len(py_ids) == 3
    assert spark.table(t).count() == catalog.load_table(t).scan().to_arrow().num_rows == 3_000


def test_catalog_commit_is_a_pointer_swap(spark, ns):
    t = f"{ns}.orders_pointer"
    create_orders_table(spark, t)
    name = t.split(".")[1]
    pointers = [rest_get(f"/v1/namespaces/{ns}/tables/{name}")["metadata-location"]]
    for i in range(2):
        orders(spark, 1_000, i * 1_000, DAY).writeTo(t).append()
        pointers.append(rest_get(f"/v1/namespaces/{ns}/tables/{name}")["metadata-location"])
    assert len(set(pointers)) == 3                                   # every commit moves the pointer
    on_storage = {u for u, _ in list_objects(pointers[0].rsplit("/", 1)[0] + "/")}
    assert set(pointers) <= on_storage                               # old versions stay, immutable


def test_storage_outlives_catalog_entry(spark, ns, catalog):
    t = f"{ns}.orders_register"
    create_orders_table(spark, t)
    orders(spark, 2_000, 0, DAY).writeTo(t).append()
    metadata_location = catalog.load_table(t).metadata_location
    spark.sql(f"DROP TABLE {t}")                                     # no PURGE: files stay on S3
    assert not catalog.table_exists(t)
    catalog.register_table(t, metadata_location)                     # re-attach from metadata.json
    assert spark.table(t).count() == 2_000


def test_federated_query_and_layer_probes(spark, ns, catalog):
    t = f"{ns}.orders_layers"
    create_orders_table(spark, t)
    orders(spark, 10_000, 0, DAY).writeTo(t).append()
    rev = federated_revenue(catalog, t, CUSTOMERS).to_pylist()
    assert [r["segment"] for r in rev] == ["enterprise", "smb"]
    assert sum(r["orders"] for r in rev) == 10_000

    status = probe_all(spark, catalog, t, arrow_orders(500, 10_000, DAY), CUSTOMERS)
    assert status == {layer.name: True for layer in LAYERS}
