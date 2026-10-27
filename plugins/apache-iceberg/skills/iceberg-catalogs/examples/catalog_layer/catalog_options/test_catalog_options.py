"""One table format, four catalog implementations, portable tables."""
import os

import pytest

from common.lakehouse import HADOOP_WAREHOUSE, ROOT_CLIENT, list_keys, rest_catalog, sql_catalog
from common.polaris import pyiceberg_client
from catalog_options.options import (POINTERS, migrate, nessie_content, nessie_history,
                                             spark_current_metadata)

MIGRATION_CATALOG = "options_migration"


@pytest.fixture
def spark_ns(spark, uid, request):
    """Namespace options_<uid> in the catalog given by the test parameter."""
    catalog = request.param if hasattr(request, "param") else "rest"
    ns = f"options_{uid}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")
    yield catalog, ns
    # NessieCatalog sets gc.enabled=false (files may be shared by branches): no PURGE there.
    purge = "" if catalog == "nessie" else " PURGE"
    for row in spark.sql(f"SHOW TABLES IN {catalog}.{ns}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{ns}.{row.tableName}{purge}")
    spark.sql(f"DROP NAMESPACE IF EXISTS {catalog}.{ns}")


def write_orders(spark, fq: str, batches: int = 2):
    spark.sql(f"CREATE TABLE {fq} (order_id bigint, country string, amount double) USING iceberg")
    for b in range(batches):
        spark.sql(f"INSERT INTO {fq} SELECT id + {b * 100}, 'DE', 10.0 FROM range(100)")


@pytest.mark.parametrize("spark_ns", ["hadoop", "rest", "polaris", "nessie"], indirect=True)
def test_same_sql_on_four_catalog_implementations(spark, spark_ns, measure):
    catalog, ns = spark_ns
    fq = f"{catalog}.{ns}.orders"
    write_orders(spark, fq)

    assert spark.sql(f"SELECT count(*) c FROM {fq}").first().c == 200
    pointer = POINTERS[catalog](ns, "orders")
    # However the catalog stores it, the pointer names the same file Spark resolved.
    assert pointer.metadata_location.replace("file:", "") == spark_current_metadata(spark, fq)
    measure(f"{catalog}_pointer", f"{pointer.mechanism}: ...{pointer.metadata_location[-40:]}")


@pytest.mark.parametrize("spark_ns", ["hadoop"], indirect=True)
def test_hadoop_catalog_is_only_a_directory_layout(spark, spark_ns, measure):
    _, ns = spark_ns
    write_orders(spark, f"hadoop.{ns}.orders", batches=3)
    meta_dir = os.path.join(HADOOP_WAREHOUSE.replace("file://", ""), ns, "orders", "metadata")
    files = sorted(os.listdir(meta_dir))

    assert "version-hint.text" in files
    assert open(os.path.join(meta_dir, "version-hint.text")).read().strip() == "4"
    assert [f for f in files if f.endswith(".metadata.json")] == [f"v{i}.metadata.json" for i in range(1, 5)]
    # The commit protocol is "write vN+1 if it does not exist yet": it needs an atomic
    # rename/create on the file system, which object stores do not offer. Hence: deprecated.

    # PyIceberg has no Hadoop catalog type at all; it can only open the table read-only by path.
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import StaticTable
    with pytest.raises(ValueError):
        load_catalog("h", type="hadoop", warehouse=HADOOP_WAREHOUSE)
    static = StaticTable.from_metadata(f"file://{meta_dir}/v4.metadata.json")
    assert static.scan().to_arrow().num_rows == 300
    measure("hadoop_metadata_files", files)


@pytest.mark.parametrize("spark_ns", ["rest"], indirect=True)
def test_register_table_moves_a_table_without_copying_data(spark, spark_ns, polaris_admin, tmp_path, measure):
    _, ns = spark_ns
    write_orders(spark, f"rest.{ns}.orders")
    source = rest_catalog()
    location = source.load_table(f"{ns}.orders").location()
    objects_before = set(list_keys(location + "/"))

    # Target 1: a JDBC catalog (SqlCatalog on SQLite). Target 2: Apache Polaris.
    jdbc = sql_catalog(str(tmp_path / "jdbc.db"))
    jdbc.create_namespace(ns)
    polaris_admin.ensure_catalog(MIGRATION_CATALOG, extra_locations=("s3://warehouse/rest/",))
    polaris = pyiceberg_client(MIGRATION_CATALOG, *ROOT_CLIENT)
    polaris.create_namespace(ns)

    for target in (jdbc, polaris):
        moved = target.register_table(f"{ns}.orders",
                                      source.load_table(f"{ns}.orders").metadata_location)
        assert moved.scan().to_arrow().num_rows == 200
        assert moved.location() == location
    assert set(list_keys(location + "/")) == objects_before          # zero bytes copied
    measure("objects_under_table_location", len(objects_before))
    polaris.drop_table(f"{ns}.orders")
    polaris.drop_namespace(ns)


@pytest.mark.parametrize("spark_ns", ["rest"], indirect=True)
def test_two_catalogs_owning_one_table_split_its_history(spark, spark_ns, tmp_path, measure):
    _, ns = spark_ns
    write_orders(spark, f"rest.{ns}.orders")
    jdbc = sql_catalog(str(tmp_path / "jdbc.db"))
    jdbc.create_namespace(ns)
    rest = rest_catalog()
    copy = migrate(rest, jdbc, f"{ns}.orders", drop_source=False)    # forgot the cutover step

    spark.sql(f"INSERT INTO rest.{ns}.orders SELECT id + 1000, 'US', 1.0 FROM range(10)")
    copy.append(copy.scan().to_arrow().slice(0, 5))                  # writer on the other catalog

    rest_rows = spark.sql(f"SELECT count(*) c FROM rest.{ns}.orders").first().c
    jdbc_rows = jdbc.load_table(f"{ns}.orders").scan().to_arrow().num_rows
    assert (rest_rows, jdbc_rows) == (210, 205)                     # two diverged histories
    rest_parent = rest.load_table(f"{ns}.orders").metadata.metadata_log[-1].metadata_file
    jdbc_parent = jdbc.load_table(f"{ns}.orders").metadata.metadata_log[-1].metadata_file
    assert rest_parent == jdbc_parent                               # forked from the same version
    measure("split_brain_rows", {"rest": rest_rows, "jdbc": jdbc_rows})


@pytest.mark.parametrize("spark_ns", ["rest"], indirect=True)
def test_cutover_drops_the_source_entry_but_keeps_the_files(spark, spark_ns, tmp_path):
    _, ns = spark_ns
    write_orders(spark, f"rest.{ns}.orders")
    jdbc = sql_catalog(str(tmp_path / "jdbc.db"))
    jdbc.create_namespace(ns)
    rest = rest_catalog()
    location = rest.load_table(f"{ns}.orders").location()

    moved = migrate(rest, jdbc, f"{ns}.orders")                     # register + drop (no purge)

    assert not rest.table_exists(f"{ns}.orders")
    assert moved.refresh().scan().to_arrow().num_rows == 200
    assert any(k.endswith(".parquet") for k in list_keys(location + "/data/"))
    jdbc.purge_table(f"{ns}.orders")


@pytest.mark.parametrize("spark_ns", ["nessie"], indirect=True)
def test_nessie_records_every_table_change_as_a_commit(spark, spark_ns, measure):
    _, ns = spark_ns
    before = len(nessie_history())
    write_orders(spark, f"nessie.{ns}.orders", batches=2)            # create + 2 inserts
    spark.sql(f"ALTER TABLE nessie.{ns}.orders ADD COLUMN channel string")

    commits = nessie_history()[: len(nessie_history()) - before]
    assert len(commits) == 4
    assert all(f"{ns}.orders" in c["message"] for c in commits)
    content = nessie_content(ns, "orders")
    assert content["type"] == "ICEBERG_TABLE"
    assert content["schemaId"] == 1
    measure("nessie_commit_messages", [c["message"] for c in reversed(commits)])
