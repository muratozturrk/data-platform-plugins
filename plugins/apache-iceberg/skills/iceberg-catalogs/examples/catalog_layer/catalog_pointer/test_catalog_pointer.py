"""The catalog is a name -> current-metadata pointer with compare-and-swap commits."""
import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from common.lakehouse import list_keys, read_json, sql_catalog
from catalog_pointer.pointer import (append_with_retry, catalog_columns, catalog_row,
                                             resolve)

SCHEMA = Schema(
    NestedField(1, "order_id", LongType(), required=True),
    NestedField(2, "country", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
)


def orders(start: int, n: int) -> pa.Table:
    return pa.table({
        "order_id": pa.array(range(start, start + n), pa.int64()),
        "country": pa.array(["DE", "US", "TR", "FR"] * (n // 4) + ["DE"] * (n % 4)),
        "amount": pa.array([10.0 + i for i in range(n)], pa.float64()),
    }, schema=SCHEMA.as_arrow())


@pytest.fixture
def catalog(tmp_path, uid):
    db = str(tmp_path / "catalog.db")
    cat = sql_catalog(db, warehouse=f"s3://warehouse/jdbc/{uid}")
    cat.create_namespace("sales")
    yield cat, db
    for t in cat.list_tables("sales"):
        cat.purge_table(t)


def test_catalog_stores_only_a_name_and_a_pointer(catalog, measure):
    cat, db = catalog
    table = cat.create_table("sales.orders", SCHEMA)

    assert catalog_columns(db) == ["catalog_name", "table_namespace", "table_name",
                                   "metadata_location", "previous_metadata_location"]
    row = catalog_row(db, "sales", "orders")
    assert row.metadata_location == table.metadata_location
    assert row.metadata_location.endswith(".metadata.json")
    assert row.previous_metadata_location is None
    # Everything else (schema, partition spec, snapshots) lives in the file the pointer names.
    meta = read_json(row.metadata_location)
    assert [f["name"] for f in meta["schemas"][0]["fields"]] == ["order_id", "country", "amount"]
    measure("catalog_columns", len(catalog_columns(db)))


def test_every_commit_swaps_the_pointer_and_keeps_old_versions(catalog, measure):
    cat, db = catalog
    table = cat.create_table("sales.orders", SCHEMA)
    pointers = [catalog_row(db, "sales", "orders").metadata_location]
    for i in range(3):
        table.append(orders(i * 100, 100))
        row = catalog_row(db, "sales", "orders")
        assert row.previous_metadata_location == pointers[-1]   # swap from exactly the last version
        pointers.append(row.metadata_location)

    assert len(set(pointers)) == 4
    # Metadata versions are immutable files: all four still exist and are listed in metadata-log.
    stored = set(list_keys(table.location() + "/metadata/"))
    assert set(pointers) <= stored
    log = read_json(pointers[-1])["metadata-log"]
    assert [e["metadata-file"] for e in log] == pointers[:-1]
    measure("metadata_versions_after_3_appends", len(pointers))


def test_engine_resolves_name_to_data_files_through_metadata(catalog, measure):
    cat, _ = catalog
    table = cat.create_table("sales.orders", SCHEMA)
    for i in range(3):
        table.append(orders(i * 100, 100))

    res = resolve(cat, "sales.orders")
    assert res.metadata_location == table.metadata_location
    assert res.snapshot_id == table.refresh().current_snapshot().snapshot_id
    assert res.manifest_list.endswith(".avro")
    assert len(res.manifests) == 3            # PyIceberg fast-append: one new manifest per commit
    assert len(res.data_files) == 3
    assert all(f.endswith(".parquet") for f in res.data_files)
    measure("resolution", f"1 pointer -> 1 snapshot -> 1 manifest list -> {len(res.manifests)} manifests "
                          f"-> {len(res.data_files)} data files")


def test_stale_writer_is_rejected_and_succeeds_after_reload(catalog, measure):
    cat, db = catalog
    cat.create_table("sales.orders", SCHEMA).append(orders(0, 100))
    writer_a = cat.load_table("sales.orders")
    writer_b = cat.load_table("sales.orders")       # both read the same version

    writer_a.append(orders(100, 100))               # A wins the race
    result = append_with_retry(cat, "sales.orders", orders(1000, 50), stale_table=writer_b)

    assert result.attempts == 2                     # B's first commit was rejected ...
    assert len(result.conflicts) == 1
    assert "branch main has changed" in result.conflicts[0]   # ... because main moved
    final = cat.load_table("sales.orders")
    assert final.scan().to_arrow().num_rows == 250  # no lost update
    assert len(final.metadata.snapshots) == 3
    measure("conflict_message", result.conflicts[0][:90])


def test_pointing_at_an_older_metadata_file_shows_the_older_table(catalog):
    cat, db = catalog
    table = cat.create_table("sales.orders", SCHEMA)
    table.append(orders(0, 100))
    v_100_rows = catalog_row(db, "sales", "orders").metadata_location
    table.append(orders(100, 100))

    # Register the old metadata file under a new name: the catalog entry alone decides the state.
    as_of = cat.register_table("sales.orders_as_of_v1", v_100_rows)
    assert as_of.scan().to_arrow().num_rows == 100
    assert cat.load_table("sales.orders").scan().to_arrow().num_rows == 200
    cat.drop_table("sales.orders_as_of_v1")         # drop the entry only; files belong to orders
