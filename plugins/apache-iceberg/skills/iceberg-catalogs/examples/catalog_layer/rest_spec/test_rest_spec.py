"""Speak the Iceberg REST Catalog protocol with nothing but HTTP + JSON.

Every test runs twice: against the Iceberg REST reference server and against Apache Polaris.
"""
import pyarrow as pa
import pytest

from common.iceberg_http import CatalogError, add_column, current_columns, main_snapshot_id, schema
from common.lakehouse import ROOT_CLIENT, list_keys, read_json, rest_catalog
from common.polaris import pyiceberg_client
from rest_spec.conftest import POLARIS_CATALOG, make_client

ORDERS = schema([("order_id", "long", True), ("country", "string", False), ("amount", "double", False)])


def engine(server):
    """A real engine (PyIceberg) on the same catalog, used where a commit needs new data files."""
    return rest_catalog() if server == "reference" else pyiceberg_client(POLARIS_CATALOG, *ROOT_CLIENT)


def test_config_handshake_tells_the_client_how_to_talk(server, measure):
    client = make_client(server)
    cfg = client.config
    assert set(cfg) >= {"defaults", "overrides"}
    if server == "polaris":
        assert client.prefix == POLARIS_CATALOG          # warehouse name becomes the URL prefix
        assert cfg["defaults"]["default-base-location"].startswith("s3://warehouse/polaris/")
    else:
        assert client.prefix is None
    assert "POST /v1/{prefix}/transactions/commit" in client.endpoints()
    measure(f"{server}_advertised_endpoints", len(client.endpoints()))


def test_namespace_and_table_lifecycle(server, ns):
    client = make_client(server)
    client.create_namespace(f"{ns}.sales", {"owner": "sales-domain"})
    assert f"{ns}.sales" in client.list_namespaces(parent=ns)

    created = client.create_table(f"{ns}.sales", "orders", ORDERS)
    assert created["metadata-location"].endswith(".metadata.json")
    assert created["metadata"]["format-version"] == 2
    assert client.table_exists(f"{ns}.sales", "orders")
    assert client.list_tables(f"{ns}.sales") == ["orders"]

    with pytest.raises(CatalogError) as missing:
        client.load_table(f"{ns}.sales", "no_such_table")
    assert (missing.value.status, missing.value.type) == (404, "NoSuchTableException")
    with pytest.raises(CatalogError) as dup:
        client.create_table(f"{ns}.sales", "orders", ORDERS)
    assert (dup.value.status, dup.value.type) == (409, "AlreadyExistsException")


def test_server_side_commit_writes_the_new_metadata_file(server, ns, measure):
    client = make_client(server)
    before = client.create_table(ns, "orders", ORDERS)
    requirements, updates = add_column(before["metadata"], "channel", "string")

    after = client.commit(ns, "orders", requirements, updates)

    # The client sent only a JSON diff; the server wrote metadata.json and swapped the pointer.
    assert after["metadata-location"] != before["metadata-location"]
    assert after["metadata-location"] in list_keys(after["metadata"]["location"] + "/metadata/")
    on_storage = read_json(after["metadata-location"])
    assert on_storage["current-schema-id"] == 1
    assert [f["name"] for f in on_storage["schemas"][1]["fields"]][-1] == "channel"
    assert on_storage["metadata-log"][-1]["metadata-file"] == before["metadata-location"]
    measure(f"{server}_metadata_files_after_1_commit",
            len([k for k in list_keys(after["metadata"]["location"] + "/metadata/")
                 if k.endswith(".metadata.json")]))


def test_stale_schema_requirement_is_rejected_with_409(server, ns, measure):
    alice, bob = make_client(server), make_client(server)
    alice.create_table(ns, "orders", ORDERS)
    seen_by_alice = alice.load_table(ns, "orders")["metadata"]
    seen_by_bob = bob.load_table(ns, "orders")["metadata"]

    alice.commit(ns, "orders", *add_column(seen_by_alice, "channel", "string"))
    with pytest.raises(CatalogError) as conflict:
        bob.commit(ns, "orders", *add_column(seen_by_bob, "discount", "double"))

    assert conflict.value.status == 409
    assert conflict.value.type == "CommitFailedException"
    assert "schema" in conflict.value.message.lower()
    meta = bob.load_table(ns, "orders")["metadata"]
    assert current_columns(meta) == ["order_id", "country", "amount", "channel"]   # Bob's change did not leak in
    # Retrying on fresh metadata succeeds.
    bob.commit(ns, "orders", *add_column(meta, "discount", "double"))
    measure(f"{server}_409_message", conflict.value.message[:80])


def test_assert_ref_snapshot_id_guards_against_a_concurrent_append(server, ns, uid):
    client = make_client(server)
    client.create_table(ns, "orders", ORDERS)
    stale = client.load_table(ns, "orders")["metadata"]
    assert main_snapshot_id(stale) is None

    tbl = engine(server).load_table(f"{ns}.orders")             # an engine appends a snapshot
    tbl.append(pa.table({"order_id": pa.array([1, 2], pa.int64()), "country": ["DE", "US"],
                         "amount": [10.0, 20.0]}, schema=tbl.schema().as_arrow()))

    # A maintenance job decided "main is empty, set a retention property" on the stale view.
    with pytest.raises(CatalogError) as conflict:
        client.commit(ns, "orders",
                      [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": None}],
                      [{"action": "set-properties", "updates": {"history.expire.max-snapshot-age-ms": "1"}}])
    assert conflict.value.status == 409
    assert "main" in conflict.value.message
    fresh = client.load_table(ns, "orders")["metadata"]
    assert "history.expire.max-snapshot-age-ms" not in fresh.get("properties", {})
    assert main_snapshot_id(fresh) is not None


def test_multi_table_transaction_is_all_or_nothing(server, ns):
    client = make_client(server)
    for t in ("orders", "order_items"):
        client.create_table(ns, t, ORDERS)
    orders_meta = client.load_table(ns, "orders")["metadata"]
    items_meta = client.load_table(ns, "order_items")["metadata"]
    req_o, upd_o = add_column(orders_meta, "channel", "string")
    _, upd_i = add_column(items_meta, "channel", "string")
    wrong = [{"type": "assert-current-schema-id", "current-schema-id": 42}]

    with pytest.raises(CatalogError) as failed:
        client.commit_transaction([(ns, "orders", req_o, upd_o), (ns, "order_items", wrong, upd_i)])
    assert failed.value.status == 409
    assert client.load_table(ns, "orders")["metadata"]["current-schema-id"] == 0   # nothing applied

    req_i, upd_i = add_column(items_meta, "channel", "string")
    client.commit_transaction([(ns, "orders", req_o, upd_o), (ns, "order_items", req_i, upd_i)])
    assert client.load_table(ns, "orders")["metadata"]["current-schema-id"] == 1
    assert client.load_table(ns, "order_items")["metadata"]["current-schema-id"] == 1
