"""Apache Gravitino as an external catalog (distributed, regional metadata governance).
An EU region keeps its metadata in its own Gravitino Iceberg REST server; the
North America data lives in an internal Polaris catalog. Users see both through Polaris."""
import uuid

import pyarrow as pa
import pytest

from common.polaris import ROOT, CatalogClient
from common.remotes import GRAVITINO_URI, federate, gravitino, unfederate

LOCATION = "s3://warehouse/gravitino/"


def orders(*values):
    return pa.table({"order_id": pa.array(list(values), pa.int64())})


@pytest.fixture(scope="module")
def regions(admin, root):
    ns = f"eu_{uuid.uuid4().hex[:6]}"
    eu = gravitino()
    eu.create_namespace(ns)
    eu.create_table(f"{ns}.orders", schema=orders(1).schema).append(orders(1, 2, 3))
    fed = f"eu_gravitino_{uuid.uuid4().hex[:4]}"
    na = f"na_lakehouse_{uuid.uuid4().hex[:4]}"
    admin.create_catalog(na, f"s3://warehouse/{na}")
    admin.grant(na, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    engine = root.pyiceberg(na)
    engine.create_namespace("sales")
    engine.create_table("sales.orders", schema=orders(1).schema).append(orders(100, 101))
    yield {"ns": ns, "eu": eu, "fed": fed, "na": na}
    unfederate(admin, fed)
    admin.drop_catalog(na)


def test_remote_catalog_name_must_match_gravitinos_catalog(admin, regions, root, measure):
    """Polaris sends remoteCatalogName as the REST 'warehouse'. Gravitino's standalone server
    names its catalog 'default_catalog'; 'default' is created in Polaris but fails on use."""
    wrong = f"eu_wrong_{uuid.uuid4().hex[:4]}"
    assert federate(admin, wrong, GRAVITINO_URI, "default", LOCATION).status_code == 201
    try:
        with pytest.raises(Exception) as e:
            root.pyiceberg(wrong).load_table(f"{regions['ns']}.orders")
        assert "Couldn't find Iceberg configuration for catalog default" in str(e.value)
        measure("wrong_remote_name", str(e.value)[:140])
    finally:
        unfederate(admin, wrong)


def test_one_identity_reads_both_regions_through_polaris(admin, regions, root, measure):
    assert federate(admin, regions["fed"], GRAVITINO_URI, "default_catalog", LOCATION).status_code == 201
    eu_rows = root.pyiceberg(regions["fed"]).load_table(f"{regions['ns']}.orders").scan().to_arrow()
    na_rows = root.pyiceberg(regions["na"]).load_table("sales.orders").scan().to_arrow()
    assert sorted(eu_rows["order_id"].to_pylist()) == [1, 2, 3]
    assert sorted(na_rows["order_id"].to_pylist()) == [100, 101]
    union = pa.concat_tables([eu_rows, na_rows])
    assert union.num_rows == 5
    measure("global_view_rows", {"eu_via_gravitino": eu_rows.num_rows, "na_internal": na_rows.num_rows})


def test_writes_through_polaris_are_committed_by_gravitino(regions, root):
    root.pyiceberg(regions["fed"]).load_table(f"{regions['ns']}.orders").append(orders(4))
    direct = regions["eu"].load_table(f"{regions['ns']}.orders")
    assert sorted(direct.scan().to_arrow()["order_id"].to_pylist()) == [1, 2, 3, 4]
    meta = CatalogClient(regions["fed"], *ROOT, vended=False).load_table(regions["ns"], "orders")
    assert meta["metadata-location"] == direct.metadata_location          # same pointer on both sides
