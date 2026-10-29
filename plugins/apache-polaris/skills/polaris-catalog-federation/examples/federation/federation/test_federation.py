"""Polaris catalog federation with an external Iceberg REST catalog.

Scenario (partner data integration): a partner team runs its own Iceberg REST catalog.
Polaris exposes it as an EXTERNAL catalog so analysts use one endpoint and one identity, and
Polaris RBAC decides who may read or write.
"""
import uuid

import pyarrow as pa
import pytest

from common.polaris import ROOT, ApiError, CatalogClient
from common.remotes import REFERENCE_URI, federate, reference, unfederate

LOCATION = "s3://warehouse/rest/"          # the reference server's warehouse


def ids(*values):
    return pa.table({"id": pa.array(list(values), pa.int64())})


@pytest.fixture(scope="module")
def fed(admin):
    ns = f"partner_{uuid.uuid4().hex[:6]}"
    remote = reference()
    remote.create_namespace(ns)
    remote.create_table(f"{ns}.sales", schema=ids(1).schema).append(ids(1, 2, 3))
    name = f"fed_ref_{uuid.uuid4().hex[:6]}"
    r = federate(admin, name, REFERENCE_URI, "default", LOCATION)
    assert r.status_code == 201, r.text
    yield {"name": name, "ns": ns, "remote": remote}
    unfederate(admin, name)


def test_remote_tables_appear_and_read_through_polaris(fed, root, measure):
    c = CatalogClient(fed["name"], *ROOT, vended=False)
    assert fed["ns"] in c.list_namespaces()
    assert c.list_tables(fed["ns"]) == ["sales"]
    rows = root.pyiceberg(fed["name"]).load_table(f"{fed['ns']}.sales").scan().to_arrow().num_rows
    assert rows == 3
    measure("read_through_polaris_rows", rows)


def test_remote_changes_are_visible_without_any_sync(fed, root):
    fed["remote"].create_table(f"{fed['ns']}.returns", schema=ids(1).schema)
    fed["remote"].load_table(f"{fed['ns']}.sales").append(ids(4))
    c = CatalogClient(fed["name"], *ROOT, vended=False)
    assert sorted(c.list_tables(fed["ns"])) == ["returns", "sales"]
    assert root.pyiceberg(fed["name"]).load_table(f"{fed['ns']}.sales").scan().to_arrow().num_rows == 4


def test_writes_through_polaris_land_in_the_remote_catalog(fed, root, measure):
    engine = root.pyiceberg(fed["name"])
    engine.load_table(f"{fed['ns']}.sales").append(ids(5))
    CatalogClient(fed["name"], *ROOT, vended=False).create_namespace(f"{fed['ns']}_from_polaris")
    direct = fed["remote"]
    assert direct.load_table(f"{fed['ns']}.sales").scan().to_arrow().num_rows == 5
    assert (f"{fed['ns']}_from_polaris",) in direct.list_namespaces()
    measure("remote_snapshots_after_polaris_append", len(direct.load_table(f"{fed['ns']}.sales").snapshots()))


def test_polaris_rbac_governs_the_external_catalog(fed, admin, uid, measure):
    name, ns = fed["name"], fed["ns"]
    p = admin.create_principal(f"partner_analyst_{uid}")
    admin.create_principal_role(f"partner_analyst_{uid}")
    admin.assign_principal_role(p.name, f"partner_analyst_{uid}")
    try:
        with pytest.raises(ApiError) as before:
            CatalogClient(name, p.client_id, p.client_secret, vended=False).list_tables(ns)
        assert before.value.status == 403
        admin.create_catalog_role(name, "partner_reader")
        # Remote namespaces are not Polaris entities: a namespace-level grant cannot be attached.
        with pytest.raises(ApiError) as ns_grant:
            admin.grant(name, "partner_reader", {"type": "namespace", "namespace": [ns], "privilege": "TABLE_READ_DATA"})
        assert ns_grant.value.status == 404 and "not found" in ns_grant.value.text
        for priv in ("TABLE_LIST", "TABLE_READ_DATA"):                 # catalog-level grants work
            admin.grant(name, "partner_reader", {"type": "catalog", "privilege": priv})
        admin.assign_catalog_role(f"partner_analyst_{uid}", name, "partner_reader")
        engine = p.pyiceberg(name)
        assert engine.load_table(f"{ns}.sales").scan().to_arrow().num_rows >= 3
        with pytest.raises(Exception) as write:
            engine.load_table(f"{ns}.sales").append(ids(99))
        text = f"{type(write.value).__name__} {write.value}"
        assert "Forbidden" in text or "ACCESS_DENIED" in text or "403" in text
        measure("rbac_on_federated", {"before_grant": before.value.status,
                                          "namespace_grant": f"{ns_grant.value.status} {ns_grant.value.text[:90]}",
                                          "read_after_catalog_grant": "ok",
                                          "append_after_catalog_grant": type(write.value).__name__})
    finally:
        admin.drop_principal(p.name)
        admin.drop_principal_role(f"partner_analyst_{uid}")
        admin.drop_catalog_role(name, "partner_reader")


def test_overlapping_catalog_locations_are_rejected(admin, fed, measure):
    r = federate(admin, f"fed_dup_{uuid.uuid4().hex[:4]}", REFERENCE_URI, "default", LOCATION)
    assert r.status_code == 400 and "overlaps" in r.text
    measure("overlap_error", r.json()["error"]["message"][:120])


def test_without_vending_polaris_serves_metadata_but_not_data(admin, fed, root, measure):
    """Federation needs a storage configuration Polaris can vend from; without it engines get the
    table metadata but no way to read files."""
    name = fed["name"]
    assert unfederate(admin, name) == 204
    assert federate(admin, name, REFERENCE_URI, "default", LOCATION, vend=False).status_code == 201
    try:
        plain = CatalogClient(name, *ROOT, vended=False).load_table(fed["ns"], "sales")
        assert plain["metadata-location"].startswith(LOCATION)
        with pytest.raises(ApiError) as e:
            CatalogClient(name, *ROOT, vended=True).load_table(fed["ns"], "sales")
        assert e.value.status == 400 and "no credentials are available" in e.value.text
        measure("no_vending_error", e.value.text[:150])
    finally:
        unfederate(admin, name)
        assert federate(admin, name, REFERENCE_URI, "default", LOCATION).status_code == 201


def test_removing_the_federation_leaves_the_remote_untouched(admin, fed):
    status = unfederate(admin, fed["name"])
    assert status == 204                                     # allowed although the remote is not empty
    assert fed["remote"].load_table(f"{fed['ns']}.sales").scan().to_arrow().num_rows == 5
    assert federate(admin, fed["name"], REFERENCE_URI, "default", LOCATION).status_code == 201
