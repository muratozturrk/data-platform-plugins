"""Lakekeeper: soft deletion, remote signing, and federation into Polaris.

Lakekeeper's own features are exercised directly; the Polaris federation is tested as well and
documents an incompatibility between the two images used here.
"""
import uuid

import pyarrow as pa
import pytest
import requests

from common.polaris import ROOT, CatalogClient
from common.remotes import LAKEKEEPER_URI, LAKEKEEPER_WAREHOUSE, federate, lakekeeper, lakekeeper_setup, unfederate


def ids(*values):
    return pa.table({"id": pa.array(list(values), pa.int64())})


@pytest.fixture(scope="module")
def lk():
    warehouse_id = lakekeeper_setup()
    ns = f"crm_{uuid.uuid4().hex[:6]}"
    cat = lakekeeper()
    cat.create_namespace(ns)
    cat.create_table(f"{ns}.accounts", schema=ids(1).schema).append(ids(1, 2))
    return {"wid": warehouse_id, "ns": ns, "cat": cat}


def test_soft_deleted_table_can_be_undropped(lk, measure):
    cat, ns, wid = lk["cat"], lk["ns"], lk["wid"]
    cat.create_table(f"{ns}.contracts", schema=ids(1).schema).append(ids(7, 8, 9))
    cat.drop_table(f"{ns}.contracts")
    assert (ns, "contracts") not in cat.list_tables(ns)
    deleted = requests.get(f"{LAKEKEEPER_URI}/management/v1/warehouse/{wid}/deleted-tabulars", timeout=30).json()
    entry = next(t for t in deleted["tabulars"] if t["name"] == "contracts" and t["namespace"] == [ns])
    r = requests.post(f"{LAKEKEEPER_URI}/management/v1/warehouse/{wid}/deleted-tabulars/undrop",
                      json={"targets": [{"type": "table", "id": entry["id"]}]}, timeout=30)
    assert r.status_code == 204
    assert cat.load_table(f"{ns}.contracts").scan().to_arrow().num_rows == 3
    measure("soft_delete", {"deleted_at": entry["deleted-at"], "expiration_date": entry["expiration-date"]})


def test_lakekeeper_signs_requests_instead_of_vending_keys(lk, measure):
    """Data access model: the client gets a signer endpoint, never storage keys."""
    cfg = requests.get(f"{LAKEKEEPER_URI}/catalog/v1/config", params={"warehouse": LAKEKEEPER_WAREHOUSE},
                       timeout=30).json()
    prefix = cfg["defaults"]["prefix"]
    load = requests.get(f"{LAKEKEEPER_URI}/catalog/v1/{prefix}/namespaces/{lk['ns']}/tables/accounts", timeout=30).json()
    conf = load["config"]
    assert conf["s3.remote-signing-enabled"] == "true" and conf["s3.signer"] == "S3V4RestSigner"
    assert "s3.access-key-id" not in conf
    measure("lakekeeper_table_config_keys", sorted(conf))


def test_polaris_federation_to_lakekeeper_fails_in_these_versions(admin, lk, root, measure):
    """Polaris 1.8.0 accepts the external catalog, but reading through it fails: Lakekeeper replies
    brotli-compressed and the Polaris image cannot load the native brotli decoder
    (java.lang.UnsatisfiedLinkError in the Polaris log). Recorded as a version incompatibility."""
    name = f"fed_lk_{uuid.uuid4().hex[:4]}"
    r = federate(admin, name, f"{LAKEKEEPER_URI}/catalog", LAKEKEEPER_WAREHOUSE, "s3://warehouse/lakekeeper/")
    assert r.status_code == 201
    try:
        with pytest.raises(Exception) as e:
            CatalogClient(name, *ROOT, vended=False).load_table(lk["ns"], "accounts")
        assert getattr(e.value, "status", None) == 500
        direct = lk["cat"].load_table(f"{lk['ns']}.accounts").scan().to_arrow().num_rows
        assert direct == 2                                       # Lakekeeper itself is healthy
        measure("federation_status", {"create_external_catalog": r.status_code, "load_via_polaris": 500,
                                          "direct_rows": direct})
    finally:
        unfederate(admin, name)
