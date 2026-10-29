"""The Iceberg REST catalog specification and where Polaris goes beyond it.

The same PyIceberg RestCatalog class and the same workload run against two REST servers: Apache
Polaris and the Apache Iceberg REST reference server. Then the servers are compared on what they
advertise and on multi-catalog support.
"""
import os
import uuid

import pyarrow as pa
import pytest
import requests
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.types import StringType

from common.polaris import S3_ENDPOINT, CatalogClient, token

REFERENCE_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")


def workload(catalog: RestCatalog, ns: str) -> dict:
    """Identical code for every REST server: namespace, table, append, evolve, read."""
    catalog.create_namespace(ns)
    data = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "city": pa.array(["Izmir", "Bonn", "Lyon"])})
    table = catalog.create_table(f"{ns}.stores", schema=data.schema)
    table.append(data)
    with table.update_schema() as s:
        s.add_column("region", StringType())
    table = catalog.load_table(f"{ns}.stores")
    out = table.scan().to_arrow()
    return {"rows": out.num_rows, "columns": out.schema.names, "snapshots": len(table.snapshots())}


@pytest.fixture(scope="module")
def polaris_catalog(admin):
    name = f"spec_{uuid.uuid4().hex[:6]}"
    admin.create_catalog(name, f"s3://warehouse/{name}")
    admin.grant(name, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    yield name
    admin.drop_catalog(name)


def test_pyiceberg_default_oauth_scope_is_rejected_by_polaris(polaris_catalog, root, measure):
    """Same client, but not zero configuration: PyIceberg asks for scope 'catalog' by default,
    Polaris only issues PRINCIPAL_ROLE:<name|ALL> scopes."""
    from pyiceberg.exceptions import OAuthError
    with pytest.raises(OAuthError) as e:
        RestCatalog("polaris_default_scope", uri="http://polaris:8181/api/catalog", warehouse=polaris_catalog,
                    credential=f"{root.client_id}:{root.client_secret}")
    assert "invalid_scope" in str(e.value)
    measure("2.3_default_scope_error", str(e.value)[:120])


def test_one_client_class_runs_unchanged_against_two_servers(polaris_catalog, root, measure):
    polaris_props = {"uri": "http://polaris:8181/api/catalog", "warehouse": polaris_catalog,
                     "credential": f"{root.client_id}:{root.client_secret}", "scope": "PRINCIPAL_ROLE:ALL",
                     "header.X-Iceberg-Access-Delegation": "vended-credentials"}
    # The reference server does not vend credentials: the engine must bring storage keys.
    reference_props = {"uri": REFERENCE_URI, "s3.endpoint": S3_ENDPOINT, "s3.path-style-access": "true",
                       "s3.access-key-id": "admin", "s3.secret-access-key": "password", "s3.region": "us-east-1"}
    ns = f"retail_{uuid.uuid4().hex[:4]}"
    results = {name: workload(RestCatalog(name, **props), ns)
               for name, props in (("polaris", polaris_props), ("reference", reference_props))}
    assert results["polaris"] == results["reference"] == {
        "rows": 3, "columns": ["id", "city", "region"], "snapshots": 1}
    measure("2.3_same_client_two_servers", {"result": results["polaris"],
                                            "props_polaris": sorted(polaris_props),
                                            "props_reference": sorted(reference_props)})


def test_servers_advertise_their_endpoints_in_config(polaris_catalog, root, measure):
    tok = token(root.client_id, root.client_secret)
    polaris = requests.get("http://polaris:8181/api/catalog/v1/config", params={"warehouse": polaris_catalog},
                           headers={"Authorization": f"Bearer {tok}"}, timeout=30).json()
    reference = requests.get(f"{REFERENCE_URI}/v1/config", timeout=30).json()
    p_eps, r_eps = set(polaris.get("endpoints", [])), set(reference.get("endpoints", []))
    assert "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}" in p_eps & r_eps
    measure("2.3_endpoints", {"polaris": len(p_eps), "reference": len(r_eps),
                              "only_polaris": sorted(p_eps - r_eps), "only_reference": sorted(r_eps - p_eps)})


def test_polaris_serves_many_catalogs_behind_one_endpoint(admin, root, polaris_catalog, measure):
    """Multi-catalog: the 'warehouse' parameter selects a catalog; config returns its URL prefix.
    The reference server is one catalog: the warehouse parameter changes nothing."""
    other = f"spec2_{uuid.uuid4().hex[:6]}"
    admin.create_catalog(other, f"s3://warehouse/{other}")
    try:
        a = CatalogClient(polaris_catalog, root.client_id, root.client_secret, vended=False).base
        b = CatalogClient(other, root.client_id, root.client_secret, vended=False).base
        assert a != b and a.endswith(polaris_catalog) and b.endswith(other)
        r1 = requests.get(f"{REFERENCE_URI}/v1/config", params={"warehouse": "x"}, timeout=30).json()
        r2 = requests.get(f"{REFERENCE_URI}/v1/config", params={"warehouse": "y"}, timeout=30).json()
        assert r1 == r2
        measure("2.3_polaris_prefixes", [a.rsplit("/v1/", 1)[-1], b.rsplit("/v1/", 1)[-1]])
    finally:
        admin.drop_catalog(other)


def test_the_server_not_the_client_writes_new_metadata(polaris_catalog, root, measure):
    """Server-side logic: a REST client sends requirements + updates; the server validates them and
    writes the new metadata.json. Shown on both servers with a client holding no storage keys."""
    ns = f"srv_{uuid.uuid4().hex[:4]}"
    engine = root.pyiceberg(polaris_catalog)
    engine.create_namespace(ns)
    engine.create_table(f"{ns}.t", schema=pa.schema([("id", pa.int64())]))
    plain = CatalogClient(polaris_catalog, root.client_id, root.client_secret, vended=False)
    before = plain.load_table(ns, "t")["metadata-location"]
    plain.set_table_properties(ns, "t", {"commit.source": "plain-http-client"})
    after = plain.load_table(ns, "t")["metadata-location"]
    assert after != before

    ref_ns = f"srv_{uuid.uuid4().hex[:4]}"
    s = requests.Session()
    s.post(f"{REFERENCE_URI}/v1/namespaces", json={"namespace": [ref_ns]}, timeout=30).raise_for_status()
    schema = {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": False}]}
    s.post(f"{REFERENCE_URI}/v1/namespaces/{ref_ns}/tables", json={"name": "t", "schema": schema},
           timeout=30).raise_for_status()
    r0 = s.get(f"{REFERENCE_URI}/v1/namespaces/{ref_ns}/tables/t", timeout=30).json()["metadata-location"]
    s.post(f"{REFERENCE_URI}/v1/namespaces/{ref_ns}/tables/t", timeout=30, json={
        "requirements": [], "updates": [{"action": "set-properties", "updates": {"commit.source": "plain-http-client"}}]
    }).raise_for_status()
    r1 = s.get(f"{REFERENCE_URI}/v1/namespaces/{ref_ns}/tables/t", timeout=30).json()["metadata-location"]
    assert r1 != r0
    measure("2.3_server_written_metadata", {"polaris": after.rsplit("/", 1)[-1], "reference": r1.rsplit("/", 1)[-1]})
