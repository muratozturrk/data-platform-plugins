"""Catalog types and the challenges of having many catalog options.

A database-backed service catalog without its own access model (PyIceberg SqlCatalog, the Python
twin of Iceberg's JDBC catalog) is compared with Polaris on three challenges:
client-side logic, configuration and authorization.
"""
import uuid

import boto3
import pyarrow as pa
import pytest
from botocore.exceptions import ClientError
from pyiceberg.catalog.sql import SqlCatalog

from common.polaris import ROOT, S3_ENDPOINT, ApiError, CatalogClient, vended_s3

ADMIN_KEYS = {"s3.access-key-id": "admin", "s3.secret-access-key": "password"}
READONLY_KEYS = {"s3.access-key-id": "reader", "s3.secret-access-key": "readonly"}
SECRET_LIKE = ("secret", "password", "access-key", "credential", "token")


def sql_catalog(db: str, keys: dict) -> SqlCatalog:
    return SqlCatalog("jdbc_like", **{
        "uri": f"sqlite:///{db}", "warehouse": "s3://warehouse/jdbc_like",
        "s3.endpoint": S3_ENDPOINT, "s3.path-style-access": "true", "s3.region": "us-east-1", **keys})


def polaris_props(catalog: str, principal) -> dict:
    return {"uri": "http://polaris:8181/api/catalog", "warehouse": catalog,
            "credential": f"{principal.client_id}:{principal.client_secret}",
            "header.X-Iceberg-Access-Delegation": "vended-credentials"}


def rows(n: int = 5) -> pa.Table:
    return pa.table({"id": pa.array(range(n), pa.int64()), "amount": pa.array([2.5] * n)})


@pytest.fixture(scope="module")
def setups(admin, root, tmp_path_factory):
    db = str(tmp_path_factory.mktemp("jdbc") / "catalog.db")
    jdbc = sql_catalog(db, ADMIN_KEYS)
    for ns in ("finance", "hr"):
        jdbc.create_namespace(ns)
        jdbc.create_table(f"{ns}.ledger", schema=rows().schema).append(rows())
    cat = f"opts_{uuid.uuid4().hex[:6]}"
    admin.create_catalog(cat, f"s3://warehouse/{cat}")
    admin.grant(cat, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    engine = root.pyiceberg(cat)
    for ns in ("finance", "hr"):
        engine.create_namespace(ns)
        engine.create_table(f"{ns}.ledger", schema=rows().schema).append(rows())
    yield {"db": db, "polaris": cat}
    admin.drop_catalog(cat)


def test_configuration_every_engine_needs(setups, root, measure):
    """Without vending, engines need catalog credentials plus storage configuration and storage credentials."""
    jdbc_conf = {"uri": f"sqlite:///{setups['db']}", "warehouse": "s3://warehouse/jdbc_like",
                 "s3.endpoint": S3_ENDPOINT, **ADMIN_KEYS}
    rest_conf = polaris_props(setups["polaris"], root)
    secret_jdbc = sorted(k for k in jdbc_conf if any(s in k for s in SECRET_LIKE))
    secret_rest = sorted(k for k in rest_conf if any(s in k for s in SECRET_LIKE))
    assert secret_jdbc == ["s3.access-key-id", "s3.secret-access-key"]      # long-lived bucket keys
    assert secret_rest == ["credential"]                                     # catalog identity only
    assert not any(k.startswith("s3.") for k in rest_conf)
    measure("2.2_config", {"jdbc_like_props": len(jdbc_conf), "jdbc_like_secrets": secret_jdbc,
                           "polaris_props": len(rest_conf), "polaris_secrets": secret_rest})


def test_client_side_catalog_needs_write_access_to_storage_for_any_commit(setups, root, measure):
    """Client-side logic: the SqlCatalog client writes metadata.json itself, so even a property
    change fails with read-only storage keys. Through Polaris the server writes the metadata file:
    the same change succeeds from a client that has no storage keys at all."""
    readonly = sql_catalog(setups["db"], READONLY_KEYS)
    table = readonly.load_table("finance.ledger")
    assert table.scan().to_arrow().num_rows == 5                         # reading works
    with pytest.raises(Exception) as e:
        with table.transaction() as tx:
            tx.set_properties(owner="finance-team")
    assert "AccessDenied" in str(e.value) or "ACCESS_DENIED" in str(e.value) or "403" in str(e.value)

    rest = CatalogClient(setups["polaris"], root.client_id, root.client_secret, vended=False)
    before = rest.load_table("finance", "ledger")["metadata-location"]
    rest.set_table_properties("finance", "ledger", {"owner": "finance-team"})
    after = rest.load_table("finance", "ledger")
    assert after["metadata-location"] != before and after["metadata"]["properties"]["owner"] == "finance-team"
    measure("2.2_client_side_commit_error", str(e.value)[:160])


def test_without_catalog_authorization_storage_keys_open_every_table(setups, measure):
    """Authorization challenge: with a catalog that has no access model, whoever holds the storage
    keys reads every table's files, and the catalog cannot narrow that down."""
    jdbc = sql_catalog(setups["db"], ADMIN_KEYS)
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id="admin",
                      aws_secret_access_key="password", region_name="us-east-1")
    readable = 0
    for ns in ("finance", "hr"):
        for task in jdbc.load_table(f"{ns}.ledger").scan().plan_files():
            bucket, key = task.file.file_path.removeprefix("s3://").split("/", 1)
            s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            readable += 1
    assert readable == 2
    measure("2.2_static_key_reach", f"{readable} of 2 tables readable with one key pair")


def test_with_polaris_access_is_per_principal_and_per_table(setups, admin, uid, measure):
    cat = setups["polaris"]
    p = admin.create_principal(f"finance_bi_{uid}")
    admin.create_principal_role(f"finance_bi_{uid}")
    admin.assign_principal_role(p.name, f"finance_bi_{uid}")
    admin.create_catalog_role(cat, "finance_reader")
    admin.grant(cat, "finance_reader", {"type": "namespace", "namespace": ["finance"], "privilege": "TABLE_READ_DATA"})
    admin.assign_catalog_role(f"finance_bi_{uid}", cat, "finance_reader")
    try:
        client = CatalogClient(cat, p.client_id, p.client_secret, vended=True)
        load = client.load_table("finance", "ledger")
        s3 = vended_s3(load)
        hr_meta = CatalogClient(cat, *ROOT).load_table("hr", "ledger")["metadata"]
        hr_file = hr_meta["snapshots"][0]["manifest-list"]
        bucket, key = hr_file.removeprefix("s3://").split("/", 1)
        with pytest.raises(ClientError):
            s3.get_object(Bucket=bucket, Key=key)                          # finance keys cannot read hr
        with pytest.raises(ApiError) as e:
            client.load_table("hr", "ledger")
        assert e.value.status == 403
        measure("2.2_polaris_scope", {"finance": "readable", "hr via catalog": e.value.status,
                                      "hr via finance keys": "AccessDenied"})
    finally:
        admin.drop_principal(p.name)
        admin.drop_principal_role(f"finance_bi_{uid}")
        admin.drop_catalog_role(cat, "finance_reader")

