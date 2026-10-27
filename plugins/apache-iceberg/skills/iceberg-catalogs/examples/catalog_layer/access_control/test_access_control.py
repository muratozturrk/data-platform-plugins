"""The catalog as the control plane: RBAC on Polaris + table-scoped vended credentials."""
import time

import pyarrow as pa
import pytest
from pyiceberg.exceptions import ForbiddenError

from common.iceberg_http import CatalogError, IcebergRest, schema
from common.lakehouse import ROOT_CLIENT, list_keys
from access_control.rbac import can, provision, teardown, vended_s3


@pytest.fixture(scope="module")
def domain(polaris_admin):
    import uuid
    suffix = uuid.uuid4().hex[:6]
    d = provision(polaris_admin, f"acl_{suffix}", suffix)
    yield d
    teardown(polaris_admin, d)


@pytest.fixture(autouse=True)
def no_static_storage_keys(monkeypatch):
    """Engines in this lab get storage access only from the catalog, never from the environment."""
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)


def test_analyst_reads_sales_but_cannot_change_it(domain, measure):
    engine = domain.analyst.engine(domain.catalog)
    orders = engine.load_table("sales.orders")
    assert orders.scan().to_arrow().num_rows == 3          # read via vended credentials only

    # Layer 1, storage: the vended keys are read-only, so writing the data file already fails.
    with pytest.raises(OSError) as append:
        orders.append(pa.table({"order_id": pa.array([4], pa.int64()), "amount": pa.array([1.0])}))
    assert "ACCESS_DENIED" in str(append.value)
    # Layer 2, catalog: even a metadata-only commit (no storage access needed) is refused.
    rest = domain.analyst.rest(domain.catalog)
    with pytest.raises(CatalogError) as commit:
        rest.commit("sales", "orders", [], [{"action": "set-properties", "updates": {"owner": "me"}}])
    assert commit.value.status == 403
    with pytest.raises(ForbiddenError):
        engine.create_table("sales.refunds", schema=orders.schema())
    with pytest.raises(ForbiddenError):
        engine.drop_table("sales.orders")
    assert engine.load_table("sales.orders").scan().to_arrow().num_rows == 3
    measure("analyst_append_error", str(append.value)[:60] + " ... ACCESS_DENIED")
    measure("analyst_commit_error", f"{commit.value.status} {commit.value.message[-60:]}")


def test_analyst_cannot_even_load_hr_tables(domain, measure):
    with pytest.raises(CatalogError) as denied:
        domain.analyst.rest(domain.catalog).load_table("hr", "salaries")
    assert denied.value.status == 403
    assert "not authorized" in denied.value.message
    measure("analyst_hr_load", f"{denied.value.status} {denied.value.message[:100]}")


def test_engineer_writes_through_an_engine_without_storage_keys(domain):
    engine = domain.engineer.engine(domain.catalog)
    orders = engine.load_table("sales.orders")
    orders.append(pa.table({"order_id": pa.array([4, 5], pa.int64()), "amount": pa.array([40.0, 50.0])}))
    assert engine.load_table("sales.orders").scan().to_arrow().num_rows == 5
    with pytest.raises(ForbiddenError):                     # writer on sales, not on hr
        engine.load_table("hr.salaries")


def test_vended_credentials_are_scoped_to_one_table_and_one_access_mode(domain, measure):
    admin_view = IcebergRest.polaris(domain.catalog, *ROOT_CLIENT)
    orders_loc = admin_view.load_table("sales", "orders")["metadata"]["location"]
    salaries_loc = admin_view.load_table("hr", "salaries")["metadata"]["location"]
    orders_file = list_keys(orders_loc + "/data/")[0]
    salaries_file = list_keys(salaries_loc + "/data/")[0]
    probe_key = orders_loc + "/data/probe-write.bin"

    analyst_s3, cfg = vended_s3(domain.analyst.rest(domain.catalog).load_table("sales", "orders"))
    engineer_s3, _ = vended_s3(domain.engineer.rest(domain.catalog).load_table("sales", "orders"))

    matrix = {
        "analyst read orders": can(analyst_s3, "read", orders_file),
        "analyst write orders": can(analyst_s3, "write", probe_key),
        "analyst read salaries": can(analyst_s3, "read", salaries_file),
        "engineer write orders": can(engineer_s3, "write", probe_key),
        "engineer read salaries": can(engineer_s3, "read", salaries_file),
    }
    assert matrix == {"analyst read orders": True, "analyst write orders": False,
                      "analyst read salaries": False, "engineer write orders": True,
                      "engineer read salaries": False}
    assert cfg["s3.access-key-id"].startswith("ASIA")       # temporary STS keys, not the admin key
    lifetime_s = (int(cfg["s3.session-token-expires-at-ms"]) / 1000) - time.time()
    assert 0 < lifetime_s <= 3600
    measure("vended_credential_matrix", matrix)
    measure("vended_credential_lifetime_s", round(lifetime_s))


def test_revoking_a_catalog_role_takes_effect_on_the_next_request(domain, polaris_admin):
    client = domain.analyst.rest(domain.catalog)
    assert client.load_table("sales", "orders")["metadata-location"]
    prole = next(r for r in domain.roles if r.startswith("analyst_"))

    polaris_admin.revoke_catalog_role(prole, domain.catalog, "sales_reader")
    try:
        with pytest.raises(CatalogError) as denied:
            client.load_table("sales", "orders")          # same token, new decision
        assert denied.value.status == 403
    finally:
        polaris_admin.assign_catalog_role(prole, domain.catalog, "sales_reader")
    assert client.load_table("sales", "orders")["metadata-location"]


def test_without_sts_the_catalog_cannot_vend_and_storage_must_enforce(polaris_admin, uid, measure):
    name = f"acl_nosts_{uid}"
    polaris_admin.create_catalog(name, vend_credentials=False)
    try:
        plain = IcebergRest.polaris(name, *ROOT_CLIENT)
        plain.create_namespace("sales")
        plain.create_table("sales", "orders", schema([("order_id", "long", True)]))
        with pytest.raises(CatalogError) as no_creds:
            IcebergRest.polaris(name, *ROOT_CLIENT, vended_credentials=True).load_table("sales", "orders")
        assert no_creds.value.status == 400
        assert "no credentials are available" in no_creds.value.message
        measure("no_sts_error", no_creds.value.message[:100])
    finally:
        polaris_admin.drop_catalog_cascade(name)
