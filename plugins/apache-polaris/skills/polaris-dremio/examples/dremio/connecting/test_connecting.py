"""Connecting Dremio to Polaris. Dremio OSS 26.0.5 (the only Dremio image on Docker Hub) against
the Iceberg REST Catalog and Snowflake Open Catalog source types, and the storage-access choice
 ("Use vended credentials" vs fs.s3a static keys) on the Polaris side."""
import pyarrow as pa
import pytest
from pyiceberg.catalog.rest import RestCatalog

from common.polaris import CATALOG_API, ApiError
from conftest import POLARIS_CATALOG

REST_SOURCE = {     # the UI's General + Advanced Options, as a Dremio source config
    "restEndpointUri": "http://polaris:8181/api/catalog",
    "propertyList": [{"name": "warehouse", "value": POLARIS_CATALOG},
                     {"name": "scope", "value": "PRINCIPAL_ROLE:ALL"},
                     {"name": "fs.s3a.aws.credentials.provider",
                      "value": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"}],
    "secretPropertyList": [{"name": "fs.s3a.access.key", "value": "admin"},
                           {"name": "fs.s3a.secret.key", "value": "password"},
                           {"name": "credential", "value": "root:s3cr3t"}],
}


@pytest.mark.parametrize("source_type", ["RESTCATALOG", "SNOWFLAKEOPENCATALOG"])
def test_oss_has_no_rest_or_open_catalog_source(dremio, admin, source_type, measure):
    """Even with the plugin's support options switched on, the OSS API rejects the source type:
    the REST catalog plugin classes ship in the image but no source type is registered for them."""
    for option in ("plugins.restcatalog.enabled", "plugins.restcatalog.mutable.enabled"):
        dremio.sql(f'ALTER SYSTEM SET "{option}" = true')
    r = dremio.session.post(f"{dremio.url}/api/v3/catalog", json={
        "entityType": "source", "name": f"polaris_{source_type.lower()}", "type": source_type, "config": REST_SOURCE})
    measure(f"source_{source_type}", {"status": r.status_code, "error": r.json().get("errorMessage")})
    assert r.status_code == 400 and "invalid value" in r.text


@pytest.mark.parametrize("mode", ["static_keys", "vended_credentials"])
def test_the_two_storage_access_modes(admin, root, mode, measure):
    """The two options for data access through a REST catalog connector:
    static fs.s3a keys with 'Use vended credentials' off, or vending on.
    Same Polaris catalog, an Iceberg REST client in place of Dremio."""
    props = {"uri": CATALOG_API, "warehouse": POLARIS_CATALOG, "credential": f"{root.client_id}:{root.client_secret}",
             "scope": "PRINCIPAL_ROLE:ALL", "s3.endpoint": "http://seaweedfs:8333", "s3.path-style-access": "true",
             "s3.region": "us-east-1"}
    if mode == "static_keys":
        props |= {"s3.access-key-id": "admin", "s3.secret-access-key": "password"}
    else:
        props["header.X-Iceberg-Access-Delegation"] = "vended-credentials"
    cat = RestCatalog(f"probe_{mode}", **props)
    if ("modes",) not in cat.list_namespaces():
        cat.create_namespace("modes")
    t = cat.create_table(f"modes.{mode}", schema=pa.schema([("id", pa.int64())]))
    t.append(pa.table({"id": pa.array([1, 2, 3], pa.int64())}))
    rows = cat.load_table(f"modes.{mode}").scan().to_arrow().num_rows
    measure(f"mode_{mode}", {"rows": rows, "engine_holds_storage_keys": mode == "static_keys"})
    assert rows == 3


def test_vending_without_header_gives_no_storage_access(admin, root, measure):
    """With vending off and no static keys the engine has nothing to read the files with."""
    c = root.catalog(POLARIS_CATALOG, vended=False)
    loaded = c.load_table("modes", "vended_credentials")
    keys = [k for k in loaded.get("config", {}) if k.startswith("s3.") and "key" in k]
    measure("no_header_storage_keys", keys)
    assert keys == []
