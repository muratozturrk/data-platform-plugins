"""Creating catalogs (FILE and S3) on the older quickstart Polaris image and on
Polaris 1.8.0 defaults, then writing a table to the FILE catalog through the shared volume."""
import os
import subprocess

import pyarrow as pa
import pytest
import requests
from pyiceberg.catalog.rest import RestCatalog

from common.servers import QUICKSTART, MEM

FILE_BODY = {"name": "polariscatalog", "type": "INTERNAL", "properties": {"default-base-location": "file:///data"},
             "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///data"]}}
S3_BODY = {"name": "polariscatalog_s3", "type": "INTERNAL", "properties": {"default-base-location": "s3://my/s3/path"},
           "storageConfigInfo": {"roleArn": "arn:aws:iam::xxxxxxxxx:role/polaris-storage", "storageType": "S3",
                                 "allowedLocations": ["s3://my/s3/path"]}}


def post(server, path, body):
    return requests.post(f"{server.mgmt}{path}", json=body, headers=server.headers(), timeout=30)


def test_token_request_with_root_credentials(measure, example_rows):
    r = requests.post(f"{QUICKSTART.catalog_api}/v1/oauth/tokens", timeout=30, data={
        "grant_type": "client_credentials", "client_id": "root_user", "client_secret": "my_secret_id",
        "scope": "PRINCIPAL_ROLE:ALL"})
    assert r.status_code == 200
    example_rows.append({"example": "Generate a bearer token", "server": "quickstart image", "status": r.status_code,
                      "verdict": "as-is", "fix": ""})


def test_file_catalog_needs_insecure_storage_types(example_rows, measure):
    on_quickstart = post(QUICKSTART, "/catalogs", FILE_BODY)
    on_defaults = post(MEM, "/catalogs", FILE_BODY)
    assert on_quickstart.status_code == 201
    assert on_defaults.status_code == 400 and "Unsupported storage type: FILE" in on_defaults.text
    example_rows.append({"example": "Create catalog (local file system)", "server": "quickstart image", "status": 201,
                      "verdict": "as-is", "fix": ""})
    example_rows.append({"example": "Create catalog (local file system)", "server": "1.8.0 defaults", "status": 400,
                      "verdict": "fails", "fix": "needs ALLOW_INSECURE_STORAGE_TYPES + FILE in SUPPORTED_CATALOG_STORAGE_TYPES"})
    measure("file_on_defaults", on_defaults.json()["error"]["message"])


def test_s3_catalog_example_with_placeholder_arn(example_rows, measure):
    on_quickstart = post(QUICKSTART, "/catalogs", S3_BODY)
    on_latest = post(MEM, "/catalogs", S3_BODY)
    assert on_quickstart.status_code == 400 and "Invalid role ARN format" in on_quickstart.text
    assert on_latest.status_code == 201
    example_rows.append({"example": "Create catalog (S3, placeholder ARN)", "server": "quickstart image", "status": 400,
                      "verdict": "fails", "fix": "use a real 12-digit account ARN"})
    example_rows.append({"example": "Create catalog (S3, placeholder ARN)", "server": "1.8.0 defaults", "status": 201,
                      "verdict": "as-is", "fix": "accepted, fails later when credentials are vended"})
    measure("placeholder_arn", {"1.1.0-SNAPSHOT": on_quickstart.status_code, "1.8.0": on_latest.status_code})


def test_list_catalogs(example_rows):
    r = requests.get(f"{QUICKSTART.mgmt}/catalogs", headers=QUICKSTART.headers(), timeout=30)
    assert "polariscatalog" in [c["name"] for c in r.json()["catalogs"]]
    example_rows.append({"example": "Verify the catalog (list catalogs)", "server": "quickstart image", "status": r.status_code,
                      "verdict": "as-is", "fix": ""})


def test_file_catalog_writes_need_writable_table_directories(measure):
    """Polaris runs as uid 10000 and writes metadata.json into the shared volume; the volume root is
    owned by root (755). quickstart_env/icebergdata/table_setup.py creates the table directories with mode 777."""
    requests.put(f"{QUICKSTART.mgmt}/catalogs/polariscatalog/catalog-roles/catalog_admin/grants",
                 json={"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}, headers=QUICKSTART.headers())
    cat = RestCatalog("quickstart", uri=QUICKSTART.catalog_api, warehouse="polariscatalog",
                      credential="root_user:my_secret_id", scope="PRINCIPAL_ROLE:ALL")
    cat.create_namespace("lab")
    with pytest.raises(Exception) as e:
        cat.create_table("lab.orders", schema=pa.schema([("id", pa.int64())]))
    assert "Failed to create file" in str(e.value)

    helper = subprocess.run(["python", "/labs/quickstart_env/icebergdata/table_setup.py", "lab.orders"], cwd="/data",
                            capture_output=True, text=True)
    assert helper.returncode == 0
    table = cat.create_table("lab.orders", schema=pa.schema([("id", pa.int64())]))
    table.append(pa.table({"id": pa.array([1, 2, 3], pa.int64())}))
    assert cat.load_table("lab.orders").scan().to_arrow().num_rows == 3

    owners = {}
    for root, _, files in os.walk("/data/lab/orders"):
        for f in files:
            kind = "metadata.json" if f.endswith(".metadata.json") else ("parquet" if f.endswith(".parquet") else
                                                                          "avro" if f.endswith(".avro") else "other")
            owners.setdefault(kind, set()).add(os.stat(os.path.join(root, f)).st_uid)
    assert owners["metadata.json"] == {10000}             # written by Polaris
    assert owners["parquet"] == {0} and owners["avro"] == {0}  # written by the engine
    measure("file_owners", {k: sorted(v) for k, v in owners.items()})
    measure("file_error_without_helper", str(e.value)[:160])
