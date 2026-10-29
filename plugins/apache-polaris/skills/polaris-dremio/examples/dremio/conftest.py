"""Shared setup: Dremio OSS with a Nessie catalog source (stand-in for a PolarisCatalog Iceberg REST
source, which needs Dremio Enterprise) and an S3 source for COPY INTO files; Polaris with a catalog
that may register tables written by Dremio."""
import json
import pathlib

import boto3
import pytest

from common.dremio import Dremio
from common.polaris import ROOT, Admin, Principal

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"
LAKE = "lake"            # Dremio source name used instead of PolarisCatalog
LANDING = "landing"      # Dremio S3 source used instead of @MySource / @S3Bucket / @CloudSource
POLARIS_CATALOG = "my_catalog"

NESSIE_SOURCE = {
    "nessieEndpoint": "http://nessie:19120/api/v2",
    "nessieAuthType": "NONE",
    "storageProvider": "AWS",
    "awsRootPath": "/warehouse/nessie",
    "credentialType": "ACCESS_KEY",
    "awsAccessKey": "admin",
    "awsAccessSecret": "password",
    "secure": False,
    "propertyList": [
        {"name": "fs.s3a.path.style.access", "value": "true"},
        {"name": "fs.s3a.endpoint", "value": "seaweedfs:8333"},
        {"name": "dremio.s3.compat", "value": "true"},
    ],
}
LANDING_SOURCE = {
    "accessKey": "admin", "accessSecret": "password", "secure": False, "compatibilityMode": True,
    "rootPath": "/", "credentialType": "ACCESS_KEY", "whitelistedBuckets": ["landing"],
    "propertyList": [{"name": "fs.s3a.endpoint", "value": "seaweedfs:8333"},
                     {"name": "fs.s3a.path.style.access", "value": "true"}],
}


def with_lab_sources(sql: str) -> str:
    """Reference SQL written for a PolarisCatalog source, with only the source names replaced."""
    for name in ("@MySource/", "@CloudSource/", "@S3Data/", "@S3Bucket/"):
        sql = sql.replace(name, f"@{LANDING}/landing/")
    return sql.replace("PolarisCatalog.", f"{LAKE}.")


@pytest.fixture(scope="session")
def dremio() -> Dremio:
    d = Dremio()
    d.wait_until_ready(600)
    d.bootstrap_admin()
    d.login()
    d.create_source(LAKE, "NESSIE", NESSIE_SOURCE)
    d.create_source(LANDING, "S3", LANDING_SOURCE)
    for ns in ("sales", "logs", "hr"):
        d.sql(f"CREATE FOLDER IF NOT EXISTS {LAKE}.{ns}")
    return d


@pytest.fixture(scope="session")
def s3():
    return boto3.client("s3", endpoint_url="http://seaweedfs:8333", aws_access_key_id="admin",
                        aws_secret_access_key="password", region_name="us-east-1")


@pytest.fixture(scope="session")
def admin() -> Admin:
    a = Admin()
    # Tables registered from Dremio live under s3://warehouse/nessie, outside the namespace
    # location Polaris expects; the catalog allows that explicitly.
    a.create_catalog(POLARIS_CATALOG, "s3://warehouse/polaris",
                     allowed=["s3://warehouse/polaris", "s3://warehouse/nessie"],
                     properties={"polaris.config.allow.unstructured.table.location": "true"})
    a.grant(POLARIS_CATALOG, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    return a


@pytest.fixture(scope="session")
def root() -> Principal:
    return Principal("root", *ROOT)


@pytest.fixture(scope="session")
def measure():
    data: dict = {}

    def _m(key: str, value) -> None:
        data[key] = value
    yield _m
    MEASUREMENTS.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


@pytest.fixture(scope="session")
def checks(measure):
    rows: list = []
    yield rows
    measure("snippet_check", rows)


def run_sql(dremio, rows, example, sql, fix=""):
    """Run one reference statement; record ok/fails with the error text, never hide a failure."""
    try:
        result = dremio.sql(with_lab_sources(sql))
        rows.append({"example": example, "status": "ok", "verdict": "as-is" if not fix else "as-is (names)", "fix": fix})
        return result
    except Exception as e:                       # noqa: BLE001 - the error is the result
        rows.append({"example": example, "status": "fails", "verdict": str(e)[:200], "fix": fix})
        raise
