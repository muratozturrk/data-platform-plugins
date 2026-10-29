import json
import pathlib

import pyarrow as pa
import pytest
import requests

from common.polaris import ROOT, Admin, Principal, token

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"
CATALOG = "lake"
POLARIS_EXT = "http://polaris:8181/api/catalog/polaris/v1/lake"     # generic tables and policies


@pytest.fixture(scope="session")
def lake():
    """Catalog 'lake' on S3 with STS vending; root writes eco.orders (100 rows) with PyIceberg."""
    a = Admin()
    a.create_catalog(CATALOG, "s3://warehouse/lake")
    a.grant(CATALOG, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    engine = Principal("root", *ROOT).pyiceberg(CATALOG)
    engine.create_namespace("eco")
    data = pa.table({"order_id": pa.array(range(100), pa.int64()),
                     "amount": pa.array([float(i % 10) * 12.5 for i in range(100)])})
    engine.create_table("eco.orders", schema=data.schema).append(data)
    return engine


@pytest.fixture(scope="session")
def h():
    return {"Authorization": f"Bearer {token(*ROOT)}"}


def trino(sql: str) -> list[list]:
    """Trino's HTTP protocol: POST the statement, follow nextUri until done."""
    r = requests.post("http://trino:8080/v1/statement", data=sql, timeout=60,
                      headers={"X-Trino-User": "lab", "X-Trino-Catalog": "lake"}).json()
    rows = []
    while True:
        if "error" in r:
            raise RuntimeError(r["error"]["message"])
        rows += r.get("data", [])
        if "nextUri" not in r:
            return rows
        r = requests.get(r["nextUri"], timeout=60).json()


@pytest.fixture(scope="session")
def measure():
    data: dict = {}

    def _m(key: str, value) -> None:
        data[key] = value
    yield _m
    MEASUREMENTS.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


@pytest.fixture(scope="session")
def claims(measure):
    rows: list = []
    yield rows
    measure("roadmap_check", rows)
