import json
import pathlib
import uuid

import pyarrow as pa
import pytest

from common.polaris import ROOT, Admin, Principal

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"


@pytest.fixture(scope="session")
def admin() -> Admin:
    return Admin()


@pytest.fixture(scope="session")
def root() -> Principal:
    return Principal("root", *ROOT)


@pytest.fixture
def uid() -> str:
    """Short unique suffix: Polaris state survives between tests, names must not collide."""
    return uuid.uuid4().hex[:6]


@pytest.fixture(scope="session")
def measure():
    """Record measured values (written to measurements.json at the end)."""
    data: dict = {}

    def _m(key: str, value) -> None:
        data[key] = value
    yield _m
    MEASUREMENTS.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def seed_table(root: Principal, catalog: str, namespace: str, table: str, rows: int = 3) -> None:
    """Root writes a small table through PyIceberg (vended credentials, no static S3 keys)."""
    engine = root.pyiceberg(catalog)
    parts = namespace.split(".")
    for i in range(1, len(parts) + 1):
        ns = tuple(parts[:i])
        if ns not in engine.list_namespaces(tuple(parts[:i - 1]) if i > 1 else ()):
            engine.create_namespace(ns)
    data = pa.table({"id": pa.array(range(rows), pa.int64()),
                     "amount": pa.array([10.0 * (i + 1) for i in range(rows)])})
    engine.create_table(f"{namespace}.{table}", schema=data.schema).append(data)
