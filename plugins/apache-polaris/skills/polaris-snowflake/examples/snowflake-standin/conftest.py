import json
import pathlib
import subprocess

import pyarrow as pa
import pytest

from common.polaris import ROOT, Admin, Principal

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"
CLI = ["polaris", "--host", "polaris", "--port", "8181", "--client-id", ROOT[0], "--client-secret", ROOT[1]]


@pytest.fixture(scope="session")
def admin() -> Admin:
    return Admin()


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


def cli(command: str) -> subprocess.CompletedProcess:
    """Run a command as documented ('./polaris ...'), adding only connection options."""
    args = command.split()
    assert args[0] == "./polaris"
    return subprocess.run(CLI + args[1:], capture_output=True, text=True, timeout=60)


def seed_table(root: Principal, catalog: str, namespace: str, table: str, rows: int = 3):
    """Root writes a small table through PyIceberg (vended credentials, no static S3 keys)."""
    engine = root.pyiceberg(catalog)
    if (namespace,) not in engine.list_namespaces():
        engine.create_namespace(namespace)
    data = pa.table({"id": pa.array(range(rows), pa.int64()),
                     "event_date": pa.array(["2025-01-01"] * rows)})
    return engine.create_table(f"{namespace}.{table}", schema=data.schema).append(data)
