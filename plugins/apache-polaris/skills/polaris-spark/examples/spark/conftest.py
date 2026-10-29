"""Session setup: run polaris_setup/bootstrap.py (host replaced), prepare FILE-storage
directories with table_setup.py, start Spark with the Polaris REST catalog configuration."""
from __future__ import annotations

import json
import pathlib
import subprocess

import pytest

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"
POLARIS = "http://polaris:8181"


def run_bootstrap() -> tuple[str, str]:
    source = pathlib.Path("/labs/polaris_setup/bootstrap.py").read_text(encoding="utf-8")
    run = subprocess.run(["python3", "-c", source.replace("http://localhost:8181", POLARIS)],
                         capture_output=True, text=True, timeout=120)
    out = run.stdout
    if "Catalog created successfully" not in out and "already exists" not in out:
        raise RuntimeError(out + run.stderr)
    client = next(l.split(":", 1)[1].strip() for l in out.splitlines() if l.startswith("Client ID  :"))
    secret = next(l.split(":", 1)[1].strip() for l in out.splitlines() if l.startswith("Client Secret:"))
    return client, secret


def table_setup(*idents: str) -> None:
    """Helper: table directories writable for Polaris and Spark."""
    for ident in idents:
        subprocess.run(["python3", "/labs/polaris_setup/icebergdata/table_setup.py", ident], cwd="/data",
                       check=True, capture_output=True)


@pytest.fixture(scope="session")
def creds():
    return run_bootstrap()


@pytest.fixture(scope="session")
def spark(creds):
    from common.spark_session import polaris_spark
    table_setup("db.events", "db.sql_events", "db.stream_events", "db.partitioned_events",
                "db.bucketed_plain", "db.bucketed_fanout")
    session = polaris_spark(*creds)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def measure():
    data: dict = {}

    def _m(key, value):
        data[key] = value
    yield _m
    MEASUREMENTS.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


@pytest.fixture(scope="session")
def checks(measure):
    rows: list = []
    yield rows
    measure("snippet_check", rows)


def record(rows, example, status, verdict, fix=""):
    rows.append({"example": example, "status": status, "verdict": verdict, "fix": fix})


def jerr(e: Exception) -> str:
    """The first Java exception line of a Py4J error (the Python header line says nothing)."""
    lines = [l.strip() for l in str(e).splitlines() if "Exception" in l and not l.strip().startswith("at ")]
    return (lines[0] if lines else str(e).splitlines()[0])[:240]
