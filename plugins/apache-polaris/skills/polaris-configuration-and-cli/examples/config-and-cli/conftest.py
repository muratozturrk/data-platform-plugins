import json
import os
import pathlib
import subprocess

import docker
import pytest
import requests

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"
CONN = ["--host", "polaris", "--port", "8181", "--client-id", "root", "--client-secret", "s3cr3t"]


def cli(line: str, conn: list[str] | None = None, env: dict | None = None, stdin: str | None = None):
    """Run a CLI line ('polaris ...'); only connection options are inserted after 'polaris'."""
    args = line.split()
    assert args[0] == "polaris"
    r = subprocess.run(args[:1] + (CONN if conn is None else conn) + args[1:], capture_output=True, text=True,
                       timeout=60, input=stdin, env={**os.environ, **(env or {})})
    r.out = (r.stdout + r.stderr).strip()
    return r


def token(server: str, client_id: str, secret: str, realm: str | None = None, header: str = "Polaris-Realm"):
    h = {header: realm} if realm else {}
    return requests.post(f"{server}/api/catalog/v1/oauth/tokens", headers=h, timeout=30, data={
        "grant_type": "client_credentials", "client_id": client_id, "client_secret": secret,
        "scope": "PRINCIPAL_ROLE:ALL"})


def container(service: str):
    client = docker.from_env()
    return next(c for c in client.containers.list(all=True)
                if c.labels.get("com.docker.compose.service") == service
                and c.labels.get("com.docker.compose.project") == "polaris-config")


@pytest.fixture(scope="session")
def measure():
    data: dict = {}

    def _m(key: str, value) -> None:
        data[key] = value
    yield _m
    MEASUREMENTS.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


@pytest.fixture(scope="session")
def example_rows(measure):
    rows: list = []
    yield rows
    measure("example_check", rows)
