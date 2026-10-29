import json
import pathlib

import docker
import pytest
import requests

MEASUREMENTS = pathlib.Path(__file__).parent / "measurements.json"


def token(server: str, client_id: str, secret: str, realm: str | None = None, header: str = "Polaris-Realm"):
    h = {header: realm} if realm else {}
    return requests.post(f"{server}/api/catalog/v1/oauth/tokens", headers=h, timeout=30, data={
        "grant_type": "client_credentials", "client_id": client_id, "client_secret": secret,
        "scope": "PRINCIPAL_ROLE:ALL"})


def container(service: str):
    client = docker.from_env()
    return next(c for c in client.containers.list(all=True)
                if c.labels.get("com.docker.compose.service") == service
                and c.labels.get("com.docker.compose.project") == "polaris-ops")


@pytest.fixture(scope="session")
def measure():
    data: dict = {}

    def _m(key: str, value) -> None:
        data[key] = value
    yield _m
    MEASUREMENTS.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
