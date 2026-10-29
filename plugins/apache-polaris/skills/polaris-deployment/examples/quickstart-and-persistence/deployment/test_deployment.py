"""Deploying Polaris locally with Docker and what changes for production: persistence that survives restarts and bootstrapping with the admin tool."""
import uuid

import requests

from common.servers import QUICKSTART, DURABLE, MEM, image_of, restart

S3_STORAGE = {"storageType": "S3", "allowedLocations": ["s3://warehouse/x"], "region": "us-east-1",
              "stsUnavailable": True}


def create_catalog(server, name):
    body = {"catalog": {"name": name, "type": "INTERNAL", "properties": {"default-base-location": "s3://warehouse/x"},
                        "storageConfigInfo": S3_STORAGE}}
    return requests.post(f"{server.mgmt}/catalogs", json=body, headers=server.headers(), timeout=30)


def catalogs(server):
    return [c["name"] for c in requests.get(f"{server.mgmt}/catalogs", headers=server.headers(), timeout=30).json()["catalogs"]]


def test_quickstart_server_starts_with_bootstrap_credentials(measure):
    assert QUICKSTART.ready()
    assert QUICKSTART.token()                                     # root_user / my_secret_id from POLARIS_BOOTSTRAP_CREDENTIALS
    measure("images", {s.service: image_of(s) for s in (QUICKSTART, MEM, DURABLE)})


def test_in_memory_persistence_forgets_everything_on_restart(measure):
    name = f"scratch_{uuid.uuid4().hex[:4]}"
    assert create_catalog(MEM, name).status_code == 201
    assert name in catalogs(MEM)
    seconds = restart(MEM)
    assert name not in catalogs(MEM)                         # state gone
    assert MEM.token()                                       # root comes back from POLARIS_BOOTSTRAP_CREDENTIALS
    measure("in_memory_restart", {"catalog_after_restart": False, "ready_after_s": seconds})


def test_relational_jdbc_persistence_survives_restart(measure):
    name = f"durable_{uuid.uuid4().hex[:4]}"
    assert create_catalog(DURABLE, name).status_code == 201
    seconds = restart(DURABLE)
    assert name in catalogs(DURABLE)                         # state kept in PostgreSQL
    measure("durable_restart", {"catalog_after_restart": True, "ready_after_s": seconds})


def test_durable_server_has_no_bootstrap_credentials_in_its_environment():
    """In the durable setup the root principal is created once by polaris-admin-tool, so the server
    container does not carry POLARIS_BOOTSTRAP_CREDENTIALS at all."""
    import docker
    client = docker.from_env()
    [c] = client.containers.list(filters={"label": ["com.docker.compose.service=polaris-durable"]})
    env = " ".join(c.attrs["Config"]["Env"])
    assert "POLARIS_BOOTSTRAP_CREDENTIALS" not in env
    assert DURABLE.token()                                   # root works anyway
