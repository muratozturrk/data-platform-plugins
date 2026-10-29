"""Root-user management calls for the S3 track: an S3 catalog with STS
credential vending, wired to the bootstrap.py principal role."""
from __future__ import annotations

import requests

FILE_SERVER = "http://polaris:8181"            # older image (1.1.0-incubating-SNAPSHOT), FILE storage
LATEST_SERVER = "http://polaris-latest:8181"   # apache/polaris:latest (1.8.0)
ROOT = ("root_user", "my_secret_id")      # POLARIS_BOOTSTRAP_CREDENTIALS in docker-compose.yml


def _headers(server: str = FILE_SERVER) -> dict:
    r = requests.post(f"{server}/api/catalog/v1/oauth/tokens", timeout=30, data={
        "grant_type": "client_credentials", "client_id": ROOT[0], "client_secret": ROOT[1],
        "scope": "PRINCIPAL_ROLE:ALL"})
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def create_s3_catalog(name: str, server: str = FILE_SERVER, principal_role: str = "polarisuserrole") -> dict:
    """Returns the storage config as the server stored it."""
    h = _headers(server)
    m = f"{server}/api/management/v1"
    base = f"s3://warehouse/{name}"
    calls = [
        ("POST", "/catalogs", {"catalog": {"name": name, "type": "INTERNAL", "properties": {"default-base-location": base},
                                           "storageConfigInfo": {
                                               "storageType": "S3", "allowedLocations": [base], "region": "us-east-1",
                                               "endpoint": "http://seaweedfs:8333", "pathStyleAccess": True,
                                               "stsEndpoint": "http://seaweedfs:8333",
                                               "roleArn": "arn:aws:iam::000000000000:role/lake-vendor"}}}),
        ("POST", f"/catalogs/{name}/catalog-roles", {"catalogRole": {"name": "writer"}}),
        ("PUT", f"/catalogs/{name}/catalog-roles/writer/grants", {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}),
        ("PUT", f"/principal-roles/{principal_role}/catalog-roles/{name}", {"catalogRole": {"name": "writer"}}),
    ]
    for method, path, body in calls:
        r = requests.request(method, m + path, json=body, headers=h, timeout=30)
        assert r.status_code in (200, 201), f"{method} {path}: {r.status_code} {r.text[:300]}"
    return requests.get(f"{m}/catalogs/{name}", headers=h, timeout=30).json()["storageConfigInfo"]
