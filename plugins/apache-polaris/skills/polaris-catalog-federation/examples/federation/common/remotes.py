"""External Iceberg REST catalogs and how Polaris federates them.

  reference   Apache Iceberg REST reference server (iceberg-rest-fixture 1.8.1)
  gravitino   Apache Gravitino Iceberg REST server 1.3.0 (standalone)
  lakekeeper  Lakekeeper 0.13.8 (Rust), no authentication configured

Polaris side: an EXTERNAL catalog with an ICEBERG_REST connection (catalog federation, enabled
with polaris.features."ENABLE_CATALOG_FEDERATION" in docker-compose.yml).
"""
from __future__ import annotations

import os

import requests
from pyiceberg.catalog.rest import RestCatalog

from common.polaris import MGMT, S3_ENDPOINT, VENDOR_ROLE_ARN, Admin

REFERENCE_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
GRAVITINO_URI = os.getenv("GRAVITINO_URI", "http://gravitino:9001/iceberg")
LAKEKEEPER_URI = os.getenv("LAKEKEEPER_URI", "http://lakekeeper:8181")
LAKEKEEPER_WAREHOUSE = "partner_wh"
STATIC_S3 = {"s3.endpoint": S3_ENDPOINT, "s3.path-style-access": "true", "s3.access-key-id": "admin",
             "s3.secret-access-key": "password", "s3.region": "us-east-1"}


def reference() -> RestCatalog:
    return RestCatalog("reference", uri=REFERENCE_URI, **STATIC_S3)


def gravitino() -> RestCatalog:
    return RestCatalog("gravitino", uri=GRAVITINO_URI, **STATIC_S3)


def lakekeeper() -> RestCatalog:
    return RestCatalog("lakekeeper", uri=f"{LAKEKEEPER_URI}/catalog", warehouse=LAKEKEEPER_WAREHOUSE, **STATIC_S3)


def lakekeeper_setup() -> str:
    """Idempotent: bootstrap the server and create the warehouse. Returns the warehouse id."""
    requests.post(f"{LAKEKEEPER_URI}/management/v1/bootstrap", json={"accept-terms-of-use": True}, timeout=30)
    existing = requests.get(f"{LAKEKEEPER_URI}/management/v1/warehouse", timeout=30).json()["warehouses"]
    for wh in existing:
        if wh["name"] == LAKEKEEPER_WAREHOUSE:
            return wh["id"]
    body = {"warehouse-name": LAKEKEEPER_WAREHOUSE, "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {"type": "s3", "bucket": "warehouse", "key-prefix": "lakekeeper",
                                "endpoint": S3_ENDPOINT, "region": "us-east-1", "path-style-access": True,
                                "sts-enabled": False, "flavor": "s3-compat"},
            "storage-credential": {"type": "s3", "credential-type": "access-key",
                                   "aws-access-key-id": "admin", "aws-secret-access-key": "password"},
            "delete-profile": {"type": "soft", "expiration-seconds": 3600}}
    r = requests.post(f"{LAKEKEEPER_URI}/management/v1/warehouse", json=body, timeout=30)
    r.raise_for_status()
    return r.json()["warehouse-id"]


def federate(admin: Admin, name: str, uri: str, remote_catalog: str, location: str, vend: bool = True):
    """Polaris EXTERNAL catalog that proxies an Iceberg REST catalog. Returns the HTTP response."""
    storage = {"storageType": "S3", "allowedLocations": [location], "region": "us-east-1",
               "endpoint": S3_ENDPOINT, "pathStyleAccess": True}
    storage |= ({"roleArn": VENDOR_ROLE_ARN, "stsEndpoint": S3_ENDPOINT} if vend else {"stsUnavailable": True})
    body = {"catalog": {"name": name, "type": "EXTERNAL", "properties": {"default-base-location": location},
                        "storageConfigInfo": storage,
                        "connectionConfigInfo": {"connectionType": "ICEBERG_REST", "uri": uri,
                                                 "remoteCatalogName": remote_catalog,
                                                 "authenticationParameters": {"authenticationType": "IMPLICIT"}}}}
    r = admin.s.post(f"{MGMT}/catalogs", json=body, timeout=30)
    if r.status_code == 201:
        admin.grant(name, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    return r


def unfederate(admin: Admin, name: str) -> int:
    """Remove only the Polaris catalog entity. Never use Admin.drop_catalog on a federated catalog:
    it drops tables and namespaces first, which would delete them in the remote catalog."""
    return admin.s.delete(f"{MGMT}/catalogs/{name}", timeout=30).status_code
