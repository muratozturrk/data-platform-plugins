"""Shared helpers: one PyIceberg REST catalog on SeaweedFS (S3) that every Python engine uses."""
import os

from pyiceberg.catalog import load_catalog

REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")  # local dev default
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")  # local dev default
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# FileIO properties in PyIceberg naming. Polars and Daft read them from the table's io.
S3_PROPS = {
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": AWS_KEY,
    "s3.secret-access-key": AWS_SECRET,
    "s3.region": AWS_REGION,
    "s3.path-style-access": "true",
}


def get_catalog(name: str = "lake"):
    """PyIceberg client for the REST catalog; S3 settings must be explicit for SeaweedFS."""
    return load_catalog(name, **{"type": "rest", "uri": REST_URI, **S3_PROPS})


def reset_namespace(catalog, ns: str) -> None:
    """Drop every table (with purge) and recreate the namespace empty."""
    if (ns,) in catalog.list_namespaces():
        for ident in catalog.list_tables(ns):
            catalog.purge_table(ident)
        catalog.drop_namespace(ns)
    catalog.create_namespace(ns)
