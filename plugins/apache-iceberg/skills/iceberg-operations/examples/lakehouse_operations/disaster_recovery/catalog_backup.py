"""Catalog state preservation and re-registration.

The catalog only maps a table name to its current metadata.json. Exporting that mapping regularly
(hourly for transactional workloads) is what makes register_table possible after
catalog loss. Uses the Iceberg REST catalog API directly as a custom export tool.
"""
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from pyspark.sql import SparkSession

from common.lakehouse import CATALOG, list_objects, s3_client

REST_URI = os.getenv("ICEBERG_REST_URI", "http://rest:8181")
BACKUP_PREFIX = "s3://warehouse-dr/catalog-backups/"


def export_catalog(namespaces: list[str], rest_uri: str = REST_URI) -> dict:
    """{"exported_at", "tables": {"ns.t": {"metadata_location", "current_snapshot_id", "location"}}}."""
    tables = {}
    for ns in namespaces:
        ids = requests.get(f"{rest_uri}/v1/namespaces/{ns}/tables", timeout=30).json()["identifiers"]
        for ident in ids:
            name = f"{ns}.{ident['name']}"
            body = requests.get(f"{rest_uri}/v1/namespaces/{ns}/tables/{ident['name']}", timeout=30).json()
            tables[name] = {"metadata_location": body["metadata-location"],
                            "current_snapshot_id": body["metadata"].get("current-snapshot-id"),
                            "location": body["metadata"]["location"]}
    return {"exported_at": datetime.now(timezone.utc).isoformat(), "tables": tables}


def save_backup(backup: dict, prefix: str = BACKUP_PREFIX) -> str:
    """Store the export in the DR bucket (another region in production), next to replicated table files."""
    url = urlparse(prefix)
    key = url.path.lstrip("/") + f"catalog-{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}.json"
    s3_client().put_object(Bucket=url.netloc, Key=key, Body=json.dumps(backup, indent=2).encode())
    return f"s3://{url.netloc}/{key}"


def load_latest_backup(prefix: str = BACKUP_PREFIX) -> dict:
    latest = max(uri for uri, _ in list_objects(prefix))      # timestamped names sort chronologically
    url = urlparse(latest)
    return json.loads(s3_client().get_object(Bucket=url.netloc, Key=url.path.lstrip("/"))["Body"].read())


def register(spark: SparkSession, table: str, metadata_file: str) -> None:
    spark.sql(f"CALL {CATALOG}.system.register_table(table => '{table}', metadata_file => '{metadata_file}')")


def restore_catalog(spark: SparkSession, backup: dict) -> list[str]:
    """Re-register every table of the backup that the catalog no longer knows. Idempotent."""
    restored = []
    for table, entry in backup["tables"].items():
        ns = table.split(".")[0]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        if not spark.catalog.tableExists(table):
            register(spark, table, entry["metadata_location"])
            restored.append(table)
    return restored


_VERSION = re.compile(r"/(\d+)-[0-9a-f-]+\.metadata\.json$")


def latest_metadata_file(table_location: str) -> str:
    """No backup at all: pick the highest-numbered metadata.json found on storage.

    Caveat: a commit that wrote its metadata.json but lost the catalog swap also leaves a high-numbered
    file. A catalog export is the reliable source; this is the last resort.
    """
    candidates = [uri for uri, _ in list_objects(table_location, "metadata/") if _VERSION.search(uri)]
    return max(candidates, key=lambda uri: int(_VERSION.search(uri).group(1)))
