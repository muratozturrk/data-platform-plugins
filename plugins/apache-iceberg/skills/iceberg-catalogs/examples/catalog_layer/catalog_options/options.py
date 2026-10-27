"""Where each catalog implementation keeps the "current metadata" pointer, and how to move a
table from one catalog to another without copying data (register_table)."""
from __future__ import annotations

import os
from dataclasses import dataclass

import requests

from common.iceberg_http import IcebergRest
from common.lakehouse import HADOOP_WAREHOUSE, NESSIE_URI, ROOT_CLIENT, SPARK_POLARIS_CATALOG


@dataclass
class Pointer:
    catalog: str
    mechanism: str
    metadata_location: str


def _norm(uri: str) -> str:
    return uri.replace("file:///", "/").replace("file:/", "/")


def hadoop_pointer(ns: str, table: str) -> Pointer:
    """HadoopCatalog: no service. version-hint.text holds the number N of vN.metadata.json."""
    meta_dir = os.path.join(_norm(HADOOP_WAREHOUSE), ns, table, "metadata")
    with open(os.path.join(meta_dir, "version-hint.text")) as f:
        version = f.read().strip()
    return Pointer("hadoop", "file version-hint.text", os.path.join(meta_dir, f"v{version}.metadata.json"))


def rest_pointer(ns: str, table: str) -> Pointer:
    loc = IcebergRest.reference().load_table(ns, table)["metadata-location"]
    return Pointer("rest", "REST LoadTable (JDBC row behind the server)", loc)


def polaris_pointer(ns: str, table: str) -> Pointer:
    loc = IcebergRest.polaris(SPARK_POLARIS_CATALOG, *ROOT_CLIENT).load_table(ns, table)["metadata-location"]
    return Pointer("polaris", "REST LoadTable (Polaris entity store)", loc)


def nessie_content(ns: str, table: str, ref: str = "main") -> dict:
    r = requests.get(f"{NESSIE_URI}/api/v2/trees/{ref}/contents/{ns}.{table}", timeout=30)
    r.raise_for_status()
    return r.json()["content"]


def nessie_pointer(ns: str, table: str, ref: str = "main") -> Pointer:
    return Pointer("nessie", f"Nessie commit log (content on branch {ref})",
                   nessie_content(ns, table, ref)["metadataLocation"])


def nessie_history(ref: str = "main", limit: int = 50) -> list[dict]:
    r = requests.get(f"{NESSIE_URI}/api/v2/trees/{ref}/history", params={"max-records": limit}, timeout=30)
    r.raise_for_status()
    return [e["commitMeta"] for e in r.json()["logEntries"]]


POINTERS = {"hadoop": hadoop_pointer, "rest": rest_pointer, "polaris": polaris_pointer,
            "nessie": nessie_pointer}


def spark_current_metadata(spark, fq_table: str) -> str:
    """The metadata file Spark itself resolved (latest metadata_log_entries row)."""
    row = spark.sql(f"SELECT file FROM {fq_table}.metadata_log_entries "
                    f"ORDER BY timestamp DESC LIMIT 1").first()
    return _norm(row.file)


def migrate(source, target, identifier: str, drop_source: bool = True):
    """Catalog migration in two steps: register the current metadata file in the target, then
    drop the source entry WITHOUT purge (the files now belong to the target)."""
    metadata_location = source.load_table(identifier).metadata_location
    moved = target.register_table(identifier, metadata_location)
    if drop_source:
        source.drop_table(identifier)          # catalog entry only; data and metadata stay
    return moved
