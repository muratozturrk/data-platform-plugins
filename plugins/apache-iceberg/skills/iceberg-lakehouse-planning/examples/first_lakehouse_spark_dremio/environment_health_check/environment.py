"""'Verify that everything is running', as code: one probe per lakehouse role.

The manual way is `docker ps` plus opening each UI by hand. A probe that talks to each service's API
proves more: the container is up AND the service answers the protocol the other layers use.
"""
import socket
from dataclasses import dataclass
from typing import Callable

import requests

from common.dremio import DREMIO_URL
from common.lakehouse import NESSIE_URI, S3_ENDPOINT, s3_client
from spark_ingestion.ingest import read_source


@dataclass(frozen=True)
class Role:
    layer: str
    classic_tool: str   # the usual tool for the role
    lab_tool: str
    host: str


ROLES = [
    Role("Operational database", "PostgreSQL", "PostgreSQL 16", "postgres"),
    Role("Data lake storage", "MinIO", "SeaweedFS (S3 API)", "seaweedfs"),
    Role("Ingestion and processing", "Apache Spark", "Spark 3.5.5 + Iceberg 1.8.1", "spark-iceberg"),
    Role("Catalog", "Nessie", "Nessie 0.76.6", "nessie"),
    Role("Query engine", "Dremio", "Dremio OSS 26.0.5", "dremio"),
]


def probe_postgres(spark) -> str:
    return f"{read_source(spark).count()} rows in fashion_sales"


def probe_storage(_spark) -> str:
    s3_client().head_bucket(Bucket="warehouse")
    return f"bucket 'warehouse' at {S3_ENDPOINT}"


def probe_spark(spark) -> str:
    return f"Spark {spark.version}"


def probe_nessie(_spark) -> str:
    cfg = requests.get(f"{NESSIE_URI}/config", timeout=5).json()
    return f"default branch {cfg['defaultBranch']}, API v{cfg['maxSupportedApiVersion']}"


def probe_dremio(_spark) -> str:
    r = requests.get(f"{DREMIO_URL}/apiv2/server_status", timeout=5)
    r.raise_for_status()
    return f"server status {r.text.strip()}"


PROBES: dict[str, Callable] = {
    "postgres": probe_postgres, "seaweedfs": probe_storage, "spark-iceberg": probe_spark,
    "nessie": probe_nessie, "dremio": probe_dremio,
}


def check_environment(spark) -> dict[str, tuple[bool, str]]:
    """{host: (ok, detail)} for every role. Never raises: a broken service is reported, not fatal."""
    report = {}
    for role in ROLES:
        try:
            report[role.host] = (True, PROBES[role.host](spark))
        except Exception as exc:                       # noqa: BLE001 - report every failure kind
            report[role.host] = (False, f"{type(exc).__name__}: {exc}")
    return report


def resolve(host: str) -> str:
    return socket.gethostbyname(host)
