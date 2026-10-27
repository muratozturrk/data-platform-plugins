"""Dremio OSS from Python: REST API for setup, Arrow Flight for SQL.

Dremio OSS 26 has no Iceberg REST catalog source (type RESTCATALOG is rejected), so the lab uses
an S3 source over the same SeaweedFS bucket. Dremio then discovers Iceberg tables the Hadoop way
(metadata/vN.metadata.json + version-hint.text), which REST-catalog tables do not have.
"""
import os
import time
import urllib.parse

import requests
from pyarrow import flight, fs

from common.lakehouse import AWS_KEY, AWS_REGION, AWS_SECRET, S3_ENDPOINT

DREMIO = "http://dremio:9047"
FLIGHT = "grpc+tcp://dremio:32010"
USER = os.getenv("DREMIO_USER", "admin")  # local dev default
PASSWORD = os.getenv("DREMIO_PASSWORD", "password123")  # local dev default
SOURCE = "lake"


def wait_until_up(timeout_s: int = 420) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if requests.get(DREMIO, timeout=5).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise TimeoutError("Dremio did not start")


def login() -> dict:
    """Create the first admin user on a fresh server (no-op afterwards) and return auth headers."""
    requests.put(f"{DREMIO}/apiv2/bootstrap/firstuser", headers={"Authorization": "_dremionull"},
                 json={"userName": USER, "firstName": "Lab", "lastName": "Admin", "email": "admin@example.com",
                       "createdAt": int(time.time() * 1000), "password": PASSWORD}, timeout=30)
    r = requests.post(f"{DREMIO}/apiv2/login", json={"userName": USER, "password": PASSWORD}, timeout=30)
    r.raise_for_status()
    return {"Authorization": f"_dremio{r.json()['token']}"}


def ensure_s3_source(headers: dict) -> None:
    r = requests.get(f"{DREMIO}/api/v3/catalog/by-path/{SOURCE}", headers=headers, timeout=30)
    if r.status_code == 200:
        return
    endpoint = S3_ENDPOINT.split("://", 1)[1]
    r = requests.post(f"{DREMIO}/api/v3/catalog", headers=headers, timeout=60, json={
        "entityType": "source", "name": SOURCE, "type": "S3",
        "config": {"accessKey": AWS_KEY, "accessSecret": AWS_SECRET, "secure": False,
                   "compatibilityMode": True, "rootPath": "/", "credentialType": "ACCESS_KEY",
                   "whitelistedBuckets": ["warehouse"], "defaultCtasFormat": "ICEBERG",
                   "propertyList": [{"name": "fs.s3a.endpoint", "value": endpoint},
                                    {"name": "fs.s3a.path.style.access", "value": "true"},
                                    {"name": "fs.s3a.endpoint.region", "value": AWS_REGION}]}})
    r.raise_for_status()


def s3fs() -> fs.S3FileSystem:
    scheme, endpoint = S3_ENDPOINT.split("://", 1)
    return fs.S3FileSystem(access_key=AWS_KEY, secret_key=AWS_SECRET, endpoint_override=endpoint,
                           scheme=scheme, region=AWS_REGION)


def publish_hadoop_pointer(table) -> int:
    """Make a REST-catalog table visible to a file-system reader: copy the current metadata file
    to vN.metadata.json and point version-hint.text at it. Returns N."""
    s3 = s3fs()
    src = table.metadata_location.removeprefix("s3://")
    meta_dir = src.rsplit("/", 1)[0]
    version = len(table.metadata.metadata_log) + 1
    s3.copy_file(src, f"{meta_dir}/v{version}.metadata.json")
    with s3.open_output_stream(f"{meta_dir}/version-hint.text") as out:
        out.write(str(version).encode())
    return version


def promote_as_iceberg(headers: dict, path: list[str]) -> None:
    fid = urllib.parse.quote("dremio:/" + "/".join(path), safe="")
    r = requests.post(f"{DREMIO}/api/v3/catalog/{fid}", headers=headers, timeout=120, json={
        "entityType": "dataset", "type": "PHYSICAL_DATASET", "path": path, "format": {"type": "Iceberg"}})
    r.raise_for_status()


class FlightSql:
    """The standard Arrow Flight pattern: auth header, get_flight_info, do_get, read_all."""

    def __init__(self):
        self.client = flight.FlightClient(FLIGHT)
        token = self.client.authenticate_basic_token(USER, PASSWORD)
        self.options = flight.FlightCallOptions(headers=[token])

    def query(self, sql: str):
        info = self.client.get_flight_info(flight.FlightDescriptor.for_command(sql), self.options)
        return self.client.do_get(info.endpoints[0].ticket, self.options).read_all()
