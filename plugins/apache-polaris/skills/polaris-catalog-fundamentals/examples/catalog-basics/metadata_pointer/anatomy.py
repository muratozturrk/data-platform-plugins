"""Read an Iceberg table's metadata layers straight from object storage, the way an engine does
after Polaris answers LoadTable: metadata.json -> manifest list -> manifests -> data files.

Only the credentials Polaris vends in the LoadTable response are used.
"""
from __future__ import annotations

import io
import json

import fastavro

from common.polaris import CatalogClient, Principal, split_uri, vended_s3


class TableFiles:
    def __init__(self, who: Principal, catalog: str, namespace: str, table: str):
        self.client = CatalogClient(catalog, who.client_id, who.client_secret, vended=True)
        self.load = self.client.load_table(namespace, table)
        self.s3 = vended_s3(self.load)
        self.metadata_location = self.load["metadata-location"]

    def read(self, uri: str) -> bytes:
        bucket, key = split_uri(uri)
        return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def metadata(self) -> dict:
        return json.loads(self.read(self.metadata_location))

    def list_keys(self, prefix_uri: str) -> list[str]:
        bucket, prefix = split_uri(prefix_uri)
        res = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [f"s3://{bucket}/{o['Key']}" for o in res.get("Contents", [])]

    def avro(self, uri: str) -> list[dict]:
        return list(fastavro.reader(io.BytesIO(self.read(uri))))

    def manifest_list(self, snapshot: dict) -> list[dict]:
        return self.avro(snapshot["manifest-list"])

    def manifest_entries(self, manifest_path: str) -> list[dict]:
        return self.avro(manifest_path)


def current_snapshot(meta: dict) -> dict:
    return next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])

