"""Walk an Iceberg table's metadata tree by reading the raw files (no Iceberg library).

catalog pointer -> metadata.json -> manifest list (Avro) -> manifests (Avro) -> data files.
Layouts follow the Iceberg table spec: https://iceberg.apache.org/spec/
"""
import io
import json
import struct
from dataclasses import dataclass, field
from datetime import date

import fastavro

from common.lakehouse import read_object, rest_get

EPOCH = date(1970, 1, 1)


@dataclass
class DataFile:
    path: str
    record_count: int
    partition: dict
    lower: dict[int, bytes]          # field id -> single-value serialized bound
    upper: dict[int, bytes]


@dataclass
class Manifest:
    path: str
    added_snapshot_id: int
    partition_summaries: list[dict]  # one per partition field: contains_null, lower_bound, upper_bound
    files: list[DataFile] = field(default_factory=list)


@dataclass
class MetadataTree:
    metadata_location: str
    metadata: dict
    manifest_list: str
    manifests: list[Manifest]

    @property
    def data_files(self) -> list[DataFile]:
        return [f for m in self.manifests for f in m.files]

    def field_id(self, name: str) -> int:
        schema = next(s for s in self.metadata["schemas"] if s["schema-id"] == self.metadata["current-schema-id"])
        return next(f["id"] for f in schema["fields"] if f["name"] == name)


def catalog_pointer(namespace: str, table: str) -> str:
    """The only thing the catalog stores for a table: where its current metadata.json is."""
    return rest_get(f"/v1/namespaces/{namespace}/tables/{table}")["metadata-location"]


def read_avro(uri: str) -> list[dict]:
    return list(fastavro.reader(io.BytesIO(read_object(uri))))


def walk(namespace: str, table: str) -> MetadataTree:
    location = catalog_pointer(namespace, table)
    metadata = json.loads(read_object(location))
    snap = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
    manifests = []
    for entry in read_avro(snap["manifest-list"]):
        if entry["content"] != 0:                       # 0 = data manifest, 1 = delete manifest
            continue
        m = Manifest(entry["manifest_path"], entry["added_snapshot_id"], entry.get("partitions") or [])
        for e in read_avro(m.path):
            if e["status"] == 2:                        # 0 existing, 1 added, 2 deleted
                continue
            df = e["data_file"]
            m.files.append(DataFile(
                df["file_path"], df["record_count"],
                # fastavro applies the Avro `date` logical type; the spec value is days since epoch.
                {k: day_ordinal(v) if isinstance(v, date) else v for k, v in df["partition"].items()},
                {kv["key"]: kv["value"] for kv in df.get("lower_bounds") or []},
                {kv["key"]: kv["value"] for kv in df.get("upper_bounds") or []}))
        manifests.append(m)
    return MetadataTree(location, metadata, snap["manifest-list"], manifests)


# --- single-value serialization (spec: binary single-value serialization) -------------------------------------
def decode_long(b: bytes) -> int:
    return struct.unpack("<q", b)[0]


def decode_int(b: bytes) -> int:
    return struct.unpack("<i", b)[0]


def day_ordinal(d: date) -> int:
    """Value of the days(ts) partition transform: days since 1970-01-01."""
    return (d - EPOCH).days


# --- the two pruning steps an engine does during planning -------------------------------
def plan(tree: MetadataTree, day: date, id_range: tuple[int, int] | None = None,
         id_column: str = "order_id") -> dict:
    """Files to scan for `order_ts` on `day` [and order_id in id_range], using metadata only.

    Step 1 (manifest list): skip manifests whose partition summary does not contain the day.
    Step 2 (manifests): skip files whose partition value or column bounds exclude the filter.
    """
    want = day_ordinal(day)
    fid = tree.field_id(id_column)
    scanned_manifests, files = 0, []
    for m in tree.manifests:
        s = m.partition_summaries[0]
        if not decode_int(s["lower_bound"]) <= want <= decode_int(s["upper_bound"]):
            continue
        scanned_manifests += 1
        for f in m.files:
            if next(iter(f.partition.values())) != want:
                continue
            if id_range and (decode_long(f.upper[fid]) < id_range[0] or decode_long(f.lower[fid]) > id_range[1]):
                continue
            files.append(f.path)
    return {"manifests_total": len(tree.manifests), "manifests_scanned": scanned_manifests,
            "files_total": len(tree.data_files), "files": sorted(files)}

