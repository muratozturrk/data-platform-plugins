"""Measure what a format version changes on disk: metadata.json keys, Avro field ids, delete files."""
import io
from dataclasses import dataclass

import pyarrow.parquet as pq

from common.lakehouse import read_avro, read_metadata, read_object


@dataclass
class VersionProfile:
    format_version: int
    metadata_keys: set[str]
    snapshot_keys: set[str]
    snapshot_sequence_numbers: list[int | None]
    manifest_list_ids: set[int]
    manifest_entry_ids: set[int]
    data_file_ids: set[int]
    delete_files: list[dict]               # live delete files of the current snapshot


def _ids(record_schema: dict) -> set[int]:
    return {f["field-id"] for f in record_schema["fields"]}


def profile(metadata_location: str) -> VersionProfile:
    md = read_metadata(metadata_location)
    snap = next(s for s in md["snapshots"] if s["snapshot-id"] == md["current-snapshot-id"])
    ml_schema, _, manifests = read_avro(snap["manifest-list"])
    m_schema, _, _ = read_avro(manifests[0]["manifest_path"])
    df_schema = next(f["type"] for f in m_schema["fields"] if f["name"] == "data_file")
    deletes = []
    for m in manifests:
        if m.get("content", 0) != 1:
            continue
        for e in read_avro(m["manifest_path"])[2]:
            if e["status"] != 2:
                df = e["data_file"]
                deletes.append({k: df.get(k) for k in ("content", "file_format", "file_path", "record_count",
                                                       "referenced_data_file", "content_offset",
                                                       "content_size_in_bytes", "file_size_in_bytes")})
    return VersionProfile(
        md["format-version"], set(md), {k for s in md["snapshots"] for k in s},
        [s.get("sequence-number") for s in md["snapshots"]],
        _ids(ml_schema), _ids(m_schema), _ids(df_schema), deletes)


def position_delete_file(uri: str) -> dict:
    """Schema (name -> field id) and rows of a v2 position delete file (Parquet)."""
    t = pq.read_table(io.BytesIO(read_object(uri)))
    return {"field_ids": {f.name: int(f.metadata[b"PARQUET:field_id"]) for f in t.schema},
            "rows": t.num_rows, "referenced_files": set(t.column("file_path").to_pylist())}
