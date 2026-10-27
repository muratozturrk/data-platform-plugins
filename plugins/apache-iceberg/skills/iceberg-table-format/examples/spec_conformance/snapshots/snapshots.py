"""Snapshot-level spec rules read from raw files: sequence-number inheritance and manifest reuse."""
from dataclasses import dataclass

from common.lakehouse import read_avro

ADDED = 1


@dataclass(frozen=True)
class Entry:
    file_path: str
    status: int
    content: int
    raw_sequence_number: int | None          # as written in the manifest (null = inherit)
    sequence_number: int                     # data sequence number after inheritance
    file_sequence_number: int
    manifest_path: str


def entries(snapshot: dict) -> list[Entry]:
    """All manifest entries of a snapshot with the spec's inheritance rule applied.

    Spec: when a manifest entry's sequence_number / file_sequence_number is null, the entry was ADDED by
    the snapshot that wrote the manifest, and the value is inherited from the manifest list entry.
    Writers leave them null so a retried commit can get a new sequence number without rewriting manifests.
    """
    out = []
    for m in read_avro(snapshot["manifest-list"])[2]:
        inherited = m.get("sequence_number") or 0          # v1 manifests have none: sequence number 0
        for e in read_avro(m["manifest_path"])[2]:
            raw, raw_file = e.get("sequence_number"), e.get("file_sequence_number")
            if raw is None and e["status"] != ADDED and m.get("sequence_number") is not None:
                raise ValueError(f"spec violation: only ADDED entries may inherit ({m['manifest_path']})")
            out.append(Entry(e["data_file"]["file_path"], e["status"], e["data_file"].get("content", 0),
                             raw, inherited if raw is None else raw,
                             inherited if raw_file is None else raw_file, m["manifest_path"]))
    return out


def manifest_paths(snapshot: dict) -> list[str]:
    return [m["manifest_path"] for m in read_avro(snapshot["manifest-list"])[2]]
