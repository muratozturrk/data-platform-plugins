"""Branch lifecycle on Project Nessie through its REST API v2 (https://projectnessie.org/develop/rest/).

Used for the stress-test scenario: clone production for a stress test without copying data, and for
multi-table changes that become visible on main in one atomic merge.
"""
from __future__ import annotations

import requests

from common.lakehouse import NESSIE_URI

API = f"{NESSIE_URI}/api/v2"


def _ok(r: requests.Response) -> dict:
    if r.status_code >= 400:
        raise RuntimeError(f"{r.request.method} {r.url}: {r.status_code} {r.text}")
    return r.json() if r.content else {}


def head(ref: str) -> str:
    return _ok(requests.get(f"{API}/trees/{ref}", timeout=30))["reference"]["hash"]


def create_branch(name: str, from_ref: str = "main") -> str:
    """Zero-copy clone: a new named pointer to the same commit. No table metadata is written."""
    source = {"type": "BRANCH", "name": from_ref, "hash": head(from_ref)}
    res = _ok(requests.post(f"{API}/trees", params={"name": name, "type": "BRANCH"},
                            json=source, timeout=30))
    return res["reference"]["hash"]


def delete_branch(name: str) -> None:
    _ok(requests.delete(f"{API}/trees/{name}@{head(name)}", timeout=30))


def merge(from_branch: str, into: str = "main", message: str | None = None) -> str:
    """Apply every commit of from_branch to `into` as one commit: all tables flip together."""
    body = {"fromRefName": from_branch, "fromHash": head(from_branch)}
    if message:
        body["message"] = message
    _ok(requests.post(f"{API}/trees/{into}@{head(into)}/history/merge", json=body, timeout=30))
    return head(into)


def branches() -> list[str]:
    refs = _ok(requests.get(f"{API}/trees", timeout=30))["references"]
    return [r["name"] for r in refs if r["type"] == "BRANCH"]


def table_metadata_location(ns: str, table: str, ref: str = "main") -> str | None:
    r = requests.get(f"{API}/trees/{ref}/contents/{ns}.{table}", timeout=30)
    if r.status_code == 404:
        return None
    return _ok(r)["content"]["metadataLocation"]


def diff(from_ref: str, to_ref: str) -> list[str]:
    """Content keys that differ between two refs."""
    res = _ok(requests.get(f"{API}/trees/{from_ref}/diff/{to_ref}", timeout=30))
    return [".".join(d["key"]["elements"]) for d in res["diffs"]]


def history(ref: str = "main", limit: int = 20) -> list[dict]:
    """Newest first: [{"commitMeta": {...}, "parentCommitHash": ...}, ...]."""
    return _ok(requests.get(f"{API}/trees/{ref}/history", params={"max-records": limit},
                            timeout=30))["logEntries"]
