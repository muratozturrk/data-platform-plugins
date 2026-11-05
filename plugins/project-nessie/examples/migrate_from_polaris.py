"""Register Iceberg tables from an Apache Polaris catalog into a Nessie branch by metadata location.
Files are not moved. Run with the Polaris catalog set read-only."""
from __future__ import annotations
import os, httpx

POLARIS = os.environ["POLARIS_URI"]            # https://polaris/api/catalog
POLARIS_TOKEN = os.environ["POLARIS_TOKEN"]
NESSIE = os.environ["NESSIE_URI"]              # https://nessie/api/v2
NESSIE_TOKEN = os.environ["NESSIE_TOKEN"]
PREFIX, BRANCH = os.environ["POLARIS_CATALOG"], os.environ["NESSIE_BRANCH"]
PH = {"Authorization": f"Bearer {POLARIS_TOKEN}"}; NH = {"Authorization": f"Bearer {NESSIE_TOKEN}", "Content-Type": "application/json"}

def namespaces():
    r = httpx.get(f"{POLARIS}/v1/{PREFIX}/namespaces", headers=PH).raise_for_status().json()
    return [".".join(ns) for ns in r["namespaces"]]

def tables(ns):
    r = httpx.get(f"{POLARIS}/v1/{PREFIX}/namespaces/{ns}/tables", headers=PH).raise_for_status().json()
    return [t["name"] for t in r["identifiers"]]

def metadata_location(ns, t):
    r = httpx.get(f"{POLARIS}/v1/{PREFIX}/namespaces/{ns}/tables/{t}", headers=PH).raise_for_status().json()
    return r["metadata-location"], r["metadata"]

def nessie_commit(ops):
    head = httpx.get(f"{NESSIE}/trees/{BRANCH}", headers=NH).raise_for_status().json()["reference"]
    body = {"commitMeta": {"message": "register tables migrated from Polaris"}, "operations": ops}
    r = httpx.post(f"{NESSIE}/trees/{BRANCH}@{head['hash']}/history/commit", headers=NH, json=body, timeout=120)
    r.raise_for_status(); return r.json()

if __name__ == "__main__":
    ops = []
    for ns in namespaces():
        ops.append({"type": "PUT", "key": {"elements": ns.split(".")}, "content": {"type": "NAMESPACE", "elements": ns.split(".")}})
        for t in tables(ns):
            loc, meta = metadata_location(ns, t)
            snap = meta.get("current-snapshot-id", -1)
            ops.append({"type": "PUT", "key": {"elements": ns.split(".") + [t]},
                        "content": {"type": "ICEBERG_TABLE", "metadataLocation": loc, "snapshotId": snap,
                                    "schemaId": meta.get("current-schema-id", 0), "specId": meta.get("default-spec-id", 0),
                                    "sortOrderId": meta.get("default-sort-order-id", 0)}})
    print(nessie_commit(ops)["hash"], len(ops), "operations committed")
