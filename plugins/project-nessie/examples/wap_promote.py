"""Write-Audit-Publish on Nessie via REST API v2 (control-plane helper)."""
from __future__ import annotations
import os, sys, httpx
from datetime import datetime, timezone

NESSIE = os.environ.get("NESSIE_URI", "https://nessie.platform.svc/api/v2")
TOKEN = os.environ["NESSIE_TOKEN"]
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def ref(name: str) -> dict:
    return httpx.get(f"{NESSIE}/trees/{name}", headers=H, timeout=30).raise_for_status().json()["reference"]

def create_branch(name: str, from_ref: str) -> dict:
    src = ref(from_ref)
    r = httpx.post(f"{NESSIE}/trees", headers=H, params={"name": name, "type": "BRANCH"},
                   json={"type": "BRANCH", "name": from_ref, "hash": src["hash"]}, timeout=30)
    r.raise_for_status(); return r.json()["reference"]

def merge(from_branch: str, into: str) -> dict:
    src = ref(from_branch)
    r = httpx.post(f"{NESSIE}/trees/{into}/history/merge", headers=H,
                   json={"fromRefName": from_branch, "fromHash": src["hash"], "message": f"WAP publish {from_branch}"}, timeout=60)
    r.raise_for_status(); return r.json()

def tag(name: str, from_ref: str) -> dict:
    src = ref(from_ref)
    r = httpx.post(f"{NESSIE}/trees", headers=H, params={"name": name, "type": "TAG"},
                   json={"type": "BRANCH", "name": from_ref, "hash": src["hash"]}, timeout=30)
    r.raise_for_status(); return r.json()["reference"]

def delete_branch(name: str) -> None:
    b = ref(name)
    httpx.delete(f"{NESSIE}/trees/{name}@{b['hash']}", headers=H, timeout=30).raise_for_status()

def run_audits(branch: str) -> bool:
    """Call the platform's quality runner against the WAP branch (GE/Soda/dbt tests/Elementary)."""
    r = httpx.post(os.environ["QUALITY_RUNNER"], headers=H, json={"ref": branch}, timeout=600)
    return r.status_code == 200 and r.json().get("status") == "pass"

if __name__ == "__main__":
    pipeline, run_id, institution = sys.argv[1:4]
    wap = f"wap-{pipeline}-{run_id}"
    create_branch(wap, f"t-{institution}")
    print("write to", wap, "then rerun with --publish")      # pipeline writes happen outside this script
    if "--publish" in sys.argv:
        if not run_audits(wap):
            print("audit failed; branch kept for inspection"); sys.exit(1)
        merge(wap, f"t-{institution}")
        if "--release" in sys.argv:
            # rel-{YYYYMMDD} per nessie-branching-workflows; one release tag per institution per day
            tag(f"rel-{datetime.now(timezone.utc):%Y%m%d}", f"t-{institution}")
        delete_branch(wap); print("published")
