#!/usr/bin/env python3
"""Plan/apply/drift for Ranger policies by GUID. Usage:
  apply_policies.py plan  desired.json
  apply_policies.py apply desired.json
  apply_policies.py drift desired.json          # exit 1 if live differs from desired or unknown managed policies exist
Env: RANGER_URL, RANGER_USER, RANGER_PASSWORD"""
import json, os, sys, httpx

URL, AUTH = os.environ["RANGER_URL"].rstrip("/"), (os.environ["RANGER_USER"], os.environ["RANGER_PASSWORD"])
API = f"{URL}/service/public/v2/api"
MANAGED = "source:policy-manifest"
KEYS = ["name", "resources", "policyType", "policyItems", "denyPolicyItems", "allowExceptions", "denyExceptions", "dataMaskPolicyItems", "rowFilterPolicyItems", "policyLabels", "zoneName", "validitySchedules", "isEnabled", "isAuditEnabled", "policyPriority"]

def norm(p): return json.dumps({k: p.get(k) for k in KEYS if p.get(k) not in (None, [], "")}, sort_keys=True)

def live(services):
    out = {}
    with httpx.Client(auth=AUTH, timeout=60) as c:
        for s in services:
            r = c.get(f"{API}/service/{s}/policy"); r.raise_for_status()
            for p in r.json(): out[p["guid"]] = p
    return out

def plan(desired):
    cur = live({p["service"] for p in desired}); want = {p["guid"]: p for p in desired}
    create = [p for g, p in want.items() if g not in cur]
    update = [dict(p, id=cur[g]["id"]) for g, p in want.items() if g in cur and norm(p) != norm(cur[g])]
    delete = [p for g, p in cur.items() if g not in want and MANAGED in p.get("policyLabels", [])]
    unmanaged = [p for g, p in cur.items() if g not in want and MANAGED not in p.get("policyLabels", [])]
    return create, update, delete, unmanaged

def main():
    mode, desired = sys.argv[1], json.load(open(sys.argv[2]))
    create, update, delete, unmanaged = plan(desired)
    print(f"plan: +{len(create)} ~{len(update)} -{len(delete)}  (unmanaged live policies: {len(unmanaged)})")
    for tag, lst in (("+", create), ("~", update), ("-", delete)):
        for p in lst: print(f"  {tag} {p['service']}/{p['name']}")
    if mode == "drift":
        for p in unmanaged: print(f"  ? unmanaged {p['service']}/{p['name']} (id {p['id']})")
        sys.exit(1 if (create or update or delete) else 0)
    if mode == "apply":
        with httpx.Client(auth=AUTH, timeout=60, headers={"Content-Type": "application/json"}) as c:
            for p in create: c.post(f"{API}/policy", json=p).raise_for_status()
            for p in update: c.put(f"{API}/policy/{p['id']}", json=p).raise_for_status()
            for p in delete: c.delete(f"{API}/policy/{p['id']}").raise_for_status()      # revokes last, after grants are in place
            info = c.get(f"{URL}/service/plugins/plugins/info"); print("plugin info status:", info.status_code)

if __name__ == "__main__":
    main()
