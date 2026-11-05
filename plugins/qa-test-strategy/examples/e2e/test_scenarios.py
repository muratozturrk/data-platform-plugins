"""E2E runner: executes YAML scenarios against the ephemeral/E2E environment. Polls, never sleeps blindly."""
import json, os, time, uuid, pathlib, pytest, yaml, httpx, trino

ENV = {k: os.environ[k] for k in ["PLATFORM_API", "PLATFORM_TOKEN", "TRINO_GATEWAY", "MARQUEZ", "SUPERSET", "RANGER_AUDIT_OS"]}
SCENARIOS = sorted(pathlib.Path(__file__).parent.glob("e2e_*.yaml"))

def wait_for(pred, timeout_s, every=10):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred(): return True
        time.sleep(every)
    raise AssertionError(f"condition not met within {timeout_s}s")

def api(method, path, **kw):
    r = httpx.request(method, f"{ENV['PLATFORM_API']}{path}", headers={"Authorization": f"Bearer {ENV['PLATFORM_TOKEN']}"}, timeout=60, **kw)
    r.raise_for_status(); return r.json() if r.content else None

def trino_scalar(sql):
    cur = trino.dbapi.connect(host=ENV["TRINO_GATEWAY"], port=443, http_scheme="https", user="svc-e2e",
                              auth=trino.auth.JWTAuthentication(ENV["PLATFORM_TOKEN"]), client_tags=["workload=e2e"]).cursor()
    cur.execute(sql); return cur.fetchone()[0]

@pytest.mark.e2e
@pytest.mark.parametrize("path", SCENARIOS, ids=[p.stem for p in SCENARIOS])
def test_scenario(path):
    sc = yaml.safe_load(path.read_text()); run_id = uuid.uuid4().hex[:6]
    inst = sc["institution"].format(run_id=run_id); ctx = {"institution": inst, "institution_u": inst.replace("-", "_"), "run_id": run_id}
    expected = json.loads(pathlib.Path(sc["fixtures"]["expected"]).read_text()); ctx["expected"] = expected
    try:
        for step in sc["steps"]:
            if "apply_manifest" in step:
                api("POST", "/v1/manifests", json={"path": step["apply_manifest"], "institution": inst})
            elif "trigger" in step:
                api("POST", f"/v1/tenants/{inst}/pipelines/{step['trigger']['pipeline']}/runs")
            elif "wait" in step:
                w = step["wait"]
                if w["condition"] == "tenant_ready":
                    wait_for(lambda: api("GET", f"/v1/tenants/{inst}")["status"]["phase"] == "Ready", w["timeout_s"])
                elif w["condition"] == "airflow_dataset_updated":
                    ds = w["dataset"].format(**ctx)
                    wait_for(lambda: api("GET", f"/v1/datasets/{ds}/events")["count"] > 0, w["timeout_s"])
        for a in sc["assertions"]:
            if "trino_sql" in a:
                val = trino_scalar(a["trino_sql"].format(**ctx)); exp = a["equals"].format(expected=expected) if isinstance(a["equals"], str) else a["equals"]
                assert str(val) == str(eval(exp.strip("{}"), {}, {"expected": expected})) if "{" in str(a["equals"]) else str(val) == str(exp), a
            elif "marquez_lineage_edge" in a:
                e = a["marquez_lineage_edge"]; src = e["from"].format(**ctx); dst = e["to"].format(**ctx)
                lineage = httpx.get(f"{ENV['MARQUEZ']}/api/v1/lineage", params={"nodeId": f"dataset:hkm:{dst}", "depth": 3}).json()
                assert any(src in n["id"] for n in lineage["graph"]), f"missing lineage {src} -> {dst}"
            elif "superset_chart_data" in a:
                s = a["superset_chart_data"]; data = httpx.get(f"{ENV['SUPERSET']}/api/v1/chart/{s['chart']}/data/", headers={"Authorization": f"Bearer {ENV['PLATFORM_TOKEN']}"}).json()
                assert str(data["result"][0]["data"][0]["revenue_amount"]) == str(expected["revenue_amount"])
            elif "ranger_audit_present" in a:
                r = a["ranger_audit_present"]; hits = httpx.get(f"{ENV['RANGER_AUDIT_OS']}/ranger-audit-trino/_search", params={"q": f"reqUser:{r['user']} AND resource:*{r['resource']}*"}).json()
                assert hits["hits"]["total"]["value"] > 0
    finally:
        if sc.get("cleanup") == "delete_tenant":
            api("DELETE", f"/v1/tenants/{inst}")
