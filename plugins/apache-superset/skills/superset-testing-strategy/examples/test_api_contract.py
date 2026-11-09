"""T-bi-010: REST API contract on the harness. Spec fetched from the running (pinned) Superset and stored per version."""
import os, json, pathlib, schemathesis, httpx, pytest

BASE = os.environ["SUPERSET_URL"]
VERSION = os.environ["SUPERSET_VERSION"]
SPEC_FILE = pathlib.Path(f"contracts/superset-openapi-{VERSION}.json")

def _auth_headers():
    r = httpx.post(f"{BASE}/api/v1/security/login", json={"username": os.environ["SUPERSET_ADMIN"], "password": os.environ["SUPERSET_ADMIN_PWD"], "provider": "db", "refresh": True})
    r.raise_for_status(); return {"Authorization": f"Bearer {r.json()['access_token']}"}

def test_openapi_spec_pinned():
    live = httpx.get(f"{BASE}/api/v1/_openapi", headers=_auth_headers()).json()
    if not SPEC_FILE.exists():
        SPEC_FILE.parent.mkdir(parents=True, exist_ok=True); SPEC_FILE.write_text(json.dumps(live, indent=1, sort_keys=True)); pytest.skip("baseline written")
    stored = json.loads(SPEC_FILE.read_text())
    assert set(live["paths"]) >= set(stored["paths"]), f"endpoints removed: {set(stored['paths']) - set(live['paths'])}"

schema = schemathesis.from_uri(f"{BASE}/api/v1/_openapi", headers=_auth_headers()) if os.environ.get("RUN_SCHEMATHESIS") else None

@pytest.mark.skipif(schema is None, reason="set RUN_SCHEMATHESIS=1")
@schema.parametrize(endpoint="^/api/v1/(chart|dashboard|dataset)/$", method="GET")
def test_read_endpoints_conform(case):
    case.headers = _auth_headers(); case.call_and_validate()

def test_chart_data_response_contract(superset_client, seeded):
    """rowcount/data/query present; result_format csv returns text/csv."""
    qc = superset_client.query_context(seeded["chart_revenue_today"])
    r = superset_client.chart_data(qc); assert r.status_code == 200
    res = r.json()["result"][0]; assert {"rowcount", "data", "query", "colnames"} <= set(res)
    qc["result_format"] = "csv"; r2 = superset_client.chart_data(qc, raw=True)
    assert r2.headers["content-type"].startswith("text/csv")
