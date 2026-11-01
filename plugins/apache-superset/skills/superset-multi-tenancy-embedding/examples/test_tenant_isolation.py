"""Fitness test: tenant A cannot see tenant B's data through dashboards, chart data or guest tokens."""
import os, requests, pytest

URL = os.environ["SUPERSET_URL"]

def login(user, pwd):
    r = requests.post(f"{URL}/api/v1/security/login", json={"username": user, "password": pwd, "provider": "db", "refresh": True})
    r.raise_for_status(); return {"Authorization": f"Bearer {r.json()['access_token']}"}

@pytest.fixture(scope="module")
def tenants():
    return {"a": login(os.environ["T_A_USER"], os.environ["T_A_PWD"]), "b": login(os.environ["T_B_USER"], os.environ["T_B_PWD"])}

def chart_rows(headers, chart_id):
    r = requests.get(f"{URL}/api/v1/chart/{chart_id}/data/", headers=headers); r.raise_for_status()
    return r.json()["result"][0]["data"]

def test_rls_fences_tenants(tenants):
    chart = int(os.environ["SHARED_CHART_ID"])          # chart on gold_sales.fct_orders grouped by tenant_id
    rows_a = chart_rows(tenants["a"], chart); rows_b = chart_rows(tenants["b"], chart)
    assert {r["tenant_id"] for r in rows_a} == {"t-a"}
    assert {r["tenant_id"] for r in rows_b} == {"t-b"}

def test_dashboard_visibility(tenants):
    r = requests.get(f"{URL}/api/v1/dashboard/", headers=tenants["a"]); r.raise_for_status()
    slugs = {d["slug"] for d in r.json()["result"]}
    assert not any(s.startswith("t-b-") for s in slugs)

def test_guest_token_scope():
    token = os.environ["GUEST_TOKEN_T_A"]               # minted by the control plane for one dashboard
    r = requests.get(f"{URL}/api/v1/dashboard/{os.environ['DASHBOARD_T_B']}", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code in (401, 403)
