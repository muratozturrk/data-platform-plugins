"""T-bi-013: Talisman/CSP, cookies, CSRF, public role, guest-token permission."""
import httpx, pytest, os

BASE = os.environ["SUPERSET_URL"]

def test_csp_frame_ancestors_only_host_app():
    h = httpx.get(f"{BASE}/login/").headers
    csp = h.get("content-security-policy", "")
    assert "frame-ancestors" in csp and os.environ["HOST_APP_ORIGIN"] in csp and "*" not in csp.split("frame-ancestors")[1].split(";")[0]

def test_session_cookie_flags(superset_client):
    cookie = superset_client.session_cookie()
    assert cookie.secure and cookie.has_nonstandard_attr("HttpOnly") and cookie.get_nonstandard_attr("SameSite") in ("None", "Lax")

def test_mutation_without_csrf_rejected(superset_client):
    r = superset_client.post("/api/v1/dashboard/", json={"dashboard_title": "x"}, csrf=False)
    assert r.status_code in (400, 401, 403)

def test_public_role_has_no_data_access(superset_client):
    perms = superset_client.get("/api/v1/security/roles/?q=(filters:!((col:name,opr:eq,value:Public)))").json()["result"]
    assert perms and not any("datasource access" in str(p) for p in perms)

def test_guest_token_endpoint_requires_permission(superset_client):
    r = superset_client.post("/api/v1/security/guest_token/", json={"user": {"username": "x"}, "resources": [], "rls": []}, as_user="t-acme-analyst")
    assert r.status_code in (401, 403)
