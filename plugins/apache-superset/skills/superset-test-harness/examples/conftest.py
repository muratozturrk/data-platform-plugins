"""Superset harness fixtures (full or light). Requires docker; set HARNESS=full|light."""
import os, json, time, pathlib, jwt, httpx, pytest
from testcontainers.compose import DockerCompose

HERE = pathlib.Path(__file__).parent
GUEST_SECRET = "test-guest-secret"

@pytest.fixture(scope="session")
def superset_url():
    if os.environ.get("SUPERSET_URL"):                       # already running (CI service / PR namespace)
        yield os.environ["SUPERSET_URL"]; return
    with DockerCompose(str(HERE), compose_file_name="docker-compose.test.yml", pull=True) as c:
        host, port = c.get_service_host("superset", 8088), c.get_service_port("superset", 8088)
        url = f"http://{host}:{port}"
        for _ in range(120):
            try:
                if httpx.get(f"{url}/health", timeout=5).text.strip() == "OK": break
            except httpx.HTTPError: pass
            time.sleep(5)
        yield url

class SupersetClient:
    def __init__(self, base): self.base = base; self._tokens = {}; self._csrf = {}
    def _login(self, user):
        if user not in self._tokens:
            r = httpx.post(f"{self.base}/api/v1/security/login", json={"username": user, "password": os.environ.get("TEST_USERS_PWD", "test"), "provider": "db", "refresh": True})
            r.raise_for_status(); self._tokens[user] = r.json()["access_token"]
            c = httpx.get(f"{self.base}/api/v1/security/csrf_token/", headers={"Authorization": f"Bearer {self._tokens[user]}"}); self._csrf[user] = (c.json()["result"], c.cookies)
        return self._tokens[user]
    def _headers(self, as_user=None, guest_token=None, csrf=True):
        if guest_token: return {"X-GuestToken": guest_token, "Content-Type": "application/json"}, None
        user = as_user or "admin"; tok = self._login(user)
        h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
        if csrf: h["X-CSRFToken"] = self._csrf[user][0]
        return h, (self._csrf[user][1] if csrf else None)
    def get(self, path, **kw):
        h, cookies = self._headers(kw.pop("as_user", None), kw.pop("guest_token", None), csrf=False)
        return httpx.get(f"{self.base}{path}", headers=h, cookies=cookies, timeout=60, **kw)
    def post(self, path, json=None, **kw):
        h, cookies = self._headers(kw.pop("as_user", None), kw.pop("guest_token", None), kw.pop("csrf", True))
        return httpx.post(f"{self.base}{path}", headers=h, cookies=cookies, json=json, timeout=120, **kw)
    def query_context(self, chart_id):
        r = self.get(f"/api/v1/chart/{chart_id}?q=(columns:!(query_context))"); r.raise_for_status()
        return json.loads(r.json()["result"]["query_context"])
    def chart_data(self, qc, raw=False, **kw):
        r = self.post("/api/v1/chart/data", json=qc, **kw)
        return r
    def session_cookie(self):
        r = httpx.get(f"{self.base}/login/"); return next(c for c in r.cookies.jar if c.name == "session")

@pytest.fixture(scope="session")
def superset_client(superset_url): return SupersetClient(superset_url)

@pytest.fixture(scope="session")
def seeded(superset_client):
    """Ids of seeded assets (seed_institutions.sh wrote them to /fixtures/seeded.json inside the container; mirrored via API lookups here)."""
    def find(kind, key, value):
        r = superset_client.get(f"/api/v1/{kind}/?q=(filters:!((col:{key},opr:eq,value:'{value}')))"); r.raise_for_status(); return r.json()["result"][0]
    return {
        "chart_revenue_today": find("chart", "slice_name", "Revenue today")["id"],
        "chart_customers_table": find("chart", "slice_name", "Customers")["id"],
        "chart_events_by_institution": find("chart", "slice_name", "Events by institution")["id"],
        "dashboard_shared_uuid": find("dashboard", "slug", "sales-overview")["uuid"],
        "dashboard_globex_uuid": find("dashboard", "slug", "t-globex-ops")["uuid"],
    }

@pytest.fixture
def guest_token():
    def mint(institution, dashboard_uuid, ttl=120, rls=None):
        now = int(time.time())
        return jwt.encode({"user": {"username": f"{institution}:embed", "first_name": "Embed", "last_name": ""},
                           "resources": [{"type": "dashboard", "id": dashboard_uuid}],
                           "rls_rules": rls if rls is not None else [{"clause": f"institution_id = '{institution}'"}],
                           "iat": now, "exp": now + ttl, "aud": "superset", "type": "guest"}, GUEST_SECRET, algorithm="HS256")
    return mint
