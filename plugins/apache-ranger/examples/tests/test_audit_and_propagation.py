"""Audit completeness and revoke propagation (T-ranger-002, T-ranger-003)."""
import os, time, httpx, pytest, trino

OS_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
RANGER, AUTH = os.environ.get("RANGER_URL", "http://localhost:6080"), (os.environ.get("RANGER_USER", "admin"), os.environ.get("RANGER_PASSWORD", "rangerR0cks!"))
POLL_S = 35

def trino_query(user, sql):
    cur = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "localhost"), port=8080, user=user).cursor(); cur.execute(sql); cur.fetchall(); return cur.stats["queryId"]

def audit_hits(query):
    r = httpx.get(f"{OS_URL}/ranger-audit*/_search", params={"q": query, "size": 5}); r.raise_for_status(); return r.json()["hits"]["hits"]

def test_allowed_and_denied_access_are_audited_with_policy_id():
    qid = trino_query("ayse", "SELECT count(*) FROM t_acme_lake.gold_sales.fct_orders")
    with pytest.raises(trino.exceptions.TrinoUserError):
        trino_query("ayse", "SELECT 1 FROM t_globex_lake.gold_sales.fct_orders")
    time.sleep(10)
    ok = audit_hits(f'reqUser:ayse AND result:1 AND resource:*fct_orders*')
    denied = audit_hits('reqUser:ayse AND result:0 AND resource:*t_globex_lake*')
    assert ok and ok[0]["_source"].get("policy") not in (None, -1)
    assert denied, "deny must be audited"

def test_revoke_propagates_within_one_poll_interval():
    pol = httpx.get(f"{RANGER}/service/public/v2/api/service/trino-interactive/policy", params={"policyName": "t-acme-ra-0001-trino-interactive"}, auth=AUTH).json()[0]
    trino_query("ayse", "SELECT 1 FROM t_acme_lake.gold_sales.fct_orders LIMIT 1")
    try:
        httpx.put(f"{RANGER}/service/public/v2/api/policy/{pol['id']}", auth=AUTH, json=dict(pol, isEnabled=False)).raise_for_status()
        deadline = time.time() + POLL_S + 10
        while time.time() < deadline:
            try: trino_query("ayse", "SELECT 1 FROM t_acme_lake.gold_sales.fct_orders LIMIT 1"); time.sleep(3)
            except trino.exceptions.TrinoUserError: break
        else:
            pytest.fail("revoke not enforced within poll interval")
        info = httpx.get(f"{RANGER}/service/plugins/plugins/info", auth=AUTH).json()
        assert info, "plugin info must list the Trino plugin with the new policy version"
    finally:
        httpx.put(f"{RANGER}/service/public/v2/api/policy/{pol['id']}", auth=AUTH, json=dict(pol, isEnabled=True)).raise_for_status()
