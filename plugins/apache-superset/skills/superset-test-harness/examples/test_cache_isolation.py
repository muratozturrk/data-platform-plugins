"""T-bi-018: cache keys include the institution; a cached result for t-acme is never served to t-globex; TTL respected."""
import time, pytest

@pytest.mark.integration
def test_no_cross_institution_cache_hit(superset_client, seeded):
    qc = superset_client.query_context(seeded["chart_events_by_institution"])
    a = superset_client.chart_data(qc, as_user="t-acme-analyst").json()["result"][0]
    b = superset_client.chart_data(qc, as_user="t-globex-analyst").json()["result"][0]
    assert {r["institution_id"] for r in a["data"]} == {"t-acme"} and {r["institution_id"] for r in b["data"]} == {"t-globex"}
    assert a.get("cache_key") != b.get("cache_key")

@pytest.mark.integration
def test_cache_ttl(superset_client, seeded):
    qc = superset_client.query_context(seeded["chart_revenue_today"])
    first = superset_client.chart_data(qc, as_user="t-acme-analyst").json()["result"][0]
    second = superset_client.chart_data(qc, as_user="t-acme-analyst").json()["result"][0]
    assert second["is_cached"] is True
    time.sleep(31)                                         # CACHE_DEFAULT_TIMEOUT=30 in the test config
    third = superset_client.chart_data(qc, as_user="t-acme-analyst").json()["result"][0]
    assert third["is_cached"] is False
