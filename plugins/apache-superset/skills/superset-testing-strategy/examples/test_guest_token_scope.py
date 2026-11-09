"""T-bi-014: guest token scope by embed UUID, expiry, RLS carried in token."""
import time, pytest

@pytest.mark.integration
def test_token_for_dashboard_a_rejected_on_dashboard_b(superset_client, seeded, guest_token):
    tok = guest_token("t-acme", seeded["dashboard_shared_uuid"])
    ok = superset_client.get(f"/api/v1/dashboard/{seeded['dashboard_shared_uuid']}", guest_token=tok); assert ok.status_code == 200
    other = superset_client.get(f"/api/v1/dashboard/{seeded['dashboard_globex_uuid']}", guest_token=tok); assert other.status_code in (401, 403)

@pytest.mark.integration
def test_chart_data_requires_dashboard_in_token(superset_client, seeded, guest_token):
    tok = guest_token("t-acme", seeded["dashboard_globex_uuid"])        # token for the other dashboard
    qc = superset_client.query_context(seeded["chart_revenue_today"])   # chart lives on the shared dashboard
    assert superset_client.chart_data(qc, guest_token=tok).status_code in (401, 403)

@pytest.mark.integration
def test_expired_token_rejected(superset_client, seeded, guest_token):
    tok = guest_token("t-acme", seeded["dashboard_shared_uuid"], ttl=2); time.sleep(3)
    assert superset_client.get(f"/api/v1/dashboard/{seeded['dashboard_shared_uuid']}", guest_token=tok).status_code == 401

@pytest.mark.integration
def test_rls_from_token_applied(superset_client, seeded, guest_token):
    tok = guest_token("t-acme", seeded["dashboard_shared_uuid"], rls=[{"clause": "institution_id = 't-globex'"}])   # host tries to widen scope
    rows = superset_client.chart_data(superset_client.query_context(seeded["chart_events_by_institution"]), guest_token=tok).json()["result"][0]["data"]
    assert {r["institution_id"] for r in rows} <= {"t-acme"}, "token RLS must not override role RLS fence"
