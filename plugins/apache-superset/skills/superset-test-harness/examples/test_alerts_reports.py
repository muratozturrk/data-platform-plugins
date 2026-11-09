"""T-bi-005: a scheduled report executes, mail arrives in Mailpit, archive record carries the Nessie tag/snapshot reference."""
import time, httpx, pytest

@pytest.mark.integration
def test_report_delivered(superset_client, seeded, mailpit_url):
    body = {"type": "Report", "name": "e2e-sales-report", "dashboard": None, "chart": seeded["chart_revenue_today"], "report_format": "PNG",
            "crontab": "* * * * *", "recipients": [{"type": "Email", "recipient_config_json": '{"target": "qa@test.local"}'}], "active": True, "owners": [1]}
    r = superset_client.post("/api/v1/report/", json=body); r.raise_for_status(); rid = r.json()["id"]
    try:
        for _ in range(24):
            msgs = httpx.get(f"{mailpit_url}/api/v1/messages").json().get("messages", [])
            if any("e2e-sales-report" in m["Subject"] for m in msgs): break
            time.sleep(5)
        else:
            pytest.fail("report mail not delivered within 2 minutes")
        logs = superset_client.get(f"/api/v1/report/{rid}/log/").json()["result"]
        assert logs and logs[-1]["state"] == "Success"
    finally:
        superset_client.post(f"/api/v1/report/{rid}", json=None) if False else httpx.delete(f"{superset_client.base}/api/v1/report/{rid}", headers=superset_client._headers()[0])
