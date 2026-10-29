"""Metrics, tracing and logging on polaris-obs, with an OpenTelemetry collector and Prometheus
scraping the management port; polaris-hard shows the JSON-log property name of Polaris 1.8.0."""
import json
import time

import pytest
import requests

from conftest import container, token

OBS = "http://polaris-obs:8181"


@pytest.fixture(scope="module")
def traffic():
    """A few requests with both request-id headers (older docs: Polaris-Request-Id; 1.8.0: X-Request-ID)."""
    tok = token(OBS, "root", "s3cr3t").json()["access_token"]
    for i in range(5):
        requests.get(f"{OBS}/api/management/v1/catalogs", timeout=30, headers={
            "Authorization": f"Bearer {tok}", "Polaris-Request-Id": f"legacy-req-{i}", "X-Request-ID": f"xreq-{i}"})
    time.sleep(8)                                   # batch span export + one Prometheus scrape
    return tok


def test_metrics_endpoint_and_port(traffic, measure):
    out = {}
    for port in (8282, 8182):
        try:
            out[port] = requests.get(f"http://polaris-obs:{port}/q/metrics", timeout=10).status_code
        except requests.ConnectionError as e:
            out[port] = type(e).__name__
    measure("metrics_port", out)
    assert out[8182] == 200 and out[8282] == "ConnectionError"


def test_metric_tags(traffic, measure):
    text = requests.get("http://polaris-obs:8182/q/metrics", timeout=10).text
    line = next(l for l in text.splitlines() if l.startswith("http_server_requests_seconds_count"))
    tags = {k: k in line for k in ('environment="prod"', 'region="us-west-2"', 'application="my-polaris-instance"',
                                   'application="Polaris"')}
    measure("metric_tags", {"sample": line[:400], "present": tags})
    assert tags == {'environment="prod"': True, 'region="us-west-2"': True,
                    'application="my-polaris-instance"': True, 'application="Polaris"': False}


def test_prometheus_scrapes_the_management_port(traffic, measure):
    q = requests.get("http://prometheus:9090/api/v1/query", params={"query": 'up{job="polaris"}'}, timeout=10).json()
    series = requests.get("http://prometheus:9090/api/v1/query",
                          params={"query": 'http_server_requests_seconds_count{environment="prod"}'}, timeout=10).json()
    measure("prometheus", {"up": q["data"]["result"], "series_with_environment_prod": len(series["data"]["result"])})
    assert q["data"]["result"][0]["value"][1] == "1" and series["data"]["result"]


def test_traces_reach_the_collector(traffic, measure):
    logs = container("otel-collector").logs().decode("utf-8", "replace")
    found = {k: k in logs for k in ("service.name: Str(polaris)", "deployment.environment: Str(prod)",
                                    "polaris.realm", "polaris.request.id", "legacy-req-", "xreq-")}
    measure("traces", found)
    assert found["service.name: Str(polaris)"] and found["deployment.environment: Str(prod)"] and found["polaris.realm"]


def test_log_format_mdc_and_request_id(traffic, measure):
    logs = container("polaris-obs").logs(tail=400).decode("utf-8", "replace").splitlines()
    request_lines = [l for l in logs if "xreq-" in l or "legacy-req-" in l]
    json_lines = [l for l in logs if l.lstrip().startswith("{")]
    result = {"json_lines_with_legacy_property": len(json_lines),
              "request_id_from_X-Request-ID": any("xreq-" in l for l in request_lines),
              "request_id_from_Polaris-Request-Id": any("legacy-req-" in l for l in request_lines),
              "realm_in_line": any(",POLARIS]" in l for l in request_lines),
              "custom_mdc_environment_in_plain_line": any("prod" in l for l in request_lines),
              "sample": request_lines[0][:300] if request_lines else ""}
    measure("logs_legacy_properties", result)
    assert result["json_lines_with_legacy_property"] == 0
    assert result["request_id_from_X-Request-ID"] and not result["request_id_from_Polaris-Request-Id"]
    assert result["realm_in_line"]


def test_json_logs_with_the_current_property(measure):
    hard = "http://polaris-hard:8181"
    tok = token(hard, "root", "s3cr3t", "POLARIS").json()["access_token"]
    requests.get(f"{hard}/api/management/v1/catalogs", timeout=30,
                 headers={"Authorization": f"Bearer {tok}", "Polaris-Realm": "POLARIS", "X-Request-ID": "json-req-1"})
    time.sleep(1)
    lines = [l for l in container("polaris-hard").logs(tail=200).decode("utf-8", "replace").splitlines() if "json-req-1" in l]
    record = json.loads(lines[0])
    measure("json_log", {"keys": sorted(record), "mdc": record.get("mdc")})
    assert record["mdc"]["requestId"] == "json-req-1" and record["mdc"]["realmId"] == "POLARIS"
    assert record["mdc"]["environment"] == "prod"


def test_log_file_rotation_defaults(measure):
    exit_code, out = container("polaris-obs").exec_run("sh -c 'ls -la /deployments/logs'")
    measure("log_files", out.decode()[-300:])
    assert exit_code == 0 and "polaris.log" in out.decode()
