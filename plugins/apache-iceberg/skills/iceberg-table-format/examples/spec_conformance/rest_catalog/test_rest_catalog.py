"""The REST catalog spec as seen by a client (config handshake, loadTable, optional endpoints)."""
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from common.lakehouse import REST_URI, read_metadata, rest_get


def post(path: str, body: dict) -> tuple[int, dict]:
    req = Request(REST_URI + path, data=json.dumps(body).encode(), method="POST",
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


@pytest.fixture(scope="module")
def table(spark, ns):
    spark.sql(f"CREATE TABLE {ns}.events (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')")
    spark.sql(f"INSERT INTO {ns}.events VALUES (1), (2)")
    return ns, "events"


def test_config_handshake_lists_the_supported_endpoints(measure):
    cfg = rest_get("/v1/config")
    endpoints = set(cfg["endpoints"])
    measure("config_keys", sorted(cfg))
    measure("endpoint_count", len(endpoints))
    assert {"defaults", "overrides", "endpoints"} <= set(cfg)
    for route in ("POST /v1/{prefix}/transactions/commit", "POST /v1/{prefix}/namespaces/{namespace}/register",
                  "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
                  "GET /v1/{prefix}/namespaces/{namespace}/views/{view}"):
        assert route in endpoints
    assert not any(e.endswith("/plan") for e in endpoints)        # scan planning not advertised


def test_load_table_returns_the_pointer_and_the_full_metadata(table):
    ns, name = table
    resp = rest_get(f"/v1/namespaces/{ns}/tables/{name}")
    on_storage = read_metadata(resp["metadata-location"])
    assert resp["metadata"] == on_storage                          # catalog serves the file it points to
    assert resp["metadata"]["format-version"] == 2


def test_scan_planning_endpoint_is_not_implemented_by_the_181_fixture(table, measure):
    ns, name = table
    status, body = post(f"/v1/namespaces/{ns}/tables/{name}/plan", {})
    measure("plan_response", (status, body.get("error", {}).get("message")))
    assert status == 400 and "No route" in body["error"]["message"]
