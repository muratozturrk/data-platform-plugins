"""Iceberg View API on Polaris: naive requests and their corrected forms."""
import uuid

import pytest

from common import spec_bodies as fix
from common.requestcheck import ICEBERG, Call

U = uuid.uuid4().hex[:4]
CAT = f"views_{U}"
FIELDS = [{"id": 1, "name": "customer_id", "required": False, "type": "long"},
          {"id": 2, "name": "orders", "required": False, "type": "long"}]
SQL_V1 = "SELECT customer_id, COUNT(*) AS orders FROM orders GROUP BY customer_id"
SQL_V2 = "SELECT customer_id, COUNT(*) AS orders, MAX(order_date) AS last_order FROM orders GROUP BY customer_id"


@pytest.fixture(scope="module")
def base(admin):
    admin.create_catalog(CAT, f"s3://warehouse/{CAT}")
    admin.grant(CAT, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    b = f"{ICEBERG}/v1/{CAT}"
    from common.polaris import ROOT, token
    t = token(*ROOT)
    for ns in ("analytics", "sales"):
        Call("POST", f"{b}/namespaces", {"namespace": [ns]}).send(t)
    yield b
    admin.drop_catalog(CAT)


def test_view_lifecycle(check, base, measure):
    check.run("List View Identifiers", Call("GET", f"{base}/namespaces/analytics/views"), "as-is", expect_status=200)

    naive_view = {"view-version": {"version-id": "v1", "schema-id": 1, "sql": SQL_V1},
                 "identifier": {"namespace": ["analytics"], "name": "customer_order_summary"}}
    r = check.run("Create a View", Call("POST", f"{base}/namespaces/analytics/views", naive_view), "fails",
                 "send name, schema and view-version {integer id, timestamp-ms, summary, representations}",
                 expect_status=400)
    created = Call("POST", f"{base}/namespaces/analytics/views",
                   fix.create_view("customer_order_summary", ["analytics"], SQL_V1, FIELDS)).send(check.bearer)
    assert created.status_code == 200
    measure("create_view_error", r.json()["error"]["message"])

    r = check.run("Load View Metadata", Call("GET", f"{base}/namespaces/analytics/views/customer_order_summary"),
                 "as-is, response differs", "SQL is in metadata.versions[].representations", expect_status=200)
    current = r.json()["metadata"]["current-version-id"]
    version = next(v for v in r.json()["metadata"]["versions"] if v["version-id"] == current)
    assert version["representations"][0]["sql"] == SQL_V1

    # Common assumption: "creating a view with an existing name replaces it". It does not.
    replace_naive = {"view-version": {"version-id": "v2", "schema-id": 1, "sql": SQL_V2},
                    "identifier": {"namespace": ["analytics"], "name": "customer_order_summary"}}
    check.run("Replace a View", Call("POST", f"{base}/namespaces/analytics/views", replace_naive), "fails",
             "replace = POST /views/{view} with add-view-version + set-current-view-version", expect_status=400)
    again = Call("POST", f"{base}/namespaces/analytics/views",
                 fix.create_view("customer_order_summary", ["analytics"], SQL_V2, FIELDS)).send(check.bearer)
    assert again.status_code == 409                               # create never replaces
    replaced = Call("POST", f"{base}/namespaces/analytics/views/customer_order_summary",
                    fix.replace_view(0, 2, ["analytics"], SQL_V2)).send(check.bearer)
    assert replaced.status_code == 200
    meta = replaced.json()["metadata"]
    assert meta["current-version-id"] == 2 and len(meta["versions"]) == 2
    measure("replace", {"second_create": again.status_code, "commit_replace": replaced.status_code,
                            "versions_kept": len(meta["versions"])})

    r = check.run("Check View Existence", Call("HEAD", f"{base}/namespaces/analytics/views/customer_order_summary"),
                 "as-is, response differs", "success is 204, not 200", expect_status=204)

    r = check.run("Rename a View", Call("POST", f"{base}/views/rename", {
        "from": {"namespace": ["analytics"], "name": "customer_order_summary"},
        "to": {"namespace": ["sales"], "name": "customer_summary"}}),
        "fails", "body keys are 'source' and 'destination'", expect_status=400)
    moved = Call("POST", f"{base}/views/rename", fix.rename((["analytics"], "customer_order_summary"),
                                                           (["sales"], "customer_summary"))).send(check.bearer)
    assert moved.status_code == 204

    check.run("Drop a View", Call("DELETE", f"{base}/namespaces/sales/views/customer_summary"), "as-is", expect_status=204)
    assert Call("HEAD", f"{base}/namespaces/sales/views/customer_summary").send(check.bearer).status_code == 404
