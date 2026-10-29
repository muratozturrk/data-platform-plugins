"""Iceberg REST catalog API as served by Polaris (config, OAuth2, namespaces, tables, metrics,
notifications, multi-table commits): naive requests and their corrected forms."""
import time
import uuid

import pyarrow as pa
import pytest
import requests

from common import spec_bodies as fix
from common.requestcheck import ICEBERG, MGMT, Call
from common.polaris import ROOT, S3_ENDPOINT

U = uuid.uuid4().hex[:4]
DEV = f"dev_{U}"                    # helper catalog: vending + purge enabled
DEFAULTS = f"defaults_{U}"          # created with the corrected body only (Polaris defaults)


@pytest.fixture(scope="module")
def cat(admin, root):
    admin.create_catalog(DEV, f"s3://warehouse/{DEV}")
    admin.grant(DEV, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    assert Call("POST", f"{MGMT}/catalogs", fix.create_catalog(DEFAULTS, f"s3://warehouse/{DEFAULTS}")).send(
        admin.s.headers["Authorization"].split()[1]).status_code == 201
    admin.grant(DEFAULTS, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    yield f"{ICEBERG}/v1/{DEV}"
    admin.drop_catalog(DEV)
    admin.drop_catalog(DEFAULTS)


def test_configuration_and_oauth(check, cat, measure):
    r = check.run("Configuration API", Call("GET", f"{ICEBERG}/v1/config"), "fails",
                 "Polaris requires ?warehouse=<catalog>", expect_status=400)
    assert "Please specify a warehouse" in r.text
    cfg = Call("GET", f"{ICEBERG}/v1/config?warehouse={DEV}").send(check.bearer).json()
    assert cfg["overrides"]["prefix"] == DEV and len(cfg["endpoints"]) > 20

    form = {"grant_type": "client_credentials", "client_id": ROOT[0], "client_secret": ROOT[1]}
    r = check.run("OAuth2 API (client credentials)", Call("POST", f"{ICEBERG}/v1/oauth/tokens", data=form),
                 "fails", "add scope=PRINCIPAL_ROLE:ALL (or one role)", expect_status=400)
    assert r.json()["error"] == "invalid_scope"
    ok = requests.post(f"{ICEBERG}/v1/oauth/tokens", data={**form, "scope": "PRINCIPAL_ROLE:ALL"}, timeout=30).json()
    assert ok["token_type"].lower() == "bearer" and ok["expires_in"] == 3600
    measure("config_defaults", cfg["defaults"])
    measure("config_endpoint_count", len(cfg["endpoints"]))


def test_namespaces(check, cat, measure):
    r = check.run("Create a Namespace",
                 Call("POST", f"{cat}/namespaces", {"namespace": ["analytics"], "properties": {
                     "owner": "data.platform@company.com", "created_by": "automation_pipeline"}}),
                 "as-is, response differs", "Polaris adds a 'location' property", expect_status=200)
    assert r.json()["properties"]["location"].endswith("/analytics/")
    check.run("Load Namespace Properties", Call("GET", f"{cat}/namespaces/analytics"), "as-is", expect_status=200)
    check.run("Check Namespace Existence", Call("HEAD", f"{cat}/namespaces/analytics"), "as-is", expect_status=204)

    body = {"updates": {"owner": "dataops@company.com", "environment": "production"}, "removals": ["created_by"]}
    r = check.run("Set or Remove Namespace Properties", Call("POST", f"{cat}/namespaces/analytics/properties", body),
                 "fails", "send updates and removals in two requests (Polaris 1.8.0 applies the updates, "
                          "skips the removals and answers 409)", expect_status=409)
    after = Call("GET", f"{cat}/namespaces/analytics").send(check.bearer).json()["properties"]
    assert after["owner"] == "dataops@company.com" and "created_by" in after          # half applied
    assert Call("POST", f"{cat}/namespaces/analytics/properties", {"removals": ["created_by"]}).send(
        check.bearer).json() == {"removed": ["created_by"], "updated": [], "missing": []}
    measure("namespace_props_409", {"status": r.status_code, "owner_updated": True, "created_by_removed": False})


def test_tables(check, cat, root, measure):
    naive_table = {"name": "customer_profiles", "schema": {"type": "struct", "fields": [
        {"id": 1, "name": "customer_id", "required": True, "type": "long"},
        {"id": 2, "name": "email", "required": False, "type": "string"},
        {"id": 3, "name": "created_at", "required": True, "type": "timestamp"}]},
        "spec": {"fields": [{"source-id": 3, "transform": "day", "name": "day_created"}]}, "stage-create": False}
    r = check.run("Create a Table", Call("POST", f"{cat}/namespaces/analytics/tables", naive_table),
                 "silent difference", "use 'partition-spec' (with field-id); 'spec' is ignored", expect_status=200)
    assert r.json()["metadata"]["partition-specs"] == [{"spec-id": 0, "fields": []}]     # NOT partitioned
    fixed = Call("POST", f"{cat}/namespaces/analytics/tables", fix.create_table("customer_profiles_by_day")).send(check.bearer)
    assert fixed.json()["metadata"]["partition-specs"][0]["fields"][0]["transform"] == "day"

    check.run("List Table Identifiers", Call("GET", f"{cat}/namespaces/analytics/tables"), "as-is", expect_status=200)
    r = check.run("Load Table Metadata", Call("GET", f"{cat}/namespaces/analytics/tables/customer_profiles"),
                 "as-is, response differs", "schema/specs/snapshots are inside 'metadata'", expect_status=200)
    assert set(r.json()) == {"metadata-location", "metadata", "config"}
    meta, ml = r.json()["metadata"], r.json()["metadata-location"]

    r = check.run("Commit Updates to a Table",
                 Call("POST", f"{cat}/namespaces/analytics/tables/customer_profiles",
                      {"updates": {"metadata-location": ml.replace("/00000-", "/00002-"), "base": ml}}),
                 "fails", "send requirements + a list of updates (spec_bodies.commit_properties)", expect_status=400)
    ok = Call("POST", f"{cat}/namespaces/analytics/tables/customer_profiles",
              fix.commit_properties(meta["table-uuid"], {"owner": "crm"})).send(check.bearer)
    assert ok.status_code == 200 and ok.json()["metadata-location"] != ml

    check.run("Check Table Existence", Call("HEAD", f"{cat}/namespaces/analytics/tables/customer_profiles"),
             "as-is", expect_status=204)

    r = check.run("Rename a Table", Call("POST", f"{cat}/rename", fix.rename((["analytics"], "customer_profiles"),
                                                                          (["analytics"], "customer_profiles_archive"))),
                 "fails", "path is /v1/{prefix}/tables/rename; success is 204, not 200", expect_status=404)
    moved = Call("POST", f"{cat}/tables/rename", fix.rename((["analytics"], "customer_profiles"),
                                                           (["analytics"], "customer_profiles_archive"))).send(check.bearer)
    assert moved.status_code == 204

    # Register: recovery use case. Unregister a table (no purge), register it again.
    engine = root.pyiceberg(DEV)
    t = engine.create_table("analytics.recovered_events", schema=pa.schema([("id", pa.int64())]))
    t.append(pa.table({"id": pa.array([1, 2, 3], pa.int64())}))
    loc = engine.load_table("analytics.recovered_events").metadata_location
    assert Call("DELETE", f"{cat}/namespaces/analytics/tables/recovered_events").send(check.bearer).status_code == 204
    check.run("Register a Table", Call("POST", f"{cat}/namespaces/analytics/register",
                                      {"name": "recovered_events", "metadata-location": loc}),
             "as-is, response differs", "response is a full LoadTableResult", expect_status=200)
    assert engine.load_table("analytics.recovered_events").scan().to_arrow().num_rows == 3

    # Metrics: free-form numbers are rejected; the spec takes Iceberg metrics reports.
    snap = engine.load_table("analytics.recovered_events").current_snapshot()
    check.run("Submit Metrics for a Table",
             Call("POST", f"{cat}/namespaces/analytics/tables/recovered_events/metrics",
                  {"metrics": {"row_count": 1823947, "last_query_duration_ms": 231, "fragmentation_score": 0.75}}),
             "fails", "send a CommitReport/ScanReport (report-type, table-name, snapshot-id, ...)", expect_status=400)
    report = Call("POST", f"{cat}/namespaces/analytics/tables/recovered_events/metrics",
                  fix.commit_report(f"{DEV}.analytics.recovered_events", snap.snapshot_id, snap.sequence_number)).send(check.bearer)
    assert report.status_code == 204

    check.run("Drop a Table (catalog only)", Call("DELETE", f"{cat}/namespaces/analytics/tables/customer_profiles_archive"),
             "as-is", expect_status=204)
    purge_default = Call("POST", f"{ICEBERG}/v1/{DEFAULTS}/namespaces", {"namespace": ["tmp"]}).send(check.bearer)
    Call("POST", f"{ICEBERG}/v1/{DEFAULTS}/namespaces/tmp/tables", fix.create_table("t")).send(check.bearer)
    r = check.run("Drop a Table (catalog + data purge)",
                 Call("DELETE", f"{ICEBERG}/v1/{DEFAULTS}/namespaces/tmp/tables/t?purgeRequested=true"),
                 "fails", "purge is off unless the catalog sets polaris.config.drop-with-purge.enabled=true",
                 expect_status=(400, 403))
    with_flag = Call("DELETE", f"{cat}/namespaces/analytics/tables/customer_profiles_by_day?purgeRequested=true").send(check.bearer)
    assert purge_default.status_code == 200 and with_flag.status_code == 204
    Call("DELETE", f"{ICEBERG}/v1/{DEFAULTS}/namespaces/tmp/tables/t").send(check.bearer)      # unregister only
    Call("DELETE", f"{ICEBERG}/v1/{DEFAULTS}/namespaces/tmp").send(check.bearer)
    measure("purge", {"default_catalog": f"{r.status_code} {r.json()['error']['message'][:100]}",
                          "catalog_with_flag": with_flag.status_code})

    r = check.run("Drop a Namespace (not empty)", Call("DELETE", f"{cat}/namespaces/analytics"), "as-is", expect_status=409)
    assert "not empty" in r.text


def test_notifications_are_for_external_catalogs(check, cat, admin, root, measure):
    r = check.run("Send Table Notifications",
                 Call("POST", f"{cat}/namespaces/analytics/tables/recovered_events/notifications",
                      {"notification": {"event_type": "compaction_complete", "summary": "June partition",
                                        "timestamp": "2025-06-20T13:45:00Z"}}),
                 "fails", "notifications register externally managed tables in an EXTERNAL catalog "
                          "(notification-type CREATE/UPDATE/DROP + payload)", expect_status=400)
    assert "Cannot update internal catalog via notifications" in r.text

    # Corrected use: an engine outside Polaris owns a table; Polaris mirrors it read-only.
    src = f"engine_owned_{U}"
    admin.create_catalog(src, f"s3://warehouse/{src}")
    admin.grant(src, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    e = root.pyiceberg(src)
    e.create_namespace("ops")
    e.create_table("ops.events", schema=pa.schema([("id", pa.int64())])).append(pa.table({"id": pa.array([1, 2], pa.int64())}))
    t = e.load_table("ops.events")
    ml, table_uuid = t.metadata_location, str(t.metadata.table_uuid)
    Call("DELETE", f"{ICEBERG}/v1/{src}/namespaces/ops/tables/events").send(check.bearer)       # files stay
    Call("DELETE", f"{ICEBERG}/v1/{src}/namespaces/ops").send(check.bearer)
    admin.m("DELETE", f"/catalogs/{src}")
    ext = f"ext_mirror_{U}"
    body = {"catalog": {"type": "EXTERNAL", "name": ext, "properties": {"default-base-location": f"s3://warehouse/{src}"},
                        "storageConfigInfo": {"storageType": "S3", "allowedLocations": [f"s3://warehouse/{src}"],
                                              "region": "us-east-1", "endpoint": S3_ENDPOINT, "pathStyleAccess": True,
                                              "stsUnavailable": True}}}
    assert Call("POST", f"{MGMT}/catalogs", body).send(check.bearer).status_code == 201
    try:
        admin.grant(ext, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        Call("POST", f"{ICEBERG}/v1/{ext}/namespaces", {"namespace": ["ops"]}).send(check.bearer)
        note = {"notification-type": "CREATE", "payload": {"table-name": "events", "timestamp": int(time.time() * 1000),
                                                           "table-uuid": table_uuid, "metadata-location": ml}}
        n = Call("POST", f"{ICEBERG}/v1/{ext}/namespaces/ops/tables/events/notifications", note).send(check.bearer)
        loaded = Call("GET", f"{ICEBERG}/v1/{ext}/namespaces/ops/tables/events").send(check.bearer)
        write = Call("POST", f"{ICEBERG}/v1/{ext}/namespaces/ops/tables/events",
                     {"requirements": [], "updates": [{"action": "set-properties", "updates": {"a": "b"}}]}).send(check.bearer)
        assert n.status_code == 204 and loaded.json()["metadata-location"] == ml
        assert write.status_code == 400 and "static-facade" in write.text
        measure("notifications", {"internal_catalog": r.status_code, "external_create": n.status_code,
                                      "commit_on_external": f"{write.status_code} {write.json()['error']['message']}"})
    finally:
        Call("DELETE", f"{ICEBERG}/v1/{ext}/namespaces/ops/tables/events").send(check.bearer)
        Call("DELETE", f"{ICEBERG}/v1/{ext}/namespaces/ops").send(check.bearer)
        admin.m("DELETE", f"/catalogs/{ext}")


def test_multi_table_commit(check, cat, root):
    engine = root.pyiceberg(DEV)
    for name in ("orders_tx", "customers_tx"):
        engine.create_table(f"analytics.{name}", schema=pa.schema([("id", pa.int64())]))
    uuids = {n: str(engine.load_table(f"analytics.{n}").metadata.table_uuid) for n in ("orders_tx", "customers_tx")}
    ml = engine.load_table("analytics.orders_tx").metadata_location
    check.run("Commit Updates to Multiple Tables",
             Call("POST", f"{cat}/transactions/commit", {"updates": [
                 {"identifier": {"namespace": ["analytics"], "name": "orders_tx"}, "base": ml, "metadata-location": ml}]}),
             "fails", "send {'table-changes': [{identifier, requirements, updates}]}", expect_status=400)
    ok = Call("POST", f"{cat}/transactions/commit", fix.transaction(
        [(["analytics"], n, u, {"release": "2025.06"}) for n, u in uuids.items()])).send(check.bearer)
    assert ok.status_code == 204
    assert all(engine.load_table(f"analytics.{n}").properties["release"] == "2025.06" for n in uuids)
    # All-or-nothing: one stale requirement aborts the whole transaction.
    bad = fix.transaction([(["analytics"], "orders_tx", uuids["orders_tx"], {"release": "2025.07"}),
                           (["analytics"], "customers_tx", "00000000-0000-0000-0000-000000000000", {"release": "2025.07"})])
    failed = Call("POST", f"{cat}/transactions/commit", bad).send(check.bearer)
    assert failed.status_code == 409
    assert engine.load_table("analytics.orders_tx").properties["release"] == "2025.06"
