"""Polaris Management API (catalogs, principals, roles): each call sent first in a commonly
written naive form, then in corrected form. Rows go to the request-check table."""
import uuid

import pytest

from common import spec_bodies as fix
from common.requestcheck import MGMT, Call
from common.polaris import token, token_response

U = uuid.uuid4().hex[:4]
CAT = f"finance_catalog_{U}"
PRINCIPAL = f"data_analyst_{U}"
PROLE = f"report_viewer_{U}"


@pytest.fixture(scope="module", autouse=True)
def cleanup(admin):
    yield
    for p in (PRINCIPAL, f"john_doe_{U}"):
        admin.drop_principal(p)
    admin.drop_principal_role(PROLE)
    if CAT in admin.catalogs():
        for r in admin.catalog_roles(CAT):
            if r != "catalog_admin":
                admin.drop_catalog_role(CAT, r)
        admin.m("DELETE", f"/catalogs/{CAT}")


# ---------- catalogs ---------------------------------------------------------------------------

def test_catalog_crud(check, measure):
    r = check.run("List Catalogs", Call("GET", f"{MGMT}/catalogs"), "as-is", expect_status=200)
    assert "catalogs" in r.json()

    naive_body = {"catalog": {"type": "INTERNAL", "name": CAT,
                             "properties": {"default-base-location": "s3://bucket/path"}}}
    r = check.run("Create a Catalog", Call("POST", f"{MGMT}/catalogs", naive_body), "fails",
                 "add required storageConfigInfo (spec_bodies.create_catalog)", expect_status=400)
    assert "storageConfigInfo: must not be null" in r.text
    created = Call("POST", f"{MGMT}/catalogs", fix.create_catalog(CAT, f"s3://warehouse/{CAT}")).send(check.bearer)
    assert created.status_code == 201
    # The response is the catalog object itself, not wrapped in {"catalog": ...}.
    assert created.json()["name"] == CAT and "catalog" not in created.json()

    r = check.run("Get Catalog Details", Call("GET", f"{MGMT}/catalogs/{CAT}"), "as-is, response differs",
                 "response also carries storageConfigInfo", expect_status=200)
    assert r.json()["entityVersion"] == 1 and "storageConfigInfo" in r.json()

    naive_update = {"currentEntityVersion": 1, "properties": {"default-base-location": "s3://new_bucket/path"}}
    r = check.run("Update a Catalog", Call("PUT", f"{MGMT}/catalogs/{CAT}", naive_update), "fails",
                 "new base location must be inside allowedLocations", expect_status=400)
    assert "not within any of the allowed locations" in r.text
    ok = Call("PUT", f"{MGMT}/catalogs/{CAT}", {"currentEntityVersion": 1,
                                                 "properties": {"default-base-location": f"s3://warehouse/{CAT}",
                                                                "owner": "finance-platform"}}).send(check.bearer)
    assert ok.status_code == 200 and ok.json()["entityVersion"] == 2
    measure("update_catalog_error", r.json()["error"]["message"][:140])


# ---------- principals -------------------------------------------------------------------------

def test_principal_crud_and_rotation(check, measure):
    r = check.run("List Principals", Call("GET", f"{MGMT}/principals"), "as-is, response differs",
                 "response is {'principals': [...]}, without credentials", expect_status=200)
    assert "principals" in r.json() and "credentials" not in r.text

    naive_body = {"principal": {"name": PRINCIPAL, "clientId": "analyst321",
                               "properties": {"team": "analytics", "region": "us-west"}},
                 "credentialRotationRequired": True}
    r = check.run("Create a Principal", Call("POST", f"{MGMT}/principals", naive_body), "silent difference",
                 "clientId is ignored (server-generated); credentialRotationRequired blocks all calls until rotation",
                 expect_status=201)
    creds = r.json()["credentials"]
    assert creds["clientId"] != "analyst321"

    # Order: get, update, (delete), rotate.
    check.run("Get Principal Details", Call("GET", f"{MGMT}/principals/{PRINCIPAL}"), "as-is", expect_status=200)
    naive_update = {"currentEntityVersion": 1, "properties": {"team": "analytics", "region": "us-east",
                                                             "project": "forecasting"}}
    r = check.run("Update a Principal", Call("PUT", f"{MGMT}/principals/{PRINCIPAL}", naive_update), "as-is",
                 expect_status=200)
    assert r.json()["entityVersion"] == 2 and r.json()["properties"]["project"] == "forecasting"

    # credentialRotationRequired: the token works, every operation is denied until self-rotation.
    tok = token(creds["clientId"], creds["clientSecret"])
    blocked = Call("GET", f"{MGMT}/principals/{PRINCIPAL}").send(tok)
    assert blocked.status_code == 403
    rotated = Call("POST", f"{MGMT}/principals/{PRINCIPAL}/rotate").send(tok)
    assert rotated.status_code == 200
    measure("rotation_required", {"before_rotation": blocked.status_code, "self_rotate": rotated.status_code,
                                      "client_id_requested": "analyst321", "client_id_issued": creds["clientId"]})

    r = check.run("Rotate Principal Credentials", Call("POST", f"{MGMT}/principals/{PRINCIPAL}/rotate"), "fails",
                 "rotation is self-service; admins use POST /principals/{name}/reset", expect_status=403)
    reset = Call("POST", f"{MGMT}/principals/{PRINCIPAL}/reset", {}).send(check.bearer)
    assert reset.status_code == 200
    assert token_response(creds["clientId"], creds["clientSecret"]).status_code == 401


# ---------- roles and grants -------------------------------------------------------------------

def test_roles_and_grants(check, measure):
    check.run("Create a Catalog Role",
             Call("POST", f"{MGMT}/catalogs/{CAT}/catalog-roles",
                  {"catalogRole": {"name": "viewer", "properties": {"permissions": "read_only"}}}),
             "as-is", expect_status=201)
    check.run("Create a Principal Role",
             Call("POST", f"{MGMT}/principal-roles",
                  {"principalRole": {"name": PROLE, "properties": {"access_scope": "read_only"}}}),
             "as-is", expect_status=201)
    check.run("List Catalog Roles", Call("GET", f"{MGMT}/catalogs/{CAT}/catalog-roles"), "as-is", expect_status=200)
    check.run("List Roles Assigned to a Principal", Call("GET", f"{MGMT}/principals/{PRINCIPAL}/principal-roles"),
             "as-is", expect_status=200)
    check.run("List All Principal Roles", Call("GET", f"{MGMT}/principal-roles"), "as-is", expect_status=200)
    check.run("List Principals Assigned to a Principal Role", Call("GET", f"{MGMT}/principal-roles/{PROLE}/principals"),
             "as-is", expect_status=200)
    check.run("Get Catalog Roles Mapped to a Principal Role",
             Call("GET", f"{MGMT}/principal-roles/{PROLE}/catalog-roles/{CAT}"), "as-is", expect_status=200)
    check.run("Get Details of a Principal Role", Call("GET", f"{MGMT}/principal-roles/{PROLE}"), "as-is",
             expect_status=200)

    grant = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
    check.run("Add a Grant to a Catalog Role", Call("PUT", f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer/grants", grant),
             "as-is", expect_status=201)
    r = check.run("Revoke a Grant from a Catalog Role",
                 Call("DELETE", f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer/grants?cascade=true", grant),
                 "fails", "use POST .../grants without ?cascade=true", expect_status=405)
    with_cascade = Call("POST", f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer/grants?cascade=true", grant).send(check.bearer)
    method, body = fix.revoke_grant("CATALOG_MANAGE_CONTENT")
    revoked = Call(method, f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer/grants", body).send(check.bearer)
    assert with_cascade.status_code == 501 and revoked.status_code == 201
    assert Call("GET", f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer/grants").send(check.bearer).json()["grants"] == []
    measure("revoke_statuses", {"DELETE (check)": r.status_code, "POST ?cascade=true": with_cascade.status_code,
                                    "POST": revoked.status_code})

    r = check.run("Assign a Catalog Role to a Principal Role",
                 Call("PUT", f"{MGMT}/principal-roles/{PROLE}/catalog-roles/{CAT}",
                      {"catalogRole": {"name": "editor", "properties": {"edit_scope": "limited"}}}),
                 "fails", "role 'editor' is never created; create it first", expect_status=404)
    assert Call("PUT", f"{MGMT}/principal-roles/{PROLE}/catalog-roles/{CAT}",
                {"catalogRole": {"name": "viewer"}}).send(check.bearer).status_code == 201

    check.run("Assign a Role to a Principal",
             Call("PUT", f"{MGMT}/principals/{PRINCIPAL}/principal-roles",
                  {"principalRole": {"name": "data_admin", "properties": {"access_level": "full"}}}),
             "fails", "role 'data_admin' is never created; create it first", expect_status=404)
    assert Call("PUT", f"{MGMT}/principals/{PRINCIPAL}/principal-roles",
                {"principalRole": {"name": PROLE}}).send(check.bearer).status_code == 201

    r = check.run("Update a Principal Role",
                 Call("PUT", f"{MGMT}/principal-roles/{PROLE}",
                      {"currentEntityVersion": 2, "properties": {"access_scope": "write_enabled"}}),
                 "fails", "currentEntityVersion must equal the role's version (1 here)", expect_status=409)
    assert Call("PUT", f"{MGMT}/principal-roles/{PROLE}",
                {"currentEntityVersion": 1, "properties": {"access_scope": "write_enabled"}}).send(check.bearer).status_code == 200
    measure("update_prole_error", r.json()["error"]["message"])

    check.run("Revoke a Role from a Principal", Call("DELETE", f"{MGMT}/principals/{PRINCIPAL}/principal-roles/{PROLE}"),
             "as-is", expect_status=204)
    check.run("Revoke a Catalog Role from a Principal Role",
             Call("DELETE", f"{MGMT}/principal-roles/{PROLE}/catalog-roles/{CAT}/viewer"), "as-is", expect_status=204)
    check.run("Delete a Principal Role", Call("DELETE", f"{MGMT}/principal-roles/{PROLE}"), "as-is", expect_status=204)
    check.run("Delete a Catalog Role", Call("DELETE", f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer"), "as-is",
             expect_status=204)


def test_deletes_last(check):
    check.run("Delete a Principal", Call("DELETE", f"{MGMT}/principals/{PRINCIPAL}"), "as-is", expect_status=204)
    check.run("Delete a Catalog", Call("DELETE", f"{MGMT}/catalogs/{CAT}"), "as-is", expect_status=204)
