"""Principals (users and services), OAuth2 client credentials, credential rotation,
the three-stage lifecycle and the lifecycle review questions."""
import uuid

import pytest

from common.polaris import Admin, ApiError, token, token_response
from conftest import seed_table
from principals.lifecycle import audit_lifecycle, jwt_claims

READER_PRIVS = ("NAMESPACE_LIST", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA")


@pytest.fixture(scope="module")
def sales(admin, root):
    cat = f"sales_{uuid.uuid4().hex[:6]}"
    admin.create_catalog(cat, f"s3://warehouse/{cat}")
    admin.grant(cat, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    seed_table(root, cat, "orders", "daily")
    admin.create_catalog_role(cat, "orders_reader")
    for priv in READER_PRIVS:
        admin.grant(cat, "orders_reader", {"type": "catalog", "privilege": priv})
    yield cat
    admin.drop_catalog(cat)


def reader_principal(admin, cat, name):
    p = admin.create_principal(name, {"kind": "service", "owner": "data-platform"})
    admin.create_principal_role(name)
    admin.assign_principal_role(name, name)
    admin.assign_catalog_role(name, cat, "orders_reader")
    return p


def cleanup(admin, *names):
    for n in names:
        admin.drop_principal(n)
        admin.drop_principal_role(n)


def test_secret_is_returned_once_and_never_listed(admin, uid, measure):
    p = admin.create_principal(f"bi_dashboard_{uid}", {"kind": "service", "owner": "bi-team"})
    try:
        stored = admin.principal(p.name)
        assert "clientSecret" not in str(stored) and stored["clientId"] == p.client_id
        assert stored["properties"] == {"kind": "service", "owner": "bi-team"}
        measure("principal_record_keys", sorted(stored))
    finally:
        admin.drop_principal(p.name)


def test_client_credentials_flow_issues_a_short_lived_jwt(admin, uid, measure):
    p = admin.create_principal(f"spark_etl_{uid}")
    try:
        claims = jwt_claims(token(p.client_id, p.client_secret))
        assert claims["sub"] == p.name and claims["iss"] == "polaris"
        lifetime_s = claims["exp"] - claims["iat"]
        assert lifetime_s == 3600
        assert token_response(p.client_id, "wrong-secret").status_code == 401
        measure("token_claims", sorted(claims))
        measure("token_lifetime_s", lifetime_s)
    finally:
        admin.drop_principal(p.name)


def test_admin_reset_invalidates_the_old_secret(admin, uid):
    p = admin.create_principal(f"svc_reset_{uid}")
    try:
        new = admin.reset_credentials(p.name)
        assert new.client_id == p.client_id and new.client_secret != p.client_secret
        assert token_response(p.client_id, p.client_secret).status_code == 401
        assert token_response(new.client_id, new.client_secret).status_code == 200
    finally:
        admin.drop_principal(p.name)


def test_rotation_is_self_service_and_admin_cannot_rotate_for_others(admin, uid, measure):
    p = admin.create_principal(f"svc_rotate_{uid}")
    try:
        with pytest.raises(ApiError) as denied:
            admin.m("POST", f"/principals/{p.name}/rotate")
        assert denied.value.status == 403
        # Rotation keeps the previous secret as a secondary secret: running jobs keep working
        # while the new secret is rolled out. A second rotation retires the original secret.
        first = Admin(p.client_id, p.client_secret).m("POST", f"/principals/{p.name}/rotate").json()
        s1 = first["credentials"]["clientSecret"]
        assert s1 != p.client_secret
        old_after_one = token_response(p.client_id, p.client_secret).status_code
        assert old_after_one == 200 and token_response(p.client_id, s1).status_code == 200
        second = Admin(p.client_id, s1).m("POST", f"/principals/{p.name}/rotate").json()
        s2 = second["credentials"]["clientSecret"]
        old_after_two = token_response(p.client_id, p.client_secret).status_code
        assert old_after_two == 401
        assert token_response(p.client_id, s1).status_code == 200      # now the secondary
        assert token_response(p.client_id, s2).status_code == 200
        measure("admin_rotate_error", denied.value.text[:170])
        measure("original_secret_status", {"after_1_rotation": old_after_one, "after_2_rotations": old_after_two})
    finally:
        admin.drop_principal(p.name)


def test_lifecycle_creation_assignment_revocation(admin, sales, uid, measure):
    name = f"analyst_jane_{uid}"
    # Stage 1, creation: the identity exists but has no access yet.
    p = admin.create_principal(name)
    admin.create_principal_role(name)
    with pytest.raises(ApiError) as before:
        p.catalog(sales).load_table("orders", "daily")
    assert before.value.status == 403
    # Stage 2, assignment: access arrives with the role, no new credentials needed.
    admin.assign_principal_role(name, name)
    admin.assign_catalog_role(name, sales, "orders_reader")
    session = p.catalog(sales)
    assert session.load_table("orders", "daily")["metadata"]["format-version"] in (2, 3)
    # Stage 3, revocation: removing the role cuts access for the SAME still-valid token.
    admin.revoke_principal_role(name, name)
    with pytest.raises(ApiError) as after:
        session.load_table("orders", "daily")
    assert after.value.status == 403
    # Deleting the principal: tokens already issued to it stop working too.
    admin.assign_principal_role(name, name)
    session2 = p.catalog(sales)
    admin.drop_principal(name)
    with pytest.raises(ApiError) as gone:
        session2.load_table("orders", "daily")
    assert gone.value.status in (401, 403)
    measure("revocation", {"after_role_revoke": after.value.status,
                               "after_principal_delete": gone.value.status})
    admin.drop_principal_role(name)


def test_lifecycle_audit_answers_three_review_questions(admin, sales, uid, measure):
    prefix = f"aud{uid}_"
    alice = reader_principal(admin, sales, f"{prefix}alice")
    bob = admin.create_principal(f"{prefix}bob")                       # created, never assigned
    leaver = reader_principal(admin, sales, f"{prefix}leaver")          # left the company
    admin.create_principal_role(f"{prefix}admin_tmp")
    admin.assign_principal_role(alice.name, f"{prefix}admin_tmp")       # not justified by the roster
    try:
        roster = {alice.name: [alice.name], bob.name: [bob.name], f"{prefix}carol": [f"{prefix}carol"]}
        report = audit_lifecycle(admin, roster, retired=[leaver.name], scope_prefix=prefix)
        assert report.missing == [f"{prefix}carol"]                          # Q1
        assert report.missing_roles == {bob.name: [bob.name]}                # Q2
        assert report.excess_roles == {alice.name: [f"{prefix}admin_tmp"]}
        assert report.not_revoked == [leaver.name]                           # Q3
        assert not report.clean
        measure("lifecycle_audit", {"missing": 1, "missing_roles": 1, "excess_roles": 1, "not_revoked": 1})
    finally:
        cleanup(admin, alice.name, leaver.name, f"{prefix}admin_tmp")
        admin.drop_principal(bob.name)
