"""Principal roles as the many-to-many link between identities and catalog roles.

Scenario: an engineering team writes its own catalog and reads a compliance catalog
(a "compliance officer + data engineer" setup); data scientists only read.
"""
import uuid

import pyarrow as pa
import pytest

from common.polaris import Admin, ApiError
from conftest import seed_table

READ = ("NAMESPACE_LIST", "TABLE_LIST", "TABLE_READ_DATA")
WRITE = READ + ("TABLE_CREATE", "TABLE_WRITE_DATA")


def is_denied(fn) -> bool:
    try:
        fn()
        return False
    except ApiError as e:
        assert e.status == 403, e
        return True
    except Exception as e:                   # PyIceberg raises ForbiddenError / OSError
        text = f"{type(e).__name__} {e}"
        assert "Forbidden" in text or "403" in text or "ACCESS_DENIED" in text or "not authorized" in text, text
        return True


@pytest.fixture(scope="module")
def org(admin, root):
    u = uuid.uuid4().hex[:6]
    eng, comp = f"engineering_{u}", f"compliance_{u}"
    for cat, ns in ((eng, "pipelines"), (comp, "audits")):
        admin.create_catalog(cat, f"s3://warehouse/{cat}")
        admin.grant(cat, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        seed_table(root, cat, ns, "events")
    roles = {  # principal role -> [(catalog, catalog role, privileges)]
        f"eng_engineer_{u}": [(eng, "eng_contributor", WRITE)],
        f"comp_viewer_{u}": [(comp, "comp_reader", READ)],
        f"data_scientist_{u}": [(eng, "eng_reader", READ)],
    }
    for prole, bindings in roles.items():
        admin.create_principal_role(prole)
        for cat, crole, privs in bindings:
            if crole not in admin.catalog_roles(cat):
                admin.create_catalog_role(cat, crole)
                for p in privs:
                    admin.grant(cat, crole, {"type": "catalog", "privilege": p})
            admin.assign_catalog_role(prole, cat, crole)
    people = {}
    for name, proles in ((f"dana_engineer_{u}", [f"eng_engineer_{u}", f"comp_viewer_{u}"]),
                         (f"sam_scientist_{u}", [f"data_scientist_{u}"]),
                         (f"lee_scientist_{u}", [f"data_scientist_{u}"]),
                         (f"kim_scientist_{u}", [f"data_scientist_{u}"])):
        people[name.split("_")[0]] = admin.create_principal(name)
        for r in proles:
            admin.assign_principal_role(name, r)
    yield {"u": u, "eng": eng, "comp": comp, "people": people, "roles": roles}
    for p in people.values():
        admin.drop_principal(p.name)
    for r in roles:
        admin.drop_principal_role(r)
    admin.drop_catalog(eng)
    admin.drop_catalog(comp)


def append_one(engine, ident):
    t = engine.load_table(ident)
    t.append(pa.table({"id": pa.array([99], pa.int64()), "amount": pa.array([1.0])}))


def test_one_principal_role_serves_many_principals(admin, org, measure):
    u = org["u"]
    members = sorted(admin.principals_with_role(f"data_scientist_{u}"))
    assert members == sorted(f"{n}_scientist_{u}" for n in ("sam", "lee", "kim"))
    for n in ("sam", "lee", "kim"):
        assert org["people"][n].pyiceberg(org["eng"]).load_table("pipelines.events").scan().to_arrow().num_rows == 3
    # One change on the catalog role reaches all three principals at once.
    admin.revoke(org["eng"], "eng_reader", {"type": "catalog", "privilege": "TABLE_READ_DATA"})
    try:
        assert all(is_denied(lambda n=n: org["people"][n].pyiceberg(org["eng"]).load_table("pipelines.events").scan().to_arrow())
                   for n in ("sam", "lee", "kim"))
    finally:
        admin.grant(org["eng"], "eng_reader", {"type": "catalog", "privilege": "TABLE_READ_DATA"})
    measure("members_of_data_scientist", len(members))


def test_one_principal_combines_roles_across_catalogs(org):
    dana = org["people"]["dana"]
    append_one(dana.pyiceberg(org["eng"]), "pipelines.events")                 # writer on engineering
    assert dana.pyiceberg(org["comp"]).load_table("audits.events").scan().to_arrow().num_rows == 3
    assert is_denied(lambda: append_one(dana.pyiceberg(org["comp"]), "audits.events"))   # read-only there


def test_token_scope_activates_only_the_requested_role(org, measure):
    dana, u = org["people"]["dana"], org["u"]
    viewer_only = dana.pyiceberg(org["comp"], scope=f"PRINCIPAL_ROLE:comp_viewer_{u}")
    assert viewer_only.load_table("audits.events").scan().to_arrow().num_rows == 3
    # Same principal, session scoped to the viewer role: the engineering write role is inactive.
    eng_with_viewer_scope = dana.pyiceberg(org["eng"], scope=f"PRINCIPAL_ROLE:comp_viewer_{u}")
    assert is_denied(lambda: eng_with_viewer_scope.load_table("pipelines.events"))
    eng_all = dana.pyiceberg(org["eng"], scope="PRINCIPAL_ROLE:ALL")
    append_one(eng_all, "pipelines.events")
    measure("scoped_session", {"comp_viewer scope on engineering": "403", "ALL scope on engineering": "write ok"})


def test_scope_naming_an_unheld_role_activates_nothing(org, measure):
    """Polaris issues a token for any requested role name, but the token is rejected when used
    (401): asking for service_admin does not escalate."""
    sam = org["people"]["sam"]
    as_admin = Admin(sam.client_id, sam.client_secret, scope="PRINCIPAL_ROLE:service_admin")
    with pytest.raises(ApiError) as e:
        as_admin.m("GET", "/principals")
    assert e.value.status == 401
    with pytest.raises(Exception) as e2:
        sam.pyiceberg(org["eng"], scope="PRINCIPAL_ROLE:service_admin").load_table("pipelines.events")
    assert "401" in str(e2.value) or "Unauthorized" in type(e2.value).__name__ or "NotAuthorized" in type(e2.value).__name__
    measure("unheld_scope", {"management": e.value.status, "pyiceberg": type(e2.value).__name__})


def test_catalog_roles_are_catalog_local_principal_roles_are_global(admin, org):
    u = org["u"]
    # The same catalog-role name in two catalogs is two different roles.
    admin.create_catalog_role(org["comp"], "eng_reader")
    try:
        assert "eng_reader" in admin.catalog_roles(org["eng"]) and "eng_reader" in admin.catalog_roles(org["comp"])
        assert admin.grants(org["comp"], "eng_reader") == []        # nothing inherited from the other catalog
        assert admin.catalog_roles_of(f"data_scientist_{u}", org["comp"]) == []
        assert is_denied(lambda: org["people"]["sam"].pyiceberg(org["comp"]).load_table("audits.events"))
    finally:
        admin.drop_catalog_role(org["comp"], "eng_reader")
    # A principal role spans catalogs: dana's roles bind into both.
    assert admin.catalog_roles_of(f"eng_engineer_{u}", org["eng"]) == ["eng_contributor"]
    assert admin.catalog_roles_of(f"comp_viewer_{u}", org["comp"]) == ["comp_reader"]
