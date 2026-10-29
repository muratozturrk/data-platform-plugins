"""Catalog roles and the documented privilege catalogue, measured operation by operation.

Part 1 runs the full privilege x operation matrix. Part 2 turns each commonly documented
privilege description into an explicit claim check. Part 3 measures where a grant applies: catalog, namespace
(including nested namespaces) or a single table.
"""
import uuid

import pytest

from common.polaris import ROOT, ApiError, CatalogClient
from conftest import seed_table
from catalog_roles.privilege_matrix import (DOCUMENTED_PRIVILEGES, MEASURED, OPERATIONS, Fixture,
                                                    probe, run_matrix)


@pytest.fixture(scope="module")
def lab(admin, root):
    u = uuid.uuid4().hex[:6]
    cat = f"privs_{u}"
    admin.create_catalog(cat, f"s3://warehouse/{cat}")
    admin.grant(cat, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    seed_table(root, cat, "sales", "orders")
    seed_table(root, cat, "sales.emea", "orders")          # nested namespace
    seed_table(root, cat, "hr", "salaries")
    CatalogClient(cat, *ROOT).create_view("sales", "v_orders", "SELECT * FROM sales.orders")
    p = admin.create_principal(f"probe_{u}")
    admin.create_principal_role(f"probe_{u}")
    admin.assign_principal_role(p.name, f"probe_{u}")
    yield {"catalog": cat, "principal": p, "prole": f"probe_{u}", "fx": Fixture(admin, root, cat)}
    admin.drop_principal(p.name)
    admin.drop_principal_role(f"probe_{u}")
    admin.drop_catalog(cat)


@pytest.fixture(scope="module")
def matrix(lab):
    return run_matrix(lab["fx"], lab["principal"], lab["prole"], DOCUMENTED_PRIVILEGES)


# ---------- part 1: the full matrix --------------------------------------------------------

def test_all_25_documented_privileges_exist_and_nothing_is_allowed_without_one(lab, matrix, measure):
    assert len(matrix) == 25
    baseline = probe(lab["fx"], lab["principal"], 999)                   # principal role, no catalog role
    assert not any(v is True for v in baseline.values())
    measure("matrix", {p: sorted(op for op, ok in row.items() if ok is True) for p, row in matrix.items()})


def test_privilege_matrix_matches_measured_reference(matrix):
    actual = {p: {op for op, ok in row.items() if ok is True} for p, row in matrix.items()}
    assert actual == MEASURED
    assert all(v in (True, False) for row in matrix.values() for v in row.values())   # only 2xx/403 seen


def test_privileges_imply_the_weaker_ones_of_the_same_object(matrix):
    allowed = lambda p: {op for op, ok in matrix[p].items() if ok is True}
    assert "table_list" in allowed("TABLE_CREATE") and "table_load" in allowed("TABLE_READ_DATA")
    assert {"data_read", "table_write_props"} <= allowed("TABLE_WRITE_DATA")
    assert "namespace_list" in allowed("NAMESPACE_CREATE")
    # ...but never across object types or into another privilege family.
    assert not allowed("TABLE_FULL_METADATA") & {"namespace_list", "view_list", "data_read"}


# ---------- part 2: documented claims, one by one ------------------------------------------

def allowed(matrix, priv):
    return {op for op, ok in matrix[priv].items() if ok is True}


def test_claim_table_full_metadata_excludes_data_access(matrix):
    """Documented: TABLE_FULL_METADATA includes all table privileges except READ_DATA and WRITE_DATA. HOLDS."""
    assert {"table_create", "table_drop", "table_write_props"} <= allowed(matrix, "TABLE_FULL_METADATA")
    assert not {"data_read", "data_write"} & allowed(matrix, "TABLE_FULL_METADATA")


def test_claim_read_data_vends_read_only_and_write_data_read_write(matrix):
    """Documented: READ_DATA issues read-only, WRITE_DATA read+write storage credentials. HOLDS."""
    assert "data_read" in allowed(matrix, "TABLE_READ_DATA") and "data_write" not in allowed(matrix, "TABLE_READ_DATA")
    assert {"data_read", "data_write"} <= allowed(matrix, "TABLE_WRITE_DATA")


def test_claim_namespace_list_lists_tables_and_views(matrix):
    """Documented: NAMESPACE_LIST 'enables listing objects (e.g., tables, views) within a namespace'.
    DOES NOT HOLD: it lists namespaces only; tables need TABLE_LIST, views VIEW_LIST."""
    assert allowed(matrix, "NAMESPACE_LIST") == {"namespace_list"}


def test_claim_manage_content_encompasses_content_privileges(matrix):
    """Documented: CATALOG_MANAGE_CONTENT encompasses MANAGE_METADATA, *_FULL_METADATA, READ/WRITE_DATA and
    catalog properties. HOLDS, and it does NOT include CATALOG_MANAGE_ACCESS (grants)."""
    content = allowed(matrix, "CATALOG_MANAGE_CONTENT")
    union = set().union(*(allowed(matrix, p) for p in (
        "CATALOG_MANAGE_METADATA", "TABLE_FULL_METADATA", "NAMESPACE_FULL_METADATA", "VIEW_FULL_METADATA",
        "TABLE_WRITE_DATA", "TABLE_READ_DATA", "CATALOG_READ_PROPERTIES", "CATALOG_WRITE_PROPERTIES")))
    assert content == union
    assert "grant_manage" not in content


def test_claim_manage_metadata_controls_roles(matrix):
    """Documented: CATALOG_MANAGE_METADATA 'grants full control over metadata and roles'.
    PARTLY: all metadata operations, but no grant management and no data access."""
    md = allowed(matrix, "CATALOG_MANAGE_METADATA")
    assert {"table_create", "view_create", "namespace_create"} <= md
    assert not {"grant_manage", "data_read", "data_write"} & md


def test_claim_catalog_read_properties_lists_catalogs(matrix):
    """Documented: CATALOG_READ_PROPERTIES 'enables listing catalogs and reading catalog properties'.
    PARTLY: reads this catalog's properties; the catalog listing stays service-admin only."""
    assert allowed(matrix, "CATALOG_READ_PROPERTIES") == {"catalog_read"}


def test_claim_manage_access_grants_permissions(matrix):
    """Documented: CATALOG_MANAGE_ACCESS allows granting/revoking and assigning catalog roles. HOLDS,
    and it is pure access control: no content operation at all."""
    assert allowed(matrix, "CATALOG_MANAGE_ACCESS") == {"grant_manage"}


# ---------- part 3: grant scope and inheritance --------------------------------------------

def with_grants(admin, lab, grants):
    role = f"scoped_{uuid.uuid4().hex[:4]}"
    admin.create_catalog_role(lab["catalog"], role)
    for g in grants:
        admin.grant(lab["catalog"], role, g)
    admin.assign_catalog_role(lab["prole"], lab["catalog"], role)
    return role


def drop_scoped(admin, lab, role):
    admin.revoke_catalog_role(lab["prole"], lab["catalog"], role)
    admin.drop_catalog_role(lab["catalog"], role)


def can_read(lab, ns, table):
    engine = lab["principal"].pyiceberg(lab["catalog"])
    try:
        return engine.load_table(f"{ns}.{table}").scan().to_arrow().num_rows > 0
    except Exception as e:                  # pyiceberg ForbiddenError / NoSuchTable for 403
        if "403" in str(e) or "not authorized" in str(e).lower() or "Forbidden" in type(e).__name__:
            return False
        raise


def test_namespace_grant_is_inherited_by_tables_and_nested_namespaces(admin, lab, measure):
    role = with_grants(admin, lab, [{"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_READ_DATA"}])
    try:
        result = {t: can_read(lab, *t) for t in (("sales", "orders"), ("sales.emea", "orders"), ("hr", "salaries"))}
        assert result == {("sales", "orders"): True, ("sales.emea", "orders"): True, ("hr", "salaries"): False}
        measure("namespace_grant_reach", {".".join(k): v for k, v in result.items()})
    finally:
        drop_scoped(admin, lab, role)


def test_table_grant_reaches_exactly_one_table(admin, lab):
    role = with_grants(admin, lab, [{"type": "table", "namespace": ["sales"], "tableName": "orders",
                                     "privilege": "TABLE_READ_DATA"}])
    try:
        assert can_read(lab, "sales", "orders") is True
        assert can_read(lab, "sales.emea", "orders") is False
    finally:
        drop_scoped(admin, lab, role)


def test_namespace_grant_is_one_grant_where_table_grants_scale_with_tables(admin, lab, measure):
    """Best practice: prefer namespace grants. Counted: grants needed to give read access to
    every table of the sales subtree."""
    cat = lab["catalog"]
    rc = CatalogClient(cat, *ROOT)
    tables = [(ns, t) for ns in ("sales", "sales.emea") for t in rc.list_tables(ns)]
    table_grants = [{"type": "table", "namespace": ns.split("."), "tableName": t, "privilege": "TABLE_READ_DATA"}
                    for ns, t in tables]
    ns_role = with_grants(admin, lab, [{"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_READ_DATA"}])
    try:
        # A table created AFTER the namespace grant is covered automatically.
        seed_table(lab["fx"].root, cat, "sales.emea", f"late_{uuid.uuid4().hex[:4]}")
        late = [t for t in rc.list_tables("sales.emea") if t.startswith("late_")][0]
        assert can_read(lab, "sales.emea", late) is True
        measure("grant_counts", {"namespace_grants": 1, "table_grants": len(table_grants),
                                     "tables_covered": len(tables)})
    finally:
        drop_scoped(admin, lab, ns_role)
    assert len(table_grants) > 1


def test_revoking_a_grant_takes_effect_on_the_next_request(admin, lab):
    grant = {"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_READ_PROPERTIES"}
    role = with_grants(admin, lab, [grant])
    try:
        session = lab["principal"].catalog(lab["catalog"], vended=False)
        assert session.load_table("sales", "orders")
        admin.revoke(lab["catalog"], role, grant)
        assert admin.grants(lab["catalog"], role) == []
        with pytest.raises(ApiError) as e:
            session.load_table("sales", "orders")
        assert e.value.status == 403
    finally:
        drop_scoped(admin, lab, role)


def test_grants_from_several_catalog_roles_add_up(admin, lab):
    r1 = with_grants(admin, lab, [{"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_READ_DATA"}])
    r2 = with_grants(admin, lab, [{"type": "namespace", "namespace": ["hr"], "privilege": "TABLE_READ_DATA"}])
    try:
        assert can_read(lab, "sales", "orders") and can_read(lab, "hr", "salaries")
    finally:
        drop_scoped(admin, lab, r1)
        drop_scoped(admin, lab, r2)
