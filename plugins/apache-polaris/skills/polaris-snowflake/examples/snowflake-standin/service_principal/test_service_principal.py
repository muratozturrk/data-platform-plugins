"""The Polaris side of the Snowflake integration. Snowflake's catalog integration is an
Iceberg REST client with OAuth client credentials: token request with OAUTH_ALLOWED_SCOPES, list
namespaces / tables (SYSTEM$LIST_*_FROM_CATALOG), load table, read files. The labs make the same
calls as a dedicated Snowflake principal."""
import json

import pyarrow as pa
import pytest

from common.polaris import ApiError, Principal, token_response
from conftest import cli, seed_table

CATALOG = "prod_catalog"
DOCUMENTED_STEPS = [
    "./polaris principals create snowflake_service",
    "./polaris principal-roles create snowflake_reader",
    "./polaris catalog-roles create --catalog prod_catalog catalog_reader",
    "./polaris privileges --catalog prod_catalog --catalog-role catalog_reader catalog grant TABLE_LIST TABLE_READ_DATA",
]
FIXED_GRANTS = [
    "./polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader TABLE_LIST",
    "./polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader TABLE_READ_DATA",
]
MISSING_ASSIGNMENTS = [
    "./polaris principal-roles grant --principal snowflake_service snowflake_reader",
    "./polaris catalog-roles grant --catalog prod_catalog --principal-role snowflake_reader catalog_reader",
]
state: dict = {}


@pytest.fixture(scope="module")
def snowflake(admin, root):
    admin.create_catalog(CATALOG, f"s3://warehouse/{CATALOG}")
    # The writer side (e.g. Spark): catalog_admin alone cannot create tables with vended
    # credentials, so root's role gets CATALOG_MANAGE_CONTENT.
    admin.grant(CATALOG, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    seed_table(root, CATALOG, "db", "events")
    return state


def test_documented_cli_steps(snowflake, checks, measure):
    results = []
    for step in DOCUMENTED_STEPS:
        r = cli(step)
        results.append((step, r.returncode, (r.stdout + r.stderr).strip().splitlines()[-1:]))
        if "principals create" in step and r.returncode == 0:
            creds = json.loads(r.stdout)
            state["principal"] = Principal("snowflake_service", creds["clientId"], creds["clientSecret"])
    codes = [rc for _, rc, _ in results]
    assert codes[:3] == [0, 0, 0] and codes[3] != 0          # the grant line is not valid CLI syntax
    for step, rc, _ in results:
        checks.append({"example": step, "status": rc, "verdict": "as-is" if rc == 0 else "fails",
                          "fix": "" if rc == 0 else "one privilege per command: privileges catalog grant --catalog ... --catalog-role ... PRIVILEGE"})
    for step in FIXED_GRANTS:
        assert cli(step).returncode == 0
    measure("cli_steps", [{"step": s, "exit": rc, "last_line": out} for s, rc, out in results])


def test_steps_without_assignments_leave_snowflake_without_access(snowflake, checks, measure):
    """Creating the principal, the principal role and the catalog role without
    assigning them to each other, leaves the token Snowflake requests without privileges."""
    p = state["principal"]
    narrow = token_response(p.client_id, p.client_secret, "PRINCIPAL_ROLE:snowflake_reader")
    with pytest.raises(ApiError) as narrow_use:
        p.catalog(CATALOG, scope="PRINCIPAL_ROLE:snowflake_reader")
    with pytest.raises(ApiError) as err:
        p.catalog(CATALOG, scope="PRINCIPAL_ROLE:ALL").list_tables("db")
    checks.append({"example": "Documented steps 1-4 (+ fixed grant): Snowflake lists db tables", "status": err.value.status,
                      "verdict": "fails", "fix": "add principal-roles grant and catalog-roles grant (assignments)"})
    measure("without_assignments", {"scope_snowflake_reader_token": narrow.status_code,
                                        "scope_snowflake_reader_first_call": narrow_use.value.status,
                                        "scope_all_list_tables": err.value.status})
    # The token is issued for a role the principal does not hold, but rejected on use.
    assert (narrow.status_code, narrow_use.value.status, err.value.status) == (200, 401, 403)
    for step in MISSING_ASSIGNMENTS:
        assert cli(step).returncode == 0


@pytest.mark.parametrize("scope", ["PRINCIPAL_ROLE:ALL", "PRINCIPAL_ROLE:snowflake_reader", "PRINCIPAL_ROLE: ALL"])
def test_oauth_allowed_scopes(snowflake, scope, measure):
    """OAUTH_ALLOWED_SCOPES values; 'PRINCIPAL_ROLE: ALL' (with a space) is a common copy-paste form."""
    p = state["principal"]
    r = token_response(p.client_id, p.client_secret, scope)
    measure(f"scope[{scope}]", {"status": r.status_code, "body": r.json() if r.status_code != 200 else "token"})
    if " " in scope:
        assert r.status_code != 200
    else:
        assert r.status_code == 200
        assert p.catalog(CATALOG, scope=scope).list_tables("db") == ["events"]


def test_what_a_table_list_and_read_data_reader_can_do(snowflake, measure):
    """The commonly recommended Snowflake reader privileges: TABLE_LIST and TABLE_READ_DATA."""
    p = state["principal"]
    c = p.catalog(CATALOG, scope="PRINCIPAL_ROLE:snowflake_reader")
    outcome = {}

    def probe(name, fn):
        try:
            fn()
            outcome[name] = "ok"
        except ApiError as e:
            outcome[name] = e.status
    probe("list_namespaces (SYSTEM$LIST_NAMESPACES_FROM_CATALOG)", c.list_namespaces)
    probe("list_tables (SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG)", lambda: c.list_tables("db"))
    probe("load_table (CREATE ICEBERG TABLE / REFRESH)", lambda: c.load_table("db", "events"))
    engine = p.pyiceberg(CATALOG, scope="PRINCIPAL_ROLE:snowflake_reader")
    rows = engine.load_table("db.events").scan().to_arrow().num_rows
    outcome["read data files with vended credentials"] = rows
    try:
        engine.load_table("db.events").append(pa.table({"id": pa.array([99], pa.int64()),
                                                        "event_date": pa.array(["2025-04-01"])}))
        outcome["append (INSERT from Snowflake)"] = "ok"
    except Exception as e:                                      # noqa: BLE001 - result
        outcome["append (INSERT from Snowflake)"] = type(e).__name__ + ": " + str(e)[:120]
    measure("reader_capabilities", outcome)
    assert rows == 3 and outcome["list_tables (SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG)"] == "ok"
    assert outcome["append (INSERT from Snowflake)"] != "ok"


def test_registered_table_is_a_pointer_until_refresh(snowflake, root, measure):
    """CREATE ICEBERG TABLE stores the metadata Snowflake loaded; ALTER ... REFRESH reloads it.
    The same pointer semantics with an Iceberg REST client: a loaded table does not see a commit
    made by another engine until it is refreshed."""
    reader = state["principal"].pyiceberg(CATALOG, scope="PRINCIPAL_ROLE:snowflake_reader")
    registered = reader.load_table("db.events")
    first_snapshot = registered.current_snapshot().snapshot_id
    root.pyiceberg(CATALOG).load_table("db.events").append(
        pa.table({"id": pa.array([3, 4], pa.int64()), "event_date": pa.array(["2025-01-02"] * 2)}))
    before = registered.scan().to_arrow().num_rows
    registered.refresh()
    after = registered.scan().to_arrow().num_rows
    as_of = registered.scan(snapshot_id=first_snapshot).to_arrow().num_rows
    measure("refresh", {"rows_before_refresh": before, "rows_after_refresh": after, "rows_at_first_snapshot": as_of})
    assert (before, after, as_of) == (3, 5, 3)


def test_namespace_list_makes_namespace_discovery_work(snowflake, measure):
    """SYSTEM$LIST_NAMESPACES_FROM_CATALOG needs NAMESPACE_LIST, which those two privileges
    do not include."""
    assert cli("./polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader NAMESPACE_LIST").returncode == 0
    namespaces = state["principal"].catalog(CATALOG, scope="PRINCIPAL_ROLE:snowflake_reader").list_namespaces()
    measure("namespaces_with_namespace_list", namespaces)
    assert namespaces == ["db"]
