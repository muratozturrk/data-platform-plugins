"""Access-control best practices and use cases as code.

1. RBAC model (multi-tenant + cross-team + sensitive data) applied idempotently, drift reverted.
2. Every persona x catalog x {read, write} combination executed and compared with the model.
3. Automated audit: clean on the model, every rule fires on an injected violation.
"""
import collections
import uuid

import pyarrow as pa
import pytest

from common.polaris import ROOT, Principal
from conftest import seed_table
from roles_as_code.audit import audit
from roles_as_code.rbac_as_code import (actual_state, apply, desired_state, destroy, load_model,
                                                 plan)


@pytest.fixture(scope="module")
def deployed(admin):
    model = load_model()
    prefix = f"t{uuid.uuid4().hex[:5]}_"
    first_plan = plan(desired_state(model, prefix), actual_state(admin, prefix))
    secrets = apply(admin, first_plan)
    root = Principal("root", *ROOT)
    for cat, spec in model["catalogs"].items():
        for ns in spec["namespaces"]:
            seed_table(root, prefix + cat, ns, "facts")
    people = {name.removeprefix(prefix): Principal(name, *secret.split(":")) for name, secret in secrets.items()}
    yield {"model": model, "prefix": prefix, "first_plan": first_plan, "people": people}
    destroy(admin, model, prefix)


def test_first_apply_builds_the_whole_model(deployed, measure):
    counts = collections.Counter(c.kind for c in deployed["first_plan"])
    assert all(c.action == "add" for c in deployed["first_plan"])
    assert counts["catalog"] == 5 and counts["principal_role"] == 6 and counts["principal"] == 6
    assert len(deployed["people"]) == 6
    measure("first_plan", dict(counts))


def test_second_plan_is_empty(admin, deployed):
    assert plan(desired_state(deployed["model"], deployed["prefix"]), actual_state(admin, deployed["prefix"])) == []


def test_manual_changes_show_up_as_drift_and_are_reverted(admin, deployed, measure):
    p = deployed["prefix"]
    admin.grant(p + "marketing", "marketing_analyst",
                {"type": "namespace", "namespace": ["campaigns"], "privilege": "TABLE_WRITE_DATA"})
    admin.assign_principal_role(p + "max_mkt_bi", p + "finance_analyst")
    drift = plan(desired_state(deployed["model"], p), actual_state(admin, p))
    assert sorted((c.action, c.kind) for c in drift) == [("remove", "grant"), ("remove", "membership")]
    apply(admin, drift)
    assert plan(desired_state(deployed["model"], p), actual_state(admin, p)) == []
    measure("drift", [f"{c.action} {c.kind} {c.key}".replace(p, "<prefix>") for c in drift])


# ---------- access matrix --------------------------------------------------------------------

WRITE_PRIVS = {"TABLE_WRITE_DATA", "CATALOG_MANAGE_CONTENT"}
READ_PRIVS = {"TABLE_READ_DATA"} | WRITE_PRIVS


def expected_access(model: dict) -> dict[tuple[str, str], str]:
    """(principal, catalog) -> 'write' | 'read' | 'none', derived from the YAML only."""
    out = {}
    for person, proles in model["principals"].items():
        for cat in model["catalogs"]:
            privs = set()
            for pr in proles:
                for crole in model["principal_roles"][pr].get(cat, []):
                    for rule in model["catalog_roles"][cat][crole]:
                        privs |= set(rule.get("catalog", [])) | set(rule.get("privileges", []))
            out[(person, cat)] = "write" if privs & WRITE_PRIVS else "read" if privs & READ_PRIVS else "none"
    return out


def observed_access(person: Principal, catalog: str, namespace: str) -> str:
    engine = person.pyiceberg(catalog)
    try:
        table = engine.load_table(f"{namespace}.facts")
        table.scan().to_arrow()
    except Exception:
        return "none"
    try:
        table.append(pa.table({"id": pa.array([7], pa.int64()), "amount": pa.array([7.0])}))
        return "write"
    except Exception:
        return "read"


def test_every_persona_gets_exactly_the_modelled_access(deployed, measure):
    model, p = deployed["model"], deployed["prefix"]
    expected = expected_access(model)
    observed = {(person, cat): observed_access(deployed["people"][person], p + cat, spec["namespaces"][0])
                for person in model["principals"] for cat, spec in model["catalogs"].items()}
    assert observed == expected
    measure("access_matrix", {f"{k[0]}@{k[1]}": v for k, v in sorted(observed.items())})
    measure("access_counts", dict(collections.Counter(observed.values())))


def test_sensitive_catalog_is_reachable_only_by_allow_listed_roles(deployed):
    expected = expected_access(deployed["model"])
    readers = {person for (person, cat), level in expected.items() if cat == "sensitive_data" and level != "none"}
    assert readers == {"cleo_comp", "pia_privacy"}


# ---------- audit ----------------------------------------------------------------------------

def run_audit(admin, deployed):
    p = deployed["prefix"]
    return audit(admin, p, {p + "sensitive_data"}, set(deployed["model"]["sensitive_access_roles"]))


def test_the_model_passes_the_audit(admin, deployed):
    assert run_audit(admin, deployed) == []


def test_every_audit_rule_fires_on_an_injected_violation(admin, deployed, measure):
    p = deployed["prefix"]
    mkt, fin, sens = p + "marketing", p + "finance", p + "sensitive_data"
    created_principals = []
    try:
        admin.create_catalog_role(mkt, "PowerUsers")                                        # R1 + R3
        admin.grant(mkt, "PowerUsers", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        admin.assign_catalog_role(p + "marketing_analyst", mkt, "PowerUsers")
        admin.grant(fin, "finance_analyst", {"type": "table", "namespace": ["ledger"],      # R2
                                             "tableName": "facts", "privilege": "TABLE_READ_DATA"})
        created_principals.append(admin.create_principal(p + "temp_contractor").name)       # R4
        admin.create_catalog_role(fin, "finance_auditor")                                   # R5
        admin.assign_catalog_role(p + "marketing_analyst", sens, "sensitive_data_viewer")    # R6
        created_principals.append(admin.create_principal(p + "ops_bot").name)               # R7
        admin.assign_principal_role(p + "ops_bot", "service_admin")

        findings = run_audit(admin, deployed)
        fired = {f.rule for f in findings}
        assert fired == {"R1", "R2", "R3", "R4", "R5", "R6", "R7"}
        measure("audit_findings", [f"{f.rule} {f.severity} {f.subject.replace(p, '<prefix>')}: {f.detail}"
                                       for f in findings])
    finally:
        admin.revoke_principal_role(p + "ops_bot", "service_admin")
        for name in created_principals:
            admin.drop_principal(name)
        admin.revoke_catalog_role(p + "marketing_analyst", sens, "sensitive_data_viewer")
        admin.revoke_catalog_role(p + "marketing_analyst", mkt, "PowerUsers")
        admin.drop_catalog_role(mkt, "PowerUsers")
        admin.drop_catalog_role(fin, "finance_auditor")
        admin.revoke(fin, "finance_analyst", {"type": "table", "namespace": ["ledger"], "tableName": "facts",
                                              "privilege": "TABLE_READ_DATA"})
    assert run_audit(admin, deployed) == []
