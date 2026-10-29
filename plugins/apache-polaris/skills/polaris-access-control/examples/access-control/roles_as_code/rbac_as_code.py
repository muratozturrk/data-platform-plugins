"""RBAC as code for Apache Polaris: declarative model -> plan -> idempotent apply -> drift check.

Same desired-state pattern as Terraform providers for catalogs/warehouses: the YAML model is
the source of truth, `plan` diffs it against the live server, `apply` executes only the diff.
"""
from __future__ import annotations

import pathlib
from dataclasses import dataclass

import yaml

from common.polaris import ROOT, Admin, CatalogClient

MODEL_PATH = pathlib.Path(__file__).with_name("rbac_model.yaml")


def load_model(path: pathlib.Path = MODEL_PATH) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def grant_key(g: dict) -> tuple:
    return (g["type"], ".".join(g.get("namespace", [])), g.get("tableName", ""), g["privilege"])


def key_to_grant(k: tuple) -> dict:
    gtype, ns, table, priv = k
    g = {"type": gtype, "privilege": priv}
    if ns:
        g["namespace"] = ns.split(".")
    if table:
        g["tableName"] = table
    return g


@dataclass
class State:
    catalogs: set
    namespaces: set          # (catalog, namespace)
    catalog_roles: set       # (catalog, role)
    grants: set              # (catalog, role, grant_key)
    principal_roles: set
    bindings: set            # (principal_role, catalog, catalog_role)
    principals: set
    memberships: set         # (principal, principal_role)


def desired_state(model: dict, prefix: str) -> State:
    s = State(set(), set(), set(), set(), set(), set(), set(), set())
    for cat, spec in model["catalogs"].items():
        s.catalogs.add(prefix + cat)
        s.namespaces |= {(prefix + cat, ns) for ns in spec["namespaces"]}
    for cat, roles in model["catalog_roles"].items():
        for role, rules in roles.items():
            s.catalog_roles.add((prefix + cat, role))
            for rule in rules:
                if "catalog" in rule:
                    s.grants |= {(prefix + cat, role, ("catalog", "", "", p)) for p in rule["catalog"]}
                else:
                    s.grants |= {(prefix + cat, role, ("namespace", rule["namespace"], "", p))
                                 for p in rule["privileges"]}
    for prole, binds in model["principal_roles"].items():
        s.principal_roles.add(prefix + prole)
        s.bindings |= {(prefix + prole, prefix + cat, cr) for cat, crs in binds.items() for cr in crs}
    for p, proles in model["principals"].items():
        s.principals.add(prefix + p)
        s.memberships |= {(prefix + p, prefix + r) for r in proles}
    return s


def actual_state(admin: Admin, prefix: str) -> State:
    s = State(set(), set(), set(), set(), set(), set(), set(), set())
    s.catalogs = {c for c in admin.catalogs() if c.startswith(prefix)}
    for cat in s.catalogs:
        s.namespaces |= {(cat, ns) for ns in CatalogClient(cat, *ROOT).list_namespaces()}
        for role in admin.catalog_roles(cat):
            if role == "catalog_admin":
                continue                                   # created by Polaris, not managed here
            s.catalog_roles.add((cat, role))
            s.grants |= {(cat, role, grant_key(g)) for g in admin.grants(cat, role)}
    s.principal_roles = {r for r in admin.principal_roles() if r.startswith(prefix)}
    for prole in s.principal_roles:
        for cat in s.catalogs:
            s.bindings |= {(prole, cat, cr) for cr in admin.catalog_roles_of(prole, cat)}
    s.principals = {p["name"] for p in admin.principals() if p["name"].startswith(prefix)}
    for p in s.principals:
        s.memberships |= {(p, r) for r in admin.principal_roles_of(p) if r.startswith(prefix)}
    return s


@dataclass(frozen=True)
class Change:
    action: str        # add | remove
    kind: str          # catalog | namespace | catalog_role | grant | principal_role | binding | principal | membership
    key: tuple | str


ORDER_ADD = ["catalog", "namespace", "catalog_role", "grant", "principal_role", "binding", "principal", "membership"]


def plan(desired: State, actual: State) -> list[Change]:
    """Adds for everything missing; removes only for access edges (grants, bindings, memberships).
    Identities and containers are never deleted automatically: that is a human decision."""
    changes = []
    for kind, attr in (("catalog", "catalogs"), ("namespace", "namespaces"), ("catalog_role", "catalog_roles"),
                       ("grant", "grants"), ("principal_role", "principal_roles"), ("binding", "bindings"),
                       ("principal", "principals"), ("membership", "memberships")):
        want, have = getattr(desired, attr), getattr(actual, attr)
        changes += [Change("add", kind, k) for k in sorted(want - have, key=str)]
        if kind in ("grant", "binding", "membership"):
            changes += [Change("remove", kind, k) for k in sorted(have - want, key=str)]
    return sorted(changes, key=lambda c: (c.action == "add", ORDER_ADD.index(c.kind)))


def apply(admin: Admin, changes: list[Change]) -> dict[str, str]:
    """Executes a plan. Returns new principal credentials (shown once, store them in a vault)."""
    secrets = {}
    for c in changes:
        k = c.key
        if c.kind == "catalog":
            admin.create_catalog(k, f"s3://warehouse/{k}")
            admin.grant(k, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        elif c.kind == "namespace":
            CatalogClient(k[0], *ROOT).create_namespace(k[1])
        elif c.kind == "catalog_role":
            admin.create_catalog_role(*k)
        elif c.kind == "grant":
            (admin.grant if c.action == "add" else admin.revoke)(k[0], k[1], key_to_grant(k[2]))
        elif c.kind == "principal_role":
            admin.create_principal_role(k)
        elif c.kind == "binding":
            (admin.assign_catalog_role if c.action == "add" else admin.revoke_catalog_role)(*k)
        elif c.kind == "principal":
            p = admin.create_principal(k, {"managed-by": "rbac-as-code"})
            secrets[k] = f"{p.client_id}:{p.client_secret}"
        elif c.kind == "membership":
            (admin.assign_principal_role if c.action == "add" else admin.revoke_principal_role)(*k)
    return secrets


def destroy(admin: Admin, model: dict, prefix: str) -> None:
    for p in model["principals"]:
        admin.drop_principal(prefix + p)
    for r in model["principal_roles"]:
        admin.drop_principal_role(prefix + r)
    for cat in model["catalogs"]:
        admin.drop_catalog(prefix + cat)
