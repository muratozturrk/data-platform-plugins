"""Automated security review of a Polaris instance, built from Polaris access-control best practices.

Rules (practice -> check):
  least privilege          -> R1 broad privileges on roles not named *_admin
  namespace-level grants   -> R2 table-level grants
  naming conventions       -> R3 role names not snake_case <domain>_<function>
  lifecycle / reviews      -> R4 principals without roles, R5 roles bound to nothing
  sensitive-data catalogs  -> R6 sensitive catalog reachable by roles outside the allow-list
  admin separation         -> R7 non-root principals holding service_admin
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from common.polaris import Admin

BROAD = {"CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_METADATA", "CATALOG_MANAGE_ACCESS",
         "TABLE_FULL_METADATA", "NAMESPACE_FULL_METADATA", "VIEW_FULL_METADATA"}
NAME_RE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)+$")     # snake_case, at least <domain>_<function>


@dataclass(frozen=True)
class Finding:
    rule: str
    severity: str
    subject: str
    detail: str


def audit(admin: Admin, prefix: str, sensitive_catalogs: set[str], sensitive_roles: set[str]) -> list[Finding]:
    """prefix scopes the audit to one tenant's objects on the shared lab server."""
    out: list[Finding] = []
    catalogs = [c for c in admin.catalogs() if c.startswith(prefix)]
    proles = [r for r in admin.principal_roles() if r.startswith(prefix)]
    bound: set[tuple[str, str]] = set()
    reach: dict[str, set[str]] = {}               # principal role -> catalogs it reaches
    for pr in proles:
        for cat in catalogs:
            for cr in admin.catalog_roles_of(pr, cat):
                bound.add((cat, cr))
                reach.setdefault(pr, set()).add(cat)

    for cat in catalogs:
        for role in admin.catalog_roles(cat):
            if role == "catalog_admin":
                continue
            grants = admin.grants(cat, role)
            broad = sorted({g["privilege"] for g in grants} & BROAD)
            if broad and not role.endswith("_admin"):
                out.append(Finding("R1", "high", f"{cat}/{role}", f"broad privileges {broad}"))
            tables = [f"{'.'.join(g['namespace'])}.{g['tableName']}" for g in grants if g["type"] == "table"]
            if tables:
                out.append(Finding("R2", "low", f"{cat}/{role}", f"{len(tables)} table-level grant(s): {tables}"))
            if not NAME_RE.match(role):
                out.append(Finding("R3", "low", f"{cat}/{role}", "catalog role name not snake_case <domain>_<function>"))
            if (cat, role) not in bound:
                out.append(Finding("R5", "medium", f"{cat}/{role}", "catalog role not bound to any principal role"))

    for pr in proles:
        if not NAME_RE.match(pr.removeprefix(prefix)):
            out.append(Finding("R3", "low", pr, "principal role name not snake_case <domain>_<function>"))
        if pr not in reach:
            out.append(Finding("R5", "medium", pr, "principal role grants nothing (no catalog roles)"))
        for cat in sorted(reach.get(pr, set()) & sensitive_catalogs):
            if pr.removeprefix(prefix) not in sensitive_roles:
                out.append(Finding("R6", "high", pr, f"reaches sensitive catalog {cat} but is not allow-listed"))

    for p in admin.principals():
        name = p["name"]
        if not name.startswith(prefix):
            continue
        roles = admin.principal_roles_of(name)
        if not roles:
            out.append(Finding("R4", "medium", name, "principal has no principal role"))
        if "service_admin" in roles:
            out.append(Finding("R7", "high", name, "non-root principal holds service_admin"))
    return sorted(out, key=lambda f: (f.rule, f.subject))
