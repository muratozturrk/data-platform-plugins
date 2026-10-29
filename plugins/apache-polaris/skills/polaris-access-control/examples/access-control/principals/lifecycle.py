"""Principal lifecycle (creation -> assignment -> revocation) and three review
questions turned into a function that compares an HR/IdP roster with Polaris."""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field

from common.polaris import Admin


def jwt_claims(token: str) -> dict:
    """Decode (not verify) the payload of a Polaris-issued JWT access token."""
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


@dataclass
class LifecycleReport:
    missing: list[str] = field(default_factory=list)                    # Q1: in roster, not in Polaris
    missing_roles: dict[str, list[str]] = field(default_factory=dict)   # Q2: roles not assigned
    excess_roles: dict[str, list[str]] = field(default_factory=dict)    # roles the roster does not justify
    not_revoked: list[str] = field(default_factory=list)                # Q3: retired but still present

    @property
    def clean(self) -> bool:
        return not (self.missing or self.missing_roles or self.excess_roles or self.not_revoked)


def audit_lifecycle(admin: Admin, roster: dict[str, list[str]], retired: list[str],
                    scope_prefix: str = "") -> LifecycleReport:
    """roster: principal -> expected principal roles (source of truth: HR system / IdP).
    retired: principals that left / services decommissioned. scope_prefix limits the audit to
    one team's principals (the lab shares one Polaris instance between tests)."""
    present = {p["name"] for p in admin.principals() if p["name"].startswith(scope_prefix)}
    report = LifecycleReport()
    for name, expected in roster.items():
        if name not in present:
            report.missing.append(name)
            continue
        actual = set(admin.principal_roles_of(name))
        if lacking := sorted(set(expected) - actual):
            report.missing_roles[name] = lacking
        if extra := sorted(actual - set(expected)):
            report.excess_roles[name] = extra
    report.not_revoked = sorted(p for p in retired if p in present)
    return report
