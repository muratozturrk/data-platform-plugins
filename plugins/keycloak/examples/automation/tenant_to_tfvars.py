#!/usr/bin/env python3
"""Tenant manifest(s) -> institutions.auto.tfvars.json for examples/terraform.

Usage:
    python tenant_to_tfvars.py tenants/*.yaml > ../terraform/institutions.auto.tfvars.json

A Tenant manifest (platform-engineering plugin) looks like:

    apiVersion: platform.hkm/v1
    kind: Tenant
    metadata: {name: t-acme}
    spec:
      displayName: ACME Genel Müdürlüğü
      roles: [analyst, engineer, steward, pii-reader, admin]
      identity:
        domains: [acme.gov.tr]
        idp: {type: oidc, alias: acme-adfs, issuer: https://adfs.acme.gov.tr/adfs,
              metadataUrl: https://adfs.acme.gov.tr/adfs/.well-known/openid-configuration,
              clientId: hkm-sso, verifiesEmail: true,
              groupMap: {"HKM-Analistler": analyst, "HKM-Yoneticiler": admin}}

Exit code 1 and a message on stderr when a manifest violates the platform rules.
"""
import json
import re
import sys

import yaml

TENANT_ID = re.compile(r"^t-[a-z0-9-]{2,30}$")
ALLOWED_ROLES = {"analyst", "engineer", "steward", "pii-reader", "admin"}
IDP_TYPES = {"oidc", "saml", "none"}


class ManifestError(ValueError):
    pass


def convert(doc):
    if doc.get("kind") != "Tenant":
        raise ManifestError("kind must be Tenant")
    tenant = doc.get("metadata", {}).get("name", "")
    if not TENANT_ID.match(tenant):
        raise ManifestError(f"tenant id {tenant!r} must match {TENANT_ID.pattern}")
    spec = doc.get("spec", {})
    roles = spec.get("roles") or sorted(ALLOWED_ROLES)
    unknown = set(roles) - ALLOWED_ROLES
    if unknown:
        raise ManifestError(f"{tenant}: unknown roles {sorted(unknown)}; allowed {sorted(ALLOWED_ROLES)}")
    if "admin" not in roles:
        raise ManifestError(f"{tenant}: an 'admin' role is required for delegated administration")
    identity = spec.get("identity", {})
    domains = identity.get("domains") or []
    if not domains:
        raise ManifestError(f"{tenant}: at least one email domain is required")
    idp = identity.get("idp") or {"type": "none"}
    if idp.get("type") not in IDP_TYPES:
        raise ManifestError(f"{tenant}: idp.type must be one of {sorted(IDP_TYPES)}")
    group_map = idp.get("groupMap") or {}
    bad = {k: v for k, v in group_map.items() if v not in roles}
    if bad:
        raise ManifestError(f"{tenant}: groupMap targets roles not granted to the tenant: {bad}")
    if idp.get("type") != "none" and not idp.get("issuer", "").startswith("https://"):
        raise ManifestError(f"{tenant}: idp.issuer must be an https URL")
    return tenant, {
        "name": spec.get("displayName") or tenant,
        "domains": domains,
        "roles": sorted(roles),
        "admin_group": "admin",
        "idp": {
            "type": idp.get("type", "none"),
            "alias": idp.get("alias", f"{tenant}-idp"),
            "issuer": idp.get("issuer", ""),
            "metadata_url": idp.get("metadataUrl", ""),
            "client_id": idp.get("clientId", ""),
            # Trust email only when the institution IdP verifies addresses (Keycloak 26.8.2 guidance).
            "trust_email": bool(idp.get("verifiesEmail", False)),
            "group_map": group_map,
        },
    }


def main(paths):
    institutions = {}
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            for doc in yaml.safe_load_all(fh):
                if not doc:
                    continue
                try:
                    tenant, value = convert(doc)
                except ManifestError as exc:
                    print(f"{path}: {exc}", file=sys.stderr)
                    return 1
                if tenant in institutions:
                    print(f"{path}: duplicate tenant {tenant}", file=sys.stderr)
                    return 1
                institutions[tenant] = value
    json.dump({"institutions": institutions}, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
