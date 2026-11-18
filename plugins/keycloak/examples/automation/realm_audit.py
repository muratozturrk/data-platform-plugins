#!/usr/bin/env python3
"""Audit a Keycloak realm export (JSON) against the HKM platform rules.

Usage:
    kc.sh export --realm hkm --file hkm.json --users skip      # or a partial export from the Admin API
    python realm_audit.py hkm.json [--profile workforce|admin|public] [--json]

Findings carry a severity (BLOCK, FIX, INFO) and the rule/standard they come from.
Exit code 1 when any BLOCK finding exists.
"""
import argparse
import json
import sys

BUILT_IN_CLIENTS = {"account", "account-console", "admin-cli", "broker", "realm-management",
                    "security-admin-console", "admin-permissions"}
PROFILES = {
    # (max SSO idle s, max SSO lifespan s, max access token s, registration allowed)
    "workforce": (3600, 86400, 900, False),   # NIST SP 800-63B-4 AAL2; BG 3.1.12.6; access token ~15 min (Cloud Native OAuth ch. 7)
    "admin": (900, 43200, 300, False),        # AAL3-like for master / admin realms
    "public": (3600, 7 * 86400, 900, True),
}


def finding(sev, where, text, basis):
    return {"severity": sev, "where": where, "finding": text, "basis": basis}


def audit_realm(realm, profile="workforce"):
    out = []
    name = realm.get("realm", "?")
    idle_max, life_max, at_max, reg_ok = PROFILES[profile]

    if realm.get("sslRequired") not in ("all", "external"):
        out.append(finding("BLOCK", name, f"sslRequired={realm.get('sslRequired')}", "ASVS 5.0 V12; BG 3.1.12.16"))
    if not realm.get("bruteForceProtected"):
        out.append(finding("BLOCK", name, "brute-force detection off", "BG 3.1.12.3"))
    if realm.get("permanentLockout") and profile == "workforce":
        out.append(finding("FIX", name, "permanent lockout on a workforce realm (DoS risk)", "plugin v1"))
    if realm.get("registrationAllowed") and not reg_ok:
        out.append(finding("BLOCK", name, "self-registration enabled outside the public realm", "plugin v1"))
    if realm.get("editUsernameAllowed"):
        out.append(finding("BLOCK", name, "username editable; it is the engine principal", "ASVS 5.0 10.3.3"))
    if not realm.get("eventsEnabled"):
        out.append(finding("FIX", name, "user events not enabled", "BG 3.2.1.3"))
    if not realm.get("adminEventsEnabled"):
        out.append(finding("BLOCK", name, "admin events not enabled", "BG 3.1.8.2, 3.1.12.5"))
    elif not realm.get("adminEventsDetailsEnabled"):
        out.append(finding("FIX", name, "admin events without representation", "plugin v1"))
    idle = realm.get("ssoSessionIdleTimeout", 1800)
    life = realm.get("ssoSessionMaxLifespan", 36000)
    if idle > idle_max:
        out.append(finding("FIX", name, f"SSO idle {idle}s > {idle_max}s", "NIST SP 800-63B-4"))
    if life > life_max:
        out.append(finding("FIX", name, f"SSO max {life}s > {life_max}s", "NIST SP 800-63B-4"))
    if realm.get("accessTokenLifespan", 300) > at_max:
        out.append(finding("FIX", name, f"access token lifespan {realm.get('accessTokenLifespan')}s > {at_max}s", "RFC 9700; Cloud Native OAuth ch. 7"))
    if realm.get("rememberMe") and profile != "public":
        out.append(finding("FIX", name, "remember-me enabled for staff", "plugin v1"))

    out += audit_credentials(realm, name, profile)
    if not realm.get("offlineSessionMaxLifespanEnabled"):
        out.append(finding("FIX", name, "offline session max not limited", "Keycloak docs 26.8, p. 5, p. 13"))
    for comp in realm.get("components", {}).get("org.keycloak.userprofile.UserProfileProvider", []):
        for raw in comp.get("config", {}).get("kc.user.profile.config", []):
            try:
                policy = json.loads(raw).get("unmanagedAttributePolicy")
            except (ValueError, AttributeError):
                continue
            if policy in ("ENABLED", "ADMIN_VIEW"):
                out.append(finding("FIX", name, f"unmanaged attributes {policy}; declare every attribute", "Keycloak docs 26.8, p. 4; KVKK minimisation"))

    for scope in realm.get("clientScopes", []):
        for m in scope.get("protocolMappers", []):
            if m.get("protocolMapper") == "oidc-group-membership-mapper" and \
                    str(m.get("config", {}).get("full.path", "true")).lower() == "true" and \
                    scope.get("name") == "hkm-claims":
                out.append(finding("BLOCK", f"scope {scope['name']}", "groups mapper emits full paths", "claims contract v1"))

    for idp in realm.get("identityProviders", []):
        alias = idp.get("alias")
        cfg = idp.get("config", {})
        if idp.get("trustEmail"):
            out.append(finding("INFO", f"idp {alias}", "trustEmail on: confirm the institution IdP verifies email (Tenant manifest verifiesEmail)", "Keycloak 26.8.2 (CVE-2026-97175)"))
        if cfg.get("validateSignature") == "false" and idp.get("providerId") in ("oidc", "keycloak-oidc"):
            out.append(finding("BLOCK", f"idp {alias}", "signature validation off", "RFC 9700; OIDC Core"))

    for client in realm.get("clients", []):
        cid = client.get("clientId", "?")
        if cid in BUILT_IN_CLIENTS or cid.endswith("-realm"):
            if cid == "admin-cli" and client.get("directAccessGrantsEnabled") and profile != "admin":
                out.append(finding("INFO", f"client {cid}", "password grant on built-in admin-cli; disable if kcadm is not used in this realm", "RFC 9700"))
            continue
        attrs = client.get("attributes", {})
        where = f"client {cid}"
        if client.get("implicitFlowEnabled"):
            out.append(finding("BLOCK", where, "implicit flow enabled", "RFC 9700; ASVS 5.0 10.4.4"))
        if client.get("directAccessGrantsEnabled"):
            out.append(finding("BLOCK", where, "password grant enabled", "RFC 9700; ASVS 5.0 10.4.4"))
        if client.get("standardFlowEnabled"):
            if attrs.get("pkce.code.challenge.method") != "S256":
                out.append(finding("FIX", where, "PKCE S256 not enforced", "ASVS 5.0 10.4.6"))
            for uri in client.get("redirectUris", []):
                if "*" in uri:
                    out.append(finding("FIX", where, f"wildcard redirect URI {uri}", "ASVS 5.0 10.4.1"))
                if uri.startswith("http://") and "localhost" not in uri and "127.0.0.1" not in uri:
                    out.append(finding("BLOCK", where, f"plain-http redirect URI {uri}", "RFC 9700"))
        if client.get("fullScopeAllowed", True) and not client.get("bearerOnly"):
            out.append(finding("FIX", where, "full scope allowed (deprecated in 26.8)", "ASVS 5.0 10.4.11"))
        if not client.get("publicClient") and client.get("clientAuthenticatorType", "client-secret") == "client-secret" \
                and not client.get("bearerOnly"):
            out.append(finding("INFO", where, "client secret authentication; prefer client-jwt or federated-jwt, else rotate", "ASVS 5.0 10.4.16"))
        if client.get("publicClient") and client.get("standardFlowEnabled") and \
                attrs.get("dpop.bound.access.tokens") != "true" and str(attrs.get("use.refresh.tokens", "true")) == "true":
            out.append(finding("INFO", where, "public client with refresh tokens without DPoP; ensure rotation", "ASVS 5.0 10.4.5"))
            if not realm.get("revokeRefreshToken") or realm.get("refreshTokenMaxReuse", 0) != 0:
                out.append(finding("FIX", where, "refresh token rotation off (Revoke Refresh Token, max reuse 0) for a public client without DPoP", "Keycloak IAM ch. 12; Cloud Native OAuth ch. 7, 13"))
        if "*" in client.get("webOrigins", []):
            out.append(finding("FIX", where, "web origin `*`; list exact origins", "RFC 9700 §2.6; Cloud Native OAuth ch. 2, 13"))
        if client.get("standardFlowEnabled") and not client.get("publicClient"):
            if not attrs.get("backchannel.logout.url"):
                out.append(finding("INFO", where, "no back-channel logout URL on a server-side client; set it, or keep the app session short if the app cannot receive it", "Keycloak IAM ch. 4, 12; Solving IdM ch. 13"))
            if client.get("frontchannelLogout"):
                out.append(finding("INFO", where, "front-channel logout on; prefer back-channel logout", "Keycloak IAM ch. 4"))
    return out


def audit_credentials(realm, name, profile):
    """Password and OTP policy (HKM D6: >= 8 with MFA in hkm, >= 15 where a password is the only factor)."""
    out = []
    policy = realm.get("passwordPolicy", "")
    if not policy:
        out.append(finding("FIX", name, "no password policy", "Keycloak docs 26.8, p. 7; HKM D6"))
    else:
        rules = {}
        for part in policy.split(" and "):
            key, _, arg = part.strip().partition("(")
            rules[key] = arg.rstrip(")")
        need = 15 if profile == "public" else 8
        if not rules.get("length", "").isdigit() or int(rules["length"]) < need:
            out.append(finding("FIX", name, f"password minimum length below {need}", "NIST SP 800-63B-4 §3.1.1.2; HKM D6"))
        for key, text in (("passwordBlacklist", "no password denylist"),
                          ("notUsername", "password may equal the username"),
                          ("notEmail", "password may equal the email")):
            if key not in rules:
                out.append(finding("FIX", name, text, "Keycloak docs 26.8, p. 7; NIST SP 800-63B-4 §3.1.1.2"))
        if rules.get("hashAlgorithm", "argon2") != "argon2":
            out.append(finding("FIX", name, f"password hash {rules['hashAlgorithm']}; keep argon2", "API Security in Action ch. 3"))
    if realm.get("otpPolicyType", "totp") != "totp" or realm.get("otpPolicyPeriod", 30) != 30 \
            or realm.get("otpPolicyCodeReusable"):
        out.append(finding("FIX", name, "OTP must be TOTP, 30 s period, codes not reusable", "NIST SP 800-63B-4 §3.1.4; ASVS 5.0 6.5.5"))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("export")
    ap.add_argument("--profile", choices=sorted(PROFILES), default="workforce")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    with open(args.export, encoding="utf-8") as fh:
        data = json.load(fh)
    realms = data if isinstance(data, list) else data.get("realms", [data])
    findings = [f for r in realms for f in audit_realm(r, args.profile)]
    if args.json:
        json.dump(findings, sys.stdout, indent=2)
        print()
    else:
        print(f"{'severity':8} {'where':32} finding  [basis]")
        for f in findings:
            print(f"{f['severity']:8} {f['where'][:32]:32} {f['finding']}  [{f['basis']}]")
        counts = {s: sum(f["severity"] == s for f in findings) for s in ("BLOCK", "FIX", "INFO")}
        print(f"\nBLOCK {counts['BLOCK']} · FIX {counts['FIX']} · INFO {counts['INFO']}")
    return 1 if any(f["severity"] == "BLOCK" for f in findings) else 0


if __name__ == "__main__":
    sys.exit(main())
