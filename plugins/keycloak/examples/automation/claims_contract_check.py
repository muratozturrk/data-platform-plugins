#!/usr/bin/env python3
"""Check an access token against the HKM claims contract (keycloak-claims-and-authorization).

Usage:
    python claims_contract_check.py --token "$ACCESS_TOKEN" --audience trino
    python claims_contract_check.py --claims-json claims.json --audience kafka

The signature is NOT verified here (resource servers do that against JWKS); this checks the
shape and content of the claims. Exit code 1 when a rule fails.
"""
import argparse
import base64
import json
import re
import sys

ISSUER = "https://sso.hkm.example/realms/hkm"
GROUP = re.compile(r"^platform_(admins|auditors|operators|t-[a-z0-9-]+_(analyst|engineer|steward|pii-reader|admin))$")
USERNAME = re.compile(r"^(service-account-[a-z0-9-]+|[a-z0-9._-]+)$")
MAX_ACCESS_TOKEN_SECONDS = 300
MAX_TOKEN_BYTES = 8192


def decode(token):
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("not a JWS compact token")
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def check(claims, audience=None, issuer=ISSUER, token_bytes=0):
    """Return a list of human-readable failures (empty = contract met)."""
    failures = []
    if claims.get("iss") != issuer:
        failures.append(f"iss is {claims.get('iss')!r}, expected {issuer!r}")
    if not claims.get("sub"):
        failures.append("sub missing")
    user = claims.get("preferred_username", "")
    if not USERNAME.match(user):
        failures.append(f"preferred_username {user!r} is not a lowercase platform principal")
    groups = claims.get("groups", [])
    if not isinstance(groups, list):
        failures.append("groups must be a list")
        groups = []
    for g in groups:
        if g.startswith("/"):
            failures.append(f"group {g!r} has a path; the group mapper must use full_path=false")
        elif not GROUP.match(g):
            failures.append(f"group {g!r} is not a platform group (platform_<tenant>_<role>)")
    tenants = {g.split("_")[1] for g in groups if GROUP.match(g) and g.split("_")[1].startswith("t-")}
    institution = claims.get("institution")
    if tenants and institution and tenants != {institution}:
        failures.append(f"groups belong to {sorted(tenants)} but institution claim is {institution!r}")
    if len(tenants) > 1 and not institution:
        failures.append(f"user holds groups of several institutions {sorted(tenants)} without an institution claim")
    if audience:
        aud = claims.get("aud", [])
        aud = [aud] if isinstance(aud, str) else aud
        if audience not in aud:
            failures.append(f"aud {aud} does not contain {audience!r}")
    if "exp" in claims and "iat" in claims and claims["exp"] - claims["iat"] > MAX_ACCESS_TOKEN_SECONDS:
        failures.append(f"access token lifetime {claims['exp'] - claims['iat']} s exceeds {MAX_ACCESS_TOKEN_SECONDS} s")
    if claims.get("typ") not in (None, "Bearer", "DPoP"):
        failures.append(f"typ {claims.get('typ')!r} is not an access token")
    if token_bytes > MAX_TOKEN_BYTES:
        failures.append(f"token is {token_bytes} bytes (> {MAX_TOKEN_BYTES}); trim roles/groups")
    return failures


def main(argv=None):
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--token")
    src.add_argument("--claims-json")
    ap.add_argument("--audience")
    ap.add_argument("--issuer", default=ISSUER)
    args = ap.parse_args(argv)
    if args.token:
        claims, size = decode(args.token), len(args.token.encode())
    else:
        with open(args.claims_json, encoding="utf-8") as fh:
            claims, size = json.load(fh), 0
    failures = check(claims, args.audience, args.issuer, size)
    for f in failures:
        print("FAIL", f)
    print("contract met" if not failures else f"{len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
