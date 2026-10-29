"""Hardening settings: token broker with RSA key files, feature flags (global form and realm
overrides) and rate limiting, on polaris-hard; an invalid rate-limiter type on its own server."""
import base64
import json
import time

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from conftest import token

HARD = "http://polaris-hard:8181"


def b64(part: str) -> bytes:
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


def admin_headers(realm):
    time.sleep(1.2)                                         # stay under the 5 requests/s limit
    tok = token(HARD, "root", "s3cr3t", realm).json()["access_token"]
    return {"Authorization": f"Bearer {tok}", "Polaris-Realm": realm}


def create_catalog(realm, name, storage):
    base = {"FILE": f"file:///tmp/{name}", "GCS": f"gs://bucket/{name}", "S3": f"s3://bucket/{name}"}[storage]
    info = {"storageType": storage, "allowedLocations": [base]}
    if storage == "S3":
        info["stsUnavailable"] = True
    time.sleep(1.2)
    return requests.post(f"{HARD}/api/management/v1/catalogs", headers=admin_headers(realm), timeout=30, json={
        "catalog": {"name": name, "type": "INTERNAL", "properties": {"default-base-location": base}, "storageConfigInfo": info}})


def test_invalid_rate_limiter_type_breaks_every_request(example_rows, measure):
    r = token("http://polaris-ratelimit-invalid:8181", "root", "s3cr3t")
    example_rows.append({"example": "polaris.rate-limiter.filter.type=token-bucket", "status": r.status_code,
                      "verdict": "fails: no such rate limiter", "fix": "polaris.rate-limiter.filter.type=default"})
    measure("invalid_rate_limiter", {"status": r.status_code, "error": r.json()["error"]["message"][:160]})
    assert r.status_code == 500 and "token-bucket" in r.text


def test_token_bucket_limits_per_realm(measure):
    """requests-per-second=5, window=PT1S on polaris-hard; 30 requests as fast as possible."""
    headers = admin_headers("POLARIS")
    time.sleep(1.5)
    codes = [requests.get(f"{HARD}/api/management/v1/catalogs", headers=headers, timeout=30).status_code for _ in range(30)]
    other_realm = token(HARD, "root", "s3cr3t", "analytics").status_code   # separate bucket per realm
    time.sleep(1.5)
    measure("rate_limit", {"200": codes.count(200), "429": codes.count(429), "analytics_realm_right_after": other_realm})
    assert codes.count(429) > 0 and codes.count(200) >= 1 and other_realm == 200


def test_rsa_key_pair_from_files(measure):
    """Tokens are signed with the mounted private key: the mounted public key verifies them."""
    tok = token(HARD, "root", "s3cr3t", "POLARIS").json()["access_token"]
    head, payload, sig = tok.split(".")
    public = serialization.load_pem_public_key(open("/secrets/public.key", "rb").read())
    public.verify(b64(sig), f"{head}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())   # raises if invalid
    measure("rsa", {"alg": json.loads(b64(head))["alg"], "verified_with_mounted_public_key": True})


def test_feature_flag_forms(example_rows, measure):
    """polaris.features.defaults."X" (older docs) vs polaris.features.realm-overrides."realm"."X"."""
    result = {
        "FILE in POLARIS (defaults form allows FILE)": create_catalog("POLARIS", "file_cat", "FILE").status_code,
        "GCS in POLARIS": create_catalog("POLARIS", "gcs_cat", "GCS").status_code,
        "GCS in analytics (override: S3 only)": create_catalog("analytics", "gcs_cat", "GCS").status_code,
        "S3 in analytics": create_catalog("analytics", "s3_cat", "S3").status_code,
    }
    measure("feature_flags", result)
    defaults_honored = result["FILE in POLARIS (defaults form allows FILE)"] == 201
    example_rows.append({"example": 'polaris.features.defaults."SUPPORTED_CATALOG_STORAGE_TYPES"=...',
                      "status": "honored" if defaults_honored else "ignored",
                      "verdict": "as-is" if defaults_honored else "ignored",
                      "fix": "" if defaults_honored else 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=[...]'})
    example_rows.append({"example": 'polaris.features.realm-overrides."<realm>"."<FLAG>"=...',
                      "status": result["GCS in analytics (override: S3 only)"], "verdict": "as-is", "fix": ""})
    assert result["GCS in POLARIS"] == 201 and result["S3 in analytics"] == 201
    assert result["GCS in analytics (override: S3 only)"] == 400

