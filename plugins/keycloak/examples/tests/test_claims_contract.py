"""T-iam-020..023: tokens carry the platform claims contract."""
import base64
import json

GOOD = {
    "iss": "https://sso.hkm.example/realms/hkm", "sub": "3f1c0d9e-1111-4c4c-9e9e-123456789abc",
    "preferred_username": "ayse.yilmaz", "groups": ["platform_t-acme_analyst", "platform_t-acme_steward"],
    "institution": "t-acme", "aud": ["trino", "account"], "iat": 1000, "exp": 1300, "typ": "Bearer",
}


def jwt(claims):
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()
    return f"{enc({'alg': 'RS256'})}.{enc(claims)}.sig"


def test_good_token_meets_contract(claims):
    assert claims.check(GOOD, audience="trino") == []
    assert claims.check(claims.decode(jwt(GOOD)), audience="trino") == []


def test_service_account_principal(claims):
    svc = dict(GOOD, preferred_username="service-account-svc-spark-etl", groups=[], institution=None)
    assert claims.check(svc) == []


def test_contract_violations_are_reported(claims):
    bad = dict(GOOD, preferred_username="Ayse.Yilmaz@acme.gov.tr", groups=["/platform_t-acme_analyst", "Analysts"],
               aud="account", exp=5000)
    msgs = " | ".join(claims.check(bad, audience="trino"))
    for expected in ["preferred_username", "has a path", "not a platform group", "aud", "lifetime"]:
        assert expected in msgs, expected


def test_cross_institution_groups_need_matching_institution(claims):
    mixed = dict(GOOD, groups=["platform_t-acme_analyst", "platform_t-beta_analyst"], institution="t-acme")
    assert any("belong to" in f for f in claims.check(mixed))
