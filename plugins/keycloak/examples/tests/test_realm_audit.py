"""T-iam-010..012: realm configuration matches platform rules (qa-test-strategy taxonomy)."""
import json


def test_good_realm_has_no_blocking_findings(realm_audit, fixtures):
    realm = json.loads((fixtures / "realm-export-good.json").read_text(encoding="utf-8"))
    findings = realm_audit.audit_realm(realm, "workforce")
    assert not [f for f in findings if f["severity"] == "BLOCK"], findings
    assert not [f for f in findings if f["severity"] == "FIX"], findings


def test_bad_realm_is_blocked_for_each_rule(realm_audit, fixtures):
    realm = json.loads((fixtures / "realm-export-bad.json").read_text(encoding="utf-8"))
    text = " | ".join(f["finding"] for f in realm_audit.audit_realm(realm, "workforce"))
    for expected in ["sslRequired=none", "brute-force detection off", "self-registration",
                     "username editable", "admin events not enabled", "implicit flow", "password grant",
                     "wildcard redirect", "plain-http redirect", "full paths", "signature validation off",
                     "SSO idle", "access token lifespan",
                     "password minimum length below 8", "no password denylist", "keep argon2",
                     "OTP must be TOTP", "offline session max not limited", "unmanaged attributes ENABLED",
                     "refresh token rotation off", "web origin `*`", "no back-channel logout URL",
                     "front-channel logout on"]:
        assert expected in text, expected


def test_cli_exit_code(realm_audit, fixtures, capsys):
    assert realm_audit.main([str(fixtures / "realm-export-good.json")]) == 0
    assert realm_audit.main([str(fixtures / "realm-export-bad.json")]) == 1
    capsys.readouterr()


def test_public_profile_needs_longer_passwords(realm_audit, fixtures):
    realm = json.loads((fixtures / "realm-export-good.json").read_text(encoding="utf-8"))
    text = " | ".join(f["finding"] for f in realm_audit.audit_realm(realm, "public"))
    assert "password minimum length below 15" in text
