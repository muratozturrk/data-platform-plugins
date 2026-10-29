"""Realms. One server with two realms (polaris-realms), per-realm bootstrap variables from older
docs (polaris-legacy-env), and the admin tool with -r/-c flags and a templated {realm} JDBC URL
(bootstrap-templated) next to a concrete database name (bootstrap-fixed)."""
import requests

from conftest import container, token

REALMS = "http://polaris-realms:8181"
MGMT = f"{REALMS}/api/management/v1"


def headers(tok, realm):
    return {"Authorization": f"Bearer {tok}", "Polaris-Realm": realm}


def test_realms_isolate_catalogs_and_principals(measure):
    polaris = token(REALMS, "root", "s3cr3t", "POLARIS").json()["access_token"]
    team = token(REALMS, "root", "t3amsecret", "my_team_realm").json()["access_token"]
    body = {"catalog": {"name": "team_cat", "type": "INTERNAL", "properties": {"default-base-location": "s3://team/cat"},
                        "storageConfigInfo": {"storageType": "S3", "allowedLocations": ["s3://team/cat"], "stsUnavailable": True}}}
    created = requests.post(f"{MGMT}/catalogs", json=body, headers=headers(team, "my_team_realm"), timeout=30)
    seen_by_team = [c["name"] for c in requests.get(f"{MGMT}/catalogs", headers=headers(team, "my_team_realm"), timeout=30).json()["catalogs"]]
    seen_by_polaris = [c["name"] for c in requests.get(f"{MGMT}/catalogs", headers=headers(polaris, "POLARIS"), timeout=30).json()["catalogs"]]
    cross_token = requests.get(f"{MGMT}/catalogs", headers=headers(polaris, "my_team_realm"), timeout=30)
    cross_secret = token(REALMS, "root", "s3cr3t", "my_team_realm")
    result = {"create_in_team_realm": created.status_code, "team_realm_sees": seen_by_team, "POLARIS_sees": seen_by_polaris,
              "POLARIS_token_in_team_realm": cross_token.status_code, "POLARIS_secret_in_team_realm": cross_secret.status_code}
    measure("isolation", result)
    assert created.status_code == 201 and seen_by_team == ["team_cat"] and seen_by_polaris == []
    assert cross_token.status_code == 401 and cross_secret.status_code == 401


def test_request_without_realm_header_uses_the_first_realm(measure):
    """Older docs: without the header and without a default realm the request fails. Polaris 1.8.0
    treats the first entry of polaris.realm-context.realms as default (require-header=false)."""
    no_header_polaris = token(REALMS, "root", "s3cr3t")
    no_header_team = token(REALMS, "root", "t3amsecret")
    measure("no_header", {"POLARIS_secret": no_header_polaris.status_code, "my_team_realm_secret": no_header_team.status_code})
    assert (no_header_polaris.status_code, no_header_team.status_code) == (200, 401)


def test_realm_header_name(example_rows, measure):
    """A header named 'realm' is ignored; the header Polaris reads is Polaris-Realm."""
    legacy = token(REALMS, "root", "t3amsecret", "my_team_realm", header="realm")
    correct = token(REALMS, "root", "t3amsecret", "my_team_realm", header="Polaris-Realm")
    example_rows.append({"example": "curl ... /oauth/tokens -H 'realm: my_custom_realm'", "status": legacy.status_code,
                      "verdict": "fails (header ignored, default realm used)", "fix": "-H 'Polaris-Realm: my_custom_realm'"})
    measure("header_name", {"realm:": legacy.status_code, "Polaris-Realm:": correct.status_code})
    assert (legacy.status_code, correct.status_code) == (401, 200)


def test_legacy_bootstrap_environment_variables_are_ignored(example_rows, measure):
    """POLARIS_BOOTSTRAP_MY_REALM_ROOT_CLIENT_ID / _SECRET (older docs): not read by Polaris 1.8.0,
    which bootstraps the realm with generated credentials instead."""
    r = token("http://polaris-legacy-env:8181", "my-client-id", "my-secret", "my_realm")
    logs = container("polaris-legacy-env").logs().decode("utf-8", "replace")
    generated = "root principal credentials:" in logs
    example_rows.append({"example": "export POLARIS_BOOTSTRAP_MY_REALM_ROOT_CLIENT_ID / _SECRET", "status": r.status_code,
                      "verdict": "ignored", "fix": "POLARIS_BOOTSTRAP_CREDENTIALS=my_realm,my-client-id,my-secret"})
    measure("bootstrap_env", {"token": r.status_code, "generated_credentials_logged": generated})
    assert r.status_code == 401 and generated


def test_admin_tool_bootstrap_flags_and_jdbc_url(example_rows, measure):
    templated = container("bootstrap-templated")
    fixed = container("bootstrap-fixed")
    templated_logs = templated.logs().decode("utf-8", "replace")
    tok = token("http://polaris-durable:8181", "client-id", "my-secret", "my_realm")
    result = {"templated_exit": templated.attrs["State"]["ExitCode"], "templated_error": next(
        (l.split("PSQLException:", 1)[1].strip() for l in templated_logs.splitlines() if "PSQLException" in l), ""),
        "fixed_exit": fixed.attrs["State"]["ExitCode"], "token_on_durable_server": tok.status_code}
    example_rows.append({"example": "bootstrap -r my_realm -c my_realm,client-id,my-secret with jdbc:postgresql://.../{realm}",
                      "status": result["templated_exit"], "verdict": "fails: {realm} is not substituted",
                      "fix": "one concrete database per JDBC URL; the -r / -c flags work"})
    measure("admin_tool", result)
    assert result["templated_exit"] != 0 and 'database "{realm}" does not exist' in result["templated_error"]
    assert result["fixed_exit"] == 0 and tok.status_code == 200
