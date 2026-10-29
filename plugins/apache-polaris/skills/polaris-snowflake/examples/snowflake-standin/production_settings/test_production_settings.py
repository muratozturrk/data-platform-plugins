"""Production advice for a Polaris exposed to Snowflake, on Polaris 1.8.0. Older docs show the settings in a
YAML layout (metaStoreManager, featureConfiguration, tokenBroker) from the pre-1.0 server; Polaris
1.x reads Quarkus properties, so polaris-prod sets the same three things as properties
(docker-compose.yml)."""
import base64
import json

import docker
import requests

from common.polaris import ApiError

DEV = "http://polaris-dev:8181"
PROD = "http://polaris-prod:8181"
DEFAULT = "http://polaris:8181"
ROOT = ("root", "s3cr3t")


def token(server, client_id, secret, scope="PRINCIPAL_ROLE:ALL"):
    return requests.post(f"{server}/api/catalog/v1/oauth/tokens", timeout=30, data={
        "grant_type": "client_credentials", "client_id": client_id, "client_secret": secret, "scope": scope})


def jwt_header(access_token):
    head = access_token.split(".")[0]
    return json.loads(base64.urlsafe_b64decode(head + "=" * (-len(head) % 4)))


def logs_of(service):
    client = docker.from_env()
    c = next(c for c in client.containers.list() if c.labels.get("com.docker.compose.service") == service
             and c.labels.get("com.docker.compose.project") == "polaris-snowflake")
    return c.logs().decode("utf-8", "replace")


def test_dev_mode_prints_root_credentials(measure):
    line = next(l for l in logs_of("polaris-dev").splitlines() if "root principal credentials:" in l)
    client_id, secret = line.split("credentials:", 1)[1].strip().split(":")
    r = token(DEV, client_id, secret)
    measure("dev_log_line", line.split("credentials:", 1)[0] + "credentials: <client-id>:<client-secret>")
    assert r.status_code == 200


def test_token_broker_and_lifetime(measure):
    default, prod = token(DEFAULT, *ROOT).json(), token(PROD, *ROOT).json()
    result = {"default": {"alg": jwt_header(default["access_token"])["alg"], "expires_in": default["expires_in"]},
              "prod (symmetric-key, PT30M)": {"alg": jwt_header(prod["access_token"])["alg"], "expires_in": prod["expires_in"]}}
    measure("tokens", result)
    assert result["default"] == {"alg": "RS256", "expires_in": 3600}
    assert result["prod (symmetric-key, PT30M)"] == {"alg": "HS256", "expires_in": 1800}


def test_credential_rotation_required(measure):
    """ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING: a principal in the rotation-required
    state can only rotate its credentials. The principal gets NAMESPACE_LIST on a catalog, so a
    namespace listing shows whether its grants are active."""
    root_h = {"Authorization": f"Bearer {token(PROD, *ROOT).json()['access_token']}"}
    m = f"{PROD}/api/management/v1"
    setup = [
        ("POST", "/catalogs", {"catalog": {"name": "rot", "type": "INTERNAL", "properties": {"default-base-location": "s3://warehouse/rot"},
                                           "storageConfigInfo": {"storageType": "S3", "allowedLocations": ["s3://warehouse/rot"], "stsUnavailable": True}}}),
        ("POST", "/principals", {"principal": {"name": "snowflake_service"}}),
        ("POST", "/principal-roles", {"principalRole": {"name": "snowflake_reader"}}),
        ("PUT", "/principals/snowflake_service/principal-roles", {"principalRole": {"name": "snowflake_reader"}}),
        ("POST", "/catalogs/rot/catalog-roles", {"catalogRole": {"name": "catalog_reader"}}),
        ("PUT", "/principal-roles/snowflake_reader/catalog-roles/rot", {"catalogRole": {"name": "catalog_reader"}}),
        ("PUT", "/catalogs/rot/catalog-roles/catalog_reader/grants", {"grant": {"type": "catalog", "privilege": "NAMESPACE_LIST"}}),
    ]
    creds = None
    for method, path, body in setup:
        r = requests.request(method, m + path, json=body, headers=root_h, timeout=30)
        assert r.status_code in (200, 201), f"{path}: {r.status_code} {r.text[:200]}"
        if path == "/principals":
            creds = r.json()["credentials"]

    def list_namespaces(client_id, secret):
        t = token(PROD, client_id, secret)
        h = {"Authorization": f"Bearer {t.json().get('access_token')}"}
        r = requests.get(f"{PROD}/api/catalog/v1/rot/namespaces", headers=h, timeout=30)
        return t.status_code, r.status_code, r.json().get("error", {}).get("message", "")[:160], h

    _, ns_new, _, _ = list_namespaces(creds["clientId"], creds["clientSecret"])
    # Polaris 1.8.0 sets the rotation-required state when an admin resets the credentials
    # (POST /principals/{name}/reset), not when the principal is created.
    reset = requests.post(f"{m}/principals/snowflake_service/reset", headers=root_h, json={}, timeout=30)
    assert reset.status_code == 200, reset.text
    issued = reset.json()["credentials"]
    tok_before, ns_before, msg_before, h = list_namespaces(issued["clientId"], issued["clientSecret"])
    rotated = requests.post(f"{m}/principals/snowflake_service/rotate", headers=h, timeout=30)
    new = rotated.json()["credentials"]
    tok_after, ns_after, _, _ = list_namespaces(new["clientId"], new["clientSecret"])
    result = {"list_namespaces_new_principal": ns_new, "admin_reset": reset.status_code,
              "token_with_reset_secret": tok_before, "list_namespaces_before_rotation": ns_before,
              "message_before": msg_before, "self_rotate": rotated.status_code,
              "token_after_rotation": tok_after, "list_namespaces_after_rotation": ns_after}
    measure("rotation_required", result)
    # Observed on 1.8.0 (in-memory persistence): the flag is accepted, the reset and self-rotation
    # work, but no call is blocked before the rotation. This pins that behavior; a release that
    # enforces it turns ns_before into 403 and fails here.
    assert (ns_new, reset.status_code, ns_before, rotated.status_code, ns_after) == (200, 200, 200, 200, 200)
