"""Superset (Flask-AppBuilder) login via Keycloak.

UI roles come from the `groups` claim; data access is enforced by Ranger through Trino
(Superset connects to Trino with impersonation of the logged-in user).
"""
from pathlib import Path

from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public"   # no access until a mapped group exists
AUTH_ROLES_SYNC_AT_LOGIN = True          # movers lose roles at next login
PERMANENT_SESSION_LIFETIME = 1800        # FAB has no back-channel logout; keep sessions short
SESSION_COOKIE_NAME = "__Host-session"   # implies Secure, Path=/ and no Domain
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"          # Lax because the OIDC callback is a cross-site redirect (HKM D44)

ISSUER = "https://sso.hkm.example/realms/hkm"

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": "superset",
            # read-only file mounted from OpenBao, not an environment variable
            "client_secret": Path("/run/secrets/superset-oauth/client-secret").read_text().strip(),
            "server_metadata_url": f"{ISSUER}/.well-known/openid-configuration",
            "api_base_url": f"{ISSUER}/protocol/",
            "client_kwargs": {"scope": "openid profile email", "code_challenge_method": "S256"},
        },
    }
]

# Flat platform group names (claim `groups`, full path off) -> Superset roles.
# Generated per institution from the Tenant manifest; never map by display names.
AUTH_ROLES_MAPPING = {
    "platform_admins": ["Admin"],
    "platform_t-acme_analyst": ["Gamma", "sql_lab", "t-acme_viewer"],
    "platform_t-acme_engineer": ["Alpha", "sql_lab", "t-acme_editor"],
}
