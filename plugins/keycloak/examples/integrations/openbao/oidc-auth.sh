#!/usr/bin/env bash
# OpenBao OIDC auth for people (platform operators, institution engineers).
# Workloads use Kubernetes auth, not Keycloak. Run with a token that may configure auth methods.
set -euo pipefail
ISSUER=https://sso.hkm.example/realms/hkm

bao auth enable oidc
bao write auth/oidc/config \
  oidc_discovery_url="$ISSUER" \
  oidc_client_id="openbao" \
  oidc_client_secret="$(cat /run/secrets/openbao-oidc/client-secret)" \
  default_role="platform"

bao write auth/oidc/role/platform \
  user_claim="preferred_username" \
  groups_claim="groups" \
  bound_audiences="openbao" \
  allowed_redirect_uris="https://bao.hkm.example/ui/vault/auth/oidc/oidc/callback" \
  allowed_redirect_uris="http://localhost:8250/oidc/callback" \
  oidc_scopes="openid,profile" \
  token_ttl="1h" token_max_ttl="8h"

# Map Keycloak groups to OpenBao external groups -> policies, e.g.:
#   bao write identity/group name=platform_operators type=external policies=platform-operator
#   bao write identity/group-alias name=platform_operators mount_accessor=<oidc accessor> canonical_id=<group id>
