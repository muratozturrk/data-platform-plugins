# Server-side web applications: confidential, code + PKCE S256, signed-JWT client authentication,
# exact redirect URIs, back-channel logout where the app supports it, no full scope.
locals {
  web_apps = {
    trino = {
      redirect_uris = ["https://trino.hkm.example/oauth2/callback", "https://trino.hkm.example/ui/api/insights/logout"]
      backchannel   = null
      extra_scopes  = ["aud-trino", "offline_access"] # Trino refresh tokens / CLI external auth
    }
    superset = {
      redirect_uris = ["https://superset.hkm.example/oauth-authorized/keycloak"]
      backchannel   = null # FAB has no back-channel logout; keep its session short
      extra_scopes  = ["aud-trino"]
    }
    grafana = {
      redirect_uris = ["https://grafana.hkm.example/login/generic_oauth"]
      backchannel   = null
      extra_scopes  = []
    }
    openmetadata = {
      redirect_uris = ["https://openmetadata.hkm.example/callback"]
      backchannel   = null
      extra_scopes  = []
    }
  }
}

resource "keycloak_openid_client" "web" {
  for_each  = local.web_apps
  realm_id  = keycloak_realm.hkm.id
  client_id = each.key
  name      = each.key
  enabled   = true

  access_type                  = "CONFIDENTIAL"
  client_authenticator_type    = "client-jwt" # app signs assertions with its own key (JWKS URL or uploaded cert)
  standard_flow_enabled        = true
  implicit_flow_enabled        = false
  direct_access_grants_enabled = false
  service_accounts_enabled     = false
  pkce_code_challenge_method   = "S256"
  full_scope_allowed           = false
  valid_redirect_uris          = each.value.redirect_uris
  web_origins                  = ["+"]
  backchannel_logout_url       = each.value.backchannel
  frontchannel_logout_enabled  = false
  extra_config = {
    "token.endpoint.auth.signing.alg" = "RS256"
  }
}

resource "keycloak_openid_client_optional_scopes" "web" {
  for_each        = local.web_apps
  realm_id        = keycloak_realm.hkm.id
  client_id       = keycloak_openid_client.web[each.key].id
  optional_scopes = concat(["organization"], each.value.extra_scopes)
}
