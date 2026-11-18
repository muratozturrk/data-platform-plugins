# The claims contract (keycloak-claims-and-authorization). Version changes are platform changes.
resource "keycloak_openid_client_scope" "hkm_claims" {
  realm_id               = keycloak_realm.hkm.id
  name                   = "hkm-claims"
  description            = "HKM claims contract v1: preferred_username, groups (flat), institution"
  include_in_token_scope = false
}

resource "keycloak_openid_group_membership_protocol_mapper" "groups" {
  realm_id            = keycloak_realm.hkm.id
  client_scope_id     = keycloak_openid_client_scope.hkm_claims.id
  name                = "groups"
  claim_name          = "groups"
  full_path           = false # flat names, identical to Ranger group names
  add_to_id_token     = true
  add_to_access_token = true
  add_to_userinfo     = true
}

resource "keycloak_openid_user_attribute_protocol_mapper" "institution" {
  realm_id            = keycloak_realm.hkm.id
  client_scope_id     = keycloak_openid_client_scope.hkm_claims.id
  name                = "institution"
  user_attribute      = "institution"
  claim_name          = "institution"
  add_to_id_token     = true
  add_to_access_token = true
  add_to_userinfo     = true
}

# One audience scope per resource server: tokens requested with this scope carry aud=<resource>.
locals {
  resource_servers = ["trino", "nessie", "kafka", "objectstore"]
}

resource "keycloak_openid_client_scope" "aud" {
  for_each               = toset(local.resource_servers)
  realm_id               = keycloak_realm.hkm.id
  name                   = "aud-${each.value}"
  include_in_token_scope = true
}

resource "keycloak_openid_audience_protocol_mapper" "aud" {
  for_each                 = toset(local.resource_servers)
  realm_id                 = keycloak_realm.hkm.id
  client_scope_id          = keycloak_openid_client_scope.aud[each.value].id
  name                     = "aud-${each.value}"
  included_custom_audience = each.value
  add_to_access_token      = true
  add_to_id_token          = false
}

resource "keycloak_realm_default_client_scopes" "hkm" {
  realm_id = keycloak_realm.hkm.id
  default_scopes = [
    "basic", "profile", "email", "roles", "web-origins", "acr",
    keycloak_openid_client_scope.hkm_claims.name,
  ]
}
