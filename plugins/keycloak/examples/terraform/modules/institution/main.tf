# One institution = one organization + platform groups + (optional) brokered IdP + admin delegation.
resource "keycloak_organization" "this" {
  realm   = var.realm_name
  name    = var.institution.name
  alias   = var.tenant_id
  enabled = true

  dynamic "domain" {
    for_each = var.institution.domains
    content {
      name     = domain.value
      verified = true # set only after DNS/ownership check during onboarding
    }
  }
  attributes = {
    tenant_id = var.tenant_id
  }
}

resource "keycloak_group" "role" {
  for_each = toset(var.institution.roles)
  realm_id = var.realm_id
  name     = "platform_${var.tenant_id}_${each.value}" # identical to the Ranger group name
}

# Brokered institution IdP (OIDC). SAML institutions use keycloak_saml_identity_provider analogously.
resource "keycloak_oidc_identity_provider" "this" {
  count                           = var.institution.idp.type == "oidc" ? 1 : 0
  realm                           = var.realm_name
  alias                           = var.institution.idp.alias
  display_name                    = var.institution.name
  issuer                          = var.institution.idp.issuer
  authorization_url               = "${var.institution.idp.issuer}/protocol/openid-connect/auth" # replace with values from the IdP discovery document
  token_url                       = "${var.institution.idp.issuer}/protocol/openid-connect/token"
  jwks_url                        = "${var.institution.idp.issuer}/protocol/openid-connect/certs"
  client_id                       = var.institution.idp.client_id
  client_auth_method              = "private_key_jwt" # realm key signs; a client secret only as a listed per-institution exception
  validate_signature              = true
  trust_email                     = var.institution.idp.trust_email # true only if the IdP verifies email
  store_token                     = false
  sync_mode                       = "FORCE"
  allow_admin_role_mapping        = false
  default_scopes                  = "openid profile email"
  organization_id                 = keycloak_organization.this.id
  org_domain                      = var.institution.domains[0]
  org_redirect_mode_email_matches = true
  hide_on_login_page              = true # identity-first login routes by email domain
}

# Allow-listed mapping: institution group -> platform group (Advanced Claim to Group mapper).
resource "keycloak_custom_identity_provider_mapper" "group" {
  for_each                 = var.institution.idp.type == "oidc" ? var.institution.idp.group_map : {}
  realm                    = var.realm_name
  name                     = "group-${each.value}"
  identity_provider_alias  = keycloak_oidc_identity_provider.this[0].alias
  identity_provider_mapper = "oidc-advanced-group-idp-mapper"
  extra_config = {
    syncMode                 = "FORCE"
    claims                   = jsonencode([{ key = "groups", value = each.key }])
    "are.claim.values.regex" = "false"
    group                    = "/${keycloak_group.role[each.value].name}"
  }
}

resource "keycloak_hardcoded_attribute_identity_provider_mapper" "institution" {
  count                   = var.institution.idp.type == "oidc" ? 1 : 0
  realm                   = var.realm_name
  name                    = "institution"
  identity_provider_alias = keycloak_oidc_identity_provider.this[0].alias
  attribute_name          = "institution"
  attribute_value         = var.tenant_id
  user_session            = false
  extra_config = {
    syncMode = "FORCE"
  }
}

# Delegation: the institution's admin group may change memberships of its own platform groups only (FGAP v2).
data "keycloak_openid_client" "admin_permissions" {
  realm_id  = var.realm_id
  client_id = "admin-permissions"
}

resource "keycloak_openid_client_group_policy" "admins" {
  realm_id           = var.realm_id
  resource_server_id = data.keycloak_openid_client.admin_permissions.id
  name               = "${var.tenant_id}-admins"
  groups {
    id              = keycloak_group.role[var.institution.admin_group].id
    path            = keycloak_group.role[var.institution.admin_group].path
    extend_children = false
  }
  logic             = "POSITIVE"
  decision_strategy = "UNANIMOUS"
}

resource "keycloak_group_admin_permissions" "manage_own_groups" {
  realm_id          = var.realm_id
  name              = "${var.tenant_id}-manage-own-groups"
  description       = "Institution admins manage membership of their institution's platform groups"
  decision_strategy = "UNANIMOUS"
  group_ids         = [for g in keycloak_group.role : g.id]
  scopes            = ["view", "view-members", "manage-membership"]
  policies          = [keycloak_openid_client_group_policy.admins.id]
}

output "organization_id" { value = keycloak_organization.this.id }
output "group_ids" { value = { for k, g in keycloak_group.role : k => g.id } }
