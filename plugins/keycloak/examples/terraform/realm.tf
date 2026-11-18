resource "keycloak_realm" "hkm" {
  realm                         = "hkm"
  display_name                  = "HKM Veri Platformu"
  enabled                       = true
  organizations_enabled         = true
  admin_permissions_enabled     = true # Fine-Grained Admin Permissions v2
  terraform_deletion_protection = true

  ssl_required                = "all"
  registration_allowed        = false
  edit_username_allowed       = false
  login_with_email_allowed    = true
  duplicate_emails_allowed    = false
  reset_password_allowed      = false # workforce passwords live at the institution IdP
  remember_me                 = false
  default_signature_algorithm = "RS256"
  revoke_refresh_token        = true
  refresh_token_max_reuse     = 0

  # Sessions (see keycloak-authentication-assurance: NIST SP 800-63B-4 AAL2, BG 3.1.12.6)
  sso_session_idle_timeout             = "30m"
  sso_session_max_lifespan             = "10h"
  client_session_idle_timeout          = "30m"
  client_session_max_lifespan          = "10h"
  offline_session_idle_timeout         = "720h"
  offline_session_max_lifespan_enabled = true
  offline_session_max_lifespan         = "720h"
  access_token_lifespan                = "5m"
  access_code_lifespan                 = "1m"
  access_code_lifespan_login           = "30m"

  # Local accounts only (HKM D5, D6): 15 chars when a password can be the only factor, no composition
  # rules, denylist, long maximum age instead of periodic change (BG 3.2.1.8).
  password_policy = "length(15) and maxLength(128) and notUsername and notEmail and passwordBlacklist(blocklist.txt) and forceExpiredPasswordChange(365)"

  security_defenses {
    headers {
      x_frame_options           = "SAMEORIGIN"
      content_security_policy   = "frame-src 'self'; frame-ancestors 'self'; object-src 'none';"
      x_content_type_options    = "nosniff"
      x_robots_tag              = "none"
      strict_transport_security = "max-age=31536000; includeSubDomains"
      referrer_policy           = "no-referrer"
    }
    brute_force_detection {
      permanent_lockout          = false
      max_login_failures         = 5
      wait_increment_seconds     = 60
      max_failure_wait_seconds   = 900
      failure_reset_time_seconds = 43200
      max_temporary_lockouts     = 0
    }
  }

  internationalization {
    supported_locales = ["tr", "en"]
    default_locale    = "tr"
  }
}

resource "keycloak_realm_events" "hkm" {
  realm_id                     = keycloak_realm.hkm.id
  events_enabled               = true
  events_expiration            = 604800 # 7 days in the DB; long-term copy in OpenSearch
  admin_events_enabled         = true
  admin_events_details_enabled = true
  events_listeners             = ["jboss-logging"] # JSON logs -> Fluent Bit -> OpenSearch / immudb
  enabled_event_types = [
    "LOGIN", "LOGIN_ERROR", "LOGOUT", "LOGOUT_ERROR",
    "CODE_TO_TOKEN", "CODE_TO_TOKEN_ERROR", "REFRESH_TOKEN", "REFRESH_TOKEN_ERROR",
    "CLIENT_LOGIN", "CLIENT_LOGIN_ERROR",
    "USER_DISABLED_BY_TEMPORARY_LOCKOUT", "USER_DISABLED_BY_PERMANENT_LOCKOUT",
    "UPDATE_CREDENTIAL", "UPDATE_CREDENTIAL_ERROR", "REMOVE_CREDENTIAL", "REMOVE_CREDENTIAL_ERROR",
    "UPDATE_EMAIL", "UPDATE_EMAIL_ERROR",
  ]
}

resource "keycloak_realm_user_profile" "hkm" {
  realm_id                   = keycloak_realm.hkm.id
  unmanaged_attribute_policy = "DISABLED"

  attribute {
    name = "username"
    permissions {
      view = ["admin", "user"]
      edit = ["admin"]
    }
  }
  attribute {
    name = "email"
    permissions {
      view = ["admin", "user"]
      edit = ["admin"]
    }
  }
  attribute {
    name = "firstName"
    permissions {
      view = ["admin", "user"]
      edit = ["admin"]
    }
  }
  attribute {
    name = "lastName"
    permissions {
      view = ["admin", "user"]
      edit = ["admin"]
    }
  }
  attribute {
    name         = "institution"
    display_name = "Kurum"
    permissions {
      view = ["admin", "user"]
      edit = ["admin"]
    }
  }
  attribute {
    name         = "department"
    display_name = "Birim"
    permissions {
      view = ["admin", "user"]
      edit = ["admin"]
    }
  }
}

# Platform-wide groups (institution groups are created by modules/institution).
resource "keycloak_group" "platform" {
  for_each = toset(["platform_admins", "platform_auditors", "platform_operators"])
  realm_id = keycloak_realm.hkm.id
  name     = each.value
}

module "institution" {
  source   = "./modules/institution"
  for_each = var.institutions

  realm_id    = keycloak_realm.hkm.id
  realm_name  = keycloak_realm.hkm.realm
  tenant_id   = each.key
  institution = each.value
}
