# Airflow 3 with the Keycloak auth manager: Keycloak Authorization Services decide Airflow
# permissions. Resources, scopes and team permissions are created by
#   airflow keycloak-auth-manager create-permissions / create-team
# (apache-airflow-providers-keycloak 0.12), not by Terraform, to stay in sync with the provider.
resource "keycloak_openid_client" "airflow" {
  realm_id                     = keycloak_realm.hkm.id
  client_id                    = "airflow"
  name                         = "airflow"
  access_type                  = "CONFIDENTIAL"
  standard_flow_enabled        = true
  direct_access_grants_enabled = false # provider guide turns it on; the platform keeps it off and uses
  # AIRFLOW_CLI_TOKEN / the JWT bearer grant (provider 0.11+) for CLI and API callers
  service_accounts_enabled   = true # used by the auth manager to evaluate permissions
  pkce_code_challenge_method = "S256"
  full_scope_allowed         = false
  # The provider guide registers "<base_url>/*"; this is a documented exception to the exact-match
  # rule, limited to the Airflow host. Revisit when the provider documents a single callback path.
  valid_redirect_uris             = ["https://airflow.hkm.example/*"]
  valid_post_logout_redirect_uris = ["https://airflow.hkm.example/*"]
  web_origins                     = ["https://airflow.hkm.example"]
  frontchannel_logout_enabled     = false # back-channel only; until Airflow supports it, keep its session short
  authorization {
    policy_enforcement_mode          = "ENFORCING"
    decision_strategy                = "AFFIRMATIVE"
    allow_remote_resource_management = true
  }
}
