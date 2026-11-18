# In-cluster workloads authenticate with projected Kubernetes service-account tokens
# (federated client authentication; supported for Kubernetes since Keycloak 26.6).
# Tokens carry preferred_username = service-account-<client_id>; create the same user in Ranger.
resource "keycloak_kubernetes_identity_provider" "cluster" {
  realm  = keycloak_realm.hkm.id
  alias  = "k8s-hkm-prod"
  issuer = "https://kubernetes.default.svc.cluster.local" # the cluster's service-account issuer
}

locals {
  workloads = {
    "svc-airflow-worker" = "system:serviceaccount:airflow:airflow-worker"
    "svc-spark-etl"      = "system:serviceaccount:spark-jobs:spark-etl"
    "svc-flink-cdc"      = "system:serviceaccount:flink:flink-cdc"
    "svc-ranger-sync"    = "system:serviceaccount:ranger:ranger-keycloak-sync"
    "svc-terraform"      = "system:serviceaccount:iam-pipeline:terraform"
  }
}

resource "keycloak_openid_client" "workload" {
  for_each                     = local.workloads
  realm_id                     = keycloak_realm.hkm.id
  client_id                    = each.key
  name                         = each.key
  access_type                  = "CONFIDENTIAL"
  standard_flow_enabled        = false
  direct_access_grants_enabled = false
  service_accounts_enabled     = true
  full_scope_allowed           = false
  client_authenticator_type    = "federated-jwt"
  extra_config = {
    "jwt.credential.issuer" = keycloak_kubernetes_identity_provider.cluster.alias
    "jwt.credential.sub"    = each.value
  }
}

# Admin REST callers need the kc-admin-api scope (Keycloak 26.8.2).
resource "keycloak_openid_client_default_scopes" "admin_api_callers" {
  for_each       = toset(["svc-ranger-sync", "svc-terraform"])
  realm_id       = keycloak_realm.hkm.id
  client_id      = keycloak_openid_client.workload[each.key].id
  default_scopes = ["basic", "profile", "roles", "kc-admin-api"]
}

# The client-authentication flow must contain the federated-jwt authenticator.
resource "keycloak_authentication_flow" "clients" {
  realm_id    = keycloak_realm.hkm.id
  alias       = "clients-with-federated-jwt"
  provider_id = "client-flow"
}

resource "keycloak_authentication_execution" "client_jwt" {
  realm_id          = keycloak_realm.hkm.id
  parent_flow_alias = keycloak_authentication_flow.clients.alias
  authenticator     = "client-jwt"
  requirement       = "ALTERNATIVE"
}

resource "keycloak_authentication_execution" "federated_jwt" {
  realm_id          = keycloak_realm.hkm.id
  parent_flow_alias = keycloak_authentication_flow.clients.alias
  authenticator     = "federated-jwt"
  requirement       = "ALTERNATIVE"
  depends_on        = [keycloak_authentication_execution.client_jwt]
}

resource "keycloak_authentication_bindings" "hkm" {
  realm_id                   = keycloak_realm.hkm.id
  client_authentication_flow = keycloak_authentication_flow.clients.alias
}
