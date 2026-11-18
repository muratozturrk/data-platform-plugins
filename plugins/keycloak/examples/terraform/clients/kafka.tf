# Kafka brokers are resource servers (validate JWT via JWKS); producers/consumers are
# workload clients (see workloads.tf) that request scope aud-kafka. Authorization: Ranger.
resource "keycloak_openid_client" "kafka_broker" {
  realm_id                     = keycloak_realm.hkm.id
  client_id                    = "kafka"
  name                         = "kafka brokers (resource server)"
  access_type                  = "CONFIDENTIAL" # bearer-only is outdated: confidential client with every flow off
  client_authenticator_type    = "client-jwt"   # only used if the brokers ever introspect
  standard_flow_enabled        = false
  direct_access_grants_enabled = false
  service_accounts_enabled     = false
  full_scope_allowed           = false
}
