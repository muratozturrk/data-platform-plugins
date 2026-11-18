---
name: keycloak-platform-integrations
description: How each HKM component authenticates against Keycloak and what it does with the identity - Trino (OAuth2 web/CLI, JWT for services, principal-field), Superset (Flask-AppBuilder OAuth, role mapping), Airflow 3 (Keycloak auth manager), Kafka on Strimzi (SASL OAUTHBEARER, Ranger authorizer), Apache Ranger (users/groups from Keycloak, admin login), OpenBao (OIDC auth for people), object storage (MinIO AIStor/ObjectScale OIDC/STS), OpenMetadata, Grafana, OpenSearch Dashboards, Nessie and Iceberg REST clients, CKAN, WSO2 API Manager, Kubernetes API and Argo CD, Spark/Flink jobs, Flowable. Use when wiring a component to Keycloak or debugging a login/claim mismatch.
---

# Platform Integrations

## Matrix
| Component | Flow | Client | Principal / claim used | Authorization decided by | Example |
|---|---|---|---|---|---|
| Trino (Web UI, CLI external auth) | OAuth2 code (+ refresh tokens) | `trino` confidential | `principal-field=preferred_username`; groups not read from token | Ranger Trino plugin (groups from usersync) | `examples/integrations/trino/` |
| Trino (service callers, Superset impersonation) | JWT bearer validated via JWKS / token exchange | caller's client | `preferred_username` | Ranger | same |
| Trino Gateway | passes `Authorization` through to coordinators | — | — | — | |
| Superset | FAB `AUTH_OAUTH`, provider `keycloak` | `superset` confidential | `preferred_username`; `groups` → `AUTH_ROLES_MAPPING` | Superset roles (UI) + Ranger via Trino, calling Trino with a token exchanged for `audience=trino` (data) | `examples/integrations/superset/` |
| Airflow 3 | Keycloak auth manager (`apache-airflow-providers-keycloak` 0.12) | `airflow` confidential with Authorization Services | user + Keycloak permissions; teams per institution | **Keycloak Authorization Services** | `examples/integrations/airflow/` |
| Kafka (Strimzi) | SASL OAUTHBEARER, JWKS validation | per producer/consumer service account | `userNameClaim=preferred_username` (`service-account-<client>`) | Ranger Kafka authorizer (not KeycloakAuthorizer) | `examples/integrations/kafka/` |
| Apache Ranger | usersync / REST push of users & groups from Keycloak; admin login via LDAP or trusted SSO proxy | `svc-ranger-sync` | same names | Ranger | apache-ranger plugin `sync_groups_keycloak.py` |
| OpenBao | OIDC auth method (people), Kubernetes/JWT auth (workloads) | `openbao` confidential | `groups` claim → identity groups → policies | OpenBao policies | `examples/integrations/openbao/` |
| Object storage (MinIO AIStor; Dell ObjectScale) | OIDC console login, STS `AssumeRoleWithWebIdentity` | `objectstore` | `policy` claim or role ARN per group | storage policies (bucket per institution) | `examples/integrations/minio/` — verify ObjectScale STS support for the installed version |
| OpenMetadata | OIDC (Keycloak/custom-oidc) | `openmetadata` | email or username → OM user; teams from groups via ingestion | OpenMetadata roles/policies | |
| Grafana | generic OAuth | `grafana` | `groups` → `role_attribute_path`; org per institution | Grafana RBAC | `examples/integrations/grafana/` |
| OpenSearch Dashboards | OpenID backend in security plugin | `opensearch-dashboards` | `roles_key: groups` → backend roles → role mappings (index per institution) | OpenSearch security | |
| Nessie / Iceberg REST clients | Quarkus OIDC on server; clients use client credentials (`oauth2` catalog props) | `nessie` (resource), job clients | `preferred_username` | Nessie authorization (CEL rules compiled from Ranger policy manifest) | |
| Spark / Flink jobs | client credentials with **federated K8s service-account token** | `svc-{job}` | `service-account-svc-{job}` | Ranger at Trino/Kafka/Nessie | |
| CKAN (public portal) | OIDC PKCE extension | `ckan` in `hkm-public` | email/username | CKAN org roles | |
| WSO2 API Manager | Keycloak as third-party Key Manager | `wso2-km` | subscriber apps as Keycloak clients | API scopes + throttling | |
| Kubernetes API, Argo CD | OIDC (structured authentication config / Argo CD OIDC) | `kubernetes`, `argocd` | `preferred_username`, `groups` with prefix `oidc:` | Kubernetes RBAC / Argo CD RBAC | `examples/integrations/kubernetes/` |
| Flowable (approval workflows) | OIDC (Spring Security) | `flowable` | groups → candidate groups | Flowable | |

## Integration rules
1. Every component uses its own client and audience; clients authenticate with `client-jwt` or `federated-jwt` (no client secrets, HKM D12) and any remaining credentials come from OpenBao via ESO; redirect URIs exact (Airflow host-limited exception documented in oauth-oidc-profile).
2. The principal name is `preferred_username` everywhere; if a component insists on `sub` or email, map it explicitly and record the exception.
3. Groups reach authorization engines by **sync**, not only via token claims (Ranger usersync/REST push), so revocation and audit work even for long sessions.
4. Resource servers validate `iss`, `aud`, signature (JWKS with caching and refresh on unknown `kid`), expiry, and `acr` where step-up matters.
5. Logout: register back-channel logout for every server-side app, with a handler that reaches the session on every replica, and keep each web session no longer than the SSO session (HKM D10); where a component's support is still to be confirmed or missing, keep its web session short.
6. Test every integration with the claims-contract check and a two-institution isolation test (qa-test-strategy T-iam-*).

## Anti-patterns
- Kafka `KeycloakAuthorizer` alongside Ranger (two policy decision points for the same resource).
- Superset/Grafana role mapping by display names that institutions can change.
- Trino `principal-field=email` (emails change; Ranger users do not).

## References
- `references/integration-matrix.md` — merged rules and component notes: web apps and SSO, APIs and resource servers, service-to-service and workload identity, Kafka, proxies and gateways.
- Component docs: see `../../references/sources.md` (Trino OAuth2, Airflow provider, Strimzi OAuth, OpenBao Keycloak provider page, MinIO Keycloak guide, OpenMetadata SSO, Argo CD Keycloak).
