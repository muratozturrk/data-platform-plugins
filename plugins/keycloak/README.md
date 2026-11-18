# keycloak (v1)

Keycloak as the identity provider (IdP) of the HKM open-source data platform. v1 is built from the Keycloak 26.x documentation and release notes (current: 26.8.2, 2026-11-05), integration documentation of the platform components, standards (RFC 9700, OAuth 2.1 draft, NIST SP 800-63-4, OWASP ASVS 5.0, Bilgi ve İletişim Güvenliği Rehberi 1.1, KVKK) and published benchmarks. v2 folds in the book summaries kept in the surrounding workspace (`summaries/` → `synthesis/` → `skills/*/references/`).

## Layout
```
agents/       keycloak-platform-architect · keycloak-integration-engineer · keycloak-config-reviewer
commands/     /keycloak-platform-design · /keycloak-tenant-onboard · /keycloak-client-register
              /keycloak-config-review · /keycloak-access-review · /keycloak-upgrade-plan
skills/       10 skills (below); each has references/ filled from book summaries
examples/     deploy/ · terraform/ · integrations/ · automation/ · tests/
references/   sources.md · standards.md · books.md
```

| Skill | Purpose |
|---|---|
| `keycloak-architecture-tenancy` | Realms vs organizations vs groups for many institutions, identity domains (workforce, public, machines), naming, where Keycloak stops and Ranger starts |
| `keycloak-deployment-kubernetes` | Operator and Keycloak CR, CloudNativePG, optimized image with providers and themes, hostname/proxy/TLS, sizing formulas, single-cluster HA, multi-cluster v2, management interface |
| `keycloak-identity-federation` | LDAP/AD user federation vs OIDC/SAML brokering per institution, organization domains and IdP routing, first-login and account linking, user profile, SCIM, lifecycle (joiner/mover/leaver) |
| `keycloak-authentication-assurance` | Flows, passkeys/WebAuthn, OTP, step-up with ACR/LoA, brute-force, password policy, sessions and timeouts mapped to NIST AAL and the BG Rehberi |
| `keycloak-clients-and-tokens` | Client archetypes, PKCE, confidential client authentication (private_key_jwt, federated Kubernetes service-account tokens), DPoP, scopes and audience, lifetimes, refresh rotation, token exchange, client policies |
| `keycloak-claims-and-authorization` | The platform claims contract, group and role model, what Keycloak decides vs Ranger/OpenBao/app, Authorization Services only where needed, delegated administration with FGAP v2 and organization admin roles |
| `keycloak-platform-integrations` | Per-component recipes: Trino, Superset, Airflow 3, Kafka (Strimzi), Ranger, OpenBao, object storage, OpenMetadata, Grafana, OpenSearch Dashboards, Nessie, CKAN, WSO2, Kubernetes/Argo CD, Spark/Flink jobs |
| `keycloak-config-as-code` | Terraform/OpenTofu with the `keycloak/keycloak` provider, module per institution, secrets in OpenBao, realm import only for bootstrap, drift detection, promotion between environments |
| `keycloak-audit-compliance` | User and admin events, shipping to OpenSearch and immudb, retention, KVKK data minimisation and erasure, access reviews, mapping to BG Rehberi, NIST SP 800-63-4, ASVS 5.0 |
| `keycloak-operations-upgrades` | SLIs and alerts, failure modes, backup/restore and DR, key and secret rotation, CVE watch, the 26.8 upgrade checklist, load tests |

## Platform decisions this plugin assumes
1. One workforce realm `hkm` with one **organization per institution** (alias = tenant id, e.g. `t-acme`); one CIAM realm `hkm-public` for open-data portal users; `master` only for bootstrap and break-glass. Realm-per-institution only by exception (see `tenancy-and-realm-design.md`).
2. Keycloak **authenticates** and issues identity (who, which institution, which groups, how strongly authenticated). **Ranger decides data access**; OpenBao decides secret access; Airflow's Keycloak auth manager is the one place where Keycloak Authorization Services decides application permissions.
3. Groups are flat, named `platform_{institution}_{role}` (same names in Ranger), emitted without path in the `groups` claim; `preferred_username` is the principal name everywhere and cannot be edited.
4. Configuration is code (Terraform module per institution, applied from Git); console changes in production are break-glass only.
5. Events and admin events go to OpenSearch (search) and immudb (tamper evidence); personal data held in Keycloak is minimised.

## How it fits the other plugins
`apache-ranger` (group and user names, usersync from Keycloak, `sync_groups_keycloak.py`), `trino` (OAuth2/JWT authentication, `principal-field`), `apache-superset` (FAB OAuth and role mapping), `platform-engineering` (Tenant manifest → `examples/automation/tenant_to_tfvars.py`), `qa-test-strategy` (T-iam-* tests; `examples/tests` here implement claim-contract and realm-audit checks), `naming-conventions` (realm, client, group and scope names).
