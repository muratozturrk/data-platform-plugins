# Configuration as code

Status: merged from Cloud Native Data Security with OAuth and Keycloak docs 26.8 (2026-11-17), API Security in Action, Identity Attack Vectors, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Manage realms, clients, scopes, organizations and identity providers with Terraform from Git through the admin API, never by editing Keycloak's database tables, and capture any emergency console change in Git so automation rolls it out everywhere. Why: reviewed, reproducible changes with drift detection, and manual repetition per stage causes errors. (research: Keycloak realm configuration tools survey 2024; Keycloak adopts Terraform provider, 2024-12; Cloud Native OAuth ch. 3, 4, 11; HKM D17)
2. Generate institution configuration from the Tenant manifest. Why: one source of truth joins Keycloak, Ranger, Kafka and OpenBao for a tenant. (plugin v1; platform-engineering plugin)
3. Authenticate automation with one realm-local client per realm (for `hkm` and `hkm-public`) using asymmetric client authentication, bound to no person and with no interactive login, never with one `master` client that manages every realm. Why: no master-realm password sits in pipelines, and a compromised automation account endangers every account it can manage, so its reach must be split by zone. (plugin v1; OWASP ASVS 5.0, 10.4.16; Identity Attack Vectors ch. 6)
4. Pin the provider and the target Keycloak version together. Why: provider behaviour depends on server version. (research: terraform-provider-keycloak documentation)
5. Create realms with Terraform as well, and leave no `KeycloakRealmImport` resources; the Operator only deploys the server. Why: realm import does not update an existing realm, and one tool must own every realm setting. (Keycloak docs 26.8, p. 16; HKM D17)

## Rules by topic

### Environments and promotion

Source: Cloud Native Data Security with OAuth, ch. 4, 11 (merged 2026-11-17).

1. Deploy the same configuration code through every stage and vary only per-stage variables (URLs, redirect URIs, secret references). Why: drift between stages causes application errors that appear only in production. (Cloud Native OAuth ch. 4, 11)
2. Use different signing and TLS keys in every stage, never copy a production realm export with its keys into test, and replace any default signing key. Why: shared keys let a token from one stage be accepted in another. (Cloud Native OAuth ch. 4)

Example:

```hcl
# stages/staging.tfvars
public_base_url   = "https://sso.staging.hkm.example"
superset_redirect = "https://superset.staging.hkm.example/oauth-authorized/keycloak"
openbao_path      = "kv/staging/keycloak"
```

### Secrets in automation

Source: Cloud Native Data Security with OAuth, ch. 4, 11 (merged 2026-11-17); API Security in Action, ch. 11 (merged 2026-11-18).

1. Keep secrets out of Git and images: read them from OpenBao in the pipeline and inject them at deploy time as protected references. Why: plain configuration secrets reach unauthorized readers. (Cloud Native OAuth ch. 4, 11; API Security in Action ch. 11)

### Policy and configuration testing

Source: API Security in Action (Madden, 2020), ch. 8; Learning Digital Identity (Windley, 2023), ch. 12 (merged 2026-11-18).

1. After every Terraform change to groups, roles, client scopes or mappers, and after every Ranger policy change, run automated access tests against a test realm that call representative endpoints as members of groups such as `platform_t-acme_analyst`, and fail the pipeline on any unexpected allow or deny. Why: changed rules combine unpredictably, and a silent mismatch can let invalid requests through for weeks. (API Security in Action ch. 8; Learning Digital Identity ch. 12)

Example:

```yaml
# tests/access-matrix.yaml: run in CI against realm hkm-test after terraform apply and the Ranger policy sync
- group: platform_t-acme_analyst
  call: { service: trino, action: select, resource: iceberg.t_acme.orders }
  expect: allow
- group: platform_t-acme_analyst
  call: { service: trino, action: select, resource: iceberg.t_other.orders }
  expect: deny
- group: platform_t-acme_analyst
  call: { service: airflow, action: trigger, resource: dag/t_acme_daily_load }
  expect: deny
```
### Realm configuration lifecycle

Source: Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 9 (merged 2026-11-18). Placed here as the closest file; the routing table names no config-as-code chapter for this source.

1. Roll out every new or tightened control (an authentication requirement, a client policy executor, a Ranger policy) in phases: first in the staging realm, and in a logging-only mode wherever the component offers one, review the requests it would have denied, then enforce step by step until access is denied by default. Why: phased rollout surfaces broken workflows before they lock users out. (Zero Trust Networks ch. 9)

Example:

```yaml
# changes/2026-12-hkm-gold-for-superset-admin.yaml (rollout record, reviewed with the Terraform change)
control: superset admin pages require acr=gold
phases:
  - stage: hkm-test           # staging realm built from the same Terraform
    review: LOGIN_ERROR and step-up failures for client superset in OpenSearch
  - stage: hkm
    scope: members of platform_t-acme_analyst first
  - stage: hkm
    scope: every client user; default deny for admin pages without gold
```
