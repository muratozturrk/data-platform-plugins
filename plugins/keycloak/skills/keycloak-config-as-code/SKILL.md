---
name: keycloak-config-as-code
description: Managing Keycloak configuration as code for the HKM platform - Terraform/OpenTofu with the keycloak/keycloak provider (v5.x) as the default, one module per institution generated from the Tenant manifest, realm baseline module, clients/scopes/mappers/organizations/IdPs/FGAP in code, secrets in OpenBao (write-only arguments), realm import CRs only for bootstrap, KeycloakOIDCClient CRs (preview), keycloak-config-cli as alternative, drift detection, environments and promotion, admin API service account with kc-admin-api scope. Use when automating Keycloak changes, onboarding an institution through CI, or reviewing a Terraform change.
---

# Keycloak Configuration as Code

## Tool choice
| Tool | Strengths | Limits | Use in HKM |
|---|---|---|---|
| **Terraform/OpenTofu + `keycloak/keycloak` provider** (adopted by the Keycloak project 2024; v5.10 current) | plan/apply/drift, modules, wide resource coverage (organizations, FGAP, user profile, keystores, IdPs, Kubernetes IdP) | state file holds secrets unless write-only args are used; pin `keycloak_version` | **the only source** for realms `hkm`, `hkm-public` (realm creation included) and all clients (HKM D17) |
| `keycloak-config-cli` (adorsys) | JSON/YAML realm files, idempotent import | less granular drift view | acceptable alternative for teams preferring realm-file shape |
| Operator `KeycloakRealmImport` | bootstrap at install | does not update an existing realm | not used: Terraform creates realms, the Operator only deploys the server (HKM D17) |
| Operator `KeycloakOIDCClient`/`KeycloakSAMLClient` CRs | GitOps per app namespace | preview in 26.8 | evaluate; not production |
| Admin console | — | no review, no history | break-glass only; every emergency change raises an alert and is written back to Terraform by pull request within 24 h (HKM D17) |

(Survey 2024: Terraform provider ~51 %, keycloak-config-cli ~16 % of respondents.)

## Repository layout (see `examples/terraform/`)
```
terraform/
  versions.tf            provider pin, keycloak_version pin
  providers.tf           client-credentials auth (client-jwt), no master admin password
  realm.tf               realm baseline: tokens, sessions, brute force, events, headers, user profile, client policies
  scopes.tf              hkm-claims, aud-* scopes, group mapper (full_path=false)
  clients/*.tf           one file per platform component
  modules/institution/   organization, domain, IdP + mappers, platform groups, FGAP for institution admins
  institutions.auto.tfvars.json   generated from Tenant manifests (tenant_to_tfvars.py)
```

## Rules
1. Pipeline: PR → `tofu fmt/validate` → `plan` against a staging realm → review (two reviewers on every change, HKM D17) → `apply` → claims-contract check → drift check nightly (`plan -detailed-exitcode`).
2. Terraform authenticates with one dedicated client per realm (not `master`) using `client-jwt`, bound to no person; give it the `realm-management` roles it needs and the `kc-admin-api` scope (26.8.2). It is an ASVS L3 client under the FAPI 2 profile (HKM D23).
3. Secrets: clients use `client-jwt`/`federated-jwt`, so there are no client secrets to manage (HKM D12); the realm authenticates to institution IdPs with a JWT signed by its own key (`private_key_jwt`); a client secret towards an IdP exists only as a listed per-institution exception held in OpenBao; the remaining credentials (SMTP, LDAP bind) are read from OpenBao in the pipeline or through the Keycloak vault, never committed; protect state (remote backend with encryption, access limited).
4. Institutions are data, not code: the Tenant manifest drives `institutions.auto.tfvars.json`; the module is the only way an organization is created.
5. Pin provider and `keycloak_version`; upgrade them in the same change as the server upgrade and re-run plan on a copy.
6. Import existing objects before managing them (`terraform import`), never recreate realms in production.
7. Protect realms with `terraform_deletion_protection = true`.

## Anti-patterns
- Exporting a realm from the console and committing it as "the code".
- Terraform authenticating as the `master` admin user with password grant.
- One giant state for every environment.

## References
- `references/config-as-code.md` — merged rules: environments and promotion, secrets in automation, policy and configuration testing, phased realm configuration rollout.
- `examples/terraform/`, `examples/automation/tenant_to_tfvars.py`
