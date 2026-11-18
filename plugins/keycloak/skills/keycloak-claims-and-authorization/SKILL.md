---
name: keycloak-claims-and-authorization
description: The identity contract between Keycloak and the rest of the HKM platform - which claims every token carries (sub, preferred_username, groups, organization, acr, aud), group and role model shared with Ranger, when Keycloak roles or Authorization Services are appropriate and when Ranger/OpenBao decide instead, user attributes for ABAC, delegated administration of institutions with Fine-Grained Admin Permissions v2 and organization admin roles, impersonation controls, and the 26.8 group-claim change. Use when designing groups/roles, adding a claim, reviewing a token, or giving an institution admin rights in Keycloak.
---

# Claims Contract and Authorization Boundary

## The contract (client scope `hkm-claims`, default scope of every workforce client)
| Claim | Value | Consumers | Rules |
|---|---|---|---|
| `iss` | `https://sso.<domain>/realms/hkm` | all resource servers | one issuer per realm; exact match (ASVS 10.5.3) |
| `sub` | Keycloak user id (UUID) | audit, immutable key | never reused; with `iss` the primary user key, kept in audit records (ASVS 10.3.3; HKM D1) |
| `preferred_username` | lowercase username from IdP | **principal name** in Trino (`principal-field`), Kafka (`userNameClaim`), Ranger users, Superset, Airflow | editing disabled; unique; never an email; never reassigned, even after deletion (HKM D1, D24) |
| `groups` | flat names `platform_{tenant}_{role}`, **full path off** | Ranger (via usersync), Superset/Grafana/OpenSearch role mapping, OpenBao groups | only platform groups; 26.8 resolves bare names to top-level groups only — keep platform groups top-level |
| `institution` | tenant id (`t-acme`) from organization membership | Ranger, applications, audit | released through `hkm-claims`; never from a user-editable attribute |
| `organization` | `{"t-acme": {...}}` when an organization scope is requested | portal, Airflow teams, audit enrichment | requested with one scope format, preferably `organization:<alias>`, by clients that need institution context |
| `acr`, `amr`, `auth_time` | LoA (`silver`/`gold`), methods | step-up checks at Superset export, PII-unmask flows | in `hkm`, `silver` = password plus a second factor, `gold` = phishing-resistant passkey; password-only `silver` only in `hkm-public` (HKM D9) |
| `aud` | resource client ids via `aud-*` scopes | each resource server | resource servers enforce |
| `azp` | requesting client | Airflow `jwt_federated_client_ids`, audit | |
| Service accounts | `preferred_username = service-account-{clientId}` | Kafka/Trino principals for jobs | create matching Ranger users |

Changing the contract is a platform change: version it, test with `examples/automation/claims_contract_check.py`, announce to plugin owners (`apache-ranger`, `trino`, `apache-superset`).

## Groups and roles
- **Groups** = who you are in which institution: `platform_t-acme_analyst`, `platform_t-acme_engineer`, `platform_t-acme_steward`, `platform_t-acme_pii-reader`, `platform_t-acme_admin`; platform-wide `platform_admins`, `platform_auditors`, `platform_operators`.
- **Realm roles**: almost none (`offline_access` for listed clients); avoid composite role trees.
- **Client roles**: only for apps whose own permission model consumes them (e.g. Grafana `Admin/Editor/Viewer` mapped from groups via role mapping in Grafana, not client roles).
- Data permissions (tables, columns, topics, masks, row filters) are **not** Keycloak roles — they live in Ranger policies keyed on the groups above.

## Where Keycloak authorization is used
| Mechanism | Use in HKM |
|---|---|
| Authorization Services (resources, scopes, policies, UMA) | Airflow 3 Keycloak auth manager only (teams = institutions); not for data access |
| FGAP v2 (supported 26.2; v1 deprecated 26.5) | Institution admins: `query-users`, `query-groups` plus Groups permissions `view`, `view-members`, `manage-membership`, `manage-membership-of-members` on their own `platform_t-acme_*` groups only; never `manage-members` (it allows creating users) or admin roles such as `view-users`/`manage-users`; no client/realm rights (HKM D57) |
| Organization admin roles (26.7: `manage-organizations`, `view-organizations`, `query-organizations`) | Platform onboarding team; institution admins do **not** get them realm-wide |
| Impersonation | Granted only to the break-glass role, every use raises an alert (HKM D29); 26.8 always emits `act` claim and impersonator in events; CVE-2026-17526 showed impersonation → admin escalation; support staff get a read-only FGAP view instead |
| AuthZEN evaluation API (experimental 26.7) | do not use |

## Attributes for ABAC
Expose only stable, IdP-owned attributes (`institution`, `department`, `clearance` if the institution provides it) through `hkm-claims` and Ranger usersync attributes. Purpose of use and dataset sensitivity stay in Ranger/OpenMetadata tags, not in Keycloak.

## Anti-patterns
- Group names with paths in some clients and without in others.
- Encoding permissions in token size-heavy role lists (tokens > 8 KB break headers).
- Granting `realm-admin` to institution IT staff.
- Using `email` as principal.

## References
- `references/claims-contract-and-delegation.md` — merged rules: claims and token content, groups and roles, coarse vs fine-grained authorization, capability links, delegated administration, ABAC attributes.
- `examples/automation/claims_contract_check.py`, `examples/terraform/scopes.tf`
