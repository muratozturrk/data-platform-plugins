---
name: keycloak-architecture-tenancy
description: Designing the Keycloak topology for the multi-institution HKM platform - realms vs organizations vs groups, identity domains (workforce staff, platform operators, public portal users, machines), the master realm, naming of realms/organizations/clients/groups/scopes, isolation guarantees and their limits, and the boundary between Keycloak (authentication, identity, claims) and Ranger/OpenBao/applications (authorization). Use when starting the IdP design, onboarding a new kind of user population, or deciding whether an institution needs its own realm.
---

# Keycloak Architecture and Tenancy

## The three questions
1. **Who authenticates?** Institution staff (workforce), platform operators, public users of the open-data portal and API (CIAM), machines (jobs, services, CI).
2. **Who owns the account?** The institution's directory (AD/LDAP or its own IdP), Keycloak itself (local accounts), or an external identity (e-Devlet, social, wallet).
3. **What must be isolated?** Login experience, password/MFA policy, administrators, keys, user lists, audit — per institution or per population.

## Recommended topology (platform default)
| Realm | Population | Account source | Why separate |
|---|---|---|---|
| `master` | personal break-glass accounts of at least two named operators (enabled, alert on every login; further emergency accounts disabled until a recorded event); temporary bootstrap admin deleted after setup | local; hardware passkey in a safe plus a password in OpenBao, two-person release, rotated after each use (HKM D32, D65) | Admin of all realms; keep empty otherwise (bootstrap guide) |
| `hkm` | Staff of all institutions, platform operators, service accounts of platform components | Institution IdP/LDAP per **organization**; local for operators | One SSO session across Trino, Superset, Airflow, Grafana…; one claims contract; organizations give per-institution membership, domains, IdP routing and delegated admins |
| `hkm-public` | Citizens, researchers, API consumers of CKAN/WSO2 | Self-registration, optional e-Devlet brokering; no social login (HKM D35) | Different assurance, registration, consent, retention (KVKK) and branding; must never share sessions or groups with staff |

- **Organizations** (supported since 26.0; organization groups 26.6; organization admin roles and FGAP resource type 26.7; shared IdPs across organizations 26.8) model institutions inside `hkm`: alias = tenant id (`t-acme`), verified email domains, linked IdP(s) with domain-based redirect (identity-first login), members (managed via IdP or unmanaged/invited).
- **Groups** carry function: flat top-level groups `platform_{institution}_{role}` (identical names in Ranger), plus platform groups (`platform_admins`, `platform_auditors`). Organization groups are an option for institution-internal structure but appear in tokens only when the organization scope is requested — the platform claims contract therefore uses realm groups.
- **Realm per institution** only for a written isolation requirement; organizations get no per-tenant security settings (HKM D17). Cost: no shared SSO, N× clients for every platform app, N issuers for every resource server, admin console and startup slow down past a few hundred realms (measured: ~300–400 per cluster practical).

## Boundary: what Keycloak decides
| Decision | Owner |
|---|---|
| Is this the person/workload it claims to be? How strongly? | Keycloak (flows, MFA, ACR) |
| Which institution, which groups, which attributes | Keycloak (organizations, groups, user profile) — sourced from the institution IdP where possible |
| May this principal read table/column/topic X? masks, row filters | **Ranger** (consumes `preferred_username` + groups) |
| May this principal read secret path Y? | **OpenBao** (OIDC auth → identity groups → policies) |
| May this user trigger DAG Z in Airflow? | Airflow Keycloak auth manager (Keycloak Authorization Services) |
| May this institution admin manage memberships of these groups? | Keycloak FGAP v2 (membership of the institution's own groups only; no user creation, HKM D57) |

## Naming (aligned with `naming-conventions`)
- Realm: lowercase, no dots: `hkm`, `hkm-public`. Organization alias = tenant id `t-{code}`; name = official institution name.
- Clients: `{component}` for platform-wide (`trino`, `superset`, `airflow`, `grafana`), `{component}-{tenant}` only when a component runs per institution; service accounts `svc-{component}`.
- Client scopes: `hkm-claims` (contract), `aud-{resource}` (audience), `organization` (built-in).
- Groups: `platform_{tenant}_{role}`; roles: analyst, engineer, steward, pii-reader, admin.

## Anti-patterns
- One realm per institution "for safety" without a stated isolation requirement.
- Putting public portal users in the workforce realm.
- Encoding data permissions as Keycloak roles and expecting engines to enforce them consistently — that is Ranger's job.
- Using `master` for applications or institution admins.
- Institution admins creating accounts: users arrive by first broker login, SCIM or organization invitation (HKM D43, D61).

## Open owner decisions
Still undecided: which organization holds vendor and auditor accounts. When a request depends on one of them, say so, ask whether it has been decided since, and point to `../../references/open-decisions.md`. Give options, never a value presented as decided.

## References
- `references/tenancy-and-realm-design.md` — merged rules: identity domains, realm/organization/group tenancy, isolation limits, authentication vs authorization boundary, control plane, naming, application design questions, federation governance.
- Keycloak docs: Organizations (Server Administration Guide), bootstrap admin, realm scaling measurements — see `../../references/sources.md`.
