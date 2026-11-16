---
name: ranger-identity-usersync
description: Getting users, groups, roles and attributes into Ranger and people into the Admin UI when Keycloak is the identity provider: usersync sources (LDAP/AD, Unix, file), syncing from an LDAP directory federated with Keycloak, pushing groups from the control plane via REST, group naming and role model, user attributes for ABAC, service principals, Admin UI authentication options, and how engines pass identity to plugins. Use when designing identity integration or debugging "group not found / policy not applied" problems.
---

# Identity and Usersync

## How identity reaches a decision
The **engine** authenticates the user (Trino: Keycloak OAuth2/JWT) and passes user + groups to the plugin in each request. Trino supplies groups from its group provider/token claims; Kafka supplies the principal (mTLS/SCRAM/OAUTHBEARER) and the plugin resolves groups from Ranger's synced user store or the configured group mapping. Policies reference **names**; if the names the engine sends differ from those in policies (case, prefix, realm suffix), nothing matches → denied. Normalise once (lowercase, no realm suffix) on both sides.

## Getting principals into Ranger (needed for policy authoring, group resolution, UI)
| Option | How | When |
|---|---|---|
| A. LDAP source of truth | An LDAP directory (AD, OpenLDAP, 389-DS) holds users/groups; Keycloak federates it (User Federation); Ranger **usersync** reads the same LDAP (`SYNC_SOURCE=ldap`, group search enabled, incremental sync) | organisations that already have AD/LDAP — cleanest |
| B. Control-plane push | Keycloak is the source; the `Tenant` controller creates/updates groups, roles and memberships in Ranger via REST (`/service/xusers/groups`, `/service/xusers/users`, `/service/roles/roles`, group-user mappings) when institutions/users change | Keycloak-only setups; platform default when no LDAP exists |
| C. File-based usersync | Generate a users/groups file from Keycloak's admin API on a schedule; usersync `SYNC_SOURCE=file` | fallback |
Usersync does not delete removed users/groups by default (deletion sync is configurable); the control plane must reconcile removals so that revoked members lose access immediately at the engine (token/group claim) and are cleaned in Ranger.

## Group and role model (platform)
- Keycloak groups `platform_{institution}_{role}` (`analyst`, `engineer`, `pii-reader`, `steward`, `admin`) → identical Ranger group names.
- No Ranger **role** objects: cross-institution functions and purposes are Keycloak groups (`platform_breakglass`, `platform_t-acme_fraud`) assigned through the manifest; team groups follow `platform_{institution}_{team}-{role}`.
- Service principals: `svc-trino`, `svc-superset`, `svc-airflow`, `svc-control-plane`, `svc-ranger-audit-reader`; workload identities `wi-{institution}-{pipeline}` are created by the control plane, not by usersync; only `svc-superset` impersonates (`trinouser` with named users).
- **User attributes** for ABAC (institution, region, clearance): synced from LDAP attributes (usersync supports additional attributes) or pushed by the control plane; usable in conditions/row filters via `USER.attr` expressions on plugin versions that support them; otherwise materialise per-institution policy items.

## Admin UI authentication
Ranger Admin supports internal users, LDAP/AD, Unix/PAM, Kerberos SPNEGO and header/JWT-based SSO (Knox-style trusted SSO). With Keycloak only: (1) LDAP auth if option A; (2) an authenticating reverse proxy (oauth2-proxy with Keycloak) in front of Admin with Ranger's SSO/trusted-proxy settings — verify the exact properties for the pinned version; (3) keep UI access to a handful of platform admins with internal accounts + MFA at the proxy. Humans rarely need the UI when policies are code; auditors use dashboards on the audit store.

## Zone admins and delegation
No human zone admin and no `delegateAdmin`: zones are administered by the deployment pipeline (`svc-ranger-ci`), and an institution manages its own assignments through the Access Administrator role in Git (`references/identity-and-principals.md`; azure-style-rbac-abac › Delegating assignment management). Auditors are groups with read access to audit and policy metadata.

## Debug checklist
1. What user/groups did the engine send? (Ranger access audit shows `reqUser` and matched policy.)
2. Do those groups exist in Ranger exactly (case)?
3. Is the resource in a zone whose policies cover it? Tag service linked?
4. Has the plugin downloaded the latest policy version (plugin status page / `policy.cache.dir` JSON timestamp)?
5. Is a deny or tag policy winning?

## Anti-patterns
- Policies on individual users
- Mixed-case or realm-suffixed names
- Relying on usersync to remove access
- Giving many humans Admin UI rights

Sources: `references/sources.md` (Ranger usersync docs, user/group REST API, Trino group provider and Ranger access-control docs, Keycloak user federation docs).

Details and book-derived rules: `references/identity-and-principals.md`.
