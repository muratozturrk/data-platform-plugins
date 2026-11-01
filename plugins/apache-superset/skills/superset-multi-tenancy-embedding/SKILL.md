---
name: superset-multi-tenancy-embedding
description: Running Superset for many tenants (pool vs silo), isolating assets with roles/DAR/RLS/tags, and embedding dashboards in host apps with the embedded SDK and guest tokens (server-side minting, RLS injection, refresh, domains, theming, events). Use when exposing dashboards to tenants or external users, or deciding per-tenant instance vs shared.
---

# Multi-Tenancy and Embedding

## Core Concepts

### Isolation models
| Model | Mechanism | Pros | Cons |
|---|---|---|---|
| Pool (shared instance) | tenant roles + dataset access roles + RLS + dashboard RBAC + tags; per-tenant DB connections | cheap, one upgrade | Superset has no native tenant object: isolation is configuration discipline; SQL Lab needs engine policy; noisy neighbours share Celery/Redis |
| Silo (instance per tenant) | operator CR per tenant, own metadata DB/Redis, own Keycloak client | hard isolation, per-tenant versions/themes | N upgrades, more cost; assets synced to N instances |
| Bridge | shared instance for standard tiers, dedicated instances for regulated/premium | | two operating modes |
The control plane's `Tenant` CRD selects `bi: pool|silo`; both paths are automated.

### Embedding (pool or silo)
Flow: host app authenticates user (Keycloak) → host backend asks the **platform's guest-token service** for a token (service account with `can_grant_guest_token`, or signs the JWT itself with `GUEST_TOKEN_JWT_SECRET`) → token carries `user`, `resources` (dashboard ids) and `rls` rules computed from the platform policy → browser calls `embedDashboard({ id, supersetDomain, mountPoint, fetchGuestToken, dashboardUiConfig })` → iframe loads → refresh before `GUEST_TOKEN_JWT_EXP_SECONDS` (default 300 s).
Rules:
- Never mint tokens in the browser; never accept `rls` from the client; derive from tenant/role server-side.
- Guest user gets `GUEST_ROLE_NAME` with only the permissions the embed needs (`can_read` on dashboard/chart/dataset data).
- Allowed embedding domains per dashboard; `TALISMAN` `frame-ancestors` matches; `DISABLE_EMBEDDED_SUPERSET_LOGOUT`.
- Theme via SDK config (6.x) to match the host; emit/consume data-mask and drill-to-detail events for host integration.
- Dashboard strategy: shared dashboard + RLS (one asset, many tenants) by default; per-tenant copies only for custom layouts (generated from templates by `sync native`).

### Tenant-aware operations
Tags `tenant:{t}`, owners = tenant admin service user, per-tenant Celery queue priority, per-tenant cache key prefix (`CACHE_KEY_PREFIX` per instance or dataset `cache_timeout`), query labels (`trino` session properties / ClickHouse `log_comment`) carrying tenant for cost attribution, Prometheus metrics labelled by tenant where derivable.

## Patterns
- Guest-token service in the control plane: `POST /v1/tenants/{t}/embed-tokens {dashboard, user}` → policy lookup → token; audited.
- Embedded dashboards registered via the API (`/api/v1/dashboard/{id}/embedded`) by the assets pipeline, with allowed domains from tenant config.
- Load test embedded dashboards per tenant tier; set `ROW_LIMIT` and native-filter defaults to keep queries bounded.

## Anti-patterns
- RLS clause with a literal tenant id in a shared dashboard template
- Long-lived guest tokens; tokens stored in localStorage
- Public role used for "embedding"
- Host app iframing Superset login page

Sources: Superset docs "Embedding Superset" and embedded SDK README (guest tokens, `can_grant_guest_token`, RLS in tokens, `GUEST_TOKEN_JWT_*`, feature flags); Preset embedded dashboards docs; Xebia "Simple embedded analytics in practice" (RLS mapping per client, Superset lacks native multi-tenancy); AWS SaaS Lens isolation models.
