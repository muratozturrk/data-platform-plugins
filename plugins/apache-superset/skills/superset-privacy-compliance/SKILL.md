---
name: superset-privacy-compliance
description: "Data-protection controls for Superset as a BI layer (GDPR/KVKK-style, regulator-neutral): why BI is its own privacy surface because it copies data (data cache, SQL Lab results backend, thumbnails, CSV/Excel export, scheduled report attachments, event logs), small-cell suppression, retention and disposal of cached copies, masking-aware export and watermarking, SQL Lab guardrails, audit logging of views/exports/denials, identity-aware cache keys, alert and report threshold governance, risky defaults to switch off, and a control-to-mechanism-to-verification table. Use when assessing or hardening a Superset instance that touches personal data, preparing for a privacy audit, or reviewing configs and dashboards for leakage paths."
---

# Superset Privacy and Compliance

Not legal advice: map each control to your regulator's wording with your privacy office. Role/RLS/auth design lives in `superset-security-model`; cache tuning in `superset-performance-caching`; this skill covers the *copies and exits* of data.

## Principle: the source engine's policy is not enough
A masking or row policy at the engine (Ranger, Trino/ClickHouse policies) protects the source table. BI then **copies** results: data cache, SQL Lab results backend, thumbnails, exports, emailed reports, browser cache, event logs, and any prompt sent to a model. Each copy is a storage medium that needs access control, encryption at rest where feasible, a retention clock, and a deletion path. Research finding (capability/gap study vs Superset 6.1, commit c83fb2bb1dcf): most Superset privacy risk comes from **defaults**, not missing features.

## Risky defaults to review first (Superset 6.1 baseline)
| Default | Why it matters | Direction |
|---|---|---|
| Cache key without user/role context | Unmasked result can be served to another user | Enable per-user / impersonation-aware cache keys (feature flags `CACHE_QUERY_BY_USER`, `CACHE_IMPERSONATION`; official docs) |
| Reports/alerts run as the owner | Output reaches recipients with the owner's data rights | Dedicated low-privilege executor; review recipients |
| Guest username unverified in embedding | Audit identity forgeable | Mint guest tokens server-side only; see `superset-multi-tenancy-embedding` |
| RLS not applied in SQL Lab | Free SQL bypasses dataset RLS | Engine-level policy plus impersonation |
| Query/event log retention unlimited; prune tasks commented out in the beat schedule | Personal data in query text kept forever | Enable prune tasks, set retention days |
| Telemetry pixel (Scarf) on by default | Outbound call from an on-prem install | Disable (`SCARF_ANALYTICS=false`, official docs); add to egress checklist |

## Control table
| Control | Superset mechanism or gap | Verification |
|---|---|---|
| Small-cell suppression (minimum group size k) | **Gap.** No native minimum group size; analysts hand-write `HAVING COUNT(*) >= k` in virtual datasets/metrics. Engine masking is per value and cannot see aggregate cell size. Industry-wide gap | Review dataset SQL for `HAVING`; test a chart with a k-1 group; same rule on export, cache, thumbnail and any AI summary. Complementary suppression (hide neighbours so totals cannot back-compute) is also needed |
| Retention of data cache | Cache backend + `cache_timeout`; external invalidation API (`/api/v1/cachekeys/invalidate`) | Source deletion triggers invalidation; TTL <= retention policy; inspect Redis key TTLs |
| Retention of SQL Lab results backend | `RESULTS_BACKEND` with TTL; no app-level encryption | TTL set; store encrypted at rest; access limited to the Superset service |
| Thumbnails | Rendered with the current user's rights by default; `THUMBNAIL_EXECUTE_AS` (official docs); stored in thumbnail cache | Disable on dashboards with personal data, or run as a restricted account; check cache TTL |
| Disposal workflow | **Gap.** No built-in "erase subject" touching cache/results/thumbnails | Documented runbook: delete at source, invalidate cache keys, purge results/thumbnails; test end to end |
| Soft-deleted assets | CLI `deletion-retention` manages purge windows (6.1) | Retention window set; forced purge by UUID tested |
| Masking-aware export | Export permission is separable from view (`can_csv`/`can_export` family); per-role row caps are a **gap** | Export roles minimal; export a masked column; confirm export re-queries under the user's identity, not from a shared cache |
| Watermark | **Gap** natively; mitigate via embedding wrapper, report template or screenshot overlay | Visible user/time marker on exported PDFs/PNGs |
| SQL Lab guardrails | Per-database `allow_dml` off, `expose_in_sqllab` deliberate, `SQL_MAX_ROW`, query timeout, `sql_lab` role only where needed; `SQL_QUERY_MUTATOR` can inject limits/comments | Role audit; DML attempt fails; engine policy proven with impersonated user |
| Audit logging | `EVENT_LOGGER` hook + logs table; coverage of exports, denied access, permission changes is partial | Custom logger to append-only store/SIEM; test that view, export, denied attempt and role change each produce a record |
| Identity-aware cache | See defaults table; impersonation must reach the engine and the cache key | Two users with different masks request the same chart; the second must not receive the first's result |
| Alert/report governance | `ALERTS_ATTACH_REPORTS` controls attachment of data; thresholds can leak small counts | Inventory schedules (owner, recipients, attachment flag); apply k to alert SQL; sensitive data only to approved domains, encrypted |
| Periodic access review | Roles as code; passive-user sync and expiring grants are **gaps** | Quarterly diff of roles vs IdP groups; revoke on role change |

## Rules
- Treat every cache, results store, thumbnail store and log table as a personal-data store in the data inventory.
- Minimise first: aggregated datasets for dashboards, raw rows only in governed SQL Lab.
- Apply the same suppression rule on every exit path (chart, export, cache, report, AI summary); a rule on one path is a false control.
- Query text and logs can contain identifiers typed by users: restrict access and apply retention to them too.
- Keep a versioned control register; each control has an owner, a config location and a test.

## Anti-patterns
- Relying on engine masking while a shared cache key is active.
- Thumbnails or email attachments on dashboards with person-level data.
- "Retention" that deletes the source row but never invalidates Superset caches.
- Public role or `PUBLIC_ROLE_LIKE` on instances with personal data.
- Report attachments to personal mailboxes or unmanaged domains.

See `superset-ai-layer` for model-bound data flows and agents `superset-asset-reviewer` / `superset-compliance-reviewer` for review. Sources: `references/sources.md`.
