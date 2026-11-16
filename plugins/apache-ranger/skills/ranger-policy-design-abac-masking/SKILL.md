---
name: ranger-policy-design-abac-masking
description: Designing Ranger policies for a multi-institution, KVKK-governed platform: layered policy set (baseline, zone resource policies, shared-data row filters, tag-based masking and deny, purpose-limitation roles), RBAC vs ABAC trade-offs and policy-count control, masking and row-filter patterns that stay identical across clients, time-bound and break-glass access, impersonation, naming/labels, decision tables and anti-patterns. Use when designing or refactoring the policy set.
---

# Policy Design: RBAC + ABAC, Masking, Row Filters

## Layered policy set
| Layer | Mechanism | Example |
|---|---|---|
| 0 Baseline (per service) | platform-folder policies | break-glass allow per zone with explicit access types; service principals; impersonation for named users; `queryid`/`systemproperty` rules; no baseline deny (default deny already applies) |
| 1 Institution zones | resource policies compiled from role assignments | `t_acme_lake.gold_sales.*` select → `platform_t-acme_analyst` on `trino-interactive` and `spark-sql`; human writes only on staging and sandbox schemas; published layers written only by pipeline identities via `trino-etl` and Spark |
| 2 Shared data | row-filter policies in the owning zone, generated from share agreements (Trino only) | `t_globex_lake.gold_risk.loans`: `platform_t-acme_marketing-analyst` → `region = 'TR'`; owner groups and the owning pipeline unfiltered; `public` → `1 = 0` |
| 3 Sensitivity (tags) | per-zone tag masks + one tag deny | `PII` and `RESTRICTED` columns: mask by infotype, `MASK_NONE` only for an activated PII Reader or Restricted Reader group; table tagged `RESTRICTED`: deny for `public` except `platform_breakglass` |
| 4 Purpose and time | eligible assignments + validity schedules + conditions | purpose group `platform_t-acme_fraud` with a time-bound assignment; PII Reader eligible up to 3 months, activations up to 8 hours; break-glass group activated with a ticket |
Each layer is generated from manifests; layers 0 and 3 are platform-owned, 1–2 per institution, 4 per request workflow (Flowable approval → manifest → policy).

## RBAC vs ABAC in Ranger
Pure RBAC (resource policies per role) is readable but multiplies with datasets × roles; tag/attribute policies collapse that product. Independent benchmarks comparing RBAC and ABAC count policy changes as the cost driver, and Ranger's maintainers have pointed out that such comparisons understate Ranger when tag policies and lookup-table row filters are not used (the platform does not use `{USER}` macros for human data access). Design rule: **structure → wildcard resource policies; sensitivity/purpose → tag policies; horizontal slicing → one row filter with a lookup table or attribute macro**. Track policy count per institution; growth should be O(1) per new dataset.

## Masking patterns
- Classification drives masking through the infotype-to-mask table (`references/attribute-and-tag-design.md`): OpenMetadata tag `PII.IBAN` → Ranger tag `PII` with attribute `type=iban` → `MASK_SHOW_LAST_4`; `type=tckn` → `MASK_HASH`; any other `PII` → `MASK_HASH`; `RESTRICTED` column → `MASK_NULL`.
- Item order: privileged group first (`MASK_NONE`), then specific groups, last a catch-all group (`public`) with the strictest mask — never leave the default unmasked.
- Custom masks as engine SQL (`regexp_replace(...)`) only when built-ins do not fit; keep them deterministic so joins on masked values behave predictably (or forbid joins on masked columns by policy).
- Same output on every path: the engine applies masks, so CLI/JDBC/Superset/exports agree; Superset gets no RLS of its own (it queries Trino as the end user); ClickHouse copies need equivalent controls before they hold governed data.

## Row-filter patterns
- Literal per-institution items (simple, explicit, many items) vs a single item with `institution_id IN (SELECT institution_id FROM governance.entitlements WHERE user_group = …)` lookup (compact; needs a governed entitlement table and care for performance) vs `USER` attribute macro (compact; requires attributes in Ranger and plugin support).
- Always include explicit items for unfiltered groups (owner groups, the owning pipeline's documented exemption) first and a final `public` item `1 = 0`; a `1 = 0` filter is a default item, not a deny rule.
- Test filters with EXPLAIN in the engine: predicates must be sargable on partition columns.

## Purpose limitation and time-bound access (KVKK)
Purpose = a group plus a time-bound assignment with a condition: the request workflow creates the assignment for a purpose group (`platform_t-acme_fraud`) with `validitySchedules` and the request id as label; no Ranger roles and no purpose exception on guardrails. Unmasked `RESTRICTED` columns need an activated Restricted Reader assignment. Break-glass: group `platform_breakglass` with no standing members; activation adds a member for a short window; every use alerts.

## Impersonation and service accounts
Only `svc-superset` impersonates, for named users the compiler expands from the allowed groups (`trinouser` holds user names, never `*`); pipelines never impersonate and run as their own workload identity; workload identities get direct assignments, and writes go through `trino-etl` and Spark only.

## Naming and labels
Follow the platform naming table (`references/azure-style-rbac-abac.md` › Platform naming): policy name `<zone>-<assignment ID>-<service>` (`t-acme-ra-0001-trino-interactive`); labels `source`, `institution`, `assignment`, `role`, `owner`, `review-by`, `manifest:<path>@<sha>`, `request:<id>`; description states intent in one sentence.

## Decision table first
For every policy change produce subject × resource × action → {allow, deny, mask type, row filter}; it is the review artefact and the test fixture (see `examples/tests/`).

## Anti-patterns
- `public` in allow items on wildcards
- Mask policies without a catch-all item
- One policy per table/column; per-user items
- Deny rules to patch over-broad allows
- A second row or mask layer in Superset beside Ranger

Sources: `references/sources.md` (Ranger row-filter/masking and tag-policy docs, NIST SP 800-162/800-205, GigaOm ABAC vs RBAC reports and the Ranger maintainers' response, Privacera papers, KVKK guidance).

Details and book-derived rules: `references/attribute-and-tag-design.md`, `references/azure-style-rbac-abac.md`, `references/role-engineering.md`.
