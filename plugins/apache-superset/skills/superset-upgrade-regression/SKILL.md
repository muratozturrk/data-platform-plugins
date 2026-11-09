---
name: superset-upgrade-regression
description: "Regression procedure for Superset version upgrades and image rebuilds: UPDATING.md and release-notes triage, feature-flag and config diff, metadata-DB migration rehearsal on a copy, assets re-import and UUID/ownership check, embedded SDK and extension/MCP compatibility, driver matrix (Trino/ClickHouse/PostgreSQL), smoke and full regression tiers, visual regression, performance parity, rollback. Use when planning or executing a Superset upgrade or rebuilding the platform image."
---

# Upgrade Regression

## Triage (before touching environments)
1. Read `UPDATING.md` between current and target versions; list breaking changes, removed/renamed feature flags, config key changes, removed endpoints (e.g. legacy `/superset/explore_json/` dependencies), Python/Node requirement changes.
2. Diff `superset_config.py` against the target's `config.py` defaults: unknown keys, flags that no longer exist, new security defaults (Talisman CSP, cookie flags).
3. Driver matrix: `sqlalchemy-trino`, `clickhouse-connect`, `psycopg2`, `authlib`, `Pillow`, Playwright/Firefox for reports — confirm compatible pins for the target.
4. Embedded SDK (`@superset-ui/embedded-sdk`) version compatible with the target; extensions (`@apache-superset/core` API) and MCP service versions.
5. Preset release notes / blog for behaviour changes (theming, native filters, chart defaults) that affect dashboards.

## Rehearsal (staging copy)
1. Snapshot production metadata DB → restore into the staging harness; run target image `superset db upgrade` + `superset init`; capture migration time and warnings (that is your maintenance window estimate).
2. Smoke tier: login (OIDC), list dashboards, open each production dashboard via chart-data API, SQL Lab query, embedded load with guest token, one Alert & Report run.
3. Assets: `superset-cli export-assets` from the upgraded instance and diff against Git — upgrades sometimes rewrite `params`/`query_context`; decide whether to accept and re-export or to fix.
4. Full regression tier: integration suite (roles/RLS/mask consistency, guest tokens, cache), E2E journeys, visual regression against baselines from the current version (expect theme diffs; review deliberately), k6 performance parity (p95 within ±15%).
5. Extensions/MCP: install the bundle, run its tests; MCP tool catalogue diff.

## Rollout
Blue/green: new image as a second release behind the ingress with the same metadata DB only if the migration is backward compatible (check `UPDATING.md`); otherwise maintenance window with DB backup → upgrade → smoke → open. Keep the previous image and a DB backup for 48 h; rollback = previous image + restore (never downgrade migrations in production unless documented safe).

## Evidence
Upgrade record: versions, migration duration, flag diff, assets diff summary, regression results (links), visual diffs accepted, performance parity, sign-off. Feeds the qa-test-strategy test report (E2E-08 parity).

## Anti-patterns
- Upgrading production before a metadata-DB copy rehearsal
- Accepting visual diffs wholesale
- Leaving removed feature flags in config (silently ignored, later surprises)
- Upgrading Superset and the embedded SDK/extensions at different times

Sources: apache/superset `UPDATING.md` and release notes; Superset docs — Upgrading Superset, Configuration; Preset 6.x release posts; `superset-architecture-deployment` skill (upgrade section); qa-test-strategy `nonfunctional-and-performance-testing` and `e2e-testing-data-platform` (E2E-08).
