---
name: superset-testing-strategy
description: "Test strategy for Superset as a platform component (not for contributing to Superset itself): static checks (config lint, assets YAML schema/UUID), unit tests (superset_config logic, custom security manager, guest-token service, Jinja macros), contract tests (REST API OpenAPI via /api/v1/_openapi + schemathesis, chart-data payload), integration tests (disposable Superset + Trino/PostgreSQL + Keycloak: roles/DAR/RLS/guest tokens/embedded/exports, assets import idempotency, Alerts & Reports), E2E Playwright journeys, performance (k6 chart-data and dashboard load), visual regression, security headers, upgrade regression; mapped to qa-test-strategy inventory ids T-bi-*. Use when planning, writing or reviewing any test that touches Superset."
---

# Superset Testing Strategy

Superset is the last hop before a human sees data, so its tests must prove three things no other component can: the **same policy produces the same masked/filtered output on screen, in exports and in embedded panels**; **assets delivered as code land unchanged and idempotently**; **dashboards stay within load budgets**. Everything else (chart logic, Superset internals) is upstream's job — Superset itself ships a pyramid of pytest unit/integration tests, Jest + React Testing Library and Playwright (replacing Cypress), plus per-PR ephemeral environments at `pr-{N}.superset.apache.org`; we reuse their conventions, not their suites.

## Pyramid for Superset-as-component
| Level | What | Tools (OSS) | Where it runs | Inventory |
|---|---|---|---|---|
| Static | `superset_config.py` lint (forbidden flags in prod, secrets via env, Talisman on, public role empty); assets bundle schema + UUID stability + naming + no credentials; dashboard lint (Few rules) | ruff, custom validators (`assets_validate.py`, `dashboard_lint.py`), gitleaks | PR | T-bi-002, T-sec-004 |
| Unit | config helpers, `PlatformSecurityManager` claim mapping, guest-token service (RLS derivation, TTL, audience), chart-data payload builder, Jinja macros allow-list | pytest, freezegun, PyJWT | PR | T-bi-001 (unit part) |
| Contract | REST API: fetch `/api/v1/_openapi` from the pinned version, run `schemathesis` against the harness; chart-data payload/response contract (`result[].rowcount`, `data`, `query`), guest-token API; embedded SDK ↔ `/embedded/{uuid}` | schemathesis, pytest | PR (fast harness) | T-api-001-like, new T-bi-010 |
| Integration | roles/DAR/RLS/dashboard RBAC decisions; **mask/filter consistency across chart-data, CSV export (`/api/v1/chart/data` with `result_format=csv`), SQL Lab, embedded guest token**; guest-token scope (dashboard/chart UUID); assets import idempotency (UUIDs, no duplicates, `is_managed_externally`); Alerts & Reports execution; cache behaviour (TTL, key prefix per institution); Keycloak login + group→role sync | pytest + Testcontainers/Compose harness (Superset + PostgreSQL + Redis + Trino + Keycloak) | PR fast band / nightly | T-bi-001, T-bi-002, T-bi-005, T-cache-001, T-trino-002 |
| E2E | Keycloak login → dashboard → native filter → cross-filter → drill-to-detail → CSV export (masked) → embedded view in host app; report delivered and archived with `rel-` tag | Playwright (`@playwright/test`), host-app stub | nightly / release | T-bi-004, E2E-10, E2E-07 |
| Performance | chart-data p95 per dashboard/institution, dashboard full load, concurrent users per tier, cache hit ratio; warehouse protection (row limits, timeouts) | k6 (HTTP), Playwright trace for TTI, Prometheus StatsD metrics | nightly / release | T-bi-003 |
| Visual regression | screenshot diff of production dashboards on fixture data, light and dark theme, 3 viewports | Playwright `toHaveScreenshot` with masks for timestamps | nightly | new T-bi-011 |
| Security | CSP/frame-ancestors, CSRF, session cookie flags, `/api/v1/security/guest_token/` requires permission, public role has no datasets, SQL Lab guardrails, export permissions | pytest + httpx, OWASP ZAP baseline | PR / release | T-bi-001, T-sec-003 |
| Upgrade regression | `UPDATING.md` breaking-change checklist, feature-flag diff, assets re-import, embedded SDK version, extension/MCP compatibility, smoke on staging copy of the metadata DB | `upgrade_check.py`, full integration + E2E tier on candidate image | per upgrade | T-bi-002, E2E-08 |

## Rules
1. **Oracle outside Superset**: expected chart values come from a Trino query on the fixture (or the fixture generator), never from another Superset call.
2. **Policy consistency is one test**: for every masked column and RLS rule, assert equality of the visible rows/values across chart-data JSON, CSV export, SQL Lab (if allowed) and embedded guest token — a difference is a Blocker.
3. **Two institutions always**: every integration test seeds `t-acme` and `t-globex`; negative assertions are mandatory (403/empty).
4. **Idempotency**: `superset-cli sync native` twice → identical asset UUIDs, counts and `changed_on` only on first run.
5. **Chart-data over UI**: integration assertions use `POST /api/v1/chart/data` with the chart's `query_context` (fetch via `GET /api/v1/chart/{id}?q=(columns:!(query_context))`); UI only in E2E.
6. **Version-pinned contract**: the OpenAPI spec and the embedded SDK version are stored per Superset version; upgrade = regenerate and diff.
7. **Budgets are numbers**: dashboard p95 < 5 s cached / < 15 s cold; chart-data p95 < 3 s; k6 thresholds fail the job.
8. **Flake control**: Playwright retries ≤ 1 with trace on retry; quarantine list with issue ids; visual diffs reviewed, not auto-approved.

## What not to test here
Chart rendering correctness, ECharts options, Superset's own migrations — covered upstream (`pytest tests/unit_tests`, `tests/integration_tests`, Jest, Playwright specs under `superset-frontend/playwright`). If a platform bug needs an upstream fix, write the test in Superset's repo following `superset-internals-contributing`.

## Mapping to qa-test-strategy
Inventory areas `superset`/`bi` (T-bi-*), plus cross-cutting `T-trino-002` (mask consistency), `T-cache-001`, `T-sec-*`, journeys E2E-07 (report release) and E2E-10 (BI UI journey). New ids proposed by this skill: T-bi-010 (API contract), T-bi-011 (visual regression), T-bi-012 (upgrade regression), T-bi-013 (security headers), T-bi-014 (guest-token scope per chart/dashboard UUID), T-bi-015 (export mask consistency).

Sources: Superset Developer Portal — Testing overview (pyramid; pytest unit/integration; Jest + RTL; Playwright replacing Cypress; Storybook), Contributing how-tos (`pytest tests/unit_tests`, `tests/integration_tests`, `npm run playwright:test`, ephemeral PR environments); apache/superset `tests/integration_tests/charts/data/api_tests.py` (chart-data API fixtures, `ROW_LIMIT`/`SQL_MAX_ROW` patching, `rowcount` assertions); PRs #43769 (embedded chart guest token scoped by embed UUID, 403 across charts), #40712 (CSV export with guest token via chart data API), #41458 (native-filter E2E migrated to Playwright); qa-test-strategy plugin skills (unit/integration/e2e for the data platform).
