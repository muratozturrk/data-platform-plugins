---
name: superset-test-harness
description: "Standing up a disposable, fully seeded Superset for automated tests: docker compose or Testcontainers stack (Superset web+worker+beat, PostgreSQL metadata, Redis, Trino single node, Keycloak test realm), test superset_config (feature flags, guest token secret, Talisman, OIDC), bootstrap (db upgrade, init, roles), seeding two institutions via REST/CLI (roles, DAR, RLS, datasets, dashboards from assets YAML), API client fixtures (login, CSRF, chart-data payload builder, guest-token minting), readiness, reset and teardown; plus a lighter 'metadata-only' harness for unit/contract tests. Use when writing or running any Superset integration, E2E or performance test."
---

# Superset Test Harness

## Two harness sizes
| Harness | Contents | Boot | Use |
|---|---|---|---|
| **Light** | `apache/superset:<pinned>` web only, SQLite or PostgreSQL metadata, no worker, `memory`/PostgreSQL datasource, DB auth with seeded users | ~60 s | contract tests, assets import tests, security headers, role/permission decision tests |
| **Full** | web + Celery worker + beat, PostgreSQL, Redis (cache, broker, GAQ), Trino single node with `memory`/`postgresql`/Iceberg-REST catalogs, Keycloak with `platform` test realm (two institutions, service clients), optional Ranger-equivalent file rules on Trino | ~3–4 min | RLS/mask consistency through Trino, guest tokens + embedded, Alerts & Reports, cache, E2E, k6 |
Both start from the platform's production image (`registry…/platform/superset:<ver>`) so drivers and the custom security manager are identical to prod.

## Test configuration (`superset_config_test.py`)
Start from the production config and override: `TESTING=True`, `WTF_CSRF_ENABLED=True` (tests must handle CSRF like clients do), `TALISMAN_ENABLED=True` with the host-app origin, `AUTH_TYPE=AUTH_DB` for the light harness / `AUTH_OAUTH` against the Keycloak container for the full one, `GUEST_TOKEN_JWT_SECRET` fixed test value, `GUEST_TOKEN_JWT_EXP_SECONDS=120` (lets expiry be tested), `FEATURE_FLAGS` identical to prod plus `EMBEDDED_SUPERSET`, `ALERT_REPORTS` with `ALERT_REPORTS_NOTIFICATION_DRY_RUN=False` and an SMTP sink (MailHog/Mailpit container), `WEBDRIVER_BASEURL` pointing at the web service, caches with short TTLs (`CACHE_DEFAULT_TIMEOUT=30`) so staleness tests run fast, `ROW_LIMIT=1000`, `SQLLAB_TIMEOUT=15`. Never `load_examples` in CI (slow, non-deterministic); seed your own fixtures.

## Bootstrap sequence
1. `superset db upgrade` → `superset init` → create admin via `superset fab create-admin` (light) or rely on OIDC service users (full).
2. Seed institutions: roles cloned from Gamma (`/api/v1/security/roles/` + permissions API) or via `superset-cli` role import; RLS via `import-rls`; databases/datasets/charts/dashboards via `superset-cli sync native fixtures/assets/{institution}` (same bundles as production, templated); register embedded dashboards (`POST /api/v1/dashboard/{id}/embedded`, allowed domains).
3. Load data fixtures into Trino `memory`/PostgreSQL (deterministic generator, see qa-test-strategy `e2e/fixtures_generator.py`) and `ANALYZE`.
4. Readiness: `GET /health` → 200, `GET /api/v1/dashboard/` as admin lists seeded dashboards, worker heartbeat via `celery inspect ping` for the full harness.

## API client fixture (pytest)
- Login: DB auth → `POST /api/v1/security/login` (`provider: db`) → bearer; CSRF → `GET /api/v1/security/csrf_token/` and header `X-CSRFToken` + session cookie for mutating calls; OIDC → client-credentials token from Keycloak used as `Authorization: Bearer` if the platform's security manager accepts JWTs, else Playwright login once and reuse `storageState`.
- Chart data: `GET /api/v1/chart/{id}?q=(columns:!(query_context))` → `json.loads(query_context)` → optionally patch `queries[0].filters`, `result_format` (`json`/`csv`), `result_type` (`full`/`samples`/`query`) → `POST /api/v1/chart/data`.
- Guest token: `POST /api/v1/security/guest_token/` as a user with `can_grant_guest_token` (`{"user": {...}, "resources": [{"type": "dashboard", "id": "<uuid>"}], "rls": [...]}`) or mint locally with the test secret; embedded calls carry `X-GuestToken` header.
- Reset between tests: truncate `logs`, clear caches (`superset cache` or Redis `FLUSHDB` on cache indices), reset dashboards from the assets bundle (`sync native --overwrite`); never rebuild the metadata DB per test.

## Seeding model (two institutions)
`t-acme` and `t-globex`: Keycloak groups `platform_{inst}_{analyst|engineer|pii-reader}`; Superset roles `{inst}_gamma`, `{inst}_alpha`; one database connection per institution to Trino (per-institution credentials); shared dataset `lake.gold_shared.events` with RLS `institution_id = '{inst}'`; `dim_customers` with `iban`/`email` masked for non `pii-reader`; one dashboard per institution + one shared template dashboard; embedded registration for the shared dashboard.

## Teardown and CI
Compose project per CI job id; `docker compose down -v` in `finally`; artifacts: Superset logs, worker logs, Playwright traces, k6 summary. Nightly job also runs the full harness against the **candidate** Superset image for upgrade regression.

## Anti-patterns
- `superset load_examples` as test data
- Disabling CSRF/Talisman "to make tests pass"
- Shared long-lived test Superset with manual edits
- Asserting via UI scraping what the chart-data API exposes directly

Sources: Superset docs — Configuration (feature flags, caches, Celery, Alerts & Reports, embedding), Security (guest tokens, `can_grant_guest_token`), REST API (`/api/v1/security/login`, `csrf_token`, `chart/data`, `dashboard/{id}/embedded`, `_openapi`); Developer Portal how-tos (`superset db upgrade`, `superset init`, `load_test_users` for upstream tests, `superset_test_config_*` pattern); preset-cli/superset-cli docs; Testcontainers Python (`DockerCompose`); qa-test-strategy harness patterns.
