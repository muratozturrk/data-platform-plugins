---
name: test-suite-taxonomy
description: Test levels, types, methods and suites for the platform: unit/contract/integration/system/e2e/acceptance levels; functional vs non-functional; white/black/grey box; manual vs automated; suites by purpose (smoke, sanity, regression, UI, performance, API, security, data-quality) and how test cases map into multiple suites. Use when naming, organizing or selecting suites for a run.
---

# Test Suite Taxonomy

## Dimensions (orthogonal)
| Dimension | Values |
|---|---|
| Level | unit, contract (schema/API/SPI/manifest), component, integration, system, end-to-end, acceptance |
| Type | functional (what it does), non-functional (how it works: performance, security, reliability, availability, scalability, capacity, compatibility, maintainability, usability, accessibility) |
| Method | white box (structural, internal boundaries/data domains), black box (interface only), grey box (interface + access to DB/design docs) |
| Execution | manual (exploratory, usability, drills), automated (repetitive, large-scale; needs tool investment and expertise) |
| Suite | smoke, sanity, regression, UI, API, performance, security, data-quality, upgrade/compatibility, DR |

## Suites (purpose, not type)
- **Smoke**: do basic functions work? Runs on every deploy/environment creation; < 10 minutes; one case per critical path (API up, auth, catalog listing, one pipeline run, one dashboard load).
- **Sanity**: did the change fix the issue without breaking neighbours? Targeted subset per change area.
- **Regression**: did changes break existing functionality? Destination of all automated cases; nightly and pre-release; tiered (fast tier per PR, full tier nightly).
- **UI**: user interactions through all layers (Superset, platform portal); Playwright on critical journeys.
- **API**: functionality, reliability, performance, security of APIs (control plane REST/CLI/SDK, Trino, Superset, catalog REST).
- **Performance**: behaviour under workload (load, stress, endurance, volume, spike, scalability) with thresholds.
- **Security**: authn/authz, isolation, secrets, policy tests, dependency/image scanning, pen-test findings.
- **Data-quality**: contract/schema, volume, uniqueness, integrity, freshness, business rules, drift — per dataset, runs with pipelines.
- **Upgrade/compatibility**: engine version matrix (Trino, Spark, Kafka, Superset, Airflow…) against the regression tier.
- **DR/operations**: backup/restore, failover, runbook drills.

A case may belong to several suites (a tenant-isolation API test is in regression, API and security); a suite has one purpose.

## Naming (per naming-conventions)
Suite ids `S-{purpose}`; case ids `T-{area}-{nnn}`; areas: `cp` control plane, `dq` data quality, `pipe` pipelines, `kafka`, `spark`, `trino`, `ch` ClickHouse, `af` Airflow, `dbt`, `iam` Keycloak/OPA, `vault`, `cat` catalog/OpenMetadata, `bi` Superset, `k8s`, `sec`, `perf`, `dr`.

## Anti-patterns
- Suites organized by team or tool instead of purpose
- Smoke suite that takes an hour
- "Regression" that is just all tests with no tiers
- Non-functional tests without numeric thresholds
