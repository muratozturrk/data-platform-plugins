---
name: e2e-testing-data-platform
description: How to design and run end-to-end tests on the HKM platform: golden-path journeys (institution onboarding → pipeline → gold → Superset; CDC → Flink → ClickHouse serving; DSR erasure → GC → evidence; CKAN publish → API download), parallel-run/regression comparison against expected business results, synthetic test data lifecycle, Playwright UI journeys, timing/polling, environment strategy (ephemeral namespace or dedicated E2E environment), flakiness control, scheduling and release gating. Use when writing or reviewing E2E tests or deciding what deserves E2E coverage.
---

# End-to-End Testing on the Data Platform

E2E tests prove that the platform delivers a business-visible result for a complete journey — not that a component works. They are few, slow, expensive and the only tests that catch hand-off failures (orchestration, lineage, policy propagation, readiness markers). Keep them at the top of the pyramid: ≤ 10–15 journeys, run nightly on the ephemeral/E2E environment and as a release gate.

## Journeys worth E2E (platform golden paths)
| Id | Journey | Observable success |
|---|---|---|
| E2E-01 Onboarding | `Tenant` manifest applied → Keycloak org/groups, OpenBao paths, Nessie branch, Kafka prefix/quota, Ranger zone, ClickHouse DB, Superset workspace, CKAN org exist; user of the institution logs in and sees only its assets | all `status.conditions` Ready; isolation checks pass |
| E2E-02 Batch pipeline | `Pipeline` manifest with source (PostgreSQL fixture) → landing → silver → gold on the institution branch via WAP → merge → Superset dashboard shows expected KPI | KPI value equals expected from fixture; lineage visible in Marquez/OpenMetadata; Elementary freshness green |
| E2E-03 CDC stream | insert/update/delete in source → Debezium → Kafka → Flink → Iceberg (Nessie) and ClickHouse serving → Grafana panel | serving table reflects final state within latency budget; tombstones applied |
| E2E-04 Quality gate | pipeline run with injected bad records → quality rule fails → run stops (fail-stop) → alert routed → DLQ/quarantine populated → no partial publish on `main` | `main` hash unchanged; alert with correct labels in Alertmanager; quarantine count |
| E2E-05 Erasure (KVKK DSR) | DSR created in Fides → rows deleted/rewritten → snapshots expired → Nessie GC → verification scan clean → crypto-shred record in immudb → DSR closed | subject key absent in all reachable snapshots; evidence record verifiable |
| E2E-06 Open-data publish | dataset passes pre-share transformation (Presidio/masking manifest) → CKAN draft → approval workflow (Flowable) → published → API (WSO2) download with quota → Matomo event | downloaded file matches transformed fixture; quota 429 after limit; analytics event present |
| E2E-07 Report release | scheduled Superset report → `rel-` tag in Nessie → archived report in WORM bucket with snapshot reference | report object + tag hash recorded; Object Lock prevents delete |
| E2E-08 Engine upgrade regression | run E2E-02/03 on the candidate engine versions | parity with baseline results |
| E2E-09 DR drill (quarterly, manual-assisted) | restore Nessie PostgreSQL + lake metadata from backup into a fresh namespace → E2E-02 passes | RTO/RPO measured |
| E2E-10 BI UI journey | Playwright: Keycloak login → dashboard → native filter → cross-filter → drill-to-detail → export CSV (masked) | screenshots/asserts; masked columns in export |

## Design rules
1. **Business-level assertions**: expected KPI values, row counts and checksums computed from the fixture by an independent oracle (SQL on the fixture or a tiny Python model), never by the pipeline code.
2. **Deterministic synthetic data**: fixture generator (Faker/mimesis with seed, schema-driven) produces institution-tagged datasets small enough to run in minutes but covering edge cases (late events, duplicates, nulls, PII patterns incl. TR identifiers); fixtures versioned in Git with expected-result files; no production data, ever.
3. **Poll, don't sleep**: wait for observable conditions (Airflow Dataset event, Nessie commit hash advanced, ClickHouse row count, Alertmanager API) with timeouts tied to SLAs; report the elapsed time as a metric.
4. **Isolation per run**: unique institution ids or branches per E2E run (`t-e2e-{run}`) so parallel runs and reruns do not collide; cleanup via the control plane's delete path (which is itself under test in E2E-01).
5. **Observability as assertion**: every journey asserts lineage edges, audit records and metrics labels in addition to data — the hand-offs are the point.
6. **Parallel-run regression**: for engine/version upgrades and refactors, run the same fixtures through old and new paths and diff results (row-level diff tool on Iceberg snapshots or Trino `EXCEPT` queries).
7. **Budget**: full E2E suite ≤ 90 minutes; each journey has an owner, a runbook for failure triage and a link to the inventory id.

## Environment strategy
- Default: ephemeral namespace per PR for infra/control-plane changes (Argo CD ApplicationSet), with the platform umbrella chart, two seeded institutions and small resource profiles; nightly on a persistent `e2e` namespace recreated from scratch every night (so drift is impossible).
- E2E needs real engines (Flink/Spark operators, Trino + Gateway, Ranger, Nessie on PostgreSQL, ClickHouse, Superset, CKAN, WSO2); Testcontainers are not enough at this layer.
- Object storage: ObjectScale test tenant or Ceph RGW dev cluster with Object Lock enabled for E2E-05/07; MinIO only for local developer runs.

## Scheduling and gates
PR: none (E2E too slow), except smoke E2E-02 when manifests/compiler change. Nightly: all except DR. Release: all + upgrade regression; **go/no-go requires E2E-01, 02, 04, 05 green** (isolation, correctness, fail-stop, erasure). Quarterly: DR drill.

## Flakiness control
Classify failures: product defect / test defect / environment; auto-retry once only for environment class; quarantine with issue; track flaky rate (< 2%); keep traces/logs/screenshots per run; seed and run id in every artifact name.

## Anti-patterns
- E2E suite as the only safety net (slow feedback, blame-shifting)
- Assertions on "job succeeded" instead of produced data
- Shared E2E environment that is never rebuilt
- Hard-coded waits; tests that pass because the previous run left data

Sources: reintech "Data Pipeline Testing Strategies: Unit, Integration, and E2E" (E2E validates delivered result; test data cleanup; unit per commit / integration per PR / E2E before deployment); dataworkers.io "Data Pipeline Best Practices 2026" (E2E as parallel-run comparison and regression suites weekly/per release); dataengineeringcompanies.com "10 Actionable Data Pipeline Testing Best Practices for 2026" (regression automation, lineage/impact analysis, snapshot/idempotency, chaos, observability in the suite); helpmetest "Ephemeral Test Environments" (per-PR stacks, no test pollution, parallel suites); Superset Developer Portal testing overview (Playwright E2E); Faker/mimesis docs; Argo CD ApplicationSet docs.
