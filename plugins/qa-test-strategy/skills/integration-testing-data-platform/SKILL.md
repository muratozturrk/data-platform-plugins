---
name: integration-testing-data-platform
description: How to write integration tests for the HKM platform against real open-source components: Testcontainers (PostgreSQL, Kafka + Karapace, Debezium Connect, Keycloak, Trino, Nessie, MinIO/S3-compatible, ClickHouse, OpenSearch), ephemeral Kubernetes namespaces per PR (kind/Argo CD ApplicationSets/vcluster), two-institution isolation tests, Ranger/Trino policy tests, contract tests (Karapace schemas, OpenAPI/schemathesis, SPI, data contracts), CDC correctness, pipeline step integration, idempotency and lineage checks. Use when writing or reviewing integration-level tests.
---

# Integration Testing on the Data Platform

Integration tests verify that components work together through their real interfaces: code → PostgreSQL, Debezium → Kafka → Karapace, Spark → Iceberg via Nessie, Trino → Ranger, control plane → Keycloak/OpenBao. Mocks lie most at this layer; Testcontainers and ephemeral namespaces replace them. They run on every PR (fast band) and nightly (full band).

## Two environments, two bands
| Band | Environment | Scope | Runtime |
|---|---|---|---|
| Fast (PR) | **Testcontainers** inside the CI runner (Docker): PostgreSQL, Kafka (KRaft) + Karapace, Debezium Connect, Keycloak, Nessie (in-memory store), MinIO or Ceph demo as S3, Trino (single node), ClickHouse, OpenSearch | component pairs/triples; CDC; catalog; policy; API contracts | ≤ 15 min |
| Full (nightly / pre-merge for infra) | **Ephemeral Kubernetes namespace per PR** created by Argo CD ApplicationSet Pull Request generator (or `kind` in CI, `vcluster` for API-level isolation) with the platform Helm umbrella at pinned versions, two seeded institutions, synthetic data | cross-component flows, isolation, Gateway routing, Ranger end to end, Flink/Spark on operators | ≤ 60 min |
Never run integration tests against shared staging with other people's state; never against production data (synthetic or anonymised fixtures only).

## Testcontainers patterns (Python, `testcontainers[...]` modules; Java for Flink/Debezium where the official extension exists)
- Session-scoped fixtures start containers once per test session; module-scoped reset state (truncate tables, delete topics/subjects, reset Nessie branch to a hash).
- Compose multi-container setups with `DockerCompose` or `Network` + `GenericContainer` (`ghcr.io/projectnessie/nessie`, `ghcr.io/aiven-open/karapace`, `quay.io/debezium/connect`, `trinodb/trino`, `clickhouse/clickhouse-server`, `quay.io/keycloak/keycloak --import-realm` with a test realm JSON).
- Debezium: official `debezium-testing-testcontainers` (Java) starts Kafka, Connect and the source DB; in Python use Connect's REST API (`PUT /connectors/{name}/config`) and a Kafka consumer to assert change events (op `c/u/d`, before/after, tombstone).
- Trino in a container with `memory`, `postgresql` and `iceberg` (REST/Nessie) catalogs; Ranger plugin configured with a file-based policy export for deterministic decisions, or `file` access control when only engine behaviour (not Ranger) is under test.
- Object storage: MinIO (dev image) is fine as an S3 stand-in in the fast band; the full band and release tests must run the S3-conformance suite against ObjectScale/Ceph (portability requirement).
- Readiness: wait on health endpoints (`/v1/info` Trino, `/q/health/ready` Nessie, Keycloak `/health/ready`, Connect `/connectors`); no sleeps.

## Test catalogue (map to inventory ids)
- **Isolation** (`T-*-001` family): two institutions (`t-acme`, `t-globex`) seeded via the control plane; every component gets the same test shape — as A list/read/write own resources succeeds; B's fail with 403/AccessDenied; audit record written (Ranger → OpenSearch).
- **Contract tests**: Karapace compatibility (register v1, attempt incompatible v2 → 409; compatible v2 → 200); OpenAPI vs implementation with `schemathesis` (WSO2/FastAPI/control plane); Kafka message contracts with consumer-driven tests (Pact-style JSON fixtures per topic version); SPI contract suites run against every plugin; **data contracts** with `datacontract-cli` (`datacontract test` against the delivered table/schema).
- **CDC**: insert/update/delete/tombstone through Debezium → topic → Flink/Spark sink → Iceberg table via Nessie; snapshot→streaming hand-off without gaps/duplicates; schema change event propagation.
- **Catalog**: Nessie branch create/commit/merge via Spark and Trino; CEL rules deny cross-institution refs; S3 signing/vending scope.
- **Pipeline step integration**: Airflow `dag.test()` on a small DAG against containers; Spark job reading Kafka/Postgres and writing Iceberg; dbt `build` against Trino container with seeds; Great Expectations/Soda suites executed against the produced tables.
- **Idempotency**: run a step twice with the same input → identical table state (row count, checksum of sorted rows, snapshot count +1 or unchanged).
- **Lineage and observability**: OpenLineage events received by a Marquez container for Airflow/dbt/Spark/Trino runs; metrics/logs carry institution and run_id.
- **Security plumbing**: Keycloak token without mapped group → denied everywhere; OpenBao (dev container) path isolation; External Secrets sync into the namespace (full band).

## Rules
1. Real interfaces, fake data: every integration test owns its fixtures (seed SQL, Avro/JSON fixtures, Iceberg seed tables) and cleans up.
2. Independent and parallelisable: unique names per test (`{prefix}_{uuid}` topics, branches, schemas); no shared mutable state between tests.
3. Assertions on observable outputs: rows in the sink table, events on the topic, decision in the audit log, HTTP status + body schema — never on logs or internal calls.
4. Version pins match production images; a compatibility matrix test runs nightly with the next engine versions.
5. Flake policy: a test that flakes twice is quarantined with an issue and a 2-week deadline; quarantined tests never gate merges silently.
6. Teardown is part of the test: namespace deletion cascades (full band); container stop in fixtures (fast band); nightly sweeper deletes leaked PR namespaces older than TTL.

## Patterns
- `conftest.py` with layered fixtures: `network` → `postgres` → `kafka` → `karapace` → `debezium` → `nessie` → `trino` → `institution_a/b` (tokens); markers `integration`, `slow`, `k8s`.
- Argo CD `ApplicationSet` with `pullRequest` generator → `Application` per PR deploying `platform-umbrella` chart into `pr-{number}` namespace with `values-test.yaml` (two institutions, small resources); CI job waits for sync/health then runs the `k8s` marker tests; namespace removed on PR close.
- Contract-first: schemas (Avro/JSON Schema), OpenAPI and `datacontract.yaml` live in Git; CI fails on incompatible change before any container test runs.

## Anti-patterns
- Integration tests on a shared long-lived cluster ("it passed on staging yesterday")
- Sleeping instead of waiting on readiness
- Asserting on mocked Kafka/DB instead of containers at this layer
- One giant test that exercises everything (that is E2E, and it hides which integration broke)

Sources: Testcontainers Python docs and qaskills.sh "Testcontainers Python: Real Integration Tests with Docker" (2026; fixtures, pyramid middle band); Debezium "Integration Testing with Testcontainers" (DebeziumContainer with Kafka Connect and source DB); OneUptime "How to Write Integration Tests with Testcontainers" (2026); Argo CD ApplicationSet Pull Request generator docs; Qovery/InstaDevOps/helpmetest comparisons of per-PR ephemeral Kubernetes environments (namespace-per-PR, vcluster, never production data); testfiesta "Testing Pyramid" (teardown as the main source of flaky integration suites); datacontract-cli docs; schemathesis docs; Karapace compatibility API; *Data Pipelines with Apache Airflow* 2e ch. 10 (containers for external systems, dag.test()).
