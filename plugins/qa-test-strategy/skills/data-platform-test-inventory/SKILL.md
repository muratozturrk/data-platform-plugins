---
name: data-platform-test-inventory
description: The test taxonomy for the HKM data platform stack, by layer: ingestion/CDC (Kafka, Kafka Connect, Debezium, Karapace, SeaTunnel, dlt, NiFi, Camel, RabbitMQ), stream/batch processing (Flink, Spark on Kubernetes, dbt), lakehouse (Iceberg + Nessie, object storage with WORM/Object Lock, ClickHouse, PostgreSQL/PostGIS/TimescaleDB, Trino + Gateway), data quality and observability (Great Expectations, Soda Core, dbt tests, Deequ, Elementary, ydata-profiling), catalog/lineage (OpenMetadata, OpenLineage + Marquez), security and privacy (Keycloak, Ranger, OpenBao + ESO, OPA retention policies, Fides, Presidio, ARX, HMAC pseudonymisation, immudb, OpenSearch), serving/BI (Superset, Grafana, Redis/Valkey, Cube/MetricFlow), open-data portal and APIs (CKAN, Flowable, WSO2 API Manager, FastAPI, Matomo), operations (Airflow, Prometheus/Alertmanager/Grafana OnCall, Velero/restic/pgBackRest, Archivematica/BagIt), spatial (Sedona, H3). Use when generating a test inventory or checking a component's test completeness.
---

# HKM Data Platform Test Inventory (taxonomy)

Machine-readable form: `examples/test-inventory.yaml`. Ids `T-{area}-{nnn}`. Areas (from the technology list): `cp` control plane/orchestration, `ing` ingestion & connectors, `cdc`, `kafka`, `flink`, `spark`, `dbt`, `lake` (Iceberg/Nessie/object storage), `ch` ClickHouse, `ods` PostgreSQL/PostGIS/Timescale, `trino`, `dq` quality, `obs` data observability, `cat` catalog/lineage, `iam` Keycloak, `ranger`, `sec` secrets/OpenBao, `pol` OPA retention policies, `priv` privacy/anonymisation/pseudonymisation, `audit` immudb/OpenSearch, `bi` Superset/Grafana, `sem` semantic layer, `portal` CKAN/Flowable, `api` WSO2/FastAPI, `geo`, `bak` backup/preservation, `alert`, `k8s`, `perf`, `dr`.

## Cross-cutting (every component)
| Test | Level | Suite | Tool / assertion |
|---|---|---|---|
| Two-tenant / two-institution isolation (read, write, list, export) | integration | security, regression | pytest as org A/B; cross access denied in Ranger audit |
| Quota and limit enforcement (group quotas, resource groups, Kafka quotas) | integration | security, performance | exceed → 429/queued/throttled |
| AuthN matrix (OIDC human, service token, expired, no-group) | integration | security, api | 401/403 table |
| Secrets only via OpenBao/ESO; none in config, logs, images | static | security | gitleaks, Trivy, log scan |
| Config/manifest/CR validation | contract | smoke, regression-fast | JSON Schema, kubeconform, conftest |
| Observability contract (metrics/logs carry institution, dataset, run_id) | integration | regression | Prometheus/Loki assertions |
| Monthly engine-upgrade compatibility | system | upgrade | regression tier on new version |
| Backup/restore and failover per stateful component | system | dr | Velero/pgBackRest/restic drill, RTO/RPO |

## Ingestion, connectors, CDC, streaming
- **Kafka (Strimzi)**: topic naming/ACL/quota per institution; retention/compaction; broker loss (chaos) within lag SLO; transactions and idempotent producers; DLQ topics receive poison messages with reason headers; replay from offset produces identical downstream state; kafka-exporter lag metrics.
- **Karapace schema registry**: BACKWARD/FORWARD/FULL compatibility enforced at the gate; incompatible schema rejected with clear error; Confluent-API drop-in compatibility tests for clients.
- **Debezium CDC**: insert/update/delete/tombstone propagation with before/after images; initial snapshot → streaming hand-off without gaps or duplicates; ordering per key; schema change events.
- **Kafka Connect / SeaTunnel / dlt / NiFi**: connector contract tests (config-only onboarding), incremental/paginated API pulls, push-pull-poll patterns, DLQ routing, idempotent re-runs, backpressure behaviour.
- **Apache Camel**: message translation to the canonical model (golden-file), protocol bridging (file/DB/RPC/messaging), error channel routing.
- **RabbitMQ**: queue durability, ack/nack/requeue, DLX routing, consumer failover.
- **Flink**: event-time/watermark correctness with late data, stateful dedup, checkpoint/savepoint recovery with exactly-once end-to-end, state TTL, job restart from savepoint, throughput/latency budgets.

## Processing and transformation
- **Spark on Kubernetes**: transform unit tests (chispa), property tests (`spark-property-based-testing` plugin), schema contracts, broadcast-join enrichment correctness, parallel JDBC extraction partitioning, idempotent reruns, namespace resource quotas, operator CR validation, volume-tier performance.
- **dbt**: parse/compile, built-in and custom tests, contracts, incremental idempotency (run twice), snapshots, macro unit tests, docs/exposures/lineage, MetricFlow metric definitions (if used).

## Lakehouse and storage
- **Iceberg + Nessie**: ACID/atomic partition overwrite, MERGE idempotency, snapshot isolation under concurrent writers, time travel and rollback, compaction/expire/VACUUM correctness, copy-on-write purge (right-to-erasure) leaves no readable copies in snapshots, Nessie branches (multi-table transactions, branch → validate → merge), catalog outage behaviour.
- **Object storage (Dell ObjectScale primary; Ceph RGW / MinIO AIStor alternates)**: S3 API conformance for all engines, versioning, lifecycle/tiering rules, Object Lock WORM (delete/overwrite rejected during retention, legal hold), erasure coding/SED verified by ops, ransomware-resilience drill, presigned URL TTL expiry; same test suite against the alternate backend (portability).
- **ClickHouse**: institution DB/role/quota/row policy; incremental materialized views equal batch recomputation; ingestion throughput; serving latency budgets; replication/failover; backup/restore; TimescaleDB/time-series query correctness where used.
- **PostgreSQL ODS + PostGIS**: point-query latency, near-real-time freshness, spatial query correctness (PostGIS vs Sedona cross-check), pgBackRest restore drill, connection pooling under load.
- **Trino + Trino Gateway**: SQL regression per catalog; Ranger policy enforcement (allow/deny/row filter/column mask) identical across CLI, JDBC, Superset, export; resource groups queueing; Gateway routing by client tags and health; FTE job completes after worker kill; connector pushdown asserted via EXPLAIN; TPC-DS subset budgets; Alluxio cache hit (optional); upgrade matrix.

## Data quality and observability
- **Rule-based quality (GE / Soda Core / dbt tests)**: the 10 rule types per dimension (completeness, uniqueness, validity, accuracy, consistency, timeliness, integrity, conformity, precision, reasonableness — as defined in the project) each with a passing and a failing fixture; failed-rows capture; acceptance thresholds; parametric rule templates (dbt macros / GE custom expectations) unit-tested; rule results land in OpenMetadata.
- **Deequ (Spark)**: distributed metrics equal single-node reference on a sample; sampling-based evaluation within tolerance; metric repository persistence.
- **Elementary**: table existence, schema change detection (add/remove field) and notification to the declaring party, freshness/staleness trend, volume anomalies with derived expected ranges, alert integrations fire once per incident; Elementary reports exported on schedule.
- **ydata-profiling / OpenMetadata profiler**: profile statistics match reference; rules generated from profiles are valid and reviewable.
- **Alert clustering (custom item 004.2.1)**: lineage + time-correlated anomalies merged into one alert; golden scenarios (upstream break → N downstream alerts → 1 cluster); no false merges across unrelated lineage.

## Catalog, lineage, governance
- **OpenMetadata**: ingestion connectors run; every manifest asset registered with owner/steward/technical-owner; classification and PII tags; KVKK status and lifecycle state displayed; glossary terms; custom column-level attributes (access-class declarations, basis document refs, field inventory open/closed/to-transform); change history; API contract.
- **OpenLineage + Marquez**: table- and column-level lineage emitted by Airflow, dbt, Spark, Flink, Trino; break-point localisation (which stage introduced the failure); downstream impact set correct for a changed column; Marquez API contract.
- **OPA retention policy engine + Git**: policy unit tests (`opa test`), "most restrictive wins" conflict resolution golden cases, hot reload picks new policy without restart, versioned bundle diff in CI.
- **Fides**: data map inventory complete vs catalog; DSR (KVKK art. 7/11) orchestration end-to-end (request → locate → erase/export → evidence); purpose/legal-basis metadata validation.
- **Flowable/Camunda**: access-request workflow (form → evaluation → approval → conditional approval → closure), role-based completion, mandatory condition acceptance, decision audit trail, notifications; BPMN golden tests.

## Security and privacy
- **Keycloak**: realm/organization provisioning, groups (citizen/institution/researcher/developer), RBAC roles and ABAC attributes (institution, region) present in tokens, group→quota and group→dataset mappings, SSO for Superset/Grafana/CKAN/WSO2, access revoked after deletion.
- **Ranger**: policies for table/column/row, masking consistency across screen/export/embedded outputs, tag-based policies (purpose limitation), audit records complete in OpenSearch, policy sync latency.
- **OpenBao + External Secrets Operator**: path isolation per institution/component, rotation propagates to pods, lease expiry, key scope per subject/entity, crypto-shredding (key destroyed → data unreadable in immutable storage and backups) proven on a fixture.
- **Presidio (TR recognizers) + ARX/Amnesia**: PII detection precision/recall on a labelled Turkish corpus (TCKN, phone, IBAN, address); declaration mismatch generated, never auto-changes the declaration; k-anonymity/l-diversity levels achieved and re-identification risk below threshold; masking methods parametric and versioned per release manifest; unprocessed field blocked from sharing.
- **HMAC pseudonymisation**: deterministic within scope, consistent across datasets with shared key scope, distinct across contexts (cluster-id salted), internal/external token spaces disjoint, keys from OpenBao only.
- **OpenDP / PipelineDP / H3 geo-fuzzing**: epsilon budgets enforced, aggregate utility within tolerance, H3 resolution policy applied, no exact coordinates leak.
- **immudb / OpenSearch WORM audit**: cryptographic verification of audit trail; tamper attempt detected; retention-period immutability; crypto-shredding records (key id, scope, responsible) present.

## Serving, BI, semantic layer
- **Superset**: role/DAR/RLS per institution; guest-token scope; assets YAML sync idempotent; dashboards load within budget; critical UI journeys (Playwright: Keycloak login, filter, cross-filter, drill, export); Alerts & Reports deliver on schedule; embedded/anonymous panels respect CSP and RLS; report archive object has Iceberg snapshot reference.
- **Grafana (ops dashboards)**: ClickHouse/ODS datasources, threshold alerts, annotations; OnCall routing.
- **Redis/Valkey cache**: TTL respected, cache key includes institution, no cross-tenant cache hits; Valkey drop-in test.
- **Semantic layer (Superset metrics; Cube / MetricFlow)**: KPI definitions produce identical results across Superset, Cube API and dbt; metric change → version bump → consumers notified.
- **Spatial (Sedona, PostGIS, H3)**: spatial joins/indexes correct vs reference, map-layer feeds valid GeoJSON/Parquet, H3 resolution policy.

## Open-data portal, APIs, distribution
- **CKAN (+scheming, dcat)**: mandatory/category-specific metadata schemas enforced, draft→publish workflow, three visibility classes (public/restricted/private) enforced in UI, API and Solr search facets, DCAT export valid, licence management, Datastore API.
- **WSO2 API Manager + FastAPI**: OpenAPI contract tests; content negotiation (JSON/CSV); group-based tiered quotas → 429 with policy actions; developer portal self-service keys/quota view; token validation against Keycloak.
- **Downloads and bulk transfer**: presigned TTL expiry, encrypted delivery, version-tagged outputs, asynchronous bulk export (Celery/Airflow + Trino/Spark CTAS + GDAL/Sedona) produces valid Parquet/GeoJSON; pre-share transformation manifest applied; unprocessed field blocked.
- **Matomo + WSO2 analytics**: view/download/call events recorded with institution/time breakdown; unique user/institution counts; privacy settings (IP anonymisation).

## Operations, backup, preservation, alerting
- **Airflow**: generated DAGs import; Datasets (readiness markers) trigger downstream; fail-stop on quality failure; retries/backfill; expiry checks (sharing/contract end); periodic erasure cycles and archive readability tests; OpenLineage emission; scheduler HA.
- **Prometheus / Alertmanager / Grafana OnCall / Jira-GitLab**: alert routing by labels (channel, priority), grouping, suppression/inhibition, on-call escalation, ticket creation; retention-overrun, erasure-failure, archive-test alarms fire on fixtures.
- **Velero / restic-Borg / pgBackRest**: 3-2-1 immutable backups, retention/rotation, automated restore drills, restore of crypto-shredded data is unreadable (expected).
- **Archivematica / BagIt**: OAIS-compliant package creation, fixity manifests verify, long-term readability test of archived formats.
- **Kubernetes**: Helm/CR lint, Gatekeeper/Kyverno policies, network policies between institution namespaces, PDB/autoscaling behaviour, storage lifecycle, cert rotation.

## Suite mapping defaults
Smoke: control-plane API up, Keycloak login, one pipeline, one Trino query via Gateway, one Superset dashboard, CKAN home. Regression-fast (PR): unit, contract, golden, dbt parse/tests, OPA/Ranger policy unit tests, manifest lint, PII recogniser regression corpus. Regression-full (nightly): integration, isolation, data-quality rules on staging data, lineage checks, Superset/CKAN smoke, engine SQL suites, Flink savepoint restore. Release: full + performance + security + privacy (anonymisation metrics) + upgrade matrix + DR drill (quarterly) + WORM/crypto-shredding evidence.
