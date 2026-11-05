---
name: nonfunctional-and-performance-testing
description: Non-functional testing for the platform: the functional vs non-functional distinction, the five-step non-functional process (planning, preparation, execution, record, analysis/improvement), types (compatibility, performance, capacity, security, reliability/availability, scalability, maintainability, usability, accessibility), performance test kinds (load, stress, endurance, volume, spike, scalability) with platform targets and tools (k6, JMeter, Locust, Trino/Kafka/Spark benchmarks). Use when defining non-functional tests or budgets.
---

# Non-Functional and Performance Testing

## Functional vs non-functional
| | Functional | Non-functional |
|---|---|---|
| Verifies | what the product does | how the product works |
| Order | before | after functional |
| Basis | customer requirements | customer expectations |
| Example | login page shows username/password fields | dashboard loads in 2 s |
| Process | plan → create cases → execute → compare actual vs expected | plan → prepare → execute → record → analyse and improve |

## Types and platform targets (set numbers in the test plan)
| Type | Platform scope | Example budget |
|---|---|---|
| Performance | Trino query latency, ClickHouse serving latency, Superset dashboard load, pipeline throughput/latency, API response | Trino p95 interactive < 3 s on gold; Superset p95 < 5 s cached; API p95 < 300 ms |
| Capacity | max tenants/pipelines/concurrent queries per cluster | 50 tenants, 20 concurrent queries/tenant |
| Scalability | throughput vs workers/partitions; autoscaling behaviour | linear to 2× workers ± 20% |
| Reliability / availability | failover (coordinator, broker, control plane), FTE retries, SLO attainment | 99.9% monthly; worker loss → ETL completes |
| Security | authn/authz, isolation, secrets, policy, dependency/image scanning, pen test | zero cross-tenant reads; no critical CVEs |
| Compatibility | engine version matrix, clients (JDBC/ODBC/Python), browsers for Superset | monthly matrix green |
| Maintainability | upgrade time, config drift detection, test suite runtime | engine upgrade < 1 day |
| Usability / accessibility | portal and Superset journeys, WCAG 2.2 AA | task completion by new user ≤ 15 min |

## Performance test kinds
- **Load**: expected load (e.g. 200 concurrent BI users) → latency/error budgets.
- **Stress**: beyond expected until breaking point → identify limits and failure mode (queueing vs errors).
- **Endurance (soak)**: expected load for hours/days → leaks, metadata growth, cache behaviour.
- **Volume**: varying data volumes (10×, 100× tables) → pipeline runtime, Trino scan, ClickHouse ingestion.
- **Spike**: sudden surges (month-end dashboards, Kafka bursts) → autoscaling, backpressure, quotas.
- **Scalability**: effect of adding workers/partitions/replicas → capacity planning.

## Tooling
k6 or JMeter/LoadRunner for HTTP/JDBC (Trino via JDBC sampler, Superset chart-data API, WSO2/FastAPI APIs, CKAN Datastore API); Locust for Python-driven scenarios; `trino-benchto`/TPC-DS and TPC-H for engine benchmarks; `kafka-producer-perf-test`/OpenMessaging for Kafka; Flink benchmarks; Spark TPC-DS; ClickHouse `clickhouse-benchmark`; pgbench for the ODS; s3-tests/warp for object storage; chaos: Litmus/Chaos Mesh (pod kill, broker loss, network delay) for reliability; OWASP ZAP/Trivy/kube-bench for security scans; ARX/Presidio evaluation scripts for privacy metrics; Playwright + axe for accessibility.

## Process (five steps) on the platform
1. Planning: targets per type, workload models per tenant tier, environments (prod-like staging), success thresholds.
2. Preparation: data generation (synthetic, scaled), fixtures, monitoring dashboards, baselines.
3. Execution: automated runs in CI (nightly perf tier, pre-release full), chaos experiments with blast-radius limits.
4. Record: raw results + engine metrics (Trino event listener, Kafka JMX, k8s) with run ids.
5. Analysis and improvement: compare to baseline and budget, open defects, update capacity plan and resource-group settings.

## Anti-patterns
- Performance tests without thresholds (nothing can fail)
- Load tests on undersized environments extrapolated to prod
- Security limited to a yearly pen test
- Soak tests never run (leaks found in prod)
