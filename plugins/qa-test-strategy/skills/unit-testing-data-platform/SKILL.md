---
name: unit-testing-data-platform
description: How to write unit tests for the HKM data platform with open-source tooling: pure transformation functions, PySpark (pytest + chispa + Hypothesis), Flink (test harnesses, MiniCluster), dbt 1.8+ unit tests with mock inputs, SQL logic on Trino with fixture tables, Airflow DAG integrity and task-callable tests, control-plane compiler golden files, policy rules (Rego, CEL, Ranger JSON) and deterministic test data. Use when writing or reviewing unit-level tests for any platform component.
---

# Unit Testing on the Data Platform

Unit tests verify one transformation, rule or function in isolation with controlled inputs and exact expected outputs; no network, no cluster, no real data. On this platform the "unit" is usually a SQL model, a DataFrame function, a Flink operator, a DAG definition or a compiler step. The data testing pyramid still has unit tests at the base, but DataFrame runtimes make them slower than classic unit tests — keep them small, local and parallel.

## What to unit-test (by component)
| Component | Unit under test | Tool | Expected-result style |
|---|---|---|---|
| Python transforms, parsers, mappers | pure function `rows -> rows` | pytest + parametrize | exact values, edge cases (empty, null, boundary, late, duplicate) |
| PySpark jobs | `DataFrame -> DataFrame` step functions | pytest, `chispa` (`assert_df_equality` with schema/nullable/ordering options), local `SparkSession` fixture (session scope, `local[2]`), Hypothesis for properties (see `spark-property-based-testing` plugin) | DataFrame equality, schema equality, invariants (row count preserved, keys unique) |
| Flink jobs | operators/process functions | Java: `OneInputStreamOperatorTestHarness`, `KeyedOneInputStreamOperatorTestHarness`, `MiniClusterWithClientResource` for small pipelines; PyFlink: `StreamExecutionEnvironment` local with collection sources/sinks | emitted records incl. watermarks/timers, state after late events |
| dbt models | SQL logic of one model | dbt ≥ 1.8 `unit_tests:` in YAML with `given:` mock inputs for `ref()`/`source()` and `expect:` rows; run `dbt test --select "test_type:unit"` on every PR (no warehouse data needed, only a connection) | expected rows; one test per business rule, incl. nulls and boundary dates |
| SQL on Trino (hand-written, non-dbt) | one query/CTE | pytest + Trino in Testcontainers with `memory` connector or tiny Iceberg tables (counts as unit if seconds-fast) | expected rows |
| Airflow | DAG files and task callables | DAG integrity test over `DagBag` (no import errors, no cycles, ids per naming, owners/tags present, retries set); TaskFlow callables tested as plain functions; `dag.test()` only at integration level | parametrised over every DAG file |
| Control plane | manifest validation, config layering, compiler | pytest; JSON Schema validation cases; **golden-file tests** (fixed manifest → expected Airflow DAG / Spark CR / dbt project / Ranger policy JSON / OPA bundle, compared byte-for-byte with normalised timestamps) | golden files reviewed in PRs |
| Policies | Rego (retention), CEL (Nessie), Ranger JSON | `opa test` with coverage; CEL evaluated with the same library in a small harness; Ranger JSON schema + decision table tests via the control plane's compiler | allow/deny matrices |
| Data-quality rules | GE custom expectations, Soda checks, dbt macros | pytest on the expectation/macro with pass and fail fixtures (the 10 rule types each need both) | rule passes on good data, fails on bad with failed rows captured |
| Privacy functions | Presidio recognisers (TR), HMAC pseudonymisation, masking functions | pytest with labelled corpus (precision/recall thresholds), determinism tests | metrics above threshold; same input → same token within scope |

## Rules
1. Deterministic inputs: fixtures and generators seeded (`Faker(seed)`, Hypothesis `@settings(derandomize=True)` in CI), fixed clocks (`freezegun`), fixed time zones (UTC).
2. One behaviour per test; name `test_{unit}_{behaviour}` and include the inventory id in the test name or marker (`@pytest.mark.tid("T-spark-001")`) so results feed the RTM.
3. Negative and boundary cases are mandatory: empty input, nulls in keys, duplicates, late/out-of-order events, schema with extra/missing columns, max lengths, leap days, DST.
4. No I/O: no object storage, Kafka, Nessie or Keycloak in unit tests; mock at the boundary (`SecretProvider`, `StorageProvider` SPI fakes), never patch deep internals.
5. Fast: unit tier ≤ 5 minutes in CI; Spark tests share one session and use `spark.sql.shuffle.partitions=2`.
6. Coverage gates: ≥ 85% core/sdk; dbt: every model with business logic has ≥ 1 unit test; every macro with a unit test; every OPA rule covered.
7. Review checklist: assertion is exact (not "not None"), fixture is minimal, test fails when the logic is broken (mutation-check), no sleep, no order dependence.

## Patterns
- **Table-driven tests** for mapping/validation logic; **golden files** for generated artefacts; **property tests** for invariants (idempotency `f(f(x)) == f(x)`, key preservation, monotonic timestamps).
- **dbt**: keep `unit_tests:` next to the model YAML; mock only direct inputs; use `overrides:` for `current_timestamp()`/vars; run unit tests in CI before `dbt build` on staging data.
- **Flink**: test business logic in operators with harnesses; reserve MiniCluster for a 3-operator pipeline smoke; watermark cases via `processWatermark`.
- **Spark**: `chispa.assert_df_equality(actual, expected, ignore_nullable=True, ignore_row_order=True)`; build expected frames with explicit schemas; test each `transform()` step separately.

## Anti-patterns
- "Unit" tests that read from the lake or Kafka
- Expected results computed by the code under test
- Spark session per test (slow) or `collect()` on large frames
- dbt data tests (unique/not_null) mistaken for unit tests — they test data, not logic

Sources: dbt docs and Paradime "Unit Testing in dbt: A Practical Guide" (dbt ≥ 1.8 unit tests, mock inputs, CI integration, test complex logic first); chispa (MrPowers) README; Apache Flink testing docs (test harnesses, MiniClusterWithClientResource); *Data Pipelines with Apache Airflow* 2e ch. 10 (DAG integrity tests, operator unit tests, pytest-mock, dag.test(), containers); qaskills.sh "Testing Apache Airflow DAGs with pytest" (2026: integrity/unit/integration layers); danielvdende "data-testing-with-airflow" (7 layers: DAG integrity, mock pipeline tests, data tests, DTAP); helpmetest "Data Pipeline Quality Assurance" (2026: layered unit/contract/integration/SLA, idempotency testing); OPA `opa test`; Hypothesis docs.
