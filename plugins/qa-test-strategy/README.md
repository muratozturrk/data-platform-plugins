# qa-test-strategy

QA process and test strategy for the HKM data platform. Built from the team's QA notes, the project technology list (`HKM_kullaniacak_tum_teknolojiler.xlsx`, 11 layers / ~80 tool rows) (quality attributes, QA/QC/testing, STLC, test case lifecycle diagram, test documentation, test case management, coverage/RTM, test reports, defect management, non-functional and performance testing, QA metrics) plus platform-specific test taxonomy.

| Kind | Name | Purpose |
|---|---|---|
| Agent | `qa-strategist` | Risk-based strategy, plans, test inventories for components/releases |
| Agent | `test-case-designer` | Writes/reviews test cases and automation skeletons with data-platform assertions |
| Agent | `defect-triager` | Classifies and triages defects with the platform severity/priority matrix |
| Command | `/test-inventory` | The list of tests a component/release/platform needs (ids, suites, tools, schedule) |
| Command | `/test-plan` | Test plan per release/project |
| Command | `/test-strategy` | Test strategy per platform/component |
| Command | `/defect-triage` | Triage a batch of reports |
| Command | `/test-report` | Iteration/release test report with results summary, coverage, defects, metrics |
| Skill | `stlc-and-test-case-lifecycle` | Six STLC phases; test case lifecycle (requirements → environment/tool → case → mapping → review → execution → regression → inspection) |
| Skill | `test-planning-and-strategy` | Strategy vs plan, contents, pyramids, automation policy, environments, tooling |
| Skill | `test-suite-taxonomy` | Levels, types, methods, execution modes, suites by purpose, naming |
| Skill | `coverage-and-traceability` | Coverage report, RTM, test case management, libraries/runs, tools |
| Skill | `defect-management` | Definitions, priority/severity, lifecycle, triage, quality debt, metrics |
| Skill | `nonfunctional-and-performance-testing` | Functional vs non-functional, types with platform budgets, load/stress/endurance/volume/spike/scalability, tools |
| Skill | `qa-metrics-and-reporting` | Result vs predictive metrics, platform metric set, test report, release rule |
| Skill | `unit-testing-data-platform` | How to write unit tests: PySpark/chispa/Hypothesis, Flink harnesses, dbt unit tests, Trino SQL, DAG integrity, compiler golden files, Rego/CEL/Ranger rules, privacy functions |
| Skill | `integration-testing-data-platform` | Testcontainers fast band + ephemeral-namespace full band; isolation, contract (Karapace/OpenAPI/SPI/data contracts), CDC, catalog, idempotency, lineage tests |
| Skill | `e2e-testing-data-platform` | 10 golden-path journeys with business-level oracles, synthetic fixtures, polling, environment strategy, release gates, flakiness control |
| Skill | `data-platform-test-inventory` | Per-layer test taxonomy for the HKM stack (ingestion → processing → lakehouse → quality → catalog/governance → security/privacy → serving → portal/API → operations) with tools and suite defaults |

`examples/`: strategy/plan/report templates, test-case YAML template, starter `test-inventory.yaml` (v2: 112 tests across control plane/Airflow, Kafka/Karapace/Debezium/connectors/Camel/RabbitMQ/Flink, Spark/dbt/Zingg, Iceberg/Nessie/object storage WORM/ClickHouse/ODS/Trino, quality & observability (GE/Soda/dbt/Deequ/Elementary/profiling/alert clustering), OpenMetadata/OpenLineage/OPA retention/Fides/Flowable, Keycloak/Ranger/OpenBao/Presidio/ARX/HMAC/DP/immudb/OpenSearch, Superset/Grafana/Redis/semantic/spatial, CKAN/WSO2/distribution/Matomo, Alertmanager/backup/preservation/Kubernetes/DR), RTM template, defect matrix, RTM generator for CI.

Related plugins in this marketplace: `tdd-workflows` (red/green/refactor), `unit-testing` (test generation), `spark-property-based-testing`, `data-governance` (DQ SLAs), `coding-standards` (test code rules), `naming-conventions` (ids and suite names).

## Sources
- Project technology list `HKM_kullaniacak_tum_teknolojiler.xlsx` (layer, tool, role, licence); team QA notes (`notes.docx`) and test case lifecycle diagram (`test_case_lifecycle_scheme.svg`); good test case structure diagram (`Test_Cases_Management_good_test_case_stucture.svg`): header fields plus an execution-steps table with step-level requirement, defect and automation ids and timestamped tester comments — reflected in `test-case-designer` and `examples/test-case-template.yaml`.
- ISTQB Foundation Level syllabus (test levels/types, lifecycle, defect management) and ISO/IEC 25010 (quality attributes) as the standard background for the notes' vocabulary.
- Tooling docs: pytest, chispa, Hypothesis, dbt (≥ 1.8 unit tests), Great Expectations, Soda Core, Deequ, Elementary, k6, JMeter, Playwright, kubeconform, conftest/OPA, Testcontainers (Python/Java; Debezium testing extension), datacontract-cli, schemathesis, Karapace, Argo CD ApplicationSets (PR generator), vcluster/kind, Chaos Mesh, Trivy, gitleaks; test management: Jira + Zephyr/Xray, TestRail, Azure DevOps Test Plans.
- Data-engineering testing practice (2026): reintech "Data Pipeline Testing Strategies", dataworkers.io "Data Pipeline Best Practices 2026", helpmetest "Data Pipeline Quality Assurance" and "Ephemeral Test Environments", dataengineeringcompanies.com "10 Actionable Data Pipeline Testing Best Practices", qaskills.sh Airflow and Testcontainers guides, Paradime dbt unit-testing guide, *Data Pipelines with Apache Airflow* 2e ch. 10, danielvdende data-testing-with-airflow (7 layers), Superset Developer Portal testing overview.
