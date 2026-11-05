# qa-test-strategy – examples

- `test-strategy-template.md`, `test-plan-template.md`, `test-report-template.md`
- `test-case-template.yaml` – mandatory fields, two filled examples (data-quality and isolation)
- `test-inventory.yaml` – starter platform inventory (ids, levels, suites, tools, schedule); extend with `/test-inventory`
- `rtm-template.csv` – Requirement Traceability Matrix columns
- `defect-matrix.md` – severity × priority guidance and defect template
- `rtm_from_results.py` – generate RTM/coverage from test-case YAML + JUnit results in CI

## Level how-to examples (added with the unit/integration/e2e skills)
- `unit/` – PySpark transform tests with chispa (+ idempotency), dbt ≥ 1.8 unit tests YAML, Airflow DAG integrity test, compiler golden-file test, OPA retention policy tests
- `integration/` – Testcontainers `conftest.py` (PostgreSQL, Kafka KRaft, Karapace, Debezium Connect, Nessie, Keycloak, Trino), CDC ordering test, Karapace compatibility test, Trino isolation test, Argo CD ApplicationSet for namespace-per-PR, CI workflow with fast and full bands
- `e2e/` – YAML scenario (E2E-02 batch pipeline), scenario runner with polling and business-level assertions (Trino, Marquez, Superset, Ranger audit), deterministic fixture generator with oracle, nightly workflow that rebuilds the E2E environment
