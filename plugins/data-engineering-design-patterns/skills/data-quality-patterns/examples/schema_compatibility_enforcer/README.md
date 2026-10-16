# Schema Compatibility Enforcer examples

| File | Shows |
|---|---|
| `kafka/schema_registry_compatibility.py` | External-service mode: FORWARD compatibility on a subject, `test_compatibility` dry run, 409 rejection of a breaking v2, accepted additive v3. |
| `spark/spark_delta_schema_enforcement.py` | Implicit mode: Delta rejects extra columns and type changes; `mergeSchema` as explicit opt-in. |
| `airflow/schema_compatibility_gate_dag.py` | CI-style gate calling the Registry compatibility endpoint before registering, plus event-driven DDL mode (PostgreSQL event trigger blocking DROP/RENAME/retype COLUMN; allowlist = `SET LOCAL dp.schema_migration = 'contract'` + membership in role `dp_schema_migrator`, used by the Schema Migrator contract step). |

Prerequisites: Confluent Schema Registry (`SCHEMA_REGISTRY_URL`), `confluent-kafka`, `requests`; Spark 3.5 + delta-spark; Airflow 2.x with common-sql/postgres providers and a `dp_postgres` connection (event triggers need superuser).
