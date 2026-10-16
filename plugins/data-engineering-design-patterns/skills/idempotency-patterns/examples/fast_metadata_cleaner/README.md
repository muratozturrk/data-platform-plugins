# Fast Metadata Cleaner -- examples

Weekly idempotency tables cleaned with metadata operations (TRUNCATE / DROP) and exposed through one union view.

| File | Shows |
|---|---|
| `airflow/weekly_tables_truncate_dag.py` | **Main implementation.** PostgreSQL weekly tables, `@task.branch` router (Monday / Jan 1), TRUNCATE flow and DROP flow (exclude from view -> drop -> create -> include), view rebuilt from `information_schema`, daily load with `none_failed_min_one_success`, `max_active_runs=1`. |
| `spark/spark_weekly_tables_metadata_cleaner.py` | Same mechanics on Delta catalog tables: DROP+CREATE or TRUNCATE (falls back to predicate-less `DELETE`, metadata-only in Delta), `CREATE OR REPLACE VIEW` from `spark.catalog.listTables`, day written with `replaceWhere`; `--full-load` for the single fully loaded table variant. |
| `kafka/kafka_weekly_topics_cleaner.py` | Kafka adaptation: one topic per week, `delete_topics` + wait + `create_topics` on a new unit, regex subscription `^visits_\d{4}_w\d{2}$` as the "view". |

Prerequisites: Airflow 2.x + `apache-airflow-providers-postgres` (connection `pg_dwh`), PySpark 3.5 + delta-spark 3.x, `confluent-kafka`.
Self-checks (no cluster needed): `python airflow/weekly_tables_truncate_dag.py`, `python spark/spark_weekly_tables_metadata_cleaner.py --self-test`, `python kafka/kafka_weekly_topics_cleaner.py --self-test`.
