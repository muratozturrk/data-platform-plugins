# Denormalizer examples

| File | Shows |
|------|-------|
| `spark/spark_denormalizer.py` | From a snowflake model: flattened One Big Table, nested-STRUCT variant, and a star schema (one dimension level) on Delta |
| `kafka/kafka_stream_table_enricher.py` | Stream-table enrichment: compacted `devices` topic cached in memory, `visits` flattened into `visits_enriched` |
| `airflow/denormalizer_dag.py` | Dataset-triggered DAG rebuilding a PostgreSQL OBT from normalized tables (CTAS + row-count guard + atomic rename swap) |

Prerequisites: `pip install pyspark delta-spark confluent-kafka`; Airflow 2.4+ (datasets) + Postgres provider,
connection `dp_postgres` from env. The Airflow DAG expects the normalized visits model
(`visits`, `visits_context`, `pages`, `categories`, `users`, `devices`, `browsers`).
