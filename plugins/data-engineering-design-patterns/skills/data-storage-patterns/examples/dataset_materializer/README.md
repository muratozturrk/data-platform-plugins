# Dataset Materializer examples

| File | Shows |
|------|-------|
| `spark/spark_dataset_materializer.py` | Full refresh (UNION of weekly Delta tables -> aggregate table) and incremental refresh (insertion_time watermark + Delta MERGE) |
| `kafka/kafka_materialized_counter.py` | Per-user counter materialized into a compacted topic, only changed keys republished, state restored from the topic |
| `airflow/dataset_materializer_dag.py` | PostgreSQL materialized view with `REFRESH ... CONCURRENTLY`, plus interval-based incremental `MERGE` guarded against double counting |

Prerequisites: `pip install pyspark delta-spark confluent-kafka`; Airflow 2.x + Postgres provider
(PostgreSQL 15+ for `MERGE`), connection `dp_postgres` from env. Cloud equivalents: BigQuery
`CREATE MATERIALIZED VIEW ... OPTIONS (enable_refresh=true, refresh_interval_minutes=15)`, Redshift `AUTO REFRESH YES`.
