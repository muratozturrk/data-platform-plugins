# Metadata Enhancer examples

| File | Shows |
|------|-------|
| `spark/spark_metadata_enhancer.py` | Parquet footer stats (pyarrow), Delta `_delta_log` `add.stats` JSON, predicate pushdown / file skipping |
| `kafka/kafka_time_index_seek.py` | `offsets_for_times` using the broker time index to skip old data; header metadata to skip without deserializing |
| `airflow/metadata_enhancer_dag.py` | PostgreSQL: detect stale planner stats via `pg_stat_user_tables.n_mod_since_analyze`, `ANALYZE` + extended statistics |

Prerequisites: `pip install pyspark delta-spark pyarrow confluent-kafka`; Airflow 2.x + Postgres provider,
connection `dp_postgres` from env. Alternative footer inspection:
`docker run --rm -v "./out:/tmp/parquet" hangxie/parquet-tools:v1.20.7 meta /tmp/parquet/<file>.parquet`.
