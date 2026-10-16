# Vertical Partitioner examples

| File | Shows |
|------|-------|
| `spark/spark_vertical_partitioner.py` | One input read (`persist`), three Delta tables via `drop`/`dropFields`/`selectExpr` + `dropDuplicates`, recombination by `visit_id` |
| `kafka/kafka_vertical_splitter.py` | Consume-split-produce: mutable events topic + compacted immutable-context topic keyed by `visit_id` |
| `airflow/vertical_partitioner_dag.py` | PostgreSQL `INSERT INTO ... SELECT DISTINCT` from JSONB into per-group tables, plus a recombining view |

Prerequisites: `pip install pyspark delta-spark confluent-kafka`, Airflow 2.x with the Postgres provider,
`KAFKA_BOOTSTRAP` env var, Airflow connection `dp_postgres` defined via environment.
The Airflow DAG assumes `dp.visits_all` (JSONB `context`) and `dp.visits_events` exist.
