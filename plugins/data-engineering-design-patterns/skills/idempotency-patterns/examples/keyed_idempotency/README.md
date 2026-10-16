# Keyed Idempotency -- examples

The same input always yields the same key, built only from immutable attributes, so the store overwrites instead of duplicating.

| File | Shows |
|---|---|
| `spark/spark_streaming_keyed_idempotency.py` | Main example: Kafka visits -> `applyInPandasWithState` sessions keyed by `sha256(user_id, min append time)` (not salted `hash()`), event-time timeout, upsert into ScyllaDB/Cassandra `PRIMARY KEY(session_id, user_id)`. `--self-test`. |
| `spark/spark_keyed_idempotency_batch.py` | Batch keys: SHA-256 of natural PK columns, or `--key-mode first-ingestion` (window `ORDER BY ingestion_time ASC, visit_time ASC`); Delta MERGE because the table does not upsert on key collision. |
| `kafka/kafka_append_time_session_keys.py` | confluent-kafka: verifies `message.timestamp.type=LogAppendTime`, builds sessions from broker append time, produces to a compacted topic keyed by session id, commits offsets only up to the oldest open session. `--self-test`. |
| `airflow/keyed_idempotency_dag.py` | Container keys from the logical date (`visits_20_11_2024.csv`, atomic rename) + PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` for daily sessions. `python keyed_idempotency_dag.py` self-tests. |

Prerequisites: PySpark 3.4+ (3.5 tested API) with spark-sql-kafka and spark-cassandra-connector, delta-spark 3.x, `confluent-kafka`, Airflow 2.x + Postgres provider. Credentials come from env (`CASSANDRA_USER/PASSWORD`, `KAFKA_SASL_*`) or Airflow connections.
