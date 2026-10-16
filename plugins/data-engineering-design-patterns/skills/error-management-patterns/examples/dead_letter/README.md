# Dead-Letter: examples

| File | Shows |
| --- | --- |
| `spark/spark_streaming_dead_letter.py` | try/except mapping in `mapInPandas` (side-output idea), `foreachBatch` split into valid Delta + Kafka/Delta dead-letter, `txnVersion` idempotent writes, error-ratio stop |
| `spark/spark_error_safe_dead_letter.py` | Error-safe `CONCAT`/`TRY_CAST` detected with an if-else on the output, `persist()` + two `replaceWhere` writes, metrics JSON, `replay` with `was_dead_lettered` |
| `kafka/kafka_dead_letter_consumer.py` | Transient errors retried, poison pills to a DLQ topic with metadata, flush-then-commit, sliding error-ratio guard, replay with headers |
| `airflow/dead_letter_monitor_dag.py` | Load job, then monitoring (fail above ratio, trend warning), optional replay, then consumer notification |

Prerequisites: PySpark 3.5 + `delta-spark` 3.x, `spark-sql-kafka-0-10`, `kafka-python`, Airflow 2.6+ with the Spark provider.
Every file supports `--self-test` (pure Python, no cluster) except the DAG. Env: `KAFKA_BOOTSTRAP_SERVERS`, `DP_DEAD_LETTER_BASE_DIR`, `DP_JOBS_DIR`.
