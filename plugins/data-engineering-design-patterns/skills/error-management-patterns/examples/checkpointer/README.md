# Checkpointer: examples

| File | Shows |
| --- | --- |
| `spark/spark_streaming_checkpointer.py` | `checkpointLocation` on a 10-minute unique-visits window, RocksDB with changelog checkpointing, idempotent MERGE sink; `inspect` reads `offsets/N` / `commits/N` |
| `kafka/kafka_offset_checkpointer.py` | Explicit checkpoints: `at-least-once` (commit after), `at-most-once` (commit before), `transactional` (offsets saved in the sink DB, seek on assignment) |
| `airflow/checkpointer_dag.py` | Checkpoint health: commit age, planned-vs-committed gap, Kafka lag from the checkpointed offsets |

Self-tests: `python spark/spark_streaming_checkpointer.py --self-test`, `python kafka/kafka_offset_checkpointer.py --self-test`.
Prerequisites: PySpark 3.5 + `delta-spark`, `spark-sql-kafka-0-10`, `kafka-python`, `psycopg2-binary`, Airflow 2.6+. Env: `KAFKA_BOOTSTRAP_SERVERS`, `PG_DSN`, `DP_CHECKPOINT_DIR`.
