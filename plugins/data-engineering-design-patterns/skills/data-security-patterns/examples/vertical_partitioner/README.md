# Vertical Partitioner examples

| File | Shows |
|---|---|
| `spark/vertical_partitioner_job.py` | `foreachBatch` split of visits into `visits_no_pii` + `user_context` topics, MERGE dedupe of user context into Delta, `delete` + `VACUUM` of one user |
| `kafka/compacted_user_context.py` | Creating the compacted `user_context` topic and sending a tombstone (key, null) |
| `airflow/user_removal_dag.py` | Daily batched removal: Delta delete + vacuum, tombstones, mark requests done |

Prerequisites: Spark 3.5 with `spark-sql-kafka` and `delta-spark` packages, a Kafka broker, Airflow 2.x.
Env vars: `KAFKA_BOOTSTRAP`, `USERS_TABLE`, `CHECKPOINT_DIR`, `REQUESTS_FILE`, `VACUUM_RETENTION_HOURS`.

Run order: `create` topic -> `split` -> `merge` -> `delete <user_id>` (or the DAG).
