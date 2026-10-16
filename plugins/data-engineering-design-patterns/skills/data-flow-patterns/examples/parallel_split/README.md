# Parallel Split examples

| File | Shows |
|---|---|
| `airflow/parallel_split_dag.py` | One sensor fanning out to per-format Spark jobs with per-branch resources |
| `spark/parallel_split_job.py` | Read once + `persist()`, two branch writes with Delta `txnAppId`/`txnVersion` idempotent appends; single-format mode for the DAG |
| `kafka/parallel_split_consumers.py` | One topic, one consumer group per branch -> independent offsets and failure domains |

Prerequisites: Airflow 2.6+ with `apache-spark` provider (`spark_default` connection); PySpark 3.3+ with
`delta-spark`; confluent-kafka 2.x and a broker at `$KAFKA_BOOTSTRAP`.
