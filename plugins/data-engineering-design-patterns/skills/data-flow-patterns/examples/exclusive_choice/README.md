# Exclusive Choice examples

| File | Shows |
|---|---|
| `airflow/exclusive_choice_dag.py` | `@task.branch` router on the logical date (migration cut-over), merge child with `NONE_FAILED_MIN_ONE_SUCCESS` |
| `spark/exclusive_choice_job.py` | Job-parameter choice via a Factory of writers; schema-based (metadata) routing |
| `kafka/content_router.py` | Transactional content-based router: header first, payload fallback, DLQ for unknowns |

Prerequisites: Airflow 2.6+; PySpark 3.3+ (+ `delta-spark` for Delta output); confluent-kafka 2.x,
broker at `$KAFKA_BOOTSTRAP` with topics `devices`, `devices_v1`, `devices_v2`, `devices_dlq`.
