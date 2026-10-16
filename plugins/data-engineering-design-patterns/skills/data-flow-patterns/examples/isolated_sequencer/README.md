# Isolated Sequencer examples

| File | Shows |
|---|---|
| `airflow/data_based_dependency_dags.py` | Producer writes data + `_SUCCESS`; consumer waits with `FileSensor`; also Airflow `Dataset` scheduling |
| `airflow/task_based_dependency_dags.py` | `ExternalTaskMarker` + `ExternalTaskSensor` (backfill-aware), and push-style `TriggerDagRunOperator` |
| `spark/producer_consumer_jobs.py` | Two independent jobs coupled only by a readiness marker + manifest (row count, schema version) |
| `kafka/readiness_events.py` | Provider publishes "dataset ready" events; each consumer team uses its own consumer group |

Prerequisites: Airflow 2.6+, PySpark 3.3+, confluent-kafka 2.x and a broker at `$KAFKA_BOOTSTRAP`.
Airflow: copy files to `dags/` and run `airflow dags test <dag_id> 2024-01-01`.
