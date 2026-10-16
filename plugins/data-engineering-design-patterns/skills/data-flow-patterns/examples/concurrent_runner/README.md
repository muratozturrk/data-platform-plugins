# Concurrent Runner examples

| File | Shows |
|---|---|
| `airflow/concurrent_runner_dag.py` | `max_active_runs=5`, `depends_on_past=False`, shared `pool` as workload management, run-scoped outputs |
| `kafka/concurrent_consumers.py` | N workers in one consumer group; concurrency capped by partition count |
| `spark/README.md` | EMR `StepConcurrencyLevel`, FAIR scheduler pools, queues/quotas |

Prerequisites: Airflow 2.6+ (create the `ingestion` pool first); confluent-kafka 2.x and a broker at
`$KAFKA_BOOTSTRAP` with a multi-partition `devices_raw` topic.
