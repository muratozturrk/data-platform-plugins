# Single Runner examples

| File | Shows |
|---|---|
| `airflow/single_runner_dag.py` | `max_active_runs=1` + `depends_on_past=True` (+ `wait_for_downstream`) for an incremental day-over-day pipeline |
| `spark/single_runner_job.py` | In-job guard when the runner has no concurrency setting: previous-run `_SUCCESS` check + lease lock (atomic create, heartbeat, stale-lock takeover after `--lock-ttl`, owner-checked release). `--self-test` checks the lock logic without Spark |
| `kafka/single_runner_fencing.py` | Stable `transactional.id` fencing so only one processor instance can write |

Other runners: AWS EMR `StepConcurrencyLevel=1`, Azure Data Factory pipeline `Concurrency=1`
(queues extra runs, HTTP 429 when the queue of 100 is full).
Prerequisites: Airflow 2.6+; PySpark 3.3+; confluent-kafka 2.x and a broker at `$KAFKA_BOOTSTRAP`.
