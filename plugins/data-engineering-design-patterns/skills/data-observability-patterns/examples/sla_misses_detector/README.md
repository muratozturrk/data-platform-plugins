# SLA Misses Detector - examples

| File | Shows |
|------|-------|
| `airflow/sla_dag.py` | Native `sla=` + `sla_miss_callback` (measured from DAG run start) and an explicit task-duration check |
| `spark/microbatch_sla_listener.py` | Per micro-batch processing-time SLA via listener; separate event-time (end-to-end) P95 SLA |
| `kafka/record_sla_monitor.py` | Per-record SLA from a `start_processing_time_ms` header vs append time, 1-min P95/MAX windows (`--self-test`) |

Prerequisites: Airflow 2.x (SLA removed in 3.0, Deadline Alerts in 3.1), Kafka on `localhost:9094`,
Pushgateway on `localhost:9091`, PySpark 3.4+.
