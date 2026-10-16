# Flow Interruption Detector - examples

| File | Shows |
|------|-------|
| `spark/delta_writer_with_freshness_metric.py` | Producer pushes a last-update-time gauge only when rows are written; Delta-history check that ignores compaction commits |
| `kafka/topic_flow_monitor.py` | Continuous vs irregular (no-data window) detection from partition high watermarks (`--self-test` included) |
| `airflow/postgres_flow_interruption_dag.py` | Hourly DAG: PostgreSQL last commit timestamp + row-count fallback |

Prometheus/Grafana variant, alert when the last 5 values are 0:

```promql
sum without(instance)(rate(kafka_server_brokertopicmetrics_messagesin_total{topic="visits"}[1m]))
```

Prerequisites: Kafka on `localhost:9094`, Prometheus Pushgateway on `localhost:9091`, PostgreSQL with
`track_commit_timestamp = on`. Install `pyspark delta-spark prometheus-client confluent-kafka apache-airflow-providers-postgres`.
