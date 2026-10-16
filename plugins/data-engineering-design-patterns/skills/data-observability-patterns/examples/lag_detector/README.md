# Lag Detector - examples

| File | Shows |
|------|-------|
| `spark/streaming_lag_listener.py` | `StreamingQueryListener` pushing per-partition, MAX and P90 offset lag; Delta version lag (written vs read) |
| `kafka/consumer_group_lag.py` | Committed offset vs high watermark for a consumer group, MAX/P90 alerting (`--self-test`) |
| `airflow/partition_lag_dag.py` | Lag on a time-partitioned store: newest partition vs last processed partition |

Lag units: Kafka offset or append time, Delta commit version, partition timestamp.
Prerequisites: Kafka on `localhost:9094`, Pushgateway on `localhost:9091`, PySpark 3.4+ (Python listener API).
Equivalent CLI check: `kafka-consumer-groups.sh --bootstrap-server localhost:9094 --describe --group <g>`.
