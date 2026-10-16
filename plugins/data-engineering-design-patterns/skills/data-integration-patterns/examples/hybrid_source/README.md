# Hybrid Source - examples

| File | Shows |
|------|-------|
| `spark/hybrid_source_job.py` | Single driver: Delta historical read (closed boundary, availableNow) then Kafka from `startingTimestamp` |
| `kafka/hybrid_consumer.py` | Bounded read of a still-active historical topic (end offsets captured at start), switch via `offsets_for_times`, state kept across the switch |
| `airflow/hybrid_source_dag.py` | Multi-job variant: big-compute historical job >> fire-and-forget real-time submission |

Flink users: `HybridSource.builder(file_source).add_source(kafka_source)` with
`KafkaOffsetsInitializer.timestamp(switch_ms)` is the native single-job form.
Prerequisites: Kafka on `localhost:9094`, Spark 3.5 + Delta, Airflow with `spark_default`.
