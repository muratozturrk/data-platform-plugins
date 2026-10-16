# Partial State Writer - examples

| File | Shows |
|------|-------|
| `spark/partial_sessions_job.py` | `applyInPandasWithState`: partial session on input (every N visits), final on inactivity timeout, MERGE upsert on a stable session id (first-seen event time kept in state) that never overwrites a final row |
| `kafka/partial_state_consumer.py` | Plain-consumer sessions with partial emissions, watermark-based closing, compacted output topic keyed by session_id (`--self-test`) |
| `airflow/README.md` | Why no DAG; batch equivalent with `is_final` |

Flink: `.trigger(ContinuousEventTimeTrigger.of(Time.minutes(10)))` on a window emits partial
results every 10 event-time minutes. `CountTrigger` and `DeltaTrigger` are alternatives.
Prerequisites: Kafka on `localhost:9094`, Spark 3.4+ (pandas, pyarrow), Delta 3.x.
