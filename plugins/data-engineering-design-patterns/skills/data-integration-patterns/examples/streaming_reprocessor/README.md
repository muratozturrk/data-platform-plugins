# Streaming Reprocessor - examples

| File | Shows |
|------|-------|
| `spark/reprocess_from_past.py` | `new-checkpoint` (fresh location + `startingTimestamp`, stateless only) and `restore-copy` (copy checkpoint up to batch N, state N+1) |
| `kafka/reset_offsets_to_timestamp.py` | Dry-run-by-default rewind of a stopped consumer group with `offsets_for_times` |
| `airflow/reprocessing_dag.py` | On-demand backup >> stop >> restart, Flink savepoint or Spark checkpoint strategy |

Flink native config: `env.enable_checkpointing(30000)`,
`RETAIN_ON_CANCELLATION`, `state.checkpoints.num-retained: 20`, `flink run -s file:///.../chk-2 ...`.
Prerequisites: Kafka on `localhost:9094` with enough retention (or tiered storage), Spark 3.5, idempotent sinks.
