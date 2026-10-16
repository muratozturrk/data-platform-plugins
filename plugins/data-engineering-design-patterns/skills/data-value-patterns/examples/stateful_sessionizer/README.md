# Stateful Sessionizer examples

- `spark/spark_streaming_sessionizer.py`: the main implementation. Kafka visits are keyed by `visit_id` and fed to `applyInPandasWithState` with `EventTimeTimeout`. The timeout is the current watermark plus a 10-minute gap, and the max event time stands in for the watermark in the first micro-batch. Replays are deduplicated, session ids are deterministic, partial sessions can be emitted (`is_completed=false`), and the Delta sink MERGEs on `session_id`. `--mode session-window` runs the low-code `session_window()` alternative.
- `kafka/kafka_stateful_sessionizer.py`: the same logic without a framework. There's an in-memory state per input partition, a co-partitioned compacted changelog topic as the fault-tolerant copy, and checkpoints (emit, changelog, flush, commit offsets). Expiry runs on a per-partition event-time watermark, and state is restored on rebalance.

Run `python <file> --self-test` for the pure-logic checks (no Spark or broker needed).
Prerequisites: PySpark 3.4+ with `spark-sql-kafka` and `delta-spark`, pandas, pyarrow; `kafka-python`.
The batch variant was removed: batch sessionization is the Incremental Sessionizer (`../incremental_sessionizer/`). Flink `EventTimeSessionWindows` is in `references/stateful_sessionizer.md`.
