# Batch Reprocessor - Kafka

The Batch Reprocessor exists **because the data is no longer in Kafka** (retention has expired), so
Kafka is only the *output* here, and there is no consumer-side code to show.

The Kafka-specific parts:
- **Raw copy**: keep a lakehouse copy of the topic with `offset`, `partition`, `timestamp` and the raw
  `value`, for example with the Zero-ETL Synchronizer or a passthrough replicator. Without it,
  batch reprocessing is impossible.
- **Origin marking**: reprocessed records get an `origin=reprocessing` header (Metadata Decorator, see
  `../spark/visits_logic.py::to_kafka_records`) or an attribute in a Wrapper payload. Consumers can then
  filter, dedupe or route them.
- **Idempotent consumers**: reprocessed records duplicate earlier ones by key. Downstream consumers
  must upsert by key (compacted topic, MERGE) instead of appending.
- **Longer retention alternatives**: tiered storage (Kafka 3.6+) or diskless topics extend the window in
  which the cheaper Streaming Reprocessor still works.
