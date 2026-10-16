# Normalizer - Kafka (equivalent, no dedicated code)

Kafka has no tables, joins or foreign keys, so normal forms are not enforced by the broker. The
normalized equivalent is **one topic per business entity**:

- `pages`, `browsers`, `devices`, `users` as **compacted topics** keyed by the entity id (latest state per
  key = a dimension/entity table). Events (`visits`) carry only the foreign keys (`page_id`, `device_id`).
- An entity update is one message on its topic instead of a change on every event - the Normalizer's
  consistency benefit.
- Schema Registry **schema references** (Avro/Protobuf/JSON Schema) let event schemas reuse entity schemas
  without copying fields.
- Consumers recombine with stream-table joins (Kafka Streams `KTable`, Flink, Spark) on co-partitioned
  topics (see `../../bucket/kafka/kafka_copartitioning_check.py`).

The mechanics of splitting an incoming payload into entity topics are in
`../../vertical_partitioner/kafka/kafka_vertical_splitter.py`; the read-side recombination (denormalized
enrichment) is in `../../denormalizer/kafka/kafka_stream_table_enricher.py`.
