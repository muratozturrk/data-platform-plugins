# FIFO Orderer examples

- `kafka/kafka_fifo_orderer.py`: consume, filter, then forward, with three producer modes (`FIFO_MODE`): `single` (produce + flush per record), `inflight1` (`max.in.flight.requests.per.connection=1` + buffering), and `idempotent` (`enable.idempotence=true`, up to 5 in flight). Offsets are committed only after the forwarded records are acknowledged.
- `spark/spark_streaming_fifo_orderer_kinesis.py`: Structured Streaming to Kinesis `PutRecord` (one record at a time), chaining `SequenceNumberForOrdering` per partition key. Ordering is scoped by repartitioning on the entity.
- `airflow/fifo_orderer_dag.py`: hourly publish to GCP Pub/Sub with `enable_message_ordering` + `ordering_key`, waiting for each ack. Uses `depends_on_past` for ordering across runs.

Prerequisites: `confluent-kafka`, PySpark 3.4+ with the Kafka package, boto3, `google-cloud-pubsub`, and Airflow 2.4+.
FIFO is not exactly-once. Pair it with the idempotency patterns.
