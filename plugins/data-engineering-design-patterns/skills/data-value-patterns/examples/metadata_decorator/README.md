# Metadata Decorator examples

Technical context (job version, batch, attempt, source position) goes into the data store's metadata layer, so the business payload stays unchanged.

- `spark/spark_kafka_headers_decorator.py`: Structured Streaming Kafka -> Kafka. Reads with `includeHeaders=true`, keeps upstream headers, replaces the keys it owns (`job_version`, `batch_version`, `processing_time`, `source`) and writes with `includeHeaders=true`. The value is passed through untouched. `--sink s3` is the object-store variant: one prefix per micro-batch, every object tagged with `put_object_tagging`.
- `kafka/kafka_headers_metadata_decorator.py`: confluent-kafka forwarder (`decorate`) that commits offsets only after the producer flush, plus an operator `inspect` mode that filters records by the `job_version` header.
- `airflow/metadata_decorator_dag.py`: the warehouse variant. The `dp.visits_context` row (execution date, loading time, code version, try number) and the weekly visits rows that reference it are replaced in one transaction. A view in `dp_public` hides the technical column from business users.

Prerequisites: PySpark 3.4+ with `spark-sql-kafka-0-10`, boto3 (s3 sink), `confluent-kafka`, Airflow 2.4+ with the common-sql and postgres providers.
Self-tests: `python spark/spark_kafka_headers_decorator.py --self-test`, `python kafka/kafka_headers_metadata_decorator.py --self-test`.
Kinesis Data Streams has no headers: use the context-table variant there.
