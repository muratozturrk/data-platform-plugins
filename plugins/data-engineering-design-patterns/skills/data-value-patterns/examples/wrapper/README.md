# Wrapper examples

- `spark/spark_record_envelope_wrapper.py`: main implementation. `--layout` picks one of four layouts: `computed_nested` (raw flat + `decorated` struct), `raw_nested` (computed flat + `raw` struct), `flat` (`raw_*` / `computed_*` prefixes), `two_tables` (raw and computed tables joined by a sha2 `wrap_id`). Every layout carries a `processing_context` struct (job_version, batch_version, processing_time). `--engine sql` runs the `NAMED_STRUCT` / `STRUCT(*)` queries. `--stream` rewrites Kafka values as `{raw_data, processing_context}`. Batch writes overwrite one Delta partition (`replaceWhere`) and emit a metrics JSON with count + checksum of the input and of the raw part re-read from the output.
- `kafka/kafka_record_wrapper.py`: consumes two provider topics with different schemas, extracts the same computed fields from each and produces `{raw, computed, processing_context}`. `raw` is the original string byte-for-byte; unwrappable records go to a DLQ topic. `--self-test` runs the wrap logic.
- `airflow/wrapper_dag.py`: daily DAG. Waits for the Bronze `_SUCCESS`, runs the Spark job with `job_version` from a Variable and `batch_version = run_id/try-N`, then fails if the raw part's count or checksum differs from the input.

Prerequisites: PySpark 3.4+ with `delta-spark` (and the Kafka package for `--stream`), `confluent-kafka`, Airflow 2.4+ with the Spark provider.
Run: `python kafka/kafka_record_wrapper.py --self-test`; Spark command line in the job's docstring.
