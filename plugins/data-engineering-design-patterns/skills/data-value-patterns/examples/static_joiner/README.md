# Static Joiner examples

- `spark/spark_static_joiner_batch.py`: batch enrichment of visits with a Delta reference. `--mode key` is the plain keyed broadcast join. `--mode scd2` is the time-sensitive join against an SCD type 2 table (`as_of >= start_date AND as_of < end_date`), with `--as-of event_time` or `--as-of execution_date` (never `NOW()`). Fails on duplicate reference keys, writes with `replaceWhere` and overwrites a per-date audit JSON. `--self-test` checks the SCD condition builder.
- `spark/spark_streaming_static_joiner.py`: stream-to-static join in `foreachBatch`. The reference is cached and reloaded every N batches only when its Delta version changes.
- `spark/spark_api_bulk_enricher.py`: enrichment from an HTTP API. Each task buffers 100 rows, asks the API once for the distinct uncached IPs and writes to Kafka. `--self-test` checks buffering, dedup and caching.
- `kafka/kafka_static_joiner_consumer.py`: the reference is a compacted topic loaded into an in-process dict and refreshed in a background thread. Tombstones delete keys, misses are flagged with `_ref_miss`, and offsets are committed after the producer flush.
- `airflow/static_joiner_dag.py`: waits for the visits and reference readiness markers, warns on a stale reference (read from `_delta_log`), runs the SCD2 job with `{{ ds }}`, then validates the audit JSON (row count kept, miss rate under 5%).

Prerequisites: PySpark 3.4+ with `delta-spark`, `requests` and `confluent-kafka` on executors, `kafka-python`, and Airflow 2.4+ with the Spark provider.
Run: `python spark/spark_static_joiner_batch.py --self-test`, `python spark/spark_api_bulk_enricher.py --self-test`.
