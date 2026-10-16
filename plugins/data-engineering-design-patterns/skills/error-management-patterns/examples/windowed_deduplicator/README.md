# Windowed Deduplicator: examples

| File | Shows |
| --- | --- |
| `spark/spark_windowed_deduplicator_batch.py` | Batch scope = the processed partition: `dropDuplicates`, `ROW_NUMBER()` window, or SQL; optional lookback anti-join on past outputs; `replaceWhere` overwrite + metrics |
| `spark/spark_streaming_deduplicator.py` | `withWatermark` + `dropDuplicates([id, event_time])` or `dropDuplicatesWithinWatermark` (3.5+), RocksDB state store |
| `kafka/kafka_streams_deduplicator.py` | Event-time window with watermark eviction, local state + snapshot before commit (local with fault tolerance) |
| `airflow/windowed_deduplicator_dag.py` | Sensor, then dedup job, then duplicate-ratio check per partition |

Prerequisites: PySpark 3.5 + `delta-spark`, `kafka-python`, Airflow 2.6+ with the Spark provider.
Self-tests: `python <file> --self-test` for the batch job and the Kafka deduplicator.
