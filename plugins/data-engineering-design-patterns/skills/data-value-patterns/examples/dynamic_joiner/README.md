# Dynamic Joiner examples

- `spark/spark_stream_stream_time_bounded_join.py`: main implementation. Visits x ads stream-to-stream `left_outer` join, `withWatermark` on both sides (GC watermark) and a time-range condition (`display_time BETWEEN event_time AND event_time + INTERVAL 2 minutes`). `foreachBatch` writes to Delta with `txnAppId`/`txnVersion` (idempotent replays) and appends matched/unmatched counts plus `lastProgress` (watermark, state rows, rows dropped by watermark) to a JSONL stats file.
- `kafka/kafka_stream_stream_joiner.py`: the same mechanics written out in a confluent-kafka consumer. Per-side time-bounded buffers, GC watermark = min(max event time per side) - lateness, eviction with NULL emission for unmatched visits, offsets committed only up to the oldest still-buffered record. `--self-test` runs the pure join logic.
- `airflow/dynamic_joiner_dag.py`: hourly health DAG over the stats file. Fails on low match rate, watermark lag, state growth or too many rows dropped as late. `python dynamic_joiner_dag.py` runs the evaluation self-check (needs Airflow importable).

Prerequisites: PySpark 3.4+ with `spark-sql-kafka-0-10` and `delta-spark`, `confluent-kafka`, Airflow 2.4+.
Run: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 spark/spark_stream_stream_time_bounded_join.py --watermark "10 minutes"`.
