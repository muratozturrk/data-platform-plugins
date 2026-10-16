# Late Data Detector: examples

| File | Shows |
| --- | --- |
| `spark/spark_late_data_detector.py` | `native`: `withWatermark` + windowed count (late rows silently dropped). `capture`: watermark built by hand in `foreachBatch` (per-partition MAX, global MIN/MAX, allowed lateness, monotonic), late rows written to their own table |
| `kafka/kafka_late_data_detector.py` | Same watermark on a consumer: per topic-partition MAX, late records to a late topic flagged `is_late`, watermark state persisted before commit |
| `airflow/late_data_monitor_dag.py` | Hourly late ratio, event skew and stuck-watermark checks; triggers the late data integrator |

The self-tests replay the watermark progression table (30-minute allowed lateness): `python <file> --self-test`.
Prerequisites: PySpark 3.5 + `delta-spark`, `spark-sql-kafka-0-10`, `kafka-python`, Airflow 2.6+.
