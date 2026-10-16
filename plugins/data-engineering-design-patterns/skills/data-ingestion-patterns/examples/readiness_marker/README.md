# Readiness Marker examples

Tell consumers when a dataset is complete; consumers pull.

- `airflow/readiness_marker_flag_file_dag.py` — flag files: producer DAG `delete_dataset >> generate_dataset >> create_readiness_file` (`COMPLETED`, last task, temp + rename); consumer DAGs with `FileSensor(mode="reschedule")` on `COMPLETED` and on Spark's own `_SUCCESS`.

- `spark/spark_readiness_marker_writer.py` — `ReadinessMarkerWriter`: after the data commit, write `_READY.tmp` with row count, schema hash and Delta version, then rename it to `_READY`; read/delete helpers.
- `spark/spark_readiness_marker_db.py` — DB marker table with `UNIQUE(dataset, partition_date)` and `ON CONFLICT DO UPDATE`; `list_ready_datasets()`.
- `kafka/kafka_readiness_marker.py` — readiness event on a compacted `pipeline.readiness` topic, key `dataset:partition_date`, `acks=all`.
- `airflow/readiness_marker_dag.py` — the producer DAG writes the marker as its last task; the consumer DAG uses a custom sensor (`mode="reschedule"`, `template_fields` so `{{ ds }}` renders) that also validates row count and schema hash.

Prerequisites: PySpark + `delta-spark`, PostgreSQL (`DB_PASSWORD` env var), `kafka-python`, Airflow 2.x.
