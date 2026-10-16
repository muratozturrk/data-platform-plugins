# Data Overwrite -- examples

Each run replaces the data it owns, so retries and backfills are safe.

| File | Shows |
|---|---|
| `spark/spark_data_overwrite.py` | Delta `replaceWhere` on the input partition column (`_ingestion_date`), empty-source guard, `overwriteSchema=false`, audit row, time-travel rollback helper. |
| `spark/spark_dynamic_partition_overwrite.py` | `partitionOverwriteMode=dynamic` backfill: only partitions present in the output are replaced; empty-output guard. |
| `kafka/kafka_topic_overwrite_replay.py` | Adaptation for an append-only log: re-publish corrected records under the same key to a compacted topic, then move the stopped downstream group to the first corrected offset. |
| `airflow/data_overwrite_dag.py` | Daily DAG with retries: Spark `replaceWhere` for `{{ ds }}`, PostgreSQL `DELETE` + `INSERT ... SELECT` in one transaction, BigQuery `WRITE_TRUNCATE` snippet. |

Run: `spark-submit --packages io.delta:delta-spark_2.12:3.2.0 spark/spark_data_overwrite.py --dates 2024-11-20 --reason fix --dry-run`; `python airflow/data_overwrite_dag.py` runs the SQL-builder self-test.
Prerequisites: PySpark 3.5 + delta-spark 3.x, `kafka-python`, Airflow 2.x with the Spark, common-sql and Postgres providers. The Spark audit write reads `PG_PASSWORD` from the environment; Kafka reads `KAFKA_BROKERS`.
