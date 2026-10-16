# Offline Observer examples

| File | Shows |
|---|---|
| `airflow/offline_observer_dag.py` | Nightly observation DAG decoupled from the producer: SqlSensor, idempotent observation state (first/last row id), per-column invalid counts into a monitoring table. |
| `spark/spark_streaming_offline_observer.py` | Independent Structured Streaming observer with its own checkpoint: NULL metrics to Delta, optional ydata-profiling report on a sample, producer lag from its checkpoint vs latest Kafka offsets. |
| `kafka/kafka_offline_observer.py` | Standalone consumer group computing per-window metrics, unknown upstream fields, and the processing group's lag (watermark - committed). |

Prerequisites: Airflow 2.x with common-sql/postgres providers and `dp_postgres`; Spark 3.5 + spark-sql-kafka + delta-spark (+ `confluent-kafka`, optional `ydata-profiling`); `confluent-kafka` and a `visits` topic.
