# Horizontal Partitioner examples

| File | Shows |
|------|-------|
| `spark/spark_horizontal_partitioner.py` | `partitionBy(year, month, day, hour)` on Delta, partition pruning on read, `replaceWhere` partition overwrite |
| `kafka/kafka_range_partitioner.py` | Custom key-range routing (Python port of a Java `RangePartitioner`) |
| `airflow/horizontal_partitioner_dag.py` | Daily DAG creating PostgreSQL `PARTITION BY RANGE(event_time)` child tables and loading one day per run |

Prerequisites: `pip install pyspark delta-spark confluent-kafka apache-airflow apache-airflow-providers-postgres`,
a local Kafka broker (`KAFKA_BOOTSTRAP`), and an Airflow connection `dp_postgres` (password from env, never hardcoded).

Run: `python spark/spark_horizontal_partitioner.py /tmp/out`, `python kafka/kafka_range_partitioner.py`,
copy the DAG into your `dags/` folder.
