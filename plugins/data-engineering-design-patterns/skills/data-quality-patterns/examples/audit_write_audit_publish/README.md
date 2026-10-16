# Audit-Write-Audit-Publish examples

| File | Shows |
|---|---|
| `airflow/awap_dag.py` | Batch AWAP: input audit (size, lines, JSON) -> transform to staging CSV -> output audit (NULLs, duplicates) -> atomic publish to PostgreSQL. |
| `spark/spark_streaming_awap_staging.py` | Staging-based streaming AWAP on Delta Lake: staging job + audit job with `is_valid` row rule, ratio-based fail, valid/invalid dispatch. |
| `kafka/kafka_window_awap.py` | Window-based streaming AWAP in a consumer: buffer, audit, publish or dispatch, commit offsets after publish. `--self-check` runs audit asserts offline. |

Prerequisites: Airflow 2.x + `apache-airflow-providers-postgres` + pandas; Spark 3.5 + delta-spark + spark-sql-kafka; `confluent-kafka`; a Kafka broker with a `visits` topic (JSON). Configure via env vars listed in each file's docstring.
