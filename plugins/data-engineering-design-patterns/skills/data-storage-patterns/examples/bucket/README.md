# Bucket examples

| File | Shows |
|------|-------|
| `spark/spark_bucket.py` | `bucketBy(8, "user_id").saveAsTable(...)` on two tables, bucket pruning in `explain()`, join plan without `Exchange` |
| `kafka/kafka_copartitioning_check.py` | Topic partitions as buckets: verifies two topics are co-partitioned and that the same key lands in the same partition number |
| `airflow/bucket_dag.py` | Athena `CLUSTERED BY ... INTO 50 BUCKETS` (Spark bucketing format), Spark job submission, bucket-count drift guard |

Prerequisites: `pip install pyspark confluent-kafka`; Airflow 2.x with amazon + apache-spark providers and AWS
credentials from the environment/connection. Run `python spark/spark_bucket.py` locally (uses a local
`spark-warehouse/`), `python kafka/kafka_copartitioning_check.py visits users` against a broker.
