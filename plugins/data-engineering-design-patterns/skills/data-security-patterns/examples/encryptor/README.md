# Encryptor examples

| File | Shows |
|---|---|
| `spark/encrypted_storage_job.py` | `sse` mode: S3A SSE-KMS server-side encryption. `parquet` mode: client-side Parquet modular (column + footer) encryption |
| `kafka/encrypted_producer_consumer.py` | TLS client config with hostname verification, and client-side Fernet payload encryption that supports key rotation through `MultiFernet` |
| `airflow/enforce_bucket_encryption_dag.py` | Daily audit/remediation of bucket SSE-KMS, a KMS key health check, and a deny-non-TLS statement **merged** into the existing bucket policy (other statements kept; `--self-test` checks the merge) |

Prerequisites: AWS KMS key + S3 bucket, `hadoop-aws`, a Kafka broker with a TLS listener, `confluent-kafka`, `cryptography`, boto3.
Env vars: `BUCKET(S)`, `KMS_KEY_ARN`, `PARQUET_FOOTER_KEY_B64`, `PARQUET_PII_KEY_B64`, `KAFKA_BOOTSTRAP`, `KAFKA_CA`, `PAYLOAD_KEY`, `PAYLOAD_OLD_KEYS`, `ENFORCE`.
The Parquet `InMemoryKMS` is for demos only. Use a real KMS client in production.
