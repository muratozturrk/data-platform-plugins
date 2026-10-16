# Secrets Pointer examples

| File | Shows |
|---|---|
| `spark/secrets_pointer_job.py` | PostgreSQL tables to JSON partitions (`export_date=` overwrite, replay-safe). Credentials resolved by secret name (JSON or plain SecretString), TTL-cached, refreshed and retried once on SQLSTATE 28P01, redacted from logs. `--self-test` checks cache/refresh/redaction |
| `kafka/sasl_from_secrets_manager.py` | SASL/SCRAM consumer that caches credentials with a TTL, then invalidates and rebuilds the client on `_AUTHENTICATION` errors after a rotation |
| `airflow/secrets_backend_dag.py` | The DAG only references `conn_id` and Variable keys. `SecretsManagerBackend` resolves them (config is in the docstring) |

Prerequisites: AWS Secrets Manager secrets created by IaC or an admin, IAM `secretsmanager:GetSecretValue` on them, boto3, `confluent-kafka`, the Airflow amazon and postgres providers.
Env vars: `DB_SECRET_ID`, `DB_USER`, `JDBC_URL`, `TABLES`, `OUTPUT_DIR`, `EXPORT_DATE`, `KAFKA_SECRET_ID`, `SECRET_TTL_SECONDS`, `AWS_REGION`.
