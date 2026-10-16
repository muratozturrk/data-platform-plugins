# Secretless Connector examples

| File | Shows |
|---|---|
| `spark/secretless_connector_job.py` | PostgreSQL JDBC with a client certificate and `sslmode=verify-full`, plus S3A using the IRSA or instance-profile credential providers |
| `kafka/mtls_client.py` | Mutual TLS producer and consumer. The certificate subject maps to the ACL principal |
| `airflow/identity_based_access_dag.py` | boto3 default chain with workload identity. Fails if static keys are present, logs the caller identity, then lists S3 objects |

Prerequisites: a PKI or CA issuing client certificates, PostgreSQL with `hostssl ... cert` in `pg_hba.conf`, a Kafka SSL listener with `ssl.client.auth=required`, an IAM role bound to the workload (IRSA, task role or instance profile).
Env vars: `JDBC_URL`, `DB_USER`, `PG_SSL_*`, `OUTPUT_PATH`, `KAFKA_BOOTSTRAP`, `KAFKA_CA`, `KAFKA_CLIENT_CERT`, `KAFKA_CLIENT_KEY`, `VISITS_BUCKET`.
