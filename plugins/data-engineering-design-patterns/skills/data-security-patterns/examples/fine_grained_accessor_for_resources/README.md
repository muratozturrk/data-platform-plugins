# Fine-Grained Accessor for Resources examples

| File | Shows |
|---|---|
| `kafka/least_privilege_acls.py` | AdminClient that creates LITERAL and PREFIXED ACLs, one principal per app, and lists the effective ACLs |
| `spark/per_bucket_assumed_role_job.py` | S3A per-bucket `AssumedRoleCredentialProvider`: a read-only role for input and a write-only role for output, with the matching IAM policy JSON |
| `airflow/scoped_roles_dag.py` | Each task uses its own `aws_conn_id` with a `role_arn`, plus a task that checks the writer role gets `AccessDenied` on the raw bucket |

Prerequisites: a Kafka broker with an authorizer (SASL_SSL), AWS IAM roles created by IaC, `hadoop-aws`, `apache-airflow-providers-amazon`.
Env vars: `KAFKA_BOOTSTRAP`, `KAFKA_SASL_USER`, `KAFKA_SASL_PASSWORD`, `INPUT_BUCKET`, `OUTPUT_BUCKET`, `INPUT_ROLE_ARN`, `OUTPUT_ROLE_ARN`, `RAW_BUCKET`, `EXPORT_BUCKET`.
