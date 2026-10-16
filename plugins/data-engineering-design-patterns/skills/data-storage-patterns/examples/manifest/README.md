# Manifest examples

| File | Shows |
|------|-------|
| `spark/spark_manifest.py` | Delta `generate('symlink_format_manifest')` for external engines; producer-written JSON manifest and a reader that loads only listed files |
| `kafka/kafka_manifest_publisher.py` | One manifest message per run on a compacted topic; fan-out readers load listed files instead of listing storage |
| `airflow/manifest_dag.py` | Frozen per-run S3 manifest (created once, reused on retries) + Redshift `COPY ... MANIFEST` in a transaction |

Prerequisites: `pip install pyspark delta-spark confluent-kafka`; Airflow 2.x + amazon provider,
`aws_default` / `redshift_default` connections and `REDSHIFT_COPY_ROLE_ARN` env var.
BigQuery external table over the Delta manifest: `file_set_spec_type = 'NEW_LINE_DELIMITED_MANIFEST'`.
