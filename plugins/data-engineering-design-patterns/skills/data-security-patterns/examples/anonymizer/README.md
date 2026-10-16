# Anonymizer examples

| File | Shows |
|---|---|
| `spark/anonymizer_job.py` | `drop` of PII columns, Faker `pandas_udf` synthetic emails and names, IP and age perturbation |
| `kafka/anonymizing_relay.py` | Consume `visits_raw`, keep allowlisted fields, perturb the IP, fake the email, produce to `visits_shared`, then commit. Run `python anonymizing_relay.py test` for an offline self-check |
| `airflow/anonymized_export_dag.py` | Weekly partner export: extract, anonymize (removal, synthesis, coarsening), leak validation, publish |

Prerequisites: `pyspark`, `pandas`, `pyarrow`, `faker`, `confluent-kafka`, `apache-airflow-providers-postgres`.
Env vars: `INPUT_PATH`, `OUTPUT_PATH`, `KAFKA_BOOTSTRAP`, `EXPORT_DIR`, `STAGING_DIR`.
