# Pseudo-Anonymizer examples

| File | Shows |
|---|---|
| `spark/pseudo_anonymizer_job.py` | `mapInPandas` country generalization and SSN masking, salary ranges through a CASE column, and a real HMAC-SHA256 pandas UDF for the email (key kept out of the query plan). `--self-test` checks the pure functions |
| `kafka/pseudonymizing_relay.py` | Per-record HMAC-SHA256 pseudonyms (stable, joinable, non-reversible without the key) and phone masking. Run `python pseudonymizing_relay.py test` for an offline self-check |
| `airflow/tokenization_vault_dag.py` | Tokenization: random tokens stored in a restricted PostgreSQL vault table, then a tokenized copy published for sharing |

Prerequisites: `pyspark`, `pandas`, `pyarrow`, `confluent-kafka`, `apache-airflow-providers-postgres` (connections `dp_vault` and `dp_postgres`).
Env vars: `PSEUDO_HMAC_KEY` (from a secrets manager), `KAFKA_BOOTSTRAP`.
