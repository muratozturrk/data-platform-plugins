# Transformation Replicator examples

Replicate production data to staging for realistic tests while PII never leaves production.

- `pii_rules.json` - the PII contract: one action per column (`keep`, `drop`, `hash`, `mask`, `nullify`, `expr`) and `default_action` (`fail` = every new column must be classified first, `drop` = allow-list).
- `spark/spark_transformation_replicator_batch.py` - prod Delta/JSON/CSV -> staging Delta: data reduction (`drop`), column-based transformations (`expr`, hash, mask), optional row-level mapping via `mapInPandas`, text sources read as strings, `replaceWhere` for idempotent partial refreshes, `--verify-only` leak check. `--self-test` runs without Spark.
- `kafka/kafka_transformation_replicator.py` - prod topic -> staging topic (confluent-kafka): same contract as a mapping function on the JSON value; key, headers, timestamp and partition preserved; unclassified/invalid records quarantined in prod; flush before commit. `--self-test` included.
- `airflow/transformation_replicator_dag.py` - push DAG in the prod Airflow: sensor on the prod readiness marker -> contract validation -> replicate -> verify -> staging marker.

Prerequisites: PySpark 3.4+ with `delta-spark` (and pandas/pyarrow for `--row-mapping`), `confluent-kafka`, Airflow 2.4+ with the Spark provider.
Secrets: `PII_HASH_SALT`, `PROD_BOOTSTRAP`, `STAGING_BOOTSTRAP` come from the environment / secret store.
