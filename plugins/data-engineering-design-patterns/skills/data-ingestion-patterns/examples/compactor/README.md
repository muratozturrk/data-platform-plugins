# Compactor examples

- `spark/spark_compactor_delta_optimize.py` — Delta `OPTIMIZE` (optional `ZORDER BY`) per target with before/after file stats; optional `--vacuum` (retention of at least 7 days).
- `spark/spark_compactor_binpack.py` — plain Parquet: scan partitions, rewrite only those with a small average file size via `coalesce(n)` into a staging dir, then swap; supports `--dry-run`.
- `kafka/kafka_topic_compaction_setup.py` — Admin API: `cleanup.policy=compact` (and `compact,delete`), `min.compaction.lag.ms`, and related settings.
- `airflow/compactor_dag.py` — nightly OPTIMIZE, VACUUM only on Sundays (BranchPythonOperator), post-compaction file-health check.

Prerequisites: PySpark + `delta-spark`, `kafka-python`, Airflow 2.x with the Spark provider.
Run: `spark-submit spark/spark_compactor_binpack.py --help`.
