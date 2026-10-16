# Full Loader examples

Reload a complete, slowly changing dataset that has no change-tracking attribute.

- `spark/full_loader_delta.py` - JSON provider export with an explicit schema -> Delta `overwrite` (transactional, readers keep the old version until commit); guards against empty or shrunken extracts; `--rollback-to N` uses `RESTORE` (time travel). `--self-test` included.
- `airflow/full_loader_versioned_view_dag.py` - PostgreSQL: `COPY` into `devices_<version>`, validate, `CREATE OR REPLACE VIEW devices`, keep the last 3 versions for rollback.
- `kafka/full_loader_initial_full_load.py` - one-shot bootstrap: read the whole table with psycopg2 and publish every row to a topic, keyed by primary key.
- `kafka/full_loader_production.json` - Kafka Connect JDBC source in `mode: bulk` (re-publishes the full table every poll, hourly here). Uses `${env:DB_PASSWORD}`, which needs `config.providers=env` on the Connect worker.

EL between identical stores needs no code: `aws s3 sync s3://input-bucket s3://output-bucket --delete`.
Prerequisites: PySpark 3.4+ with `delta-spark`, PostgreSQL + Airflow 2.4+ with the Postgres provider, `kafka-python`, `psycopg2-binary`.
Run: `spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark/full_loader_delta.py --input <path> --table-path <path>`.
