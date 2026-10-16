# Merger -- examples

CDC increments MERGEd on the immutable key `(type, version)` with soft deletes.

| File | Shows |
|---|---|
| `spark/spark_cdc_merge_soft_delete.py` | Main implementation. Delta MERGE: matched+deleted → DELETE (or `--soft-delete-target` flag), matched → UPDATE, not matched and not deleted → INSERT. Per-key dedup on `(change_ts, change_lsn)`, null-key guard, first-run table creation, `--dry-run`. |
| `kafka/kafka_cdc_merge_postgres.py` | Debezium-style topic → temp table `LIKE dp.devices` → PostgreSQL 15 `MERGE`, in one DB transaction. Kafka offsets are committed only after the DB commit. |
| `airflow/merger_dag.py` | FileSensor → one SQL task (`CREATE TEMPORARY TABLE ... LIKE`, `COPY`, dedup, `MERGE` with soft deletes) → duplicate-key check. `max_active_runs=1`. |

Self-tests without dependencies: `python <file> --self-test` (Spark and Kafka files).
Prerequisites: PySpark 3.5 + delta-spark 3.x; `confluent-kafka`, `psycopg2`, PostgreSQL 15+ (`PG_PASSWORD` from env); Airflow 2.6+ with `common-sql` and `postgres` providers and a `dp_postgres` connection.
