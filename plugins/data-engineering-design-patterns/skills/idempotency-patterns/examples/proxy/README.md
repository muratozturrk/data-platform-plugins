# Proxy -- examples

Write-once versions of a full dataset behind a single access point.

| File | Shows |
|---|---|
| `airflow/immutable_tables_view_dag.py` | **Main implementation.** PostgreSQL view-based approach: `devices_internal_<dag_run.start_date>`, COPY, row-count validation, freeze (owner transfer + REVOKE writes), `CREATE OR REPLACE VIEW dp.devices`, revoke reads on the previous table, `max_active_runs=1`. |
| `spark/spark_versioned_tables_proxy.py` | All three approaches (view, manifest, versioned): `--mode view` (errorifexists table + view), `--mode manifest` (`--step load` then `--step manifest`, atomic temp+rename publish), `--mode versioned` (Delta overwrite + `VERSION AS OF`, long retention properties). |
| `kafka/kafka_versioned_topics_proxy.py` | Kafka adaptation: one `devices_v<run_start>` topic per snapshot (infinite retention), pointer record in a compacted topic, reader resolves the pointer and consumes the snapshot to its end offsets. |

Prerequisites: Airflow 2.x + `apache-airflow-providers-postgres` (connections `pg_dwh`, optional `pg_dwh_admin`), PySpark 3.5 + delta-spark 3.x, `confluent-kafka`.
Self-checks: `python airflow/immutable_tables_view_dag.py`, `python spark/spark_versioned_tables_proxy.py --self-test`, `python kafka/kafka_versioned_topics_proxy.py --self-test`.
