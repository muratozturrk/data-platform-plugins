# Zero-ETL Synchronizer - examples

| File | Shows |
|------|-------|
| `kafka/README.md` | Configuration only: Aiven Iceberg topics (tiered storage RSM), Bufstream archive, Confluent Tableflow |
| `spark/synced_table_housekeeping.py` | Snapshot freshness check, `rewrite_data_files` / `expire_snapshots` / `rewrite_manifests`, transformation into a NEW table |
| `airflow/housekeeping_dag.py` | Nightly orchestration of the housekeeping job |

There is no pipeline code by design: the sync is configured, not written.
Prerequisites: an Iceberg REST catalog (`http://localhost:8181`), S3/MinIO warehouse, Spark 3.5 + Iceberg 1.6.
