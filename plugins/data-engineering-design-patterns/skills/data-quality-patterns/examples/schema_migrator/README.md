# Schema Migrator examples

| File | Shows |
|---|---|
| `spark/spark_delta_schema_migrator.py` | Delta rename in three phases: add + backfill `referral`, dual-write during grace period, drop `from_page` with column mapping after the deadline. |
| `kafka/kafka_schema_migrator_producer.py` | Avro type change (flat `login/email/age` -> `user` struct): transition schema carries both, final schema drops the defaulted deprecated fields under non-transitive FULL. |
| `airflow/schema_migrator_dag.py` | PostgreSQL migration DAG: add column, keep it in sync, branch on the deadline Variable before dropping the old column. |

Prerequisites: Spark 3.5 + delta-spark; `confluent-kafka[avro]` with Kafka + Schema Registry; Airflow 2.x with common-sql/postgres providers. The Airflow drop task coexists with the DDL guard from `schema_compatibility_enforcer`: it runs on connection `dp_postgres_migrator` (user in role `dp_schema_migrator`) and issues `SET LOCAL dp.schema_migration = 'contract'` in the same transaction as the DROP; the guard lets breaking DDL through only when both hold.
