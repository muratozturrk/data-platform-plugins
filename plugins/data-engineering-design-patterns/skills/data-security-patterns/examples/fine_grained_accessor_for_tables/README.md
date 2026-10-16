# Fine-Grained Accessor for Tables examples

| File | Shows |
|---|---|
| `airflow/postgres_fine_grained_access_dag.py` | Idempotent PostgreSQL column `GRANT SELECT(cols)`, RLS policy `login = current_user`, a guard view fallback, and a verification step against `information_schema` |
| `spark/unity_catalog_mask_and_filter.py` | Databricks Unity Catalog column mask (`is_account_group_member`) + `ROW FILTER` function via `spark.sql` |
| `kafka/README.md` | Why Kafka has no column/row ACLs, and what to do instead (split topics, sink to a store with RLS) |

Prerequisites: PostgreSQL + `apache-airflow-providers-postgres` + connection `dp_postgres_admin`; Databricks with Unity Catalog for the Spark file.
Env vars: `UC_CATALOG`, `UC_SCHEMA`.
