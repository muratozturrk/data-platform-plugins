# project-nessie – examples

- `helm-values.yaml` – production Nessie (2 replicas, JDBC/PostgreSQL, Keycloak OIDC, authorization, OTel, S3 warehouses for ObjectScale)
- `application.properties` – equivalent Quarkus config (for reference / advancedConfig)
- `authorization-rules.properties` – platform CEL rule set (admin, control plane, institution read/write, S3 signing, main promotion)
- `engines/trino-t_acme_lake.properties` (native nessie type), `engines/trino-t_acme_lake-rest.properties` (REST + vending), `engines/spark-defaults.conf`, `engines/flink-catalog.sql`, `engines/clickhouse-catalog.sql`, `engines/pyiceberg.yaml`
- `k8s/nessie-gc-cronjob.yaml` – nightly GC with cut-off policies and dry-run flag
- `wap_promote.py` – write-audit-publish helper using the REST API v2 (create WAP branch, run audits, merge, tag release, delete)
- `migrate_from_polaris.py` – register tables from a Polaris catalog into a Nessie branch by metadata location
- `test_branch_protection.py` – fitness test: institution role cannot commit to main, cannot see other institution's branch
