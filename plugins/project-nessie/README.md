# project-nessie

Project Nessie (0.108.x) as the Iceberg catalog of the HKM data platform. **Nessie is the chosen catalog**; the `apache-polaris` plugin in this marketplace is kept for reference and for the migration source (see `nessie-cli-api-and-migration`). Do not install both as "active" guidance for new work.

| Kind | Name | Purpose |
|---|---|---|
| Agent | `nessie-platform-architect` | Repository/branch layout, Nessie Catalog + object-store security, deployment, CEL authorization with Ranger, engine configs, GC/erasure, Polaris migration |
| Agent | `nessie-config-reviewer` | PR review of Nessie config, rules, engine catalogs, GC jobs |
| Command | `/design-nessie-platform` | Full design with Helm values, properties, rules, engine configs, GC, migration |
| Command | `/review-nessie-config` | Severity-ranked config review |
| Skill | `nessie-catalog-fundamentals` | Repository, references, commits, keys, content types, conflicts; Nessie vs Polaris/HMS/Lakekeeper |
| Skill | `nessie-iceberg-rest-catalog` | Nessie Catalog endpoint, warehouses, S3 signing vs vended credentials, client configs |
| Skill | `nessie-deployment` | Helm/Quarkus, JDBC/PostgreSQL version store, HA, OIDC, OTel, backups, upgrades |
| Skill | `nessie-authentication-authorization` | Keycloak OIDC, CEL rule set per institution, S3 signing rule, protected main, Ranger combination |
| Skill | `nessie-engine-integrations` | Trino (nessie vs rest), Spark extensions, Flink, ClickHouse, dbt, PyIceberg, compatibility matrix |
| Skill | `nessie-branching-workflows` | Branch-per-institution, write-audit-publish, release tags, rollback, multi-table commits |
| Skill | `nessie-gc-and-retention` | nessie-gc identify/expire/sweep, cut-offs, CronJob, KVKK erasure procedure with crypto-shredding evidence |
| Skill | `nessie-cli-api-and-migration` | nessie-cli, REST v2, export/import, migration from Polaris/HMS by metadata location |

`examples/`: Helm values and `application.properties` for ObjectScale, CEL authorization rules, engine configs (Trino native/REST, Spark, Flink, ClickHouse, PyIceberg), GC CronJob, WAP promotion helper, Polaris→Nessie migration script, branch-protection fitness test.

## Sources
See `skills/nessie-catalog-fundamentals/references/sources.md` (Nessie docs and releases 0.106–0.108, Helm chart, Trino/ClickHouse/Iceberg docs, community walkthroughs, Iceberg/Polaris books).
