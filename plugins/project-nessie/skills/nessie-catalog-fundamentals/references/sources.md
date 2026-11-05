# Sources

## Documentation
- projectnessie.org — Overview, Features (branches/tags, commits, merge/transplant, time travel, metadata authorization), Server configuration (version stores, OIDC, authorization rules, Nessie Catalog object-store settings), Iceberg REST ("Nessie Catalog"), Tools (nessie-cli, nessie-gc, export/import), Integrations (Trino, Spark, Flink, Dremio, ClickHouse, PyIceberg), Releases (0.106–0.108, 2026). https://projectnessie.org
- Nessie Helm chart README (`charts.projectnessie.org`): version store types, OIDC authentication values, authorization rules values, advancedConfig.
- Trino docs — Iceberg connector: Nessie catalog type (`iceberg.nessie-catalog.*`) and REST catalog type; Nessie docs "Trino with Iceberg REST" (S3 only, vending mandatory, OAuth2 token property).
- ClickHouse docs — "Nessie catalog" (Iceberg REST integration, July 2026).
- Apache Iceberg docs — Nessie catalog for Spark/Flink; Spark extensions (`nessie-spark-extensions`).
- Medium (nsalexamy), "Building a Production-Ready Data Lakehouse Locally: Iceberg, Nessie, Trino, Spark on Kubernetes" (native `nessie` type preferred over REST for Trino in practice).
- MinIO blog, "Migrate your entire Iceberg catalog to AIStor" (`mc table migrate --source-type nessie`) — shows cross-catalog migration patterns.

## Books
- Tomer Shiran, Jason Hughes, Alex Merced. *Apache Iceberg: The Definitive Guide*. O'Reilly, 2024 — catalog chapter covers Nessie, branching and WAP.
- Alex Merced. *Architecting an Apache Iceberg Lakehouse*. Manning, 2026 — catalog choice, Nessie/Polaris, governance.
- Alex Merced, Andrew Madson, Tomer Shiran. *Apache Polaris: The Definitive Guide*. O'Reilly, 2025 — for the migration source.
