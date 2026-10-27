# Sources

## Books
- Alex Merced. *Architecting an Apache Iceberg Lakehouse: A scalable, open-source data platform*. Manning, 2026. Architecture guide for modular Iceberg lakehouses with Spark, Flink, Dremio, Polaris.
- Alex Merced, Andrew Madson, Tomer Shiran. *Apache Polaris: The Definitive Guide*. O'Reilly, 2025.
- Tomer Shiran, Jason Hughes, Alex Merced. *Apache Iceberg: The Definitive Guide*. O'Reilly, 2024.
- Denny Lee et al. *Delta Lake: The Definitive Guide*. O'Reilly, 2025 (alternative table format).
- Bas Harenslak, Julian de Ruiter. *Data Pipelines with Apache Airflow*, 2nd ed. Manning.
- Joe Reis, Matt Housley. *Fundamentals of Data Engineering*. O'Reilly, 2022 (data engineering lifecycle, undercurrents: security, governance, orchestration).
- Mark Richards, Neal Ford. *Fundamentals of Software Architecture*, 2nd ed. (microkernel; see plugin-architecture skill).

## Vendor / project documentation
- Azure Databricks architecture overview: control plane vs compute plane; classic compute plane runs in the customer's Azure subscription. https://learn.microsoft.com/azure/databricks/getting-started/overview
- Azure Data Factory concepts: pipeline, activity, dataset, linked service, integration runtime (`connectVia`), trigger — JSON resource model. https://learn.microsoft.com/azure/data-factory/concepts-linked-services
- Stackable Data Platform: operators (Kafka, Spark, Airflow, Trino, Superset, NiFi, Druid, HBase, OpenSearch, Hive, OPA), `stackablectl`, end-to-end security demo with Keycloak + OPA, release 26.3. https://docs.stackable.tech , https://github.com/stackabletech
- Apache Polaris (ASF top-level project, Feb 2026): Iceberg REST catalog, credential vending. https://polaris.apache.org
- Lakekeeper (Rust REST catalog), Project Nessie (Git-style catalog), Unity Catalog OSS (Apache 2.0 since June 2024), Apache Gravitino (federated catalog of catalogs).
- Iceberg REST catalog comparison guides: RisingWave "Iceberg Catalog Comparison", e6data "Iceberg Catalogs 2025", LakeOps "Apache Polaris: Deploy a Production Iceberg REST Catalog".
- Trino security: file-based, OPA and Ranger system access control; Trino blog "Open Policy Agent for Trino arrived" (Trino 438). https://trino.io/docs/current/security/overview
- Apache Kafka operations, Multi-Tenancy chapter: user spaces via topic prefixes, prefix ACLs (KIP-290), client/server quotas, controller mutation quotas (KIP-599). https://kafka.apache.org/documentation/#multitenancy
- Conduktor, "How to approach multi-tenancy in Kafka": prefixes + ACLs + quotas vs cluster per tenant vs virtual clusters at the proxy layer.
- Strimzi: `KafkaUser` ACLs with `prefix` pattern type and quotas. https://strimzi.io
- Keycloak Organizations feature (preview KC 25, GA KC 26) vs realm-per-tenant; Phase Two keycloak-orgs extension. https://www.keycloak.org/docs/latest/server_admin/#_managing_organizations
- OpenMetadata: domains, data products, column-level lineage from SQL/dbt/Airflow/Spark, OpenLineage endpoint; OpenLineage spec (jobs, runs, datasets). https://docs.open-metadata.org
- ClickHouse access control: SQL RBAC, row policies, quotas, settings profiles for multi-tenant use. https://clickhouse.com/docs/operations/access-rights
- Apache Superset: embedded dashboards SDK, row level security. https://superset.apache.org/docs
- Backstage software templates (golden paths). https://backstage.io
