---
name: oss-reference-architecture
description: Reference architecture for building an Azure-Data-Factory/Synapse-class data platform entirely from open-source components (Kafka, Spark, Iceberg + REST catalog, Trino, ClickHouse, Airflow, dbt, Keycloak, Vault, OPA/Ranger, OpenMetadata, Superset) on Kubernetes, with tenant isolation per component. Use when choosing components, mapping an Azure capability to an OSS equivalent, deciding build-vs-adopt (e.g. Stackable), or onboarding a new component into the platform.
---

# OSS Reference Architecture (Azure data platform → open source)

Goal: an open-source platform that offers what ADF/Synapse/Fabric offer — workspaces, linked services, pipelines, integration runtimes, triggers, governance, BI — without vendor lock-in, deployable on Azure (AKS), on-prem, or at a customer site.

## When to Use This Skill

- Choosing or replacing a data-plane component
- Mapping an Azure/Fabric capability to its OSS equivalent
- Deciding whether to adopt an existing distribution (Stackable) or write operators yourself
- Onboarding a new component: tenancy, identity, secrets, catalog, observability wiring

## Core Concepts

### The four planes
| Plane | Role | Owner | Replaceable? |
|---|---|---|---|
| **Control plane** | Tenants, workspaces, manifests, compiler, APIs, UI, metering, policy | You — this is the product | No |
| **Data plane** | Kafka, Spark, Trino, ClickHouse, Airflow, dbt, Flink… | Operators / vendors | Yes, per component |
| **Supervision plane** | OpenMetadata, OPA, Vault, OpenTelemetry/Prometheus/Grafana, audit | You wire, OSS implements | Mostly |
| **Substrate** | Kubernetes (AKS / OpenShift / on-prem), object storage (ADLS Gen2 / MinIO / S3) | Infra | Yes |

Databricks runs this way: the control plane (web app, orchestration, job scheduling, cluster management) is managed centrally and never processes data; the compute plane runs in the customer's cloud account. Copy the separation, not the vendor.

### Layer map: Azure → OSS → what you must write
| Capability | Azure | OSS candidate | Control-plane work (yours) |
|---|---|---|---|
| Identity | Entra ID | Keycloak (Organizations feature, GA since KC 26; realm-per-tenant only for hard isolation) | Tenant claim in every token; SCIM-style onboarding |
| Secrets | Key Vault | HashiCorp Vault / OpenBao (K8s auth, per-tenant policies & mounts) | `SecretProvider` SPI; tenant-scoped paths |
| Object storage | ADLS Gen2 / OneLake | ADLS Gen2 on Azure, MinIO/S3 elsewhere | `StorageProvider` SPI; silo/pool/bridge resolver |
| Table format + catalog | Fabric/Synapse tables | Apache Iceberg + REST catalog (Apache Polaris, TLP since Feb 2026; Lakekeeper for lightweight; Nessie for Git-style) with **credential vending** | Catalog-per-tenant provisioning; vending policy |
| Authorization | Entra RBAC, Purview policies | OPA (Trino, Kafka, Airflow, HDFS via Stackable authorizers) **or** Ranger (Trino/Spark/Kafka/Hive); OpenMetadata RBAC for metadata; ClickHouse row policies | One policy source of truth → per-engine adapters |
| Metadata / lineage | Purview | OpenMetadata (domains, data products, column lineage) + OpenLineage events from Spark/Airflow/dbt | Auto-register every manifest-created asset |
| Ingestion / CDC | ADF Copy, Dataflows | Kafka Connect + Debezium, Airbyte, dlt | `SourceConnector`/`SinkConnector` SPI, registry |
| Streaming | Event Hubs, Stream Analytics | Kafka (Strimzi), Flink or Kafka Streams | Topic prefix = tenant, prefix ACLs, quotas |
| Batch / transform | Synapse Spark, Dataflows | Spark (Spark operator), dbt, Trino | Manifest → Spark job / dbt project compiler |
| Orchestration | ADF pipelines & triggers | Airflow (or Dagster) | DAGs generated from `Pipeline` + `Trigger` manifests, never hand-written |
| Serving / OLAP | Synapse SQL, KQL DB | ClickHouse (Altinity operator), Trino | Tenant DB + role + quota + row policy |
| BI | Power BI | Superset (embedded SDK, RLS) | Workspace per tenant, embedded guest tokens |
| Quality | Purview DQ | Great Expectations / Soda | `quality:` block in manifest |
| Observability | Azure Monitor | OpenTelemetry, Prometheus, Grafana, Loki | Tenant label on every signal; per-tenant Grafana org |
| Deployment | ARM / Bicep | Kubernetes operators, Helm, ArgoCD, Crossplane | `Tenant` CRD + reconciler |
| Portal | ADF Studio / Fabric | Backstage (templates, catalog) or your own UI | Golden paths, self-service |

### Build vs adopt: Stackable
Stackable Data Platform (SDP) is an open-source distribution of Kubernetes operators for Kafka, Spark, Airflow, Trino, Superset, NiFi, Druid, HBase, OpenSearch, Hive Metastore, OPA and more, with a `stackablectl` CLI, Keycloak + OPA end-to-end security demos, OpenShift certification, and monthly-ish releases (26.3 supports K8s 1.31–1.35). Writing and maintaining 10+ operators yourself is the opposite of Thinnest Viable Platform. Default decision: **adopt Stackable operators for the data plane, write only the control plane**. Caveats to record in an ADR: Stackable's own operators are OSL-3.0 (reciprocal; evaluate for your commercial packaging), ClickHouse is not in SDP (use Altinity operator), and SDP has no tenant/workspace concept — that is exactly the gap your control plane fills.

## Patterns

### Tenancy per component (what "silo / pool / bridge" means concretely)
| Component | Pool | Bridge | Silo |
|---|---|---|---|
| Keycloak | Organization in shared realm | — | Realm per tenant |
| Vault | Policy + path prefix `tenants/{t}/` | Namespace (Enterprise) / separate mount | Separate Vault |
| Object storage | Prefix `tenants/{t}/` + vended creds | Container/bucket per tenant, shared account | Storage account per tenant |
| Iceberg catalog | Namespace per tenant in shared catalog | Catalog per tenant, shared Polaris | Polaris per tenant |
| Kafka | Prefix `t.<tenant>.` + prefix ACL + quotas | Node pool / virtual cluster per tenant | Cluster per tenant (Strimzi CR) |
| Spark | Namespace + ResourceQuota per tenant | Dedicated node pool | Cluster per tenant |
| Airflow | Pools + tags, DAG folder per tenant | Separate scheduler/workers | Airflow per tenant |
| Trino | Catalog/schema grants via OPA | Resource groups per tenant | Cluster per tenant |
| ClickHouse | Database + role + quota + row policy | Dedicated replicas | Cluster per tenant |
| Superset | RLS + workspace | — | Instance per tenant |
| OpenMetadata | Domain + team per tenant | — | Instance per tenant |
Kafka note: quotas cover bandwidth and request rate but not storage; prefix ACLs are deny-based and one bad rule exposes a topic. Treat Kafka tenancy as "prefix + ACL + quota + monitoring", or isolate at a proxy/virtual-cluster or cluster level for premium tiers.

### Credential vending as the storage security model
Engines never hold long-lived storage keys. The REST catalog authenticates the engine (OAuth2 via Keycloak), checks catalog grants, and vends short-lived scoped credentials (Azure SAS, STS, signed URLs) only for the tables the principal may read. Every engine that speaks the Iceberg REST protocol inherits the same governance.

### One policy, many enforcement points
Author access policy once (tenant, role, dataset, classification) in your control plane; publish to OPA as data; let each engine's OPA/Ranger adapter enforce. Audit logs flow back to one place. Do not let five engines each hold their own truth.

### Everything a manifest creates is registered
When the compiler materialises a pipeline it also: registers the dataset in OpenMetadata under the tenant's domain, emits OpenLineage from the generated Spark/Airflow/dbt jobs, creates the catalog namespace/table, applies the policy, and tags the metrics. If any step is manual, the platform is not a product yet.

### Install order (first wave, TVP)
1. Kubernetes + storage + ingress + cert-manager
2. Keycloak, Vault, OpenTelemetry/Prometheus/Grafana
3. Iceberg REST catalog (Polaris) with credential vending
4. Kafka (Strimzi), Spark operator, Airflow
5. OpenMetadata + OpenLineage wiring, OPA
6. **Your control plane** (Tenant CRD, manifests, compiler, API, CLI)
Second wave: Trino, ClickHouse, Superset, Flink, dbt, Great Expectations, Backstage.

## Anti-patterns
- Writing your own Kafka/Spark/Trino operators
- Hive Metastore as the catalog in 2026 (no REST, no vending, no multi-engine governance)
- Users hand-writing Airflow DAGs or dbt projects outside the manifest model
- Per-engine authorization with no central policy
- Tenant known only to the UI, not to the storage path, topic name, catalog, secret path and metric label

See `references/sources.md`.
