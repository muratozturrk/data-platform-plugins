---
name: control-plane-design
description: Design the control plane that turns a set of open-source data-plane components into a product: tenant/workspace model, ADF-style resource kinds (LinkedService, IntegrationRuntime, Dataset, Pipeline, Trigger), a manifest compiler that generates Airflow/Spark/dbt artifacts, reconciliation via Kubernetes CRDs, public API/CLI, metering and policy distribution. Use when designing or reviewing anything in the control plane, the manifest schema, the compiler, or tenant onboarding.
---

# Control Plane Design

The control plane is the only part of the platform that is yours. Databricks' control plane handles web app, orchestration, job scheduling and cluster management and never touches data; the compute plane does the work in the customer's account. ADF's control plane is a resource model (factory → linked services, datasets, pipelines, activities, triggers, integration runtimes) plus a compiler/executor behind it. Your control plane is the same idea over OSS engines.

## When to Use This Skill

- Defining the resource model and manifest kinds
- Designing the manifest compiler (manifest → Airflow DAG / Spark job / dbt project / Kafka Connect config)
- Designing tenant onboarding and reconciliation
- Designing the public API, CLI and UI contract
- Deciding what state the control plane owns vs what it delegates to engines

## Core Concepts

### Resource model (ADF-inspired, engine-agnostic)
| Kind | ADF analogue | Meaning on your platform |
|---|---|---|
| `Tenant` | Subscription/Entra tenant | Customer or business unit; isolation choices, plan, quotas |
| `Workspace` | Data Factory / Synapse workspace | Project boundary inside a tenant; owners, environments |
| `IntegrationRuntime` | IR (Azure / self-hosted) | A compute pool: Spark namespace, Airflow pool, Trino resource group, ClickHouse cluster, or a customer-hosted runtime |
| `LinkedService` | Linked service | A connection: engine type + secret ref + `connectVia: <IntegrationRuntime>` |
| `Dataset` | Dataset | A named, typed reference to data in a LinkedService (table, topic, path, Iceberg table) |
| `Pipeline` | Pipeline | DAG of activities (copy, transform, quality, notify) over Datasets |
| `Trigger` | Trigger | Schedule, event (Kafka topic / storage event), tumbling window, manual |
| `Policy` | Purview policy | Access, masking, retention, classification; compiled to OPA data + engine grants |
| `Plugin` | Connector | Registered SPI implementation with version and config schema |

Every kind: `apiVersion`, `kind`, `metadata{name, workspace, labels}`, `spec`, `status`. Tenant and environment come from context, never from the file.

### Compiler, not interpreter
The control plane compiles manifests into artifacts the engines already understand, then hands them to operators:
- `Pipeline` + `Trigger` → generated Airflow DAG file (or Dagster job) in the tenant's DAG folder
- transform activity with `plugin: spark-sql@v1` → SparkApplication CR for the Spark operator
- transform activity with `plugin: dbt@v1` → dbt project + profiles + run command
- copy activity → Kafka Connect connector config or Airbyte connection
- `Policy` → OPA bundle data + Polaris grants + ClickHouse row policies + Superset RLS
- `Dataset` → OpenMetadata entity + Iceberg table (if managed)
Compilation is deterministic, dry-runnable (`platform render`), and versioned; the generated artifact carries the manifest hash. Users never edit generated artifacts.

### Reconciliation
Model the control plane as controllers over CRDs (or an equivalent reconciler loop):
- `Tenant` controller → Keycloak org/realm, Vault policy+mount, storage container/prefix + vending config, Polaris catalog/namespace, Kafka prefix ACL + quota (Strimzi `KafkaUser`), Spark namespace + ResourceQuota, Airflow pool, Trino/OPA data, ClickHouse DB/role/quota, Superset workspace, OpenMetadata domain/team, Grafana org — idempotent, with `status.conditions`
- `Workspace` controller → folders, default IR, default LinkedServices, RBAC bindings
- `Pipeline` controller → compile, validate, publish artifact, register lineage, watch runs
Crossplane compositions are a reasonable way to express the tenant composition if you prefer not to write a controller.

### State ownership
Control plane owns: resource definitions, tenant/plan/quota, policy source, compiled-artifact versions, run index, usage ledger, audit. Engines own: data, runtime state, metrics. Never duplicate engine runtime state; index it.

### Public surface
`platform` CLI (thin, generated from OpenAPI), REST API `/v1/tenants/{t}/workspaces/{w}/<kind>`, SDK for plugin authors, manifest JSON Schemas, SPI. All five versioned (see api-versioning-sdk).

### Metering
Every run emits `UsageEvent{tenant, workspace, pipeline, runtime, dimension, amount, ts}` (compute-seconds, bytes read/written, rows, storage-bytes-days, API calls). The ledger is control-plane state; engines are instrumented via OpenTelemetry with tenant labels. Quotas are checked at submit time against the plan.

## Patterns

### Thin core, fat operators
The control plane never SSHes into engines or calls proprietary APIs directly; it writes CRs/configs that operators reconcile. This keeps engines replaceable and the core small.

### Golden path CLI
`platform tenant create`, `platform workspace init --template batch-ingest`, `platform pipeline validate|render|apply|run`, `platform plugin new --type source`. Backstage templates can front the same API for a UI.

### Environments without copies
`dev/test/prod` are overlays on the same manifests (config layering), deployed to different IRs; promotion is a Git merge plus `apply`, mirroring ADF's publish-from-Git flow.

### Customer-hosted runtime
Like ADF's self-hosted IR, allow an `IntegrationRuntime` of type `remote` registered with a token; the control plane ships compiled artifacts to it. This is the bridge model for customers whose data cannot leave their network.

## Anti-patterns
- Control plane embedding engine SDK calls (coupling to one Spark/Airflow version)
- Manifests that are thin wrappers around raw DAG code
- Tenant created in the UI but not in Kafka/Vault/catalog
- Usage derived from engine logs after the fact instead of emitted at run time
- Generated artifacts committed by hand and edited

See `references/sources.md`.
