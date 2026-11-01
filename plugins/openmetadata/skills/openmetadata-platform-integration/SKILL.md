---
name: openmetadata-platform-integration
description: "Connecting OpenMetadata 1.13.3 to a typical on-prem or air-gapped data platform (Kafka + Schema Registry, Flink, NiFi, Airflow, dbt, Spark, MinIO + Iceberg, Trino, ClickHouse, Great Expectations, Ranger, Vault): which connector exists per component and what it really extracts; how lineage is produced (query-log parsing, cross-database lineage, Airflow inlets/outlets, dbt, Spline, OpenLineage over HTTP or Kafka, manual API edges); how ingestion workflows are written, run and scheduled and how secrets are handled; and source-verified gaps with workarounds: no Iceberg connector, no Spark producer in the repo, Flink and NiFi without dataset lineage, empty Ranger extension point, no Vault provider, no Kafka event destination. Use when planning or reviewing OpenMetadata ingestion for such a stack, writing workflow YAML, debugging missing lineage, choosing a lineage method per engine, or covering a component OpenMetadata cannot see."
---

# OpenMetadata Platform Integration

Everything below is pinned to **OpenMetadata 1.13.3** (tag `1.13.3-release`, commit `255f6694913b`),
verified in the source tree; DataHub v1.7.0 and Apache Atlas 2.5.0 appear only for comparison.
Paths are repo-relative to the OpenMetadata source. Evidence types: **K** = verified in source code,
**D** = official docs, **İ** = issue/roadmap. "Not found in 1.13.3 (searched: …)" means a repo
search came up empty — a weaker claim than "unsupported". Architecture, entity model and secrets
internals are in `openmetadata-architecture`; the full prioritised gap list in `openmetadata-gap-closing`.

## When to Use This Skill

- Planning which OpenMetadata services and ingestion pipelines a data platform needs.
- Writing or reviewing workflow YAML (metadata, lineage, usage, profiler, dbt, test suite).
- Lineage is missing or broken between two systems and you need to know which mechanism should produce it.
- Choosing between query-log parsing, OpenLineage, Spline, Airflow inlets/outlets or manual API edges.
- A component (Iceberg, Spark, Ranger, Vault) has no or a partial connector and you need a workaround.
- Running ingestion in an air-gapped network (scheduler choice, images, secrets).

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| What OM extracts from each stack component | Coverage matrix + capability flags | [connector-matrix.md](references/connector-matrix.md) |
| Table/column lineage from SQL engines | `<engine>-lineage` source on query history | [lineage.md](references/lineage.md) |
| Trino tables ↔ the same tables in ClickHouse/PostgreSQL | `processCrossDatabaseLineage` + `crossDatabaseServiceNames` | [lineage.md](references/lineage.md) |
| Spark / Flink / custom jobs lineage | OpenLineage → `POST /api/v1/openlineage/lineage` or Kafka topic | [lineage.md](references/lineage.md) |
| Airflow task lineage | `inlets` / `outlets` with table or container FQNs | [lineage.md](references/lineage.md) |
| Impact analysis, CSV export | `GET /api/v1/lineage/getLineage` with depths, `/export` | [lineage.md](references/lineage.md) |
| Write and run a workflow | YAML `source / sink / workflowConfig` + `metadata ingest` | [ingestion-workflows.md](references/ingestion-workflows.md) |
| Schedule ingestion | Managed Airflow, K8s jobs, or own DAG with `MetadataWorkflow` | [ingestion-workflows.md](references/ingestion-workflows.md) |
| Keep passwords out of YAML | `${ENV}` expansion; `secretsManager` options | [ingestion-workflows.md](references/ingestion-workflows.md) |
| Iceberg on MinIO is invisible | Trino route now; dedicated connector later | [missing-connectors-and-workarounds.md](references/missing-connectors-and-workarounds.md) |
| PII tags in OM vs Ranger masking | Catalog→Ranger sync + drift report outside OM | [missing-connectors-and-workarounds.md](references/missing-connectors-and-workarounds.md) |
| React to catalog changes from Kafka/Flink | Webhook → bridge → Kafka | [missing-connectors-and-workarounds.md](references/missing-connectors-and-workarounds.md) |

## Coverage at a Glance

| Component | OM 1.13.3 | Extracts | Lineage produced by |
|---|---|---|---|
| Kafka + Schema Registry | built-in `kafka` (registry inside it) | topics, config, Avro/Protobuf/JSON schemas, samples | not by this connector (Kafka Connect, OpenLineage, API) |
| Flink | built-in `flink` (REST API) | jobs as pipelines, tasks, run status | **none** — lineage method is empty (`flink/metadata.py:125-128`) |
| NiFi | built-in `nifi` | process groups as pipelines, processors, status | **pipeline→pipeline only**; dataset lineage open (issue #6950) |
| Airflow | built-in `airflow` + lineage backend/operator | DAGs, tasks, status, owners | DAG `inlets`/`outlets` (`PipelineLineage`) |
| dbt (+ Spark adapter) | built-in `dbt` (manifest/catalog/run_results) | models, descriptions, tags, owners, tests + results | `DbtLineage` from the manifest |
| Spark | **schema only** (`sparkConnection.json` has no fields) | — | Spline connector, OpenLineage, or the separate OM agent (not in repo) |
| MinIO / S3 | built-in `s3` storage + `datalake` | buckets/containers via manifest; file schemas | none (paths are containers) |
| Iceberg | **not found** (searched `*iceberg*` in `ingestion/src`, `openmetadata-spec/src`) | only indirectly via Trino (`TableType.Iceberg`) | via Trino query log / OpenLineage |
| Trino | built-in `trino` | schema, usage, lineage, profiler, dbt, data diff | query history `system.runtime.queries` |
| ClickHouse | built-in `clickhouse` | schema, usage, lineage, profiler, dbt | `system.query_log` |
| PostgreSQL | built-in `postgres` | schema, usage, lineage, profiler, dbt, data diff | `pg_stat_statements` |
| Great Expectations | checkpoint action (GE 0.18 and 1.x) | validation results as OM test cases | — |
| Ranger | `securityService` scaffolding only, no connector | — | — |
| Vault | not a secrets-manager option | — | — |

Score: 11 of 13 platform components covered by a built-in connector (T4); DataHub covers 13/13
(Iceberg built-in, Spark/Airflow as separate modules), Atlas 4/13. Details and evidence:
[connector-matrix.md](references/connector-matrix.md).

## How Lineage Is Produced

Every edge records its origin in `lineageDetails.source` (`entityLineage.json:70`): `Manual`,
`ViewLineage`, `QueryLineage`, `PipelineLineage`, `DashboardLineage`, `DbtLineage`, `SparkLineage`,
`OpenLineage`, `ExternalTableLineage`, `CrossDatabaseLineage`, `ChildAssets`. Three families:

1. **Parse** — SQL from query history, view definitions, a CSV query log (`queryLogFilePath`) or one
   statement via `metadata lineage`; parsers sqlglot → sqlfluff → sqlparse (`queryParserConfig.json:12-16`),
   column lineage included (K, `lineage/parser.py:24-96`). Static; misses non-SQL jobs.
2. **Runtime** — Airflow inlets/outlets, dbt manifests, Spline execution plans, Kafka Connect configs.
3. **OpenLineage** — OM *consumes* events (HTTP endpoint or Kafka/Kinesis connector), including the
   columnLineage facet; it is not an OpenLineage producer (D3.5 = 2, K).

Not found in 1.13.3: per-edge confidence, broken-chain detection, the `function` (transformation
expression) being filled by any built-in extractor, "what-if" simulation (D3.1–D3.4 notes).

## Minimal Workflow Shape

```yaml
source:
  type: trino                     # trino-lineage / trino-usage for the other pipelines
  serviceName: lake_trino
  serviceConnection:
    config: {type: Trino, hostPort: trino:8080, username: om_ingest, catalog: iceberg}
  sourceConfig:
    config: {type: DatabaseMetadata}
sink: {type: metadata-rest, config: {}}
workflowConfig:
  openMetadataServerConfig:
    hostPort: http://openmetadata:8585/api
    authProvider: openmetadata
    securityConfig: {jwtToken: "${OM_BOT_JWT}"}   # expanded from env by the CLI loader
```

Run with `metadata ingest -c trino.yaml` (verified commands in `cmd.py:46-56`). More shapes in
[ingestion-workflows.md](references/ingestion-workflows.md).

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Assuming "has a connector" means "has lineage" | Flink yields none, NiFi only pipeline→pipeline, Kafka none | Check the lineage column; add OpenLineage or API edges |
| Pointing `datalake`/`s3` at an Iceberg warehouse | Catalogues raw data/metadata files, not tables or snapshots | Catalogue Iceberg through Trino; plan a connector |
| Spark lineage via Spline into MinIO paths | Spline source resolves only `jdbc` and `dbfs` datasources (`spline/metadata.py:135-160`) | OpenLineage with symlink facets, or explicit edges |
| OpenLineage events before OM has the schemas | Auto-create needs an existing schema (`OpenLineageEntityResolver.java:426`) | Ingest metadata first; set `namespaceToServiceMapping` |
| dbt run before the warehouse service is ingested | dbt lineage only attaches to tables already in `serviceName` (`dbt/metadata.py:790`) | Order: metadata → dbt → lineage/usage |
| Trino lineage with default query history only | `system.runtime.queries` is in-memory and short-lived | Schedule often or set `queryHistoryTable` to a persisted log |
| Treating `PII.Sensitive` tags as enforced masking | OM does not push to Ranger | Sync/drift job (see workarounds) |
| JWTs and passwords in committed YAML | Example files embed tokens | `${ENV}` placeholders, bot tokens, secrets manager |
| Expecting OM to emit change events to Kafka | Subscription types: Webhook, Slack, MsTeams, GChat, Email, ActivityFeed, GovernanceWorkflowChangeEvent | Custom `AbstractEventConsumer` via subscription `className`, or webhook → small bridge → Kafka |

## References

- [references/connector-matrix.md](references/connector-matrix.md): per-component connector, config keys, extracted metadata, lineage method, evidence, DataHub/Atlas comparison.
- [references/lineage.md](references/lineage.md): lineage model, query-log parsing, cross-database, Airflow, dbt, Spline, Kafka Connect, OpenLineage resolution rules, manual edges, impact analysis.
- [references/ingestion-workflows.md](references/ingestion-workflows.md): workflow anatomy, verified YAML per component, CLI, scheduling options, secrets, air-gapped notes, custom connectors.
- [references/missing-connectors-and-workarounds.md](references/missing-connectors-and-workarounds.md): Iceberg/MinIO, Spark, Flink, NiFi, Ranger, Vault, Kafka events — routes and effort.
- [references/sources.md](references/sources.md): research basis and source locations.
- Related skills: `openmetadata-architecture` (server, entity model, custom properties, secrets, licensing), `openmetadata-gap-closing` (impact × ease of each gap), `openmetadata-kvkk-compliance` (why the Iceberg and Ranger gaps matter for erasure and masking evidence), `openmetadata-governance-assessment` (D3/D5/D10/D11 scoring), `openmetadata-capability-design` (new agentic capabilities such as policy-drift agents).
