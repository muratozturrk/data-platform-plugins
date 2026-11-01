---
name: openmetadata-architecture
description: "How OpenMetadata 1.13.3 is built and what can be extended, with source-code evidence: Java/Dropwizard server, React UI, Python ingestion; the two mandatory dependencies (MySQL or PostgreSQL, Elasticsearch or OpenSearch) and optional ones (Airflow pipeline client, Redis, RDF/Fuseki, secrets managers); deployment, footprint, HA, migrations, backup/DR; the JSON Schema entity model, custom-property types and limits, versioning and rollback, REST/OpenAPI, SDKs, metadata CLI, CSV import/export, change events; extension points (apps, plug-in classes on EXT_CLASSPATH, empty NoOp points reserved for the commercial edition), secrets providers (no Vault), what needs a fork, and the Apache-2.0 vs Collate licence split. Use when deploying OpenMetadata on-prem or air-gapped, deciding where a new capability should live, checking what OM can model or emit, planning backup or upgrades, or assessing licence risk."
---

# OpenMetadata Architecture and Extension Points

Everything here is pinned to **OpenMetadata 1.13.3** (`1.13.3-release`, commit `255f6694913b`) and
comes from a source-code study of that tag. Evidence codes: **K** = verified in source code,
**D** = official docs, **İ** = issue/RFC/roadmap, **B** = vendor claim; **K+** = verified in source
while writing this skill (not part of the original scored study). Paths are repo-relative; `SVC` =
`openmetadata-service/src/main/java/org/openmetadata/service/`, `SPEC` =
`openmetadata-spec/src/main/resources/json/schema/`. "Not found in 1.13.3 (searched: …)" is a
weaker claim than "absent"; read it that way. This is the foundation the other
`openmetadata-*` skills point to.

## When to Use This Skill

- Sizing and deploying OM on-prem or air-gapped: what must run, what can be switched off.
- Planning HA, upgrades, backup and DR for the catalog itself.
- Deciding whether a requirement fits the entity model (custom property) or needs code.
- Choosing where new code lives: app, plug-in class, ingestion connector, or a fork.
- Explaining why some schema/interface exists but does nothing (NoOp / schema-only).
- Integrating OM events with an event backbone (Kafka) or a secrets store (Vault).
- Judging licence exposure of contributions (`ingestion/` vs server core).

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Minimum runtime footprint | DB + search engine only; Airflow off via `NoopClient` | [components-and-deployment.md](references/components-and-deployment.md) |
| Run ingestion without Airflow | `K8sPipelineClient` + OMJob operator, or `metadata` CLI from your scheduler | [components-and-deployment.md](references/components-and-deployment.md) |
| Several server replicas | Shared DB + search; Quartz `isClustered=true` | [components-and-deployment.md](references/components-and-deployment.md) |
| Backup / DR | Back up the relational DB; rebuild search with `reindex` | [components-and-deployment.md](references/components-and-deployment.md) |
| Add a field to tables/columns/glossary terms | Custom property (17 types) | [entity-model-and-apis.md](references/entity-model-and-apis.md) |
| Add a new entity type | Not possible without changing the spec (`TypeResource.java:78-85`) | [entity-model-and-apis.md](references/entity-model-and-apis.md) |
| Bulk edit / round-trip metadata | CSV `/name/{name}/export` and `/import` (+ `Async`, `dryRun`) | [entity-model-and-apis.md](references/entity-model-and-apis.md) |
| React to catalog changes | Event subscription (webhook etc.) with offset + retry | [entity-model-and-apis.md](references/entity-model-and-apis.md) |
| Publish change events to Kafka | Custom `AbstractEventConsumer` via `className` (K+), or webhook bridge | [extension-points.md](references/extension-points.md) |
| Natural-language search with a local LLM | Implement `NLQService`, set `providerClass` | [extension-points.md](references/extension-points.md) |
| Scheduled server-side job | Internal app (`AbstractNativeApplication`) or external Python app | [extension-points.md](references/extension-points.md) |
| HashiCorp Vault for connector secrets | Not a provider; closed enum → fork, or keep `db` | [extension-points.md](references/extension-points.md) |
| Publishing your own code openly | Mind Apache-2.0 core vs CCL `ingestion/` | [extension-points.md](references/extension-points.md) |

## Components at a Glance

| Layer | 1.13.3 | Evidence |
|---|---|---|
| Server | Java 21, Dropwizard 5.0.0, embedded Jetty, Maven multi-module | `pom.xml:80,1067-1071` (K) |
| UI | React 18 + TypeScript 5 + Ant Design 4, built with Vite 7.3.5 (repo `CLAUDE.md` wrongly says Webpack) | `openmetadata-ui/.../package.json:15-16,262` (K) |
| Ingestion | Python package `openmetadata-ingestion` 1.13.3.0, CLI `metadata`; `requires-python >=3.9` (repo guide says 3.10–3.11) | `ingestion/pyproject.toml:8-9,16,36-37` (K) |
| Mandatory deps | (1) MySQL **or** PostgreSQL, (2) Elasticsearch **or** OpenSearch; no "none" search option | `conf/openmetadata.yaml:255-262,460-466`; `SPEC/configuration/elasticSearchConfiguration.json:96-99` (K) |
| Default-on dep | Airflow (`pipelineServiceClientConfiguration.enabled: true`, `AirflowRESTClient`) | `conf/openmetadata.yaml:543-547` (K) |
| Optional | Redis cache (`none`), RDF/Fuseki (`false`), external secrets manager (`db`), embedded `/metrics` (Prometheus format) | `conf/openmetadata.yaml:526-527,623-624,716-737` (K) |
| Not needed | Kafka, ZooKeeper (Kafka is only a connector) | T3 footnote om-opt (K) |

Rubric D12.1 (deployment footprint) = 3; DataHub v1.7.0 also needs only two (its Neo4j is not
mandatory in code/Docker defaults, though the Helm `prerequisites` chart still enables it);
Apache Atlas 2.5.0 needs four (HBase, Solr, Kafka, ZooKeeper).

## Entity Model in One Paragraph

The model is JSON Schema draft-07 in a separately versioned `openmetadata-spec` artefact; Java
and Python classes are generated from it (D1.3 = 3). 37 schemas carry `@om-entity-type`
(tables, columns, dashboards, pipelines, topics, containers, ML/AI assets, glossary, domains,
data products, contracts…); these and only these can take **custom properties** (D1.2 = 2):
17 typed property types, validated, searchable as facets, exported in CSV — but **no
"required" flag**, no custom properties on users/teams/services/policies, and **no new entity
types** at runtime. Every write creates a Major.Minor version with a field-level diff (D1.4 = 3).
Detail: [entity-model-and-apis.md](references/entity-model-and-apis.md).

## Extension Points: Three Tiers

| Tier | What | Examples (all K/K+) |
|---|---|---|
| **No code** | Configure through API/UI | Custom properties, classifications/tags, event subscriptions, governance workflows, CSV import, search weights |
| **Plug-in, no fork** | Your jar on `EXT_CLASSPATH` (`bin/openmetadata-server-start.sh:47-49`) + a class name in config or in an entity | `NLQService` providerClass, subscription `className` (`AbstractEventConsumer`), `eventHandlerClassNames`, `authorizerConfiguration.className`, `pipelineServiceClientConfiguration.className`, internal apps; Python: custom connectors, external apps |
| **Fork** | Closed enums / switch statements | New secrets provider (Vault), new subscription destination type, new entity type, new custom-property type, new search engine |

Several extension points exist but are **empty** in OSS: `NoOpNLQService`,
`securityServiceMetadataPipeline.json` (Ranger), `slaValidation.json` (no evaluator),
`aiPlatformConfiguration.json`, the three `CollateAI*` agent types. The research's conclusion:
they are mostly not forgotten but **reserved for the commercial edition (Collate)**.
Detail: [extension-points.md](references/extension-points.md).

## Licence Boundary

| Tree | Licence | Evidence |
|---|---|---|
| `openmetadata-service`, `-spec`, `-ui`, `-sdk`, `common` | Apache-2.0 (0 of 1 626 service `.java` files carry a CCL header) | `LICENSE:1-2` (K) |
| `ingestion/` (connectors included) | Collate Community License v1.0 — 1 053 of 1 404 `ingestion/src` `.py` files (~73 %) | `ingestion/LICENSE:1-2` (K) |
| `openmetadata-airflow-apis/` | Folder `LICENSE` says Apache-2.0, yet 53 of 68 source files carry a CCL header | K+ |

The CCL restriction targets offering a competing SaaS/PaaS/IaaS; internal on-prem use is
allowed. It matters when you **publish** code: put reusable capabilities in Apache-2.0 trees
(server plug-in jar, separate repo) rather than inside `ingestion/`. Not legal advice.

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Treating Airflow as optional by default | `enabled` defaults to `true`; ingestion UI depends on it | Choose explicitly: Airflow, K8s client, or `NoopClient` + external scheduler |
| Keeping the shipped `FERNET_KEY` | `conf/openmetadata.yaml:621` ships a fixed default key that encrypts stored secrets | Set your own `FERNET_KEY` before first start |
| Air-gapped K8s pipeline client with defaults | `ingestionImage` defaults to `docker.getcollate.io/...:latest` (`conf/openmetadata.yaml:590`) | Mirror and pin the image |
| Backing up the search index | Rebuildable; DB is the only state | DB backup + `openmetadata-ops.sh reindex` on restore |
| Expecting a backup command | No `backup.py`; `restore.py` exists but is not registered in the CLI (D12.8 = 0) | Native DB tooling; test restores per version |
| Expecting migration rollback | `migrate` has no rollback path (D12.7 = 2) | Snapshot DB before every upgrade |
| Running `drop-create` on a live DB | Deletes all tables and recreates indexes (`OpenMetadataOperations.java:1094-1099`) | Only for fresh installs |
| Modelling mandatory metadata as custom properties | No required flag | Enforce via governance workflow / rules (see `openmetadata-gap-closing`) |
| Waiting for Kafka publishing in a subscription | Destination enum is Webhook/Slack/MsTeams/GChat/Email/ActivityFeed/… only | Custom consumer class or webhook→Kafka bridge |
| Contributing a new connector inside `ingestion/` and publishing it as Apache-2.0 | It sits in a CCL tree | Separate package that depends on the SDK |

## Related Skills

- `openmetadata-platform-integration` — connectors and lineage for Airflow, NiFi, Kafka, Flink, Trino, ClickHouse, dbt/Spark, MinIO/Iceberg; ingestion workflows and workarounds.
- `openmetadata-gap-closing` — the 24-gap catalog (Vault, Kafka events, backup/DR, licence…) and how to close each on these structures.
- `openmetadata-kvkk-compliance` — modelling KVKK (Law No. 6698 on the Protection of Personal Data) metadata with custom properties and tags.
- `openmetadata-governance-assessment` — the 14-domain rubric behind the D-scores quoted here.
- `openmetadata-capability-design` — designing new LLM/agentic/ontology capabilities on these extension points.

## References

- [references/components-and-deployment.md](references/components-and-deployment.md): server, DB, search, pipeline clients, auth/SSO, footprint, HA, migrations, backup/DR, ops CLI.
- [references/entity-model-and-apis.md](references/entity-model-and-apis.md): spec, entity types, custom properties, versioning, REST/OpenAPI, SDKs, CLI, CSV, change events.
- [references/extension-points.md](references/extension-points.md): apps, reflection-loaded plug-ins, empty extension points, secrets, fork-only changes, licence.
- [references/sources.md](references/sources.md): where the evidence comes from.
