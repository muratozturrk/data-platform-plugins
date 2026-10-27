# platform-engineering

Claude Code plugin for building a data platform that stays **generic, extensible and productizable** across customers and projects.

## What it gives you

| Kind | Name | Purpose |
|---|---|---|
| Agent | `platform-architect` | Classifies every change as core / plugin / config and rejects tailor-made leakage |
| Agent | `product-engineer` | Turns capabilities into packaged, metered, documented product features |
| Agent | `extensibility-reviewer` | PR reviewer focused only on genericity; BLOCK/FIX/APPROVE verdicts |
| Command | `/genericity-review` | Scores a design/PR for productizability and lists fixes + fitness functions |
| Command | `/extract-plugin` | Plans moving customer-specific logic out of core into a plugin/config |
| Command | `/platform-maturity-assess` | CNCF maturity model assessment + productization readiness roadmap |
| Command | `/component-onboarding` | Plan how a new OSS component joins the platform (operator, tenancy, identity, policy, catalog, observability, SPI) |
| Skill | `platform-as-product` | Users, golden paths, metrics, TVP, multiplane platform |
| Skill | `config-driven-design` | Declarative manifests, schemas, layered config, registries |
| Skill | `plugin-architecture` | Microkernel core + versioned SPI, discovery, isolation, contract tests |
| Skill | `multi-tenancy-patterns` | Silo/pool/bridge, tenant context, Snowflake/Databricks/lake specifics |
| Skill | `api-versioning-sdk` | Versioning the five public contracts, deprecation policy |
| Skill | `architecture-fitness-functions` | Automated governance of all the above in CI |
| Skill | `oss-reference-architecture` | Azure → OSS layer map (Kafka, Spark, Iceberg+Polaris, Trino, ClickHouse, Airflow, dbt, Keycloak, Vault, OPA, OpenMetadata, Superset), per-component tenancy, build-vs-adopt (Stackable), install waves |
| Skill | `control-plane-design` | ADF-style resource model (Tenant, Workspace, IntegrationRuntime, LinkedService, Dataset, Pipeline, Trigger, Policy), manifest compiler, CRD reconciliation, metering |

`examples/` contains runnable starting points (manifests + JSON Schema, a Python SPI with default plugin and contract test, tenant-aware storage resolver, import-linter rules, forbidden-token scanner, CI job), plus `oss-stack/` (stack map, Strimzi tenant user, OPA tenant-isolation policy, ClickHouse tenant SQL) and `control-plane/` (Tenant CRD, LinkedService, IntegrationRuntime, Trigger, Policy kinds).

## Sources this plugin is built on

- Fournier & Nowland, *Platform Engineering* (O'Reilly, 2024)
- CNCF Platforms WG, *Platforms White Paper* and *Platform Engineering Maturity Model*
- Skelton & Pais, *Team Topologies* (Thinnest Viable Platform)
- Richards & Ford, *Fundamentals of Software Architecture*, 2nd ed. (Microkernel architecture)
- Dehghani, *Data Mesh* (self-serve data platform, data product as architecture quantum)
- Ford, Parsons, Kua, Sadalage, *Building Evolutionary Architectures*, 2nd ed. (fitness functions)
- AWS Well-Architected *SaaS Lens*; AWS *Guidance for Multi-Tenant Architectures*
- Snowflake, *Design Patterns for Building Multi-Tenant Applications*; Databricks community guide on multi-tenant Unity Catalog; DataHub multi-tenancy docs
- Config-driven ETL frameworks (Samara, dpone, Drune, factl) as manifest prior art
- Merced, *Architecting an Apache Iceberg Lakehouse* (Manning, 2026); *Apache Polaris: The Definitive Guide*; *Apache Iceberg: The Definitive Guide*
- Azure Data Factory concepts (pipeline/dataset/linked service/integration runtime/trigger) and Azure Databricks architecture (control vs compute plane)
- Stackable Data Platform docs and operators; Apache Polaris, Lakekeeper, Nessie, Unity Catalog OSS; Trino security (OPA/Ranger); Apache Kafka multi-tenancy chapter; Strimzi; Keycloak Organizations; OpenMetadata & OpenLineage; ClickHouse access control; Superset embedded/RLS

Each skill's `references/sources.md` lists the specific sources it draws on.

## Suggested CLAUDE.md snippet for the platform repo

```
## Platform principles
- Every change must work unchanged for a customer we have never met.
- Core knows interfaces, never implementations. Behaviour varies via manifests and plugins.
- No domain nouns, no hard-coded environment, no implicit tenant in core.
- Public surface (API, CLI, SDK, manifest schemas, SPI) is versioned with a deprecation policy.
- Rules are enforced by fitness functions in CI. Run /genericity-review before opening a PR on core.
```
