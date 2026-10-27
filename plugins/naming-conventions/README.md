# naming-conventions

Research-backed naming conventions for a data platform, with the tooling to audit and enforce them.

| Kind | Name | Purpose |
|---|---|---|
| Agent | `naming-architect` | Designs the scheme + glossary; produces machine-readable rules and fitness functions |
| Agent | `naming-reviewer` | Reviews names only (scheme, limits, vocabulary, linguistic antipatterns, breaking renames) |
| Command | `/naming-audit` | Audit repo/schema/topics/DAGs/IaC/API against the scheme |
| Command | `/naming-scheme` | Design or extend the scheme, glossary, limits matrix, `naming-scheme.yaml`, CI |
| Command | `/rename-plan` | Safe migration plan for public renames (columns, topics, API fields, DAG ids, resources) |
| Skill | `identifier-naming-principles` | Code identifiers, grounded in Feitelson, Butler, Arnaoudova, Boswell, McConnell, Martin, Ousterhout |
| Skill | `data-asset-naming` | Layers, tables, columns, keys, dbt models, Iceberg, lake paths |
| Skill | `streaming-naming` | Kafka topics/subjects/groups/connectors/DLQs, tenant prefixes |
| Skill | `pipeline-naming` | Manifests, Airflow DAG/task ids, Spark apps, dbt jobs, runs, alerts |
| Skill | `infra-resource-naming` | Kubernetes, Helm, operator CRs, Vault, Keycloak, Azure CAF, buckets, repos |
| Skill | `api-naming` | REST paths/fields/enums/errors/events, CLI, SDK, OpenAPI |
| Skill | `glossary-and-abbreviations` | Controlled vocabulary, abbreviation policy, casing matrix, dispute resolution |

`examples/`: `naming-scheme.yaml`, `glossary.yaml`, `check_names.py` (CI fitness function), sqlfluff and ruff naming configs, OPA topic-name policy.

## Sources
- Feitelson et al., "How Developers Choose Names", IEEE TSE 2022; Alpern et al., "Reproducing, Extending, and Analyzing Naming Experiments", 2024
- Butler et al., "Relating Identifier Naming Flaws and Code Quality", WCRE 2009; Arnaoudova et al., "Linguistic Antipatterns", EMSE 2016; Lawrie et al., "What's in a Name?", ICPC 2006
- Boswell & Foucher, *The Art of Readable Code*; McConnell, *Code Complete* 2e; Martin, *Clean Code*; Ousterhout, *A Philosophy of Software Design* 2e
- PEP 8; Google Python/TypeScript style guides; Palantir PySpark Style Guide
- dbt style guide and project-structure guide; GitLab SQL style guide; Simon Holywell SQL Style Guide; Kimball, *The Data Warehouse Toolkit* 3e; Databricks medallion architecture
- Apache Kafka Multi-Tenancy docs; Confluent and Conduktor topic-naming guides; Strimzi
- Kubernetes object names and recommended labels; Helm conventions; Vault policies; Keycloak admin guide
- Microsoft Cloud Adoption Framework naming and abbreviations; Azure resource name rules
- Google AIP, Microsoft and Zalando REST API guidelines; Moesif/Zuplo REST naming references; Semantic Versioning; Conventional Commits
Per-skill `references/sources.md` files give the exact items used.
