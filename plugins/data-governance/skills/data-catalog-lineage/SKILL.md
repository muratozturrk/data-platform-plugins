---
name: data-catalog-lineage
description: "Vendor-neutral guidance for choosing and running a metadata catalog and data lineage, backed by a frozen 0-4 capability rubric and source-code evidence. Covers what a catalog must model (metadata model, custom properties, versioning, APIs, change events, export; search; table- and column-level lineage, extraction method, impact analysis, OpenLineage; connectors, webhooks, plugins, governance-as-code; footprint, HA, SSO, backup, community health), how lineage is extracted (runtime events vs query/SQL parsing vs manual) and what each method misses, column-level lineage and impact analysis practice, connector-coverage questions, and a measured comparison of OpenMetadata 1.13.3, DataHub 1.7.0 and Apache Atlas 2.5.0 with a which-catalog-for-which-situation guide. Use when writing catalog requirements or an RFP, shortlisting or scoring catalogs, designing lineage capture for a pipeline stack, judging whether lineage is trustworthy enough for impact analysis, or checking OpenLineage and connector fit."
---

# Data Catalog & Lineage

A catalog is only as useful as what it can **model**, **find** and **connect**. This skill gives
you a product-neutral yardstick (the D1, D2, D3, D11 and D12 domains of a frozen 14-domain,
90-capability rubric, scored 0–4) and the measured results of applying it to three open-source
catalogs from source code. Bottom line: **no catalog is a lineage oracle.** All three measured
products reach 3 ("automatic, queryable") on table lineage, none detects broken lineage chains,
none offers what-if impact simulation, and none is an OpenLineage *producer*. Choose on the
differences that are real — connector coverage, column lineage depth, alerting, footprint,
governance model — and design lineage capture so the blind spots of one extraction method are
covered by another.

Evidence tags: **K** = verified in source code, **D** = official docs, **İ** = issue/roadmap,
**B** = vendor claim. All comparison scores are K unless marked. `?`/`null` means "not
determined", never 0.

## When to Use This Skill

- Writing catalog requirements, an RFP or acceptance criteria ("what must a catalog do?").
- Shortlisting or scoring OpenMetadata, DataHub, Apache Atlas or any other catalog.
- Designing lineage capture for a stack (orchestrator, Spark, SQL engines, dbt, BI, streaming).
- Deciding whether lineage is trustworthy enough for impact analysis, incident root cause,
  or personal-data scoping.
- Checking OpenLineage compatibility or connector coverage before committing.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| What to require, at which level | D1/D2/D3/D11/D12 level ladders, "require ≥" column | [catalog-capabilities.md](references/catalog-capabilities.md) |
| Questions for connector coverage | Built-in vs separate module vs type-model-only check | [catalog-capabilities.md](references/catalog-capabilities.md) |
| Which extraction method for which source | Runtime vs query parsing vs view/manifest vs manual | [lineage-methods.md](references/lineage-methods.md) |
| Why lineage has gaps | Blind-spot table per method | [lineage-methods.md](references/lineage-methods.md) |
| Column lineage and impact analysis | Edge model, transform text, downstream query + export | [lineage-methods.md](references/lineage-methods.md) |
| OpenLineage fit | Consumer vs producer; what each product does | [lineage-methods.md](references/lineage-methods.md) |
| OM vs DataHub vs Atlas scores | 32-capability matrix with evidence | [catalog-comparison.md](references/catalog-comparison.md) |
| Architecture, connectors, community | Mandatory dependencies, 13-component coverage, release cadence | [catalog-comparison.md](references/catalog-comparison.md) |
| Which catalog for which situation | Selection guide | [catalog-comparison.md](references/catalog-comparison.md) |

## What a Catalog Must Model (summary)

| Domain | Capabilities | The level that separates products |
|---|---|---|
| D1 Metadata management | entity model, custom properties, schema openness, versioning, API/SDK, change events, export | **3**: typed custom properties on *all* entity types with required/validation rules; full version history with diff; documented event schema with filtered subscriptions |
| D2 Discovery | relevance, facets, natural language, recommendations, personalisation, entity page | **3**: field boosts + fuzzy + popularity; dynamic facets incl. custom properties |
| D3 Lineage & impact | table, column, extraction method, impact analysis, OpenLineage | **3**: automatic from runtime *or* query logs, queryable by API; column lineage through joins/aggregations/CASE; filterable, exportable downstream |
| D11 Integration | connector breadth, ingestion framework, event bus, webhooks, plugins, as-code | **3**: ≥40 connectors in ≥4 categories on one config model; custom connectors run on the same scheduler |
| D12 Platform | footprint, HA, tenancy, SSO, i18n, community, upgrades, backup | **3**: ≤2 mandatory infrastructure components; active-active; IdP provisioning (SCIM) |

Level 4 is by definition rare (audit-grade, cross-system, closed-loop). Do not demand 4 across
the board; demand it only where it is the reason you are buying a catalog.

## Measured Results at a Glance (domain averages, 0–4)

| Domain | OpenMetadata 1.13.3 | DataHub 1.7.0 | Apache Atlas 2.5.0 |
|---|---|---|---|
| D1 Metadata management (7) | 2.7 | **3.3** | 3.0 |
| D2 Discovery (6) | 3.0 | **3.2** | 1.5 |
| D3 Lineage & impact (5) | 2.8 | **3.0** | 2.2 |
| D11 Integration (6) | **2.8** | 2.7 | 1.5 |
| D12 Platform (8) | **1.9** | **1.9** | 1.4 |

Averages hide the decisive cells. Read them in
[catalog-comparison.md](references/catalog-comparison.md):

- **Column lineage:** DataHub 4 (stores the transform expression and chains source column →
  BI field); OpenMetadata 3 (has a `function` field that no built-in extractor fills); Atlas 3
  (Hive only carries the expression).
- **OpenLineage:** OpenMetadata 2, DataHub 2 (official consumers, column-lineage facet mapped);
  Atlas 0 (zero matches in the repo, ATLAS-4550 still open). None publishes OpenLineage.
- **Alerting:** OpenMetadata 4 (webhook/Slack/Teams/GChat/email, retries, dead-letter, delivery
  status); DataHub 2; Atlas 0.
- **Connectors:** DataHub 116 registered source plugins (4); OpenMetadata ~95 (3); Atlas 9 (1).
- **Footprint:** OpenMetadata and DataHub 2 mandatory components (relational DB + search);
  Atlas 4 (HBase, SolrCloud, Kafka, ZooKeeper).

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Treating the lineage graph as complete | Every extraction method has blind spots; no measured product flags broken chains | Combine runtime + query parsing; track coverage per source; reconcile against orchestrator DAGs |
| Scoring connectors by count | A connector may be a type model with no producer code (Atlas Spark/Flink/S3, OpenMetadata Spark) | Classify each needed source: built-in / separate module / type model only / absent |
| "Supports OpenLineage" taken as two-way | All three are at most consumers | Keep OpenLineage producers in your pipelines; export from the catalog via its own API |
| Trusting column lineage without the transform | A `from → to` edge says nothing about `SUM` vs `COPY` | Require the transform expression where it matters (DataHub stores it; OM schema has it but extractors leave it empty) |
| Comparing an open-source edition with a vendor's cloud matrix | DataHub keeps proposals, notifications and assertion evaluation in its paid cloud | Score the edition you will run |
| Assuming "DataHub requires Neo4j" | False for 1.7.0 code and Docker defaults (graph on Elasticsearch/OpenSearch) | Note only that the Helm *prerequisites* chart still enables Neo4j by default |
| Comparing GitHub stars across ASF and vendor projects | Atlas's repo is an ASF mirror; issues live in JIRA | Use commits, releases, issue flow and governance model |
| Writing "absent" for something you did not search twice | Absence is the hardest claim | Write "not verified" and record where you looked |
| Ignoring licence splits | OpenMetadata `ingestion/` is under the Collate Community License, not Apache-2.0 | Check licences per module you will deploy |

## References

- [references/catalog-capabilities.md](references/catalog-capabilities.md): what to require — D1, D2, D3, D11, D12 level definitions condensed, minimum levels, connector questions.
- [references/lineage-methods.md](references/lineage-methods.md): extraction methods and blind spots, column lineage, impact analysis, OpenLineage.
- [references/catalog-comparison.md](references/catalog-comparison.md): OpenMetadata / DataHub / Atlas scores with `path:line` evidence, architecture, connectors, community, selection guide.
- [references/sources.md](references/sources.md): research files, product versions/commits, standards and papers.
- Related skills: `data-contracts`, `data-quality-slas`, `access-control-policies` (this plugin);
  for OpenMetadata-specific work, `openmetadata-architecture` and
  `openmetadata-platform-integration` in the `openmetadata` plugin.
