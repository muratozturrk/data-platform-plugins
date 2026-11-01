# Catalog Capabilities: What to Require

The capability ladders below come from a frozen, product-neutral rubric (14 domains, 90
capabilities, levels 0–4) derived from DAMA-DMBOK2 knowledge areas (KA10 Metadata for D1/D2,
KA6 Data Integration & Interoperability for D3/D11, KA2/KA4 for D12). The rubric was frozen
**before** any product was scored, so it was not shaped around a product. Only the five domains
relevant to cataloguing and lineage are reproduced here, condensed; the wording keeps the
observable test of each level.

## Scoring rules worth copying into your own evaluation

| Rule | Why |
|---|---|
| 0 = absent, 1 = indirect/manual/one narrow case, 2 = structured but manual, 3 = automatic + configurable + reported, 4 = rare differentiator | Gives two evaluators the same answer |
| Level *n* includes all of level *n−1* | Stops a product scoring 3 on a flashy feature while missing the basics |
| No subjective adjectives ("rich", "robust", "mature") in a level text | They cannot be observed |
| A 0 needs **two** independent searches (e.g. code + docs, or docs + issue tracker) | Absence is the weakest claim |
| Unsure → `null` with a note starting "searched:" | `null` ("unknown") ≠ 0 ("absent") |
| Every score carries evidence and an evidence type (K code, D docs, İ issue, B vendor claim) | Report the share of vendor-claim (B) scores; high B = low confidence |

## D1 — Metadata management and model extensibility

| ID | Capability | 2 (basic) | 3 (mature) | 4 (differentiator) |
|---|---|---|---|---|
| D1.1 | Entity model coverage | DB, table, column, view, basic pipeline | + dashboard, ML model, topic, storage container, document collection | Schema-based model where users define **new entity types** |
| D1.2 | Custom properties | Typed fields (text, number, date, enum) via UI, only on some entity types | On **all** entity types, nested, with **required/validation rules**, managed by API | Definitions versioned, backward-compatibility checked, validated and published by CI/CD |
| D1.3 | Schema standard & openness | Product-specific machine-readable schema (JSON Schema, IDL) | Mapped to a recognised open metadata standard, schema in a separate versioned repo | Third-party tools consume it directly; compatibility tests published |
| D1.4 | Versioning & history | Change log for some fields | Full history for **all** fields, rollback, diff between versions | Event-sourced; point-in-time catalog state queryable |
| D1.5 | API / SDK | Read-write REST/GraphQL, no official SDK | Official SDK in ≥1 language, bulk import/export, pagination, token/OAuth | Multi-language SDKs, CLI, schema-first API, documented compatibility guarantee |
| D1.6 | Change events / webhooks | Events on a queue/topic but schema undocumented | **Documented event schema**, webhook or bus, subscriptions filtered by event type | Open event standard, at-least-once delivery, **replay**, subscriptions managed by API |
| D1.7 | Bulk export & portability | All entity types **with relationships** exported in a structured format | Lossless restore into another install of the same product, documented | Export follows an open interchange standard; official converter to a third-party product |

Level 1 for D1.7 is a trap worth naming: CSV list exports that **lose lineage, glossary links
and ownership** look like portability but are not.

## D2 — Discovery, search and catalog

| ID | Capability | 2 | 3 | 4 |
|---|---|---|---|---|
| D2.1 | Relevance & ranking | Full-text with term-frequency score | Field boosting (name > description), **fuzzy** matching, **popularity** ranking | Learning-to-rank from interaction signals; admin-tunable weights |
| D2.2 | Faceting | Several fixed facets, static counts | **Dynamic** counts, custom properties as facets, multi-select per facet | Per-role facets, hierarchical facets, **saved shareable views** |
| D2.3 | Natural-language search | Semantic/vector search | Free-text questions routed to relevant assets, re-ranked by intent | LLM turns the question into a structured query and summarises results |
| D2.4 | Recommendations | Related assets (same lineage chain / same tag) | Personalised from user history | Learned co-usage model with an explanation |
| D2.5 | Personalisation | Recent views, saved searches, notification preferences | Home page by team/role/interest; shareable views | Adaptive UI; admin-defined role defaults |
| D2.6 | Entity page | Description, owner, tags, schema on one page with links | + lineage, quality, usage stats, glossary terms on one page | Page layout customisable per entity type; graph navigation |

## D3 — Lineage and impact analysis

| ID | Capability | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|
| D3.1 | Table-level lineage | Free text only | Structured edges entered manually, shown as a graph | **Extracted automatically** from pipeline runtime or query logs, **queryable by API** | End-to-end across ETL, BI and streaming tools merged automatically; **broken chains detected and reported** |
| D3.2 | Column-level lineage | Manual, one edge at a time | Automatic for copy/rename only | Automatic through **join, aggregation, CASE/WHEN** from SQL/transform logic, served by API | The **transform expression** is shown on the graph; chains cross system boundaries (source DB column → BI field) |
| D3.3 | Extraction method | Static parsing of config files of predefined tools | General SQL parsing, **≥3 dialects** | Query parsing **plus** runtime observation, results merged | Pluggable extractors users can write and register; **per-edge confidence** reported |
| D3.4 | Impact analysis | Direct (one-hop) downstream list | Multi-hop downstream graph | Downstream **filterable** (e.g. only production dashboards, only critical tags) and owner list **exportable / API** | **What-if** simulation before the change; affected owners notified / approval triggered |
| D3.5 | OpenLineage | One-way via community plugin | Official **consumer** of Job/Run/Dataset events, mapped to the internal model | Consumer **and producer**; schema and data-quality facets | Column-lineage facet and job/run facets fully supported; conformance verified by a test suite |

For D3.3, note how the ladder reads: level 3 assumes query parsing *and* runtime capture.
A product that captures lineage only at runtime through per-system hooks fits the ladder poorly;
record the method explicitly next to the score (see [lineage-methods.md](lineage-methods.md)).

## D11 — Integration and extensibility

| ID | Capability | 2 | 3 | 4 |
|---|---|---|---|---|
| D11.1 | Connector breadth | 10–39 connectors in ≥2 categories | **≥40** connectors in **≥4** categories on **one** config model and auth scheme; list versioned in release notes | >40, open to community contributions, per-connector status (experimental/stable/deprecated) and quality tracked centrally |
| D11.2 | Ingestion framework / custom connectors | Documented framework (connect, extract, transform, load); you write only extract | Custom connector registered as a testable plugin, **same scheduler and monitoring** as built-ins | Official SDK + starter repo + test skeleton; community connectors in a registry |
| D11.3 | Event bus integration | Consumes metadata/data events from one bus in a structured format | Consumes **and publishes** its own changes; >1 bus technology | Schema-registry validation, at-least-once, backpressure, config fully via API/code |
| D11.4 | Webhooks, alerts, notifications | User picks event types and several webhook endpoints, structured payload | + official email/chat/incident-tool channel; rules filter by event type **and severity** | Routing rules managed centrally as code/API; **retries**; **delivery status in an audit trail** |
| D11.5 | Plugin / app framework | Separate plugin packages installed at deploy time | SDK for several extension points (UI page, workflow step, rule engine); enable/disable without restart | Marketplace; per-plugin permission model; plugin versions independent of core |
| D11.6 | Governance as code | Declarative files (YAML) pushed by CLI/API | Definitions in Git, validated and applied by CI/CD; **drift detected** | Everything (model, policy, workflow, access rule) as code; repo is source of truth; drift auto-corrected or blocked |

D1.6 vs D11.4: D1.6 asks whether **metadata changes** leave the catalog as documented events;
D11.4 asks whether **operational events** (ingestion failed, policy violated) reach people and
systems reliably. A product can be strong in one and weak in the other.

## D12 — Platform, architecture and operational maturity

| ID | Capability | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|
| D12.1 | Footprint (mandatory infra) | 4 components, one heavy (graph DB / distributed engine) | 3 (e.g. RDBMS + search + queue) | **≤2** (e.g. RDBMS + search); the rest optional | One RDBMS or fully embedded |
| D12.2 | HA & horizontal scale | Read replicas, manual failover | Active-passive with automatic failover | **Active-active** writes; manual/semi-automatic scaling | Autoscaling; multi-region; failover tested |
| D12.3 | Multi-tenancy | Naming convention only | Tenant access isolation on shared storage | Per-tenant config, quota, usage metering | Physical isolation option; tenant export |
| D12.4 | AuthN / SSO | One SSO protocol | Several protocols; IdP groups mapped manually | **Provisioning (e.g. SCIM)**; deprovisioning closes access | Several IdPs at once; attribute-derived roles; auth events audited |
| D12.5 | i18n | UI language switch only | + user content in several languages | Per-user language; fallback; missing-translation report | Translation workflow; MT suggestions |
| D12.6 | Community health | Release notes only | Public contribution history; at least yearly releases | <70 % of last-12-month contributions from one organisation; security process documented | Independent foundation governance; LTS and roadmap public |
| D12.7 | Upgrades | Documented, manual, step by step | Automated migration tool; breaking changes listed | Minor-version compatibility guaranteed; deprecation notice period | Zero-downtime rolling upgrade; rollback; support windows committed |
| D12.8 | Backup / DR | Docs only, manual | Product tool covering **all mandatory components** | Scheduled backup; old backup restorable on new version | Point-in-time restore; multi-region; committed RPO/RTO |

## Suggested minimum levels for a lineage-centric catalog

This is guidance derived from the ladders, not a research result. Adjust to your use case.

| Requirement | Minimum | Why |
|---|---|---|
| D3.1 table lineage | 3 | Manual lineage decays within weeks |
| D3.2 column lineage | 3 | Root cause, PII propagation and schema-change impact need columns |
| D3.3 extraction method | Two methods merged, with edge provenance recorded | See blind spots in [lineage-methods.md](lineage-methods.md) |
| D3.4 impact analysis | 3 | Filter + export of affected owners is what a change process consumes |
| D3.5 OpenLineage | 2 | Lets pipelines emit once and keeps the catalog replaceable |
| D1.2 custom properties | 3 if you model regulatory attributes; else 2 | Required-field rules are the difference between data and convention |
| D1.6 change events | 3 | Downstream automation without polling |
| D1.7 export | 2 (3 if lock-in is a selection criterion) | Relationships must survive export |
| D11.1 connectors | Coverage of **your** list, not a count | See questions below |
| D11.4 alerting | 3 | Ingestion failures that nobody sees produce stale lineage |
| D12.1 footprint | 3 | Each mandatory component is one more thing to patch, back up and scale |
| D12.8 backup | 2, or a written runbook for the underlying stores | Catalog metadata is hard to recreate (owners, glossary, manual edges) |

## Connector coverage: questions to ask

Count-based scoring hides the difference between "parses the source" and "has a JSON type for
it". For each source system in your stack ask:

1. **Where does the connector live?** In the main ingestion framework (*built-in*), in a separate
   module/agent with its own lifecycle (*separate module*), in a third-party repo
   (*community*), or only as an entity-type definition with no producer code (*type model only*)?
2. **What does it extract?** Schema only, or also lineage, usage/query history, profiles, tags,
   owners? Table or column lineage?
3. **Pull or push?** Scheduled crawler, query-log reader, or runtime listener inside the job?
4. **Which versions and auth methods?** Same config model and secret handling as other connectors?
5. **What is its support status?** Certified/incubating/testing labels, beta lists, tests.
6. **What licence covers it?** The connector code may sit under a different licence from the
   server (OpenMetadata `ingestion/` → Collate Community License v1.0).
7. **How are object-store and table-format sources handled?** S3-compatible stores (e.g. MinIO)
   usually work through an endpoint override, not a dedicated connector; open table formats
   (Iceberg) may be catalogued only through a query engine's connector.
8. **Can you write the missing one?** Is there a scaffold, and does a custom connector run on the
   same scheduler and monitoring as built-ins (D11.2 level 3)?

A worked example of this classification for 13 components of a typical open lakehouse and
streaming stack is in [catalog-comparison.md](catalog-comparison.md#connector-coverage-for-a-typical-open-stack).
