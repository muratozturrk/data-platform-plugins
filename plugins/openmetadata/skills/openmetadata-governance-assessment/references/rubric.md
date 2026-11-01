# Capability Reference Model (Rubric v1.0)

Product-neutral model for scoring data governance software. Frozen on 2026-08-11 before any
product was inspected. **14 domains, 90 capabilities, 48 registered sources.** Each capability
has a definition, a rationale, at least one source and five levels (0-4). Below: the domain map,
then every capability with its **level-2 (basic)** and **level-4 (differentiator)** anchors,
condensed and translated. Level 0 is always "concept absent"; level 1 is "free text / manual /
one narrow case"; level 3 is "level 2 + automatic, fine-grained, reported or wired into a
workflow" (see the scale in `SKILL.md`). When a score hinges on the 2/3 or 3/4 boundary, write
the full level text into your own rubric copy before scoring.

## Structure

```yaml
version: "1.0"
frozen: true
frozen_at: "2026-08-11"
domains:
- id: D1                          # ^D\d+$
  name: Metadata management and model extensibility
  dmbok: dmbok2-ka10 (Metadata)
  capabilities:
  - id: D1.1                      # ^D\d+\.\d+$, prefix must match the domain
    name: Entity model coverage
    definition: ...
    rationale: ...                # why it matters; for D14, which D6/D9 capability it builds on
    sources: ["ref:dmbok2-ka10", "ref:..."]   # resolved in sources.yaml, URL must be non-empty
    levels: {"0": ..., "1": ..., "2": ..., "3": ..., "4": ...}   # all five required
```

Level-writing rules: observable (two people reach the same score), no subjective adjectives
("good", "advanced", "comprehensive", "mature", "strong", "sufficient", "rich"), no product names,
monotonic (levels 3 and 4 say "in addition to level 2/3"), every number/duration/article verified
against an official source, at least one (preferably two) sources per capability.

## Domains

| ID | Domain | Caps | Derived from |
|---|---|---|---|
| D1 | Metadata management and model extensibility | 7 | DMBOK2 KA10 Metadata |
| D2 | Discovery, search and catalog | 6 | DMBOK2 KA10 Metadata |
| D3 | Lineage and impact analysis | 5 | DMBOK2 KA6 Data Integration and Interoperability |
| D4 | Business glossary, taxonomy and semantic layer | 5 | DMBOK2 KA3 Data Modeling; KA8 Reference and Master Data |
| D5 | Data quality | 7 | DMBOK2 KA11 Data Quality |
| D6 | Security, privacy and compliance | 8 | DMBOK2 KA5 Data Security |
| D7 | Governance processes and organisation | 6 | DMBOK2 KA1 Data Governance and Stewardship |
| D8 | Master and reference data | 4 | DMBOK2 KA8; KA9 DW/BI |
| D9 | Data products, domains and mesh | 6 | Modern: data mesh governance |
| D10 | Observability and operations | 6 | DMBOK2 KA4 Data Storage and Operations |
| D11 | Integration and extensibility | 6 | DMBOK2 KA6 |
| D12 | Platform, architecture and operational maturity | 8 | DMBOK2 KA2 Data Architecture; KA4 |
| D13 | AI and ML governance | 5 | Modern: EU AI Act |
| D14 | KVKK and regulatory compliance | 11 | Turkish law: Law No. 6698 and secondary legislation; no counterpart in DMBOK2/DCAM/ISO |

KA numbers are the rubric's own `dmbok2-ka1..ka11` keys, not DMBOK2 book chapter numbers.
KA7 (Documents and Content) is registered but used by no capability: a deliberate scope limit.

## Capabilities

### D1 Metadata management and model extensibility

| ID | Capability | Level 2 (basic) | Level 4 (differentiator) |
|---|---|---|---|
| D1.1 | Entity model coverage | Database, table, column, view, basic pipeline entities | Schema-based model where users define new entity types |
| D1.2 | Custom property definition | Typed custom fields (text, number, date, enum) via UI, only for some entity types | Definitions versioned, backward-compatibility checked, validated and published via CI/CD-like process |
| D1.3 | Schema standard and openness | Product-specific but machine-readable schema (JSON Schema, IDL) published | Fully open, consumed directly by third-party tools; compatibility tests for schema versions published |
| D1.4 | Metadata versioning and history | Change log for some fields listed in the UI | Event-sourced; point-in-time catalog state queryable and rebuildable via API |
| D1.5 | API/SDK maturity | Read/write REST or GraphQL API, no official SDK | Official SDKs in several languages, CLI, schema-first API; versioning/compatibility guarantee documented |
| D1.6 | Metadata change events | Events on a queue/topic, schema undocumented or unstable | Open event standard, at-least-once delivery, replay, subscriptions managed via API |
| D1.7 | Whole-catalog export and portability | All entity types with relationships exportable in a structured format | Export follows an open metadata-exchange standard; official converter or mapping guide to a third-party product |

### D2 Discovery, search and catalog

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D2.1 | Search relevance and ranking | Full-text search ranked by term-frequency relevance | Ranking tuned continuously by interaction signals or learning-to-rank; admin-configurable weights |
| D2.2 | Faceted filtering | Several predefined facets (type, owner, tag), static counts | Per-user/role facets, nested facets, saved and shareable views |
| D2.3 | Natural-language search | Semantic/vector search matches related concepts | LLM turns the question into a structured query and answers with a summary (conversational discovery) |
| D2.4 | Recommendations | Directly related entities (same lineage chain or tag) suggested | Model learned from usage (co-queried tables) recommends, with an explanation |
| D2.5 | Personalisation | Per-user recent items, saved searches, notification preferences | Adaptive UI from user behaviour; admin-defined role-based default views |
| D2.6 | Entity page and navigation | Description, ownership, tags and schema on one page with links | Per-type customisable page (templates/visuals) plus graph/network navigation |

### D3 Lineage and impact analysis

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D3.1 | Table-level lineage | Upstream/downstream defined by hand and shown as a graph | Cross-system, cross-tool end-to-end lineage stitched automatically; broken chains detected |
| D3.2 | Column-level lineage | Automatic for simple transforms (copy, rename) only | Transform formula shown on the graph; column chains across systems (source DB column to BI field) |
| D3.3 | Lineage extraction method | Static SQL parsing, at least three SQL dialects | Pluggable extractors users can write and register; confidence level per edge |
| D3.4 | Impact analysis and downstream query | Multi-level downstream chain shown graphically | What-if simulation; automatic notification/approval to affected owners |
| D3.5 | OpenLineage compatibility | Officially consumes OpenLineage Job/Run/Dataset events | Full column-lineage and job/run facets; compliance verified by a test suite |

### D4 Business glossary, taxonomy and semantic layer

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D4.1 | Glossary and term lifecycle | Terms in categories with owners; status changed by hand | Configurable state machine; term changes versioned; impact on linked assets shown before approval |
| D4.2 | Hierarchy and relationship modelling | Multi-level parent-child hierarchy | User-defined relation types, poly-hierarchy, automatic integrity (cycle) checks |
| D4.3 | Auto-tagging and term suggestion | Content sampling with pattern matching (e.g. national ID, card number) suggests tags | LLM contextual suggestion (schema + glossary + usage); bulk accept/reject with rationale |
| D4.4 | Ontology/RDF/OWL/SKOS support and export | Structured export (JSON/XML), not a semantic-web standard | Full RDF/OWL: URIs, links to external ontologies, SPARQL endpoint or equivalent |
| D4.5 | Business term to technical asset mapping | Many-to-many manual links shown on the asset page | Mapping down to value/cell scope; integrity maintained automatically when assets change |

### D5 Data quality

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D5.1 | Rule definition and authoring | Parameterised rule templates (not-null, unique, range) in the UI | Natural-language-to-rule or rules suggested from history; shared, versioned rule libraries |
| D5.2 | Profiling | Column stats (null ratio, distinct count, min/max) computed automatically | Trends over time, correlations, candidate keys; profiles feed rule suggestions |
| D5.3 | Test execution and scheduling | Cron-like periodic execution | Driven by external orchestrators via API; incremental execution; run history/performance tracked |
| D5.4 | Quality incident and SLA management | Each violation becomes an incident with status and assignee | Two-way integration with an external incident/ITSM tool; root cause and recurrence reported |
| D5.5 | Quality score and reporting | Multi-dimension score per asset (completeness, accuracy, consistency) | Configurable dimension weights; business-impact weighting; automatic periodic reports |
| D5.6 | Anomaly/drift detection | Dynamic threshold from moving average/stddev, no seasonality | Thresholds learned from run history; new rules suggested; user feedback tunes parameters |
| D5.7 | External quality tool integration | Results of at least one external/OSS tool ingested via an official integration | Two-way: catalog rules pushed to the tool and results written back; pluggable for own tools |

### D6 Security, privacy and compliance

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D6.1 | PII detection and auto-classification | Pattern classification on sample content; sensitivity tag auto-assigned | LLM contextual detection (name + description + sample); adaptable to custom taxonomies; bulk labelling |
| D6.2 | Policy engine (RBAC/ABAC) | Custom roles with view/edit/manage per asset or folder | Level 3 RBAC/ABAC via API + API that explains which policy and rule decided allow/deny for a user-asset pair |
| D6.3 | Masking and pseudonymisation | Column masking policy enforced at query time; one method | Selectable methods, role/attribute rules, reversible pseudonymisation for authorised users |
| D6.4 | Row- and column-level access control | Column permissions plus a fixed-condition row filter | Compound conditions, performance monitoring, "what would this user see" simulation |
| D6.5 | Retention policy definition | Structured retention field per asset; expired assets listed | Enforcement integrated with storage (deletion/archival verifiable); exception workflow |
| D6.6 | Audit trail and immutability | Config/metadata changes (who, when, what) logged and listed | Immutable (append-only or signed) logs streamed in real time to external log/SIEM |
| D6.7 | External authorisation engine | Coarse-grained decisions from an external policy server over a standard protocol | Policies authored centrally in the external engine; product only consumes decisions; open, documented protocol |
| D6.8 | Anonymisation and privacy-enhancing techniques | Aggregation/generalisation policy (e.g. minimum group size) recorded on the asset | Differential-privacy budget or synthetic data; audit trail of technique and parameters |

### D7 Governance processes and organisation

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D7.1 | Ownership and stewardship | Structured owner (user/group) with business vs technical owner | Ownership suggested/inherited from org or domain hierarchy; approval and hand-off history |
| D7.2 | Approval and review workflow | Single-step, single-approver gate for some change types | Workflow engine (conditions, timeouts, escalation) triggered and tracked via API; templates per type |
| D7.3 | Policy lifecycle management | Policy status field (draft/active/retired) changed by hand | Version diffs, bound scope shown on change, approval and effective dates |
| D7.4 | Certification and trust indicator | Fixed certification states set by hand, shown as a badge | Criteria-based (quality, owner, docs) automatic suggestion/re-evaluation; expiry |
| D7.5 | Bulk remediation campaigns | Task list assigned to users; done/not done tracked | Recurring campaigns; cross-team progress reports; completion as a governance metric |
| D7.6 | Collaboration | Threads, @mentions with notifications | Discussions become assigned tasks with status; per-user channel/frequency preferences |

### D8 Master and reference data

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D8.1 | Reference data set management | Value-label sets defined and linked to columns as allowed values | Managed via API; value changes approved; impact on all using assets shown |
| D8.2 | Code lists and cross-mapping | 1:1 mappings between two code sets stored in the product | Real-time code-translation service via API; unmapped codes reported |
| D8.3 | MDM integration | Periodic batch import of MDM entity definitions | Two-way: catalog enrichment written back; MDM match/quality scores shown |
| D8.4 | Golden record and entity resolution visibility | Golden record as an entity with static links to contributing sources | Per-source match confidence; users flag wrong merges, request flows back to MDM |

### D9 Data products, domains and mesh

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D9.1 | Domain modelling and ownership | Domain object with owner/team; assets assigned | Federated: local domain rules plus automatically enforced central minimum standards |
| D9.2 | Data product as first-class entity | Product with name, owner, input/output ports, linked assets | SLA compliance measured in real time; product dependency graph; breach handling |
| D9.3 | Data contract definition and validation | Structured contract (schema, types, required), not validated | Breaking-change analysis; incompatible change blocked without consumer approval or grace period |
| D9.4 | Access request and approval | Request routed to owner/steward, approved or rejected in product, status visible | Sensitivity-based approval chains; full history via API; periodic access review |
| D9.5 | Marketplace and consumer experience | Product page (description, owner, SLA summary), browse by domain | Personalised recommendations; one self-service flow (request, sample query, connection info) |
| D9.6 | External sharing and agreements | External consumer and share object modelled; who accesses what recorded | Zero-copy sharing via an open protocol; recipient-side usage visible |

### D10 Observability and operations

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D10.1 | Freshness monitoring | Expected update frequency set by hand; delay indicator on the page | Expected cadence learned with seasonality; downstream impact of staleness |
| D10.2 | Volume and row-count monitoring | Row count measured periodically and charted | Seasonal/trend-aware anomaly detection with explanation (expected range vs observed) |
| D10.3 | Schema drift detection | Periodic scan detects added/removed/retyped columns | Drift reported with downstream impact; pipeline halted on breaking change types |
| D10.4 | Pipeline run monitoring | Last run status/time from the orchestrator shown on the asset | Duration/resource trends with anomaly detection; health of the whole upstream chain in one view |
| D10.5 | Usage and popularity analytics | Query count / last queried from source query logs | Usage joined with lineage for "who breaks if removed"; spikes/drops flagged |
| D10.6 | Cost and resource analytics | Platform cost/usage data shown periodically on the asset | Cost alerts; low-use/high-cost optimisation suggestions via API |

### D11 Integration and extensibility

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D11.1 | Connector ecosystem breadth | 10-39 connectors in at least two categories, no shared config/auth model | Over 40, open to community connectors, per-connector status (experimental/stable/deprecated) documented |
| D11.2 | Ingestion framework and custom connectors | Documented framework splitting connect/extract/transform/load; user writes extract | Official SDK and starter template with test scaffolding; central registry of community connectors |
| D11.3 | Event bus / message queue integration | Consumes metadata/data events from one queue/stream | Schema-registry validated, at-least-once, backpressure; configuration via API/code |
| D11.4 | Webhooks, alerts and notifications | User picks event types and several webhook endpoints; structured payload | Routing rules (event, channel, threshold) managed centrally as code/policy; retries and delivery-status audit |
| D11.5 | Plugin/app framework | Deploy-time plugins; at least one plugin type | Marketplace/registry; per-plugin permission model; plugin versions decoupled from core |
| D11.6 | Governance as code (GitOps) | Declarative files (YAML) pushed via CLI/API, one-way | Whole configuration as code; reviewed, tested; repository is the source of truth |

### D12 Platform, architecture and operational maturity

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D12.1 | Deployment footprint and mandatory dependencies | Three mandatory external components (typically RDBMS, search engine, message queue) | Fully functional on one RDBMS or embedded components; everything else optional |
| D12.2 | High availability and horizontal scaling | Active-passive failover; read load balanced | Metric-driven autoscaling; survives loss of a region/data centre |
| D12.3 | Multi-tenancy and isolation | Tenant-level access isolation on shared storage | Physically/logically isolated tenant storage; tenant export to a standalone install |
| D12.4 | Authentication, SSO and authorisation | Several SSO protocols; roles mapped by hand from IdP groups | Several IdPs at once; fine-grained role/attribute mapping derived dynamically from IdP attributes |
| D12.5 | Localisation (i18n) | Besides the UI, user content (e.g. term descriptions) entered per language | Translation management open to contributions; machine pre-translation; language coverage versioned |
| D12.6 | Community health and sustainability | Public contribution history; at least yearly regular releases | Independent foundation/consortium governance; public roadmap and LTS commitment |
| D12.7 | Upgrades and backward compatibility | Automated migration tool; major-version breaking changes in a migration guide | Zero-downtime rolling upgrade; rollback; documented support/patch windows |
| D12.8 | Backup, restore and disaster recovery | Product-provided backup/restore tool covering all mandatory components | Point-in-time restore, cross-region replication, committed RPO/RTO |

### D13 AI and ML governance

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D13.1 | ML model and feature catalog | Model versions tracked (which in production); features linked | Performance and drift metrics over time; feature change shows affected models |
| D13.2 | AI lineage (training data → model → output) | Manual dataset-to-model lineage | Chain feeds automatic impact analysis from data change to models to decision points |
| D13.3 | Dataset cards and model cards | Fixed-section card templates filled by hand | LLM-drafted card content; card tied to real performance/drift |
| D13.4 | AI risk classification and compliance record | Fixed risk category (high/limited/minimal) set by hand | Rule-based category suggestion; continuous compliance monitoring (documents, human oversight) |
| D13.5 | GenAI asset governance (prompts, embeddings, vector stores, RAG sources) | Prompt templates and vector collections as searchable entities with owners | Prompt/embedding/re-index history; any output traceable to prompt + source + model |

### D14 KVKK and regulatory compliance

Builds on D6/D9 without double counting: the generic mechanism is scored there, D14 scores only
the KVKK-specific delta (e.g. D6.5 retention field is a prerequisite of D14.3, not re-scored).
Law No. 6698 art. 28/2 partial exemption can narrow the scope of D14.1, D14.5 and D14.10 per
processing activity. Detail in the `openmetadata-kvkk-compliance` skill.

| ID | Capability | Level 2 | Level 4 |
|---|---|---|---|
| D14.1 | Personal data processing inventory derived from the catalog (feeds VERBİS, the Data Controllers' Registry) | Data category, data-subject group, recipient group as structured fields per asset/column | Inventory kept current by source scans; added/removed personal-data fields reported as dated diffs |
| D14.2 | Processing purpose and legal basis per asset | Purpose and legal basis picked from a predefined list of the law's processing conditions | Several purpose/basis pairs per asset, each with its own retention; in-scope vs out-of-scope reported |
| D14.3 | Destruction method record and periodic destruction | Method (delete, destroy, anonymise) from a closed list; retention + method exportable as a retention/destruction table | Destruction records immutable, kept at least three years; recipients to notify derived from lineage; alert when the periodic task misses its window |
| D14.4 | Explicit consent status and downstream effect of withdrawal | Consent status (given/not given/withdrawn), consent text version and date as fields | Withdrawal creates a destruction-workflow task; closure tracked per affected asset |
| D14.5 | Data subject request: locate the person's data, request workflow | Person identifier searchable across assets/columns; result exportable | Affected set expanded via lineage incl. derived copies and recipients; per-request justification |
| D14.6 | Breach scope: affected assets and person categories | Report of personal-data categories and subject groups for a chosen asset set, exportable | Breach record (scope, categories, consequences, measures, assignee) with a separate *date of becoming aware*; 72-hour countdown runs from it; record immutable |
| D14.7 | Cross-border transfer and its legal mechanism | Recipient country and domestic/abroad flag per transfer record | Missing or expired mechanisms flagged automatically; storage region read where the source exposes it |
| D14.8 | Special-category personal data and extra safeguards | Each special category listed in art. 6/1 (health, biometric, genetic, criminal convictions, religion, ethnicity, political opinion, union membership, sex life, ...) as its own label on columns | Status of extra safeguards (scoped/timed access, periodic review, 2FA for remote access, encryption) read from source or declared; gaps reported; category policy auto-applied to newly found columns |
| D14.9 | Data processor relationship and contract basis | Counterparty identity and legal role (controller/processor/joint) as fields | Processor's reachable asset set derived via lineage; contract end triggers destruction/return list |
| D14.10 | Privacy notice versioned and bound to assets | Notice as an object with version and effective date, linked to assets | Drift between asset purpose/basis/recipients and the bound notice reported; update prompted |
| D14.11 | Publicly published personal data | Published flag, URL and date on the asset or its output | Published content re-scanned; personal-data items without masking/anonymisation or legal basis reported |
