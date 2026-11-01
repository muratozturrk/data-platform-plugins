# Capability Reference Model (Rubric v1.0, frozen)

A product-neutral model for scoring data governance software, or a whole governance platform made
of several tools. Frozen on 2026-08-11 **before** any product was inspected. **14 domains, 90
capabilities, 48 registered sources** (frameworks 17, legal texts 12, academic 9, standards 6,
specifications 2, guide 1, analyst 1). Every capability has a definition, a rationale, at least one
source and five levels.

How to read the tables:

- **Level 0** is always "the concept is absent from the product model; done outside or by hand",
  except **D12.1**, where 0 = more than four mandatory external infrastructure components.
- **1** = free text, manual, unofficial plug-in or one narrow case; **2** = structured, part of the
  product, but fed by hand; **3** = level 2 + automatic, fine-grained, reported or wired into a
  workflow; **4** = level 3 + the rare behaviour (auditability, immutability, cross-system
  inference, automatic non-conformity detection, closed loop).
- Cells for 3 and 4 are cumulative ("+" = everything in the level below, plus...).
- **KA** = DAMA-DMBOK2 knowledge area the capability cites in its `sources` (ordinal numbering
  KA1-KA11, see [dmbok-coverage.md](dmbok-coverage.md)); "—" = no DMBOK2 source (post-DMBOK
  additions: data mesh, AI, local law, and a few platform items).
- Anchors are condensed English translations. When a score hinges on a 2/3 or 3/4 boundary, copy
  the full level text from the research rubric into your assessment notes before scoring.

## Domain map

| ID | Domain | Caps | Derived from |
|---|---|---|---|
| D1 | Metadata management and model extensibility | 7 | DMBOK2 KA10 Metadata |
| D2 | Discovery, search and catalog | 6 | KA10 Metadata |
| D3 | Lineage and impact analysis | 5 | KA6 Data Integration and Interoperability |
| D4 | Business glossary, taxonomy and semantic layer | 5 | KA3 Data Modeling and Design; KA8 Reference and Master Data |
| D5 | Data quality | 7 | KA11 Data Quality |
| D6 | Security, privacy and compliance | 8 | KA5 Data Security |
| D7 | Governance processes and organisation | 6 | KA1 Data Governance and Stewardship |
| D8 | Master and reference data | 4 | KA8 Reference and Master Data; KA9 DW/BI |
| D9 | Data products, domains and mesh | 6 | Data mesh governance (post-DMBOK) |
| D10 | Observability and operations | 6 | KA4 Data Storage and Operations |
| D11 | Integration and extensibility | 6 | KA6 Data Integration and Interoperability |
| D12 | Platform, architecture and operational maturity | 8 | KA2 Data Architecture; KA4 Data Storage and Operations |
| D13 | AI and ML governance | 5 | EU AI Act Art. 10 (post-DMBOK) |
| D14 | KVKK and regulatory compliance | 11 | Turkish Law No. 6698 and secondary legislation (no DMBOK/DCAM/ISO counterpart) |

Per-domain counts: `D1:7 D2:6 D3:5 D4:5 D5:7 D6:8 D7:6 D8:4 D9:6 D10:6 D11:6 D12:8 D13:5 D14:11`.

## D1 Metadata management and model extensibility

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D1.1 | Entity model coverage | 10 | Fixed table + column only | DB, table, column, view, basic pipeline | + dashboards, ML models, topics, containers, object-store collections | User can define new entity types (schema-based model) |
| D1.2 | Custom properties | 10 | Free-text tag or one description field | Typed fields via UI, some entity types only | All types, nested, required/validation rules, API-managed | Versioned definitions, compatibility check, CI/CD-style publish |
| D1.3 | Schema standard and openness | 10 | Documented (PDF/wiki), not machine-readable | Product-specific machine-readable (JSON Schema, IDL) | Mapped to a recognised open standard; schemas in a separate versioned repo | Third-party tools consume it directly; compatibility tests published |
| D1.4 | Versioning and history | 10 | Last modified time/user only | Change log for some fields | Full history all fields, rollback, version diff | Event-sourced; point-in-time catalog state queryable via API |
| D1.5 | API/SDK maturity | 10 | Read-only API | Read/write REST/GraphQL, no official SDK | Official SDK, bulk import/export, paging, token/OAuth | Multi-language SDKs + CLI + schema-first API; documented versioning/compat guarantee |
| D1.6 | Change events / webhooks | 10 | Internal notice only, not exposed | Events on a queue/topic, schema undocumented | Documented event schema via webhook or bus; filtered subscriptions | Open event standard, at-least-once, replay, API-managed subscriptions |
| D1.7 | Whole-catalog export and portability | 10 | Partial list/CSV, relationships lost | All types with relationships, structured format | Lossless restore into another install, documented | Open exchange standard + official converter to a third-party product |

## D2 Discovery, search and catalog

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D2.1 | Search relevance and ranking | 10 | Exact/substring match | Full-text with term-frequency score | Field weighting, fuzzy match, popularity ranking | Learning-to-rank from interaction signals; admin-tunable weights |
| D2.2 | Faceted filtering | 10 | One criterion | Several predefined, static facets | Dynamic counts, custom fields as facets, multi-select | Per-role facets, hierarchical facets, shared saved views |
| D2.3 | Natural-language search | 10 | Synonym expansion | Semantic/vector search | NL questions routed to assets, re-ranked by intent | LLM turns the question into a structured query; conversational answer |
| D2.4 | Recommendations | — | Static most-viewed / newest | Directly related assets (lineage, tag) | Personalised from user history | Learned from usage (co-queried sets), with explanation |
| D2.5 | Personalisation | 10 | Bookmarks | Recents, saved searches, notification prefs | Home page per team/role, shareable saved views | Adaptive UI; central role-based default views |
| D2.6 | Navigation and asset page | 10 | Name, type, static schema | Description, owner, tags, schema on one page | + lineage, quality, usage, glossary terms on one page | Per-type page templates; graph navigation |

## D3 Lineage and impact analysis

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D3.1 | Table-level lineage | 6 | Free-text only | Manual structured graph | Automatic from runtime or query logs; API | Stitched end-to-end across systems; broken chains detected |
| D3.2 | Column-level lineage | 6 | Manual, one by one | Automatic for copies/renames | Parsed SQL incl. joins, aggregates, CASE; API | Transformation formula shown; cross-system column chains (DB → BI field) |
| D3.3 | Extraction method | 6 | Static from specific tool configs | SQL parsing, >= 3 dialects | + runtime observation; results merged | Pluggable extractors; per-edge confidence |
| D3.4 | Impact analysis | 6 | Direct downstream only | Transitive graph | Filtered downstream; affected assets/owners exportable | What-if simulation; owner notification/approval triggered |
| D3.5 | OpenLineage compatibility | 6 | One-way community plug-in | Official consumer (Job/Run/Dataset) | Consumer + producer; core facets | Column-lineage and job/run facets; compliance test suite |

## D4 Business glossary, taxonomy and semantic layer

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D4.1 | Glossary and term lifecycle | 3, 8 | Flat list, no status | Categories, owner, manual status | Approval flow draft → review → approved → deprecated, role-gated | Configurable state machine; versioned terms; impact shown before approval |
| D4.2 | Hierarchy and relations | 3 | Single-level grouping | Multi-level parent-child | Typed relations (synonym, related, antonym), graph view | User-defined relation types, poly-hierarchy, integrity checks |
| D4.3 | Auto-tagging and term suggestion | 3 | Name regex | Content-sampling patterns | Statistical/ML from name + description + sample; confidence; approval | LLM context-aware; bulk accept/reject with reasons |
| D4.4 | Ontology / RDF / OWL / SKOS | 3 | Flat CSV, hierarchy lost | JSON/XML with hierarchy, no standard | SKOS export (broader/narrower/related) | Full RDF/OWL, URIs, linked data, SPARQL or equivalent |
| D4.5 | Term ↔ technical asset mapping | 8, 3 | Manual 1:1 | Many-to-many, shown on asset page | Bulk or via suggestions; assets per term via API | Value/cell-level scope; integrity checked on change |

## D5 Data quality

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D5.1 | Rule authoring | 11 | Code/script only | UI templates (not-null, unique, range) | + custom expression/SQL; cross-asset rules | NL → rule or rules inferred from history; shared versioned libraries |
| D5.2 | Profiling | 11 | Row/column counts | Null ratio, distinct, min/max, automatic | Histograms, type consistency, patterns, samples; scheduled | Trends, correlations, candidate keys; feeds rule suggestions |
| D5.3 | Test execution and scheduling | 11 | Manual one-off | Cron schedule | Pipeline/event-triggered; circuit breaker | Orchestrator API, incremental runs, run history/performance |
| D5.4 | Incident and SLA management | 11 | Log only | Incident record with status + assignee | Severity + resolution SLA; escalation on breach | Two-way ITSM integration; RCA; recurring-pattern reports |
| D5.5 | Quality score and reporting | 11 | Pass/fail ratio | Multi-dimensional asset score | Rolled up to domain/org, trend dashboard, API export | Configurable weights tied to business impact; scheduled reports |
| D5.6 | Anomaly detection | 11 | Fixed manual threshold | Dynamic threshold (moving avg/std) | Seasonality/trend models; false-positive feedback | Thresholds learned, new rules proposed with reasons |
| D5.7 | External DQ tool integration | 11, 6 | Manual upload | >= 1 official automatic integration | Several tools via standard API; unified with native results | Two-way: rules pushed to the tool; pluggable |

## D6 Security, privacy and compliance

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D6.1 | PII detection and classification | 5 | Fixed patterns on column names | + sample content patterns; sensitivity tag | ML, >= 10 types, confidence, user feedback | LLM context-aware; custom taxonomies; bulk tagging via workflow |
| D6.2 | Policy engine (RBAC/ABAC) | 5 | Fixed roles | Custom roles, per-asset permissions | + attribute conditions (department, sensitivity tag); API-managed | + per user-asset decision explanation via API |
| D6.3 | Masking and pseudonymisation | 5 | Static, on sample/preview only | Column policy at query time, one method | Selectable methods (partial, hash, constant, FPE) per role | + reversible pseudonymisation under controlled re-identification |
| D6.4 | Row- and column-level access | 5, 4 | Hide columns, no row filter | + fixed row filter | Row filter bound to user attributes; same in UI and API | Compound rules, performance monitoring, "what would X see" preview |
| D6.5 | Retention policy | 5, 4 | Free-text note | Structured retention field; expired list | Action on expiry; bulk assignment by type/tag | Storage-integrated enforcement verified; exceptions via workflow; audited |
| D6.6 | Audit trail and immutability | 5, 1 | Admin actions only | Config/metadata changes (who/when/what) | + data access/query level; filter, export, retention | Immutable (append-only/signed); streamed to SIEM; integrity verifiable |
| D6.7 | External authorisation engine | 5, 6 | Identity/groups from directory, decision internal | Coarse decisions from external policy server | Fine-grained decisions delegated to an external PDP | Policies authored centrally outside; open protocol; shared by several products |
| D6.8 | Anonymisation and PETs | — | Manual hash/field drop | Aggregation/generalisation policy (min group size) | Re-identification risk measured (e.g. k-anonymity) | Differential privacy budget or synthetic data; technique/parameters audited |

## D7 Governance processes and organisation

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D7.1 | Ownership and stewardship | 1 | Free-text owner name | Structured user/group owner; ownership type | Several ownership types at once; bulk; ownerless report | Rule-based suggestion/inheritance from org/domain; hand-off history and approval |
| D7.2 | Approval and review workflow | 1 | Notification after the fact | One-step, one-approver gate for some changes | Multi-step, per-type flows; history + rejection reason | Workflow engine: branching, timeouts, escalation, API, reusable templates |
| D7.3 | Policy lifecycle | 1 | Policy recorded, no status | Manual status draft/active/retired | Approval, effective/review dates, reminders | Version diff; policy ↔ asset/rule links; impact on change; audited |
| D7.4 | Certification and trust signal | 1, 10 | Free-text "trusted" label | Fixed certification states, manual | Approval-gated; expiry; search narrowing by status | Criteria-based (quality, owner, docs) suggestion; auto-revoke on expiry |
| D7.5 | Remediation campaigns | 1 | Manual list of gaps | Task list assigned, done/not done | Auto-created from criteria; progress %; due dates | Recurring; cross-team comparison; completion as governance KPI |
| D7.6 | Collaboration | 1, 10 | Free-text comments | Threads, @mentions with notification | Event subscriptions; external channel (e-mail/chat) | Discussion → task with tracking; per-user channel/frequency; API history |

## D8 Master and reference data

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D8.1 | Reference data sets | 8 | Static manual value list | Structured value-label pairs bound to assets | Versioned, history, bulk import/export; usage query | API-managed, value changes via approval; impact of deprecated codes |
| D8.2 | Code lists and crosswalks | 8 | Mapping kept outside | 1:1 code pairs stored | Many-to-many with match quality; bulk import/export | Real-time code-translation API; unmapped codes reported |
| D8.3 | MDM integration | 8, 6 | Static link to MDM entity | Batch sync of MDM entity definitions | Incremental/event sync; lineage to physical tables | Two-way (enrichment written back); MDM match/quality scores shown |
| D8.4 | Golden record and entity-resolution visibility | 8, 9 | Only in the external MDM report | Golden record as an asset linked to source records | Merge rules visible; merge history | Per-source match scores; user flags wrong merge → sent to MDM |

## D9 Data products, domains and mesh

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D9.1 | Domain modelling and ownership | 1 | Free-text domain tag | Domain object with owner | Sub-domains; domain-scoped admin rights | Federated local rules + enforced central minimums; domain health board |
| D9.2 | Data product as first-class asset | 10 | Free-text collection | Own type: owner, input/output ports | + SLO commitments, lifecycle status | SLO compliance measured live; product dependency graph; downstream breach notice |
| D9.3 | Data contract definition and validation | 6 | Free-text document | Structured schema/field rules, no checks | Automatic validation, violation alerts, versioned | Breaking-change analysis; incompatible change blocked without consumer consent |
| D9.4 | Access request and approval | 5 | Form, not tracked | Routed to owner, approve/reject, status | Time-boxed auto-expiring grants; justification required | Sensitivity-based approval chains; API history; periodic recertification |
| D9.5 | Marketplace and consumer experience | 10 | Filtered product list | Product page with SLO summary | Ratings, examples, side-by-side comparison | Personalised suggestions; one self-service consume flow; consumption metrics |
| D9.6 | External sharing and agreements | — | File handed over, not recorded | External consumer and share modelled | Time-boxed conditional share; auto-revoke; auditable | Open sharing protocol without copying; recipient usage visible |

## D10 Observability and operations

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D10.1 | Freshness monitoring | 4 | Static last-updated timestamp | Manual expected cadence, page warning | Continuous, alerts, history chart | Learned cadence, seasonal thresholds, downstream impact |
| D10.2 | Volume / row-count monitoring | 4 | On-demand count | Periodic count with chart | Threshold alerts per asset | Seasonal model; anomaly with explanation |
| D10.3 | Schema drift detection | 4, 10 | Manual comparison | Periodic scan with change list | Alerts; severity by change type | Downstream impact report; pipeline stop/approval on breaking change |
| D10.4 | Pipeline run status | 4, 6 | Job name as text | Last run status from orchestrator | Run history; failure alerts | Performance trends/anomalies; upstream chain health view |
| D10.5 | Usage and popularity | 4, 10 | Catalog page views | Query counts from source logs | Per user/team breakdown; most/least used board | Joined with lineage for removal impact; usage trend alerts |
| D10.6 | Cost and resource analytics | 4 | Raw size metric | Platform cost/usage per asset | Rolled up per asset/domain/team over time | Cost alerts; optimisation suggestions (low use + high cost) |

## D11 Integration and extensibility

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D11.1 | Connector breadth | 6, 10 | One category, < 10 connectors | >= 2 categories, 10-39 | >= 4 categories, >= 40, one config/auth model | > 40, open to community; per-connector status and quality documented |
| D11.2 | Ingestion framework / custom connectors | 6 | Hand-written scripts on the write API | Documented framework/SDK | Custom connector as testable plug-in, same scheduling/monitoring | Official SDK + starter kit + test scaffold; shared registry |
| D11.3 | Event bus / message queue | 6, 4 | Topic as an asset only | Consumes from one technology | Consumes and produces; several technologies | Schema registry validation, at-least-once, backpressure, code-managed |
| D11.4 | Webhooks, alerts, notifications | 6, 4 | One fixed webhook, few events | Chosen event types, several endpoints | + e-mail/chat/incident channel; filter by type and severity | Central programmatic routing; retries; delivery status audited |
| D11.5 | Plug-in / app framework | 6, 10 | One narrow extension point | Deploy-time plug-in packages | Official SDK for several extension points; hot enable/disable | Marketplace; plug-in permission model; independent versioning |
| D11.6 | Governance as code (GitOps) | 6 | Partial export, not re-importable | Declarative files applied by CLI/API | Versioned repo + CI/CD; drift detection | Everything as code; reviewed, tested; repo is source of truth |

## D12 Platform, architecture and operational maturity

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D12.1 | Deployment footprint (0 = > 4 mandatory components) | 2, 4 | 4 mandatory, incl. graph DB or distributed engine | 3 mandatory (RDBMS, search, queue) | <= 2 mandatory (RDBMS + search) | One RDBMS or fully embedded; all else optional |
| D12.2 | HA and horizontal scaling | 4, 2 | Read replicas, single writer | Active-passive auto failover | Active-active writes; manual/semi-auto scaling | Autoscaling; survives region loss; tested |
| D12.3 | Multi-tenancy and isolation | 4, 5 | Naming convention only | Per-tenant access isolation | Per-tenant config, quotas; usage measured per tenant | Physical/logical storage isolation; tenant export; noisy-neighbour isolation |
| D12.4 | Authentication, SSO, identity | 5 | One SSO protocol | Several protocols; roles from IdP groups | Standard provisioning (e.g. SCIM); deprovision closes access | Several IdPs at once; fine-grained roles from IdP attributes |
| D12.5 | Localisation (i18n) | — | Few UI languages, content single-language | Multilingual user content | Per-user language; fallback; missing-translation report | Translation workflow; machine pre-translation; coverage tracked |
| D12.6 | Community health and sustainability | — | Release notes only | Public history; >= yearly releases | < 70 % of last-12-month commits from one organisation; security process documented | Independent foundation; public roadmap, LTS, decision process |
| D12.7 | Upgrade and backward compatibility | 4, 2 | Documented manual step-by-step | Automated migration tool | Minor-version compat guaranteed; deprecation notice period | Zero-downtime rolling upgrade; rollback; committed support windows |
| D12.8 | Backup, restore, DR | 4 | Documented manual steps | Product backup/restore tool for all mandatory components | Scheduled backups; cross-version restore guaranteed | Point-in-time restore; multi-region replicas; committed RPO/RTO |

## D13 AI and ML governance

| ID | Capability | KA | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|---|
| D13.1 | ML model and feature catalog | 10, 9 | Version as free text | Structured versions; feature list | + training run, hyper-parameters, metrics; registry integration | Metrics/drift over time; feature → model impact |
| D13.2 | AI lineage (data → model → output) | 6 | Free-text training source | Manual dataset → model edge | Automatic from training runtime; outputs included | Feeds impact analysis; chain exportable for audit |
| D13.3 | Dataset and model cards | — | Free-text description | Fixed card template | Fields auto-filled from catalog; completeness report | LLM-drafted cards kept in sync with real metrics |
| D13.4 | AI risk classification and compliance record | 1 | Free-text risk note | Fixed risk tier assigned manually | Required-document checklist per tier; compliance board | Rule-based tier suggestion; continuous compliance tracking |
| D13.5 | Generative-AI assets (prompts, embeddings, vector stores, RAG) | — | Prompt kept outside | Prompts and vector collections as assets | RAG chain (source → embedding model → collection) as lineage; policies | Any output reconstructable from prompt + source + model versions |

## D14 KVKK and regulatory compliance (local-law slice)

D14 measures only the **regulatory delta** on top of D6/D9 mechanisms (double-counting rule:
D6.5 retention, D6.1 detection, D6.8 anonymisation technique and D9.6 sharing are not re-scored).
Every D14 rationale names the D6/D9 capability it builds on. For another jurisdiction, replace
D14 with an equivalent local slice and log it as a rubric change (see
[assessment-procedure.md](assessment-procedure.md)).

| ID | Capability | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|
| D14.1 | Processing inventory derived from the catalog | PII assets listable, inventory fields absent | Category, data-subject group, recipient group as fields | + max retention, transfers, measures; auto-filled | Continuous; new/lost PII fields reported as dated diff; versioned |
| D14.2 | Purpose and legal basis per asset | Purpose in free text | Purpose + legal basis from a list | + column-level; out-of-scope (full exemption) as a value | Several purpose/basis pairs each with own retention; in/out-of-scope reports |
| D14.3 | Destruction method and periodic destruction | Free-text note | Method from closed list (erase, destroy, anonymise) | + periodic job <= 6 months, each run recorded | Immutable records >= 3 years; third parties to notify from lineage |
| D14.4 | Consent status and withdrawal downstream | Free-text tag | Structured consent status | Read from a consent registry; withdrawal lists downstream via lineage | Withdrawal creates destruction task; closure tracked per asset |
| D14.5 | Data subject request: find data and workflow | Manual search | Identifier → assets/columns from catalog | Request record: all Art. 11/1 (a)-(ğ) types, date, assignee, 30-day countdown | Scope expanded via lineage incl. copies and recipients; full trail |
| D14.6 | Breach scope inference | Manual from access logs | Categories/subject groups for a chosen asset set | + downstream copies via lineage; special-category split | Breach record with separate learned-at date driving the 72 h clock |
| D14.7 | Cross-border transfer and legal mechanism | Recipient as free text | Recipient country; domestic vs abroad | + mechanism (adequacy, safeguards, incidental exception) and document | Transfers without valid basis flagged; region read where source exposes it |
| D14.8 | Special-category data and extra measures | One generic sensitivity tag | Law's categories as separate labels | Per-category policy that overrides the generic one | Extra measures' status recorded from source or declaration |
| D14.9 | Processor relationship and contract | Counterparty as free text | Counterparty identity and legal role | + contract reference, validity; missing/expired flagged | Processor's asset set via lineage; termination → return/destroy list |
| D14.10 | Versioned privacy notice linked to assets | Link in description | Notice object (version, effective date) linked to assets | + PII assets without a notice flagged; which version applied when | Asset fields compared with notice sections; drift reported |
| D14.11 | Publicly published personal data | Free-text note | Published flag with address and date | + masking/anonymisation method, time on line; expired items listed | Live items rescanned; unmasked or basis-less items flagged |

Legal sources for D14 (article numbers and durations verified against official texts) are listed
in [sources.md](sources.md). The rubric is not legal advice.
