# OpenMetadata 1.13.3 Assessment Results

Rubric v1.0 (frozen 2026-08-11), all products inspected 2026-08-11 from release-tag clones.
Evidence paths are relative to each product's repository root at the pinned commit.

| Product | Tag | Commit | Scored | Evidence mix | Score distribution 0/1/2/3/4 |
|---|---|---|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b` | 90/90 | 87 K, 3 D, B 0% | 4 / 13 / 28 / 39 / 6 |
| DataHub | `v1.7.0` | `7f81ccbfe27b` | 90/90 | 86 K, 4 D, B 0% | 4 / 16 / 33 / 30 / 7 |
| Apache Atlas | `release-2.5.0` | `234608315f29` | 90/90 | 84 K, 3 D, 3 İ, B 0% | 21 / 28 / 29 / 10 / 2 |

Unweighted mean of all 90 scores: OpenMetadata 2.33, DataHub 2.22, Atlas 1.38. A single overall
number hides the profile; use the domain table.

## Domain Means

Mean over scored capabilities; every cell has full coverage (n/n). Spread = max - min of the three
means; near 0 means the domain does not separate these products.

| Domain | OM | DataHub | Atlas | Spread |
|---|---|---|---|---|
| D1 Metadata management and model extensibility | 2.7 | **3.3** | 3.0 | 0.6 |
| D2 Discovery, search and catalog | 3.0 | **3.2** | 1.5 | 1.7 |
| D3 Lineage and impact analysis | 2.8 | **3.0** | 2.2 | 0.8 |
| D4 Glossary, taxonomy, semantic layer | **3.4** | 2.4 | 1.8 | 1.6 |
| D5 Data quality | **2.4** | 1.4 | 0.3 | 2.1 |
| D6 Security, privacy and compliance | **1.8** | 1.6 | 1.2 | 0.5 |
| D7 Governance processes and organisation | **2.5** | 1.8 | 0.5 | 2.0 |
| D8 Master and reference data | **1.2** | 1.0 | 1.0 | 0.2 |
| D9 Data products, domains and mesh | **2.2** | 1.8 | 0.7 | 1.5 |
| D10 Observability and operations | **2.7** | 2.3 | 1.0 | 1.7 |
| D11 Integration and extensibility | **2.8** | 2.7 | 1.5 | 1.3 |
| D12 Platform and operational maturity | 1.9 | 1.9 | 1.4 | 0.5 |
| D13 AI and ML governance | 2.2 | **2.8** | 1.0 | 1.8 |
| D14 KVKK and regulatory compliance | 1.7 | **2.1** | 1.8 | 0.4 |

Reading: OpenMetadata leads in governance *process* (D4, D5, D7, D9, D10); DataHub leads in model
extensibility, lineage depth and ML (D1, D3, D13). Both are weak where a catalog cannot reach:
D6 (no data-path enforcement), D8 (not an MDM system), D12, and D14 (no structured legal objects).

## Full Matrix

`score`, with evidence type in brackets when not `K`.

| ID | Capability | OM | DH | Atlas |
|---|---|---|---|---|
| D1.1 | Entity model coverage | 3 | 4 | 4 |
| D1.2 | Custom properties | 2 | 4 | 4 |
| D1.3 | Schema standard and openness | 3 | 3 | 2 |
| D1.4 | Versioning and history | 3 | 3 | 2 |
| D1.5 | API/SDK maturity | 3 | 3 | 3 |
| D1.6 | Change events | 3 | 3 | 3 |
| D1.7 | Whole-catalog export | 2 | 3 | 3 (D) |
| D2.1 | Search relevance | 4 | 3 | 2 |
| D2.2 | Faceted filtering | 3 | 4 | 2 |
| D2.3 | Natural-language search | 2 | 2 | 1 |
| D2.4 | Recommendations | 2 | 3 | 1 |
| D2.5 | Personalisation | 3 | 3 | 1 |
| D2.6 | Entity page and navigation | 4 | 4 | 2 |
| D3.1 | Table lineage | 3 | 3 | 3 |
| D3.2 | Column lineage | 3 | 4 | 3 |
| D3.3 | Lineage extraction method | 3 | 3 | 3 |
| D3.4 | Impact analysis | 3 | 3 | 2 |
| D3.5 | OpenLineage | 2 | 2 | 0 (İ) |
| D4.1 | Glossary and term lifecycle | 3 | 3 | 1 |
| D4.2 | Hierarchy and relations | 4 | 3 | 3 |
| D4.3 | Auto-tagging / term suggestion | 3 | 1 | 0 |
| D4.4 | RDF/OWL/SKOS | 4 | 2 | 2 |
| D4.5 | Term-to-asset mapping | 3 | 3 | 3 |
| D5.1 | Rule authoring | 3 | 1 | 0 |
| D5.2 | Profiling | 3 | 3 | 1 |
| D5.3 | Test execution and scheduling | 2 | 0 | 0 |
| D5.4 | Incidents and SLA | 2 | 2 | 0 |
| D5.5 | Quality score | 3 | 1 | 0 |
| D5.6 | Anomaly detection | 1 | 0 | 0 |
| D5.7 | External DQ tool integration | 3 | 3 | 1 |
| D6.1 | PII detection | 3 | 1 | 0 |
| D6.2 | Policy engine RBAC/ABAC | 4 | 3 | 2 |
| D6.3 | Masking/pseudonymisation | 1 | 1 | 0 |
| D6.4 | Row/column access control | 0 | 1 (D) | 1 |
| D6.5 | Retention policy | 2 | 2 | 2 |
| D6.6 | Audit trail | 3 | 3 | 2 |
| D6.7 | External authoriser | 1 | 2 | 3 |
| D6.8 | Anonymisation/PETs | 0 | 0 | 0 |
| D7.1 | Ownership/stewardship | 3 | 3 | 1 |
| D7.2 | Approval workflow | 3 | 1 | 0 (İ) |
| D7.3 | Policy lifecycle | 1 | 2 | 0 |
| D7.4 | Certification | 3 | 2 | 1 |
| D7.5 | Remediation campaigns | 2 | 2 | 1 |
| D7.6 | Collaboration | 3 | 1 | 0 |
| D8.1 | Reference data sets | 2 | 2 | 2 |
| D8.2 | Code lists and mapping | 2 | 1 | 2 |
| D8.3 | MDM integration | 1 (D) | 1 | 0 |
| D8.4 | Golden record | 0 (D) | 0 | 0 |
| D9.1 | Domains | 3 | 3 | 1 |
| D9.2 | Data product entity | 3 | 2 | 1 (İ) |
| D9.3 | Data contracts | 3 | 2 | 1 |
| D9.4 | Access requests | 1 | 1 | 0 |
| D9.5 | Marketplace | 2 | 2 | 0 |
| D9.6 | External sharing | 1 (D) | 1 | 1 |
| D10.1 | Freshness | 1 | 2 | 1 |
| D10.2 | Volume | 3 | 2 | 1 |
| D10.3 | Schema drift | 3 | 3 | 2 |
| D10.4 | Pipeline runs | 3 | 3 | 1 |
| D10.5 | Usage analytics | 3 | 3 | 0 |
| D10.6 | Cost analytics | 3 | 1 | 1 |
| D11.1 | Connector breadth | 3 | 4 | 1 |
| D11.2 | Ingestion framework | 3 | 2 | 2 |
| D11.3 | Event bus | 2 | 3 | 2 |
| D11.4 | Webhooks/alerts | 4 | 2 | 0 |
| D11.5 | Plugin/app framework | 3 | 2 | 2 |
| D11.6 | Governance as code | 2 | 3 | 2 |
| D12.1 | Deployment footprint | 3 | 3 | 1 |
| D12.2 | HA and scaling | 3 | 3 (D) | 2 |
| D12.3 | Multi-tenancy | 1 | 1 (D) | 1 |
| D12.4 | AuthN/SSO | 3 | 1 | 2 |
| D12.5 | i18n | 1 | 1 | 0 |
| D12.6 | Community health | 2 | 2 | 2 (D) |
| D12.7 | Upgrades | 2 | 2 (D) | 2 |
| D12.8 | Backup/DR | 0 | 2 | 1 (D) |
| D13.1 | ML model/feature catalog | 2 | 3 | 1 |
| D13.2 | AI lineage | 2 | 4 | 2 |
| D13.3 | Dataset/model cards | 2 | 3 | 1 |
| D13.4 | AI risk classification | 2 | 2 | 1 |
| D13.5 | GenAI assets | 3 | 2 | 0 |
| D14.1 | Processing inventory (VERBİS) | 2 | 2 | 2 |
| D14.2 | Purpose and legal basis | 2 | 3 | 2 |
| D14.3 | Destruction method, periodic destruction | 2 | 2 | 2 |
| D14.4 | Explicit consent | 2 | 2 | 2 |
| D14.5 | Data subject requests | 2 | 2 | 2 |
| D14.6 | Breach scope | 2 | 2 | 2 |
| D14.7 | Cross-border transfer | 1 | 2 | 1 |
| D14.8 | Special-category data | 2 | 2 | 3 |
| D14.9 | Processor relationship | 1 | 2 | 1 |
| D14.10 | Privacy notice binding | 1 | 2 | 1 |
| D14.11 | Public publication | 2 | 2 | 2 |

## OpenMetadata Level-4 Cells

| ID | What earns 4 | Evidence |
|---|---|---|
| D2.1 | Field weights, exact/phrase/fuzzy boosts, usage and vote signals as default function-score boosts; admin-editable with `/search/preview` | `searchSettings.json:89-126,270-375`; `ElasticSearchSourceBuilderFactory.java:720-950` |
| D2.6 | Table page merges schema, profile/quality, lineage, queries, contract, terms, custom properties; per-persona page layout for 27 entity types; lineage and knowledge-graph tabs | `TableClassBase.ts:60-145`; `system/ui/page.json:8-45` |
| D4.2 | User-defined relation types (inverse, `rdfPredicate`, cardinality, symmetric/transitive); multi-parent broader/narrower; cycle detection enforced | `glossaryTermRelationSettings.json:21-94`; `GlossaryTermRepository.java:1626-1680` |
| D4.4 | OWL ontology importing SKOS/DCAT/PROV; glossary export as Turtle/RDF-XML/N-Triples/JSON-LD; term URIs; SPARQL query/update. Off by default (`RDF_ENABLED=false`, `conf/openmetadata.yaml:717`), needs Fuseki | `rdf/ontology/openmetadata.ttl:26-33,223-254`; `RdfResource.java:392-451,792-861` |
| D6.2 | RBAC + 13 SpEL attribute functions (`isOwner`, `hasDomain`, `matchAnyTag`, `matchTeam`...); `/permissions/debug/evaluate` explains which policy and rule decided; deny precedence fixed in code. Decisions are not written to the audit log | `RuleEvaluator.java:55-398`; `PermissionsResource.java:296-345` |
| D11.4 | `eventSubscription` entity: webhook, Slack, Teams, GChat, email; SpEL routing by event type, severity, owner, domain, field change; configurable retries, failed-event records, delivery status | `events/eventSubscription.json:98-200,333-338`; `AbstractEventConsumer.java:144-200`; `events/subscriptionStatus.json` |

## OpenMetadata Level-0 and Notable Level-1 Cells

| ID | Score | Finding (searched places in brackets) | Evidence |
|---|---|---|---|
| D6.4 | 0 | No column- or row-scoped operation in the access model; product not in the data path; contract `rowFilters` are documentary only [code + governance docs] | `resourceDescriptor.json:13-71`; docs how-to-guides/data-governance |
| D6.8 | 0 | No k-anonymity, differential privacy, synthetic data or generalisation concept; only fixed masking [code + docs] | `security/mask/PIIMasker.java:45-48` |
| D8.4 | 0 | No schema/code for golden record, entity resolution, survivorship [repo + docs] | docs how-to-guides/data-governance |
| D12.8 | 0 | No backup command; `restore.py` exists but is not registered in the CLI command list; no backup doc in repo; rely on the database backup (search index rebuilt via reindex) | `ingestion/src/metadata/cmd.py:44-55`; `ingestion/src/metadata/cli/restore.py:26-63` |
| D6.3 | 1 | Masking only of catalog-held samples, profiles, query text and email; one fixed tag (`PII.Sensitive`), fixed `********` | `PIIMasker.java:45-48,54-120` |
| D6.7 | 1 | Identity from external IdP, decision inside OM; pluggable `authorizerConfiguration.className`, no ready OPA/Ranger decision point | `conf/openmetadata.yaml:330-342`; `Authorizer.java:56` |
| D7.3 | 1 | Policy has only `enabled`; no draft/review state or effective/review dates (those exist only on AI governance policies) | `policies/policy.json:18-119` |
| D9.4 | 1 | No access-request task type; approval grants nothing; no time-boxed access | `feed/thread.json:9-21,49-101` |
| D10.1 | 1 | Last-updated timestamp shown; freshness test not among the 25 built-in tests; contract `refreshFrequency`/`maxLatency` never evaluated | `type/lifeCycle.json` |
| D5.6 | 1 | Only a `useDynamicAssertion` flag; no anomaly model in the repo or docs | `tests/testCase.json:150-153` |
| D12.3, D12.5 | 1 | No tenant concept (domains/teams only); UI in 19 languages incl. Turkish but user content single-language | `DomainAccessPolicy.json:1-27`; `i18nextUtil.ts:25-44` |
| D14.7, D14.9, D14.10 | 1 | No transfer, counterparty or privacy-notice object; only free text, custom properties or contract `termsOfUse` | `dataContract.json:57-127,222-234`; `docStore/document.json` |

Representative 3s: PII detection with Presidio/spaCy NER over name + samples, 41 PII types,
feedback workflow (D6.1, `ingestion/src/metadata/pii/scanners/ner_scanner.py:57-79`); Flowable-based
multi-step approval workflows with JsonLogic branching, no timeout/escalation (D7.2,
`openmetadata-service/pom.xml:918-928`); data contracts validated on schedule and via `/validate`,
ODCS import/export, no breaking-change gate (D9.3, `dataContract.json:285-411`); append-only
`audit_log_event` table, no cryptographic integrity (D6.6, `AuditLogResource.java:51,172-192`);
only RDBMS + search engine mandatory (D12.1, `docker-compose-quickstart/docker-compose.yml:19-63`).

## Where DataHub or Atlas Score Higher

| ID | OM | Other | Why (other product's evidence) |
|---|---|---|---|
| D1.1 | 3 | DH 4, Atlas 4 | OM cannot define new entity types (`TypeResource.java:78-85`); DH via custom model plugin, Atlas via typedef REST |
| D1.2 | 2 | DH 4, Atlas 4 | DH structured properties on all types incl. schema fields, versioned (`StructuredPropertyDefinition.pdl:104-150`); Atlas business metadata with compatibility enforcement |
| D3.2 | 3 | DH 4 | DH stores the transform formula and chains to BI fields (`FineGrainedLineage.pdl:44-57`) |
| D6.7 | 1 | Atlas 3 | Atlas delegates decisions to Apache Ranger (`AtlasAuthorizerFactory.java:32-86`) |
| D11.1 | 3 | DH 4 | DH: 116 source plugins sharing one config/secret model (`metadata-ingestion/setup.py:1128-1245`) |
| D11.3, D11.6 | 2 | DH 3 | DH consumes and produces Kafka topics with schema registry; YAML + CLI with diff/dry-run |
| D12.8 | 0 | DH 2 | DH quickstart backup/restore + index rebuild |
| D13.2 | 2 | DH 4 | DH ML lineage written into the unified lineage graph (`MLModelLineageInfo.pdl:93-119`) |
| D14.2, D14.7, D14.9, D14.10 | 1-2 | DH 2-3 | DH closed-list structured properties and forms flag empty values; document entity links to assets |
| D14.8 | 2 | Atlas 3 | Atlas: separate classification per special category with its own policy |

Note: several DataHub governance features (approval workflow, discussions, assertion evaluation)
are in the commercial cloud edition, which is why its D5 and D7 scores are low on open source.

## DAMA-DMBOK2 Coverage

Capability-level mapping through each capability's `ref:dmbok2-kaN` sources (95 mappings).

| KA | Knowledge area | Caps | OM | DH | Atlas |
|---|---|---|---|---|---|
| KA1 | Data Governance and Stewardship | 9 | **2.6** | 2.1 | 0.8 |
| KA2 | Data Architecture | 3 | 2.7 | 2.7 | 1.7 |
| KA3 | Data Modeling and Design | 5 | **3.4** | 2.4 | 1.8 |
| KA4 | Data Storage and Operations | 15 | 2.2 | 2.2 | 1.2 |
| KA5 | Data Security | 10 | **1.9** | 1.6 | 1.3 |
| KA6 | Data Integration and Interoperability | 17 | 2.6 | **2.7** | 1.6 |
| KA7 | Documents and Content | 0 | not covered | not covered | not covered |
| KA8 | Reference and Master Data | 6 | **1.8** | 1.7 | 1.3 |
| KA9 | DW/BI | 2 | 1.0 | **1.5** | 0.5 |
| KA10 | Metadata | 21 | 2.9 | 2.9 | 1.8 |
| KA11 | Data Quality | 7 | **2.4** | 1.4 | 0.3 |

KA7 is a rubric scope gap, not a product result: no conclusion can be drawn. It matters for
privacy work because privacy notices, destruction reports and contracts are documents (see
D14.3, D14.10). Suggested for a next rubric version: a versioned document entity, and two-way
document-to-asset links inside retention/destruction. KA9 rests on two capabilities only. D14's
11 capabilities map to no DMBOK2 area; D9 and D13 map only indirectly.

## Community Sustainability (GitHub/JIRA data pulled 2026-08-11)

| Metric | OpenMetadata | DataHub | Apache Atlas |
|---|---|---|---|
| Stars / forks | 14,848 / 2,309 | 12,516 / 3,655 | 2,132 / 903 (mirror; misleading) |
| Contributors (cumulative) | 424 | 411 | 145 |
| Commits last 12 months (default branch) | 4,414 | 3,140 | 178 |
| Stable releases last 12 months | 59 (two maintenance lines 1.12.x and 1.13.x) | 15 | 1 (2.5.0; gaps of ~25 and ~16 months) |
| Open / closed issues | 724 / 9,788 (93% closed) | 518 / 2,283 (82%) | 1,053 / 4,266 in JIRA (80%) |
| Licence | Apache-2.0 core; `ingestion/` under Collate Community License (`ingestion/LICENSE:1-30`) | Apache-2.0 | Apache-2.0 |
| Governance | Single vendor (Collate), no foundation | Single-vendor weighted, no foundation | ASF top-level project, PMC-voted releases |
| D12.6 score | 2 | 2 | 2 |

OpenMetadata's release pace is also an upgrade burden. Atlas is the only one without
single-vendor risk, but its development flow is an order of magnitude lower.

## Cross-Check Findings

- **D11.4 spread (OM 4, DH 2, Atlas 0): real difference, kept.** OM meets all three level-4
  conditions (central programmatic routing, retries, delivery-status audit). DataHub's Actions
  framework lacks a generic webhook sink, severity filtering and API-managed routing. Atlas only
  logs operational events to file (code and docs searched).
- **Atlas D3.5 and D7.2 weak-zero flags: false alarms.** Notes cite a code search and an open or
  absent JIRA item; the keyword heuristic missed "JIRA". All 21 Atlas zeros were read: each
  documents two search places. No evidence file changed.
- **Three-product re-run (computed for this skill, not verified in the study):** with only these
  three products, D6.8 (anonymisation) and D8.4 (golden record) are 0 everywhere and would trip
  the all-zero flag. Both notes document two search places in each product, so they read as a
  category limit (catalogs are not anonymisation engines or MDM systems), not a rubric defect.
