# Modelling KVKK Compliance Metadata in OpenMetadata 1.13.3 Today

> **Not legal advice.** Field names and value lists below are a technical starting point; the
> legal-basis list, data categories and retention periods must be set with counsel.

Everything here uses structures that exist in OpenMetadata `1.13.3-release` without code changes.
Each row says whether the product **enforces** something or whether it is a **convention** your
team must keep. Evidence is `path:line` in the OM source at commit `255f6694913b`
(`spec/` = `openmetadata-spec/src/main/resources/json/schema/`,
`svc/` = `openmetadata-service/src/main/java/org/openmetadata/service/`,
`data/` = `openmetadata-service/src/main/resources/json/data/`).

## Ground rules (verified constraints)

| Constraint | Consequence | Evidence |
|---|---|---|
| Custom properties are typed and JSON-Schema-validated but have **no required flag** | An empty "legal basis" is legal for the product; completeness needs a rule check | `spec/type/customProperty.json:1-71`; `svc/jdbi3/TypeRepository.java:164-192` |
| Custom properties attach only to entity types whose schema has an `extension` field: table (and column), database, databaseSchema, container, glossaryTerm, glossary, domain, dataProduct, dataContract… | **Not** on team, user, services, policy, classification, docStore document (no `extension` in their schemas) | `spec/entity/data/table.json:374,1266`; `spec/entity/teams/team.json`, `spec/entity/services/databaseService.json` (searched: no `extension`) |
| No new entity types; only new properties on existing types | "Transfer record", "breach record", "DSAR", "counterparty" cannot be first-class objects | `svc/resources/types/TypeResource.java:78-85` |
| Available property types include enum (single/multi-select), date-cp, dateTime-cp, duration, markdown, email, hyperlink-cp, entityReference(List), table-cp, timestamp, plus string/integer | Closed lists → `enum`; versions/dates → `string`/`date-cp`; documents → `hyperlink-cp` | `spec/type/basic.json:27-157`; `spec/type/customProperties/enumConfig.json:9-19`; `spec/type/customProperties/complexTypes.json:136-157` |
| Access policies govern **metadata operations**, not data reads; no row/column operations | A "special-category policy" in OM hides catalog metadata; data-plane enforcement belongs in the query/storage layer (e.g. Ranger) | `spec/entity/policies/accessControl/resourceDescriptor.json:13-71` (D6.4 = 0) |
| Masking applies only to catalog-held sample data, profiles, query text, e-mail; hard-wired to tag `PII.Sensitive`, value `********` | New special-category tags are **not** masked | `svc/security/mask/PIIMasker.java:45-48,54-120` |
| Policy conflicts: deny evaluated first, fixed in code | A narrower category policy cannot *override* a generic one; it can only add denies | `svc/security/policyevaluator/PolicyEvaluator.java:44-79` (DENY pass at :66) |

## Built-in pieces to reuse

| Piece | What it gives you | Evidence |
|---|---|---|
| `PII` classification (`Sensitive`, `NonSensitive`, `None`) with auto-classification (`minimumConfidence` 0.6, `conflictResolution: highest_priority`) | Presidio/spaCy scanner over sample values + column names, 41 PII types, false-positive feedback loop | `data/tags/piiTagsWithRecognizers.json:1-12`; `ingestion/src/metadata/pii/scanners/ner_scanner.py:57-79`; `spec/type/recognizerFeedback.json:21-105` (D6.1 = 3) |
| `PersonalData` classification (`Personal`, `SpecialCategory`), `mutuallyExclusive: true`, GDPR-worded | A single, undivided special-category tag — not the KVKK Art. 6/1 list | `data/tags/personalDataTags.json` |
| `retentionPeriod` (ISO 8601 duration) on table, database, databaseSchema, container; inherited from parent | Structured max-retention; **no action on expiry** | `spec/entity/data/table.json:1210-1213`; `spec/type/basic.json:65-69` (D6.5 = 2) |
| `dataProduct.visibility` enum `PRIVATE·INVITATION·ORGANISATION·DATASPACE·PUBLIC` (default `PRIVATE`) | Ready "published" flag for data products only | `spec/entity/domains/dataProduct.json:36-41` |
| `dataContract` consumers ("Consumer AD Groups", free text) and `termsOfUse` (versioned markdown) | Closest thing to a recipient/agreement — unstructured | `spec/entity/data/dataContract.json:57-127,222-234` |
| Glossary terms: versioned, approval workflow, linkable to assets, accept custom properties | Controlled vocabularies and "document-like" objects (privacy notices, counterparties) | `data/governance/workflows/GlossaryApprovalWorkflow.json:58-206`; `spec/entity/data/glossaryTerm.json` (`extension`) |
| Entity version history with field-level diff | Who changed purpose/basis and when; base for VERBİS 7-day diffs | `spec/type/entityHistory.json:9-104`; `svc/jdbi3/EntityRepository.java:2408-2507` (D1.4 = 3) |
| Recursive CSV export with an `extension` column | Inventory/retention tables out of the catalog | `GET /api/v1/databaseSchemas/name/{fqn}/export?recursive=true` (`svc/resources/databases/DatabaseSchemaResource.java:589`); `data/entity/entityCsvDocumentation.json:5-183` (CSV `extension` at :100) |
| Lineage query and export with depth and filter; downstream-owner notification | Find copies for DSAR, consent withdrawal, breach scope | `GET /api/v1/lineage/getLineage?fqn=&upstreamDepth=&downstreamDepth=&query_filter=`, `/api/v1/lineage/export` (`svc/resources/lineage/LineageResource.java:190-259,402-486`) (D3.4 = 3) |
| Governance workflows (Flowable) with `checkEntityAttributesTask` (JsonLogic rules → true/false branch) and approval tasks | Non-conformity detection and routing to a person | `spec/governance/workflows/elements/nodes/automatedTask/checkEntityAttributesTask.json`; `spec/governance/workflows/workflowDefinition.json` (D7.2 = 3) |
| `entitySemantics` rules (JsonLogic, per entity type, enable/ignore lists) | Declarative "must have" checks | `spec/configuration/entityRulesSettings.json`; `spec/type/basic.json:242-305` |
| Event subscriptions: webhook, Slack, Teams, GChat, e-mail; retries, dead-letter, delivery status | Alerts on field changes (e.g. purpose changed) | `spec/events/eventSubscription.json:98-200` (D11.4 = 4) |
| Custom-property API | `PUT /api/v1/metadata/types/{id}` (operation `addProperty`) | `svc/resources/types/TypeResource.java:503-528` |

## Recommended compliance attribute set

Scope **T** = table, **C** = column, **DP** = data product, **G** = glossary term.
"Enforced" means the product guarantees it; "convention" means only your process does.

| KVKK attribute | Driven by | OM structure | Type / values | Scope | Enforced? |
|---|---|---|---|---|---|
| Is personal data / sensitivity | Law Art. 3, 6 | `PII` classification (auto) | `PII.Sensitive` / `NonSensitive` | C | Detection automatic (suggest/confidence); correctness is reviewed |
| Special category | Law Art. 6/1 | New classification `KVKK_SpecialCategory`, `mutuallyExclusive: false`, 11 tags | race & ethnic origin, political opinion, philosophical belief, religion/sect/other beliefs, dress & appearance, association/foundation/trade-union membership, health, sex life, criminal conviction & security measures, biometric, genetic | C | Tag values enforced; assignment is convention |
| Data category (VERBİS) | VERBİS Reg. Art. 4/1(h) | custom property | enum, multi-select, your VERBİS category list | T, C | Values enforced; presence convention |
| Data-subject group | VERBİS Reg. Art. 4/1(h) | custom property | enum, multi-select | T | Values enforced; presence convention |
| Recipient group | VERBİS Reg. Art. 4/1(h); Law Art. 16/3 | custom property | enum multi-select (or `entityReferenceList` to glossary terms) | T | Convention |
| Processing purpose | Law Art. 4/2(c) | custom property | enum multi-select | T, C | Convention |
| Legal basis | Law Art. 5, 6; Information Comm. Art. 5/1(h) | custom property | enum: each Art. 5 condition (incl. explicit consent), each Art. 6/3 (a–g) condition, plus **"Out of scope — Art. 28/1(b)"**, **"Out of scope — Art. 28/1(ç)"** … | T, C | Values enforced; presence via rule check only |
| Max retention | Law Art. 4/2(d); VERBİS Reg. Art. 9/4 | built-in `retentionPeriod` | ISO 8601 duration (e.g. `P2Y`) | T (inherits from schema/db) | Format enforced; **no expiry action** |
| Destruction method | Destruction Reg. Art. 4/1(c), 7/4 | custom property | enum `Erasure · Destruction · Anonymisation` | T | Convention |
| Periodic destruction interval | Destruction Reg. Art. 11/2 | custom property | `duration` (keep ≤ `P6M`) | T or schema | **Not** enforced — no bound check; validate in your job |
| Consent status | Law Art. 3/1(a), 5/1 | custom properties | enum `Obtained · Not obtained · Withdrawn`; consent text version (string); consent date (`date-cp`) | T, C | Convention; keep separate from notice (Information Comm. Art. 5/1(f)) |
| Data location country | Circular 2019/12; Law Art. 9 | custom property on **database / container** (services cannot hold properties) | enum of countries | database, container | Convention |
| Transfer abroad + mechanism | Law Art. 9; Cross-border Reg. Art. 6, 10 | custom properties | `transferAbroad` enum Yes/No; `transferMechanism` enum: Adequacy decision · Safeguard: agreement + Board permission · Safeguard: BCR · Safeguard: standard contract · Safeguard: undertaking + Board permission · Incidental exception Art. 9/6(x); `mechanismDocument` hyperlink-cp; `mechanismValidUntil` date-cp | T | Convention; one value per asset — cannot express several transfers |
| Counterparty and legal role | Law Art. 12/2; Cross-border Reg. Art. 4/1(ğ),(h) | glossary "Counterparties": one term per party with role enum `Controller · Processor · Joint controller`, contract hyperlink, contract date, valid-until; link assets → term via `entityReferenceList` | G + T | Convention |
| Privacy notice (aydınlatma metni) | Law Art. 10; Information Comm. Art. 5/1(e) | glossary "Privacy Notices": one term per notice, properties version (string), effective date (`date-cp`), document link; assign term to assets | G + T | Term versioned by product; "which version was linked on date X" must be read from version history |
| Publication status | PD 2026/1301 | `visibility=PUBLIC` for data products; else custom properties `publicationStatus` enum, `publicationUrl` hyperlink-cp, `publishedOn` date-cp, `maskingMethod` enum, `publishUntil` date-cp | DP, T | Convention; no expiry list |

Why glossary terms for notices and counterparties: they are the only objects that are versioned,
approval-gated, linkable to assets **and** accept custom properties. The research flags this as
"not clean but works" for notices; for counterparties the researched route via Team does not work
in 1.13.3 because `team.json` has no `extension` field (verified) — glossary terms do.
`entityReferenceList` values are validated only for existence of the referenced entity
(`svc/util/EntityUtil.java:1236-1287`), so an asset can point at the counterparty term.

## Turning conventions into checks

1. **Non-conformity list** (D14.2 level 3 behaviour): a governance workflow whose
   `checkEntityAttributesTask` JsonLogic rule tests "has `PII.Sensitive` (or a special-category tag)
   AND legal basis empty", false branch → approval/review task to the data steward. Same pattern for
   "personal data AND no privacy-notice term", "processor role AND contract expired".
2. **Change alerts**: an event subscription filtered on changes to purpose/recipient fields notifies
   the privacy team (Information Comm. Art. 5/1(b): re-inform *before* processing).
3. **Inventory export**: recursive schema CSV export → a small script maps columns and the
   `extension` column to the VERBİS layout. Keep the script in version control.
4. **Copies of a person's data**: search columns by PII tag / name (column index), then
   `getLineage` downstream from each hit; export with `/lineage/export`.
5. **Retention/destruction table** (Destruction Reg. Art. 6/1(g)): CSV of `retentionPeriod` +
   destruction method + data category.

These checks can run on a timetable inside OpenMetadata with a governance workflow that uses the
cron-scheduled `periodicBatchEntityTrigger` (`periodicBatchEntityTrigger.json:12-75`: schedule,
entity types, search filter, batch size). What OpenMetadata lacks is a due-date field on tasks and
requests, so anything that runs from a per-case start date (30 days from an application, 72 h from
learning of a breach, 5 business days from signatures) must be timed by an external orchestrator
such as Airflow, which then writes its result back to the catalog. Actual destruction of data also
runs outside the catalog.

## What not to model in OpenMetadata

| Tempting choice | Why not | Do instead |
|---|---|---|
| Rely on `PersonalData.SpecialCategory` for KVKK | One undivided, GDPR-worded tag; mutually exclusive with `Personal` | Own 11-tag classification |
| Rely on `DataRetention` app for destruction | It only cleans catalog-internal records (orphans, old events) | External destruction job; catalog stores method and result |
| Treat a data-quality "incident" as a breach record | "Incident" is a DQ test-failure concept | Breach record outside OM (or a convention on a task) until a dedicated object exists |
| Encode legal basis in free-text description | Not auditable or queryable | Enum custom property |
| Model consent-based transfer abroad as a general basis | Law No. 7499: consent is only an incidental exception; not for public-law activities (Art. 9/7) | Use the three-tier enum |
| Expect OM policies to stop data reads | Policies are metadata-only (D6.4 = 0) | Enforce in the engine/storage layer; record the control in the catalog |
