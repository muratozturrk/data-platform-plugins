# D6 — Security, Privacy and Compliance: Rubric and Product Comparison

D6 is one of the 14 domains (90 capabilities) of a frozen capability rubric built for comparing
open-source data catalogs, mapped to DMBOK2 Knowledge Area 5 (Data Security). It has **8
capabilities**, each scored 0-4 against written level definitions. A level is reached only if all
lower levels' conditions hold; `?`/`null` means not measured and is never averaged as 0.

Products measured: OpenMetadata `1.13.3-release` (`255f6694913b`), DataHub `v1.7.0`
(`7f81ccbfe27b`), Apache Atlas `release-2.5.0` (`234608315f29`). Every score in this file has
evidence type **K** (verified in source code) unless marked; paths are repo-relative.

## 1. Score matrix

| ID | Capability | OM | DH | Atlas | Spread |
|---|---|---|---|---|---|
| D6.1 | PII detection and auto-classification | **3** | 1 | 0 | 3 |
| D6.2 | Policy engine (RBAC/ABAC) | **4** | 3 | 2 | 2 |
| D6.3 | Masking and pseudonymisation | 1 | 1 | 0 | 1 |
| D6.4 | Row- and column-level access control | 0 | 1 (D) | 1 | 1 |
| D6.5 | Retention policy definition | 2 | 2 | 2 | 0 |
| D6.6 | Audit trail and immutability | 3 | 3 | 2 | 1 |
| D6.7 | External authorisation engine integration | 1 | 2 | **3** | 2 |
| D6.8 | Anonymisation and privacy-enhancing techniques | 0 | 0 | 0 | 0 |
| | **Sum (max 32)** | **14** | **13** | **10** | |

Related capabilities in other domains: D9.4 access request and approval (OM 1, DH 1, Atlas 0),
D7.3 policy lifecycle (OM 1, DH 2, Atlas 0), D11.4 webhooks/alerts used to trigger syncs
(OM 4, DH 2, Atlas 0).

## 2. Level definitions (condensed)

| ID | 0 | 1 | 2 | 3 | 4 |
|---|---|---|---|---|---|
| D6.1 | Manual tags only | Fixed patterns on column **names** | Names + **sampled content**, tags auto-applied | ML/statistical, ≥ 10 types, **confidence**, accept/reject **feedback loop** | **LLM** contextual (name + description + samples), custom taxonomies, bulk tagging with approval workflow |
| D6.2 | Source system's access only | Fixed built-in roles | Custom roles with per-asset view/edit/manage (RBAC) | + **ABAC** (user department, data sensitivity tag), policies managed via API | + per user-asset **decision explanation** via API, every decision **audited**, conflict precedence explained |
| D6.3 | None | Static mask on **sample/preview data only** | Column masking policy applied **at query time**, ≥ 1 method | Selectable method (partial, hash, constant, FPE), **per role/attribute** | + **reversible pseudonymisation**, controlled re-identification, each re-identification audited |
| D6.4 | Table-level all-or-nothing | Column exclusion, no row filter | + **fixed-condition** row filter | Row filter bound **dynamically** to user attributes, consistent in UI and API | Composite expressions, performance monitoring, **"what does user X see" simulation** |
| D6.5 | No concept | Free-text note/tag | Structured retention field; expired assets listed | **Automatic action** on expiry; bulk assignment by type/tag | Integrated with storage (deletion/archival **verifiable**), exception approval, evidence in audit |
| D6.6 | No log | Admin actions only | Config/metadata changes logged and listable | + **data access/query-level** events, filter/export via API, retention | **Immutable** (append-only/signed), real-time export to SIEM, integrity independently verifiable |
| D6.7 | Own mechanism only | Identity/groups from external directory; decision internal | Coarse (product/module) decisions from an external policy server | **Fine-grained** (asset/column) decisions delegated at runtime to an external PDP; product is only a PEP | Policies authored centrally in the external engine; open, documented protocol; shared by several products |
| D6.8 | No concept | Manual hash/field drop | Aggregation/generalisation policy recorded on asset | Re-identification risk **measured** (e.g. k-anonymity) | Differential-privacy budget or synthetic data, with audit of technique and parameters |

## 3. Per capability: scores and why

### D6.1 — PII detection
- **OM 3.** Presidio + spaCy NER over samples and names, confidence threshold, feedback that turns
  false positives into exceptions through approval. No LLM analysis, so not 4.
  `ingestion/src/metadata/pii/scanners/ner_scanner.py:57-79`; `ingestion/src/metadata/pii/algorithms/tags.py:25-111`;
  `openmetadata-spec/src/main/resources/json/schema/type/recognizerFeedback.json:21-105`.
- **DH 1.** Built-in classifier removed in v1.7.0; only source-system tag import (warehouse policy
  tags, a third-party scanner connector). `metadata-ingestion/src/datahub/ingestion/glossary/classifier_registry.py:1-21`.
- **Atlas 0.** No detection; classification propagation is not detection.
  `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/tasks/ClassificationPropagateTaskFactory.java:37-80`.

### D6.2 — Policy engine (metadata plane)
- **OM 4.** RBAC + 13 SpEL attribute functions (`isOwner`, `hasDomain`, `matchAnyTag`, `matchTeam`, ...)
  managed via API; `/v1/permissions/debug/evaluate` explains which policy/rule decided; deny-first
  precedence fixed in code. Gap even at 4: decisions are not written to the audit log.
  `openmetadata-service/src/main/java/org/openmetadata/service/security/policyevaluator/RuleEvaluator.java:55-398`;
  `.../resources/permissions/PermissionsResource.java:296-345`.
- **DH 3.** Conditions on `TAG`, `DOMAIN`, `CONTAINER`, `GLOSSARY`, `OWNER`, `GROUP_MEMBERSHIP`;
  managed via GraphQL/OpenAPI; decisions not audited, so not 4.
  `metadata-auth/auth-api/src/main/java/com/datahub/authorization/EntityFieldType.java:4-38`.
- **Atlas 2.** Custom roles with conditions on type, id, classification, label, business metadata —
  but the simple authoriser reads a policy file once; not API-managed, no decision explanation.
  `authorization/src/main/java/org/apache/atlas/authorize/simple/AtlasSimpleAuthzPolicy.java:204-297`.

### D6.3 — Masking
- **OM 1.** Masks only what the catalog stores (samples, profiles, query text, e-mail) with a fixed
  `********`, keyed to the single tag `PII.Sensitive`; live queries untouched, no method choice,
  no reversible pseudonymisation. Unmasked view only for admins, bots and owners.
  `openmetadata-service/src/main/java/org/openmetadata/service/security/mask/PIIMasker.java:45-48,54-120`;
  `.../security/DefaultAuthorizer.java:190-193`.
- **DH 1.** No masking of its own; sample values can just be not collected. Indirect route: write a
  tag to Snowflake and let Snowflake's tag-based masking act.
  `datahub-actions/src/datahub_actions/plugin/action/snowflake/snowflake_util.py:80-116`.
- **Atlas 0.** No masking code; the architecture leaves masking to Apache Ranger; Atlas only
  scrubs metadata in search results. `authorization/src/main/java/org/apache/atlas/authorize/AtlasAuthorizer.java:77-99`.

### D6.4 — Row/column access
- **OM 0.** No column- or row-scoped `MetadataOperation`; the product is not in the data path;
  `dataContract.rowFilters` is documentation only (nothing in the service evaluates it).
  `openmetadata-spec/src/main/resources/json/schema/entity/policies/accessControl/resourceDescriptor.json:13-71`;
  `.../entity/data/dataContract.json:94`.
- **DH 1 (D).** `schemaField` view restriction hides columns in the catalog; no row filter concept;
  underlying data not controlled. Source: DataHub authorisation policies documentation.
- **Atlas 1.** Column entities can be excluded from permission and hidden from search; no row
  filter or data masking in Atlas (left to Ranger).
  `authorization/src/main/java/org/apache/atlas/authorize/simple/AtlasSimpleAuthorizer.java:256-283`.

### D6.5 — Retention
- **All 2.** A structured duration field exists and expired assets can be listed; nothing acts on
  expiry. OM: ISO 8601 `retentionPeriod` on table/database/schema/container, inherited
  (`openmetadata-spec/.../entity/data/table.json:1210-1213`); its `DataRetention` app cleans
  catalog-internal records only. DH: date/number structured property + search filter
  (`metadata-models/src/main/pegasus/com/linkedin/structured/StructuredPropertyDefinition.pdl:36-64`).
  Atlas: typed Business Metadata attribute + comparison operators
  (`intg/src/main/java/org/apache/atlas/model/typedef/AtlasBusinessMetadataDef.java:41-46`).
  Route to 3: a scheduled job that opens a task or deletion request when the period ends.

### D6.6 — Audit trail
- **OM 3.** Separate append-only `audit_log_event` table, retention, admin filter and async export;
  no cryptographic integrity, no data-access events.
  `bootstrap/sql/migrations/native/1.12.0/mysql/schemaChanges.sql:9-32`;
  `openmetadata-service/.../resources/audit/AuditLogResource.java:51,172-192`.
- **DH 3.** `POST /openapi/v1/events/audit/search` filters by event type, entity, actor; usage
  events exported on Kafka; no immutable/signed storage.
  `metadata-service/openapi-servlet/src/main/java/io/datahubproject/openapi/v1/event/ExternalEventsController.java:204-253`.
- **Atlas 2.** Metadata changes listable via API; reads go only to a plain-text `audit.log`; audit
  rows can be aged out on a schedule, so not immutable.
  `repository/src/main/java/org/apache/atlas/repository/audit/HBaseBasedAuditRepository.java:555-631`.

### D6.7 — External authorisation engine
- **OM 1.** Identity and groups come from an external provider; decisions are internal. A custom
  class can be plugged in via `authorizerConfiguration.className` (default `DefaultAuthorizer`),
  but no OPA/Ranger PDP integration ships. `conf/openmetadata.yaml:330-342`;
  `openmetadata-service/.../security/Authorizer.java:24-56`. The Ranger-shaped `securityService`
  scaffold (see [policy-enforcement.md](policy-enforcement.md) §7) points the other way and does
  not change this score.
- **DH 2.** External `Authorizer` plugins (e.g. Ranger) load through an isolated class loader, but
  the chain **short-circuits on the first ALLOW**, so an external engine cannot impose a DENY; the
  product is not a pure PEP.
  `metadata-service/factories/src/main/java/com/linkedin/gms/factory/auth/AuthorizerChainFactory.java:78-99`;
  `metadata-service/auth-impl/src/main/java/com/datahub/authorization/AuthorizerChain.java:88-91`.
- **Atlas 3.** `atlas.authorizer.impl=RANGER` maps to the hard-coded
  `org.apache.ranger.authorization.atlas.authorizer.RangerAtlasAuthorizer`; fine-grained decisions
  are delegated at runtime and Atlas acts as PEP. No open protocol (XACML/OPA), so not 4.
  `authorization/src/main/java/org/apache/atlas/authorize/AtlasAuthorizerFactory.java:32-86`.

### D6.8 — Anonymisation
- **All 0.** No k-anonymity, differential privacy, generalisation or synthetic data in code or
  official docs of any of the three. Out of scope here — see the `data-anonymization` skill.

## 4. Cross-product findings

1. **No catalog sits in the data path.** D6.3 and D6.4 share one root: the catalog is never on the
   query path. This is an architectural choice, not a defect. Expect the catalog to *derive and
   verify* data-plane controls, not apply them. Row-level security has no record (feature or
   roadmap) in any of the three.
2. **Metadata-plane strength is not data-plane strength.** OM's D6.2 = 4 is the best policy engine
   measured — over catalog operations.
3. **Opposite strengths.** OM leads on knowing (D6.1 = 3); Atlas leads on delegating (D6.7 = 3),
   inherited from its co-design with Ranger in the Hadoop ecosystem. Atlas's architecture gives the
   right answer for masking: it is the authorisation engine's job.
4. **Recording vs acting.** D6.5 is 2 everywhere: fields exist, nothing happens when they expire.
   The same pattern recurs across the rubric.
5. **The bridge, by direction of difficulty.** Forward sync (catalog tags -> enforcer tag store) is
   the easy direction and already exists (Atlas -> Ranger tagsync; a commercial, one-way,
   tags-only sink for OpenMetadata). Reverse reconciliation (enforcer policy vs catalog tag ->
   drift report) is **absent in every product examined**. Full delegation (D6.7) is the hardest
   and adds least to data protection. Build in that order.
6. **Scores are version-bound.** OpenMetadata's "Policy Agent" work (PR #27396, merged to `main`
   May 2026, migration pointing to 2.0.0) applies real grants in Databricks/Snowflake/Unity Catalog
   with a hierarchical scope down to column, plus `supportsMaskedAccess` / PR #28595 (İ). If it
   ships, OM D6.4 (column level only) and D6.3 change; row level stays empty. Re-measure before
   building on the 1.13.3 scores.

## 5. How to use this for your own platform

- Score your **combined** stack, not the catalog alone: catalog D6.1/D6.2/D6.6 + enforcer
  D6.3/D6.4 + bridge. A platform with OM-style detection, Ranger enforcement and a drift job can
  reach 3 on D6.3/D6.4 even though every catalog scores ≤ 1.
- For each capability, write the level you need, the component that delivers it and the evidence
  an auditor will see (policy JSON in Git, drift report, engine audit extract).
- Reconfirm counts that come from summaries against code. Example: the research summary says
  OpenMetadata detects "41 PII types"; the 1.13.3 code shows 38 `PIITag` entity types
  (`tags.py:25-86`) and 43 shipped recognizers (`piiTagsWithRecognizers.json`).
