# D14 Gaps in OpenMetadata 1.13.3 and How to Close Them

> **Not legal advice.** Technical gap analysis only.

D14 ("KVKK and regulatory compliance") is the local-law domain of the research rubric: 11
capabilities with no counterpart in DMBOK/DCAM. Each is scored 0–4 against level definitions (see
the `openmetadata-governance-assessment` skill). Evidence type for every D14 score below is **K**
(verified in source code at `1.13.3-release`, commit `255f6694913b`).

**Headline:** none of the 11 reaches 3 in OpenMetadata, DataHub v1.7.0 or Apache Atlas 2.5.0
(sole exception: Atlas D14.8 = 3). The route is the same everywhere — the organisation defines,
fills and polices custom fields.

Impact and Ease are 1–5; **Priority = Impact × Ease**. Themes: **L** = LLM, **A** = agentic /
scheduled task, **O** = ontology / semantic layer.

| ID | Capability | OM | DH | Atlas | Impact | Ease | Priority | Themes |
|---|---|---|---|---|---|---|---|---|
| D14.2 | Purpose and legal basis per asset | 2 | **3** | 2 | 5 | 5 | **25** | L, O |
| D14.1 | Processing inventory from the catalog | 2 | 2 | 2 | 5 | 4 | 20 | L, A |
| D14.5 | Data subject request: find data + workflow | 2 | 2 | 2 | 5 | 4 | 20 | A, L |
| D14.8 | Special-category taxonomy + extra measures | 2 | 2 | **3** | 5 | 4 | 20 | O, L |
| D14.11 | Publicly published personal data | 2 | 2 | 2 | 4 | 4 | 16 | A, L |
| D14.3 | Destruction method + periodic destruction | 2 | 2 | 2 | 5 | 3 | 15 | A, O |
| D14.4 | Consent status + withdrawal downstream | 2 | 2 | 2 | 5 | 3 | 15 | A |
| D14.6 | Breach scope extraction | 2 | 2 | 2 | 5 | 3 | 15 | A, L |
| D14.9 | Processor relationship + contract basis | 1 | 2 | 1 | 4 | 3 | 12 | O |
| D14.10 | Versioned privacy notice linked to assets | 1 | 2 | 1 | 4 | 3 | 12 | O, A, L |
| D14.7 | Cross-border nature + legal mechanism | 1 | 2 | 1 | 5 | 2 | 10 | O, A |

## Three structural roots

1. **No required flag on custom properties (D1.2 = 2).** In 20+ of the 49 obligations the weakest
   link is D1.2. Until fields can be made mandatory (or checked by a rule), no row rises above
   `partial`.
2. **No time-bound modelling.** None of the six verified durations (30 days, 72 h, 6 months,
   5 business days, 4 years, 7 days) can be a countdown: `thread.json` and `userApprovalTask` have
   no `dueDate`/`timeout`. The bases to build on are strong: alerting D11.4 = 4
   (webhook/Slack/Teams/e-mail, SpEL filters, retries, dead-letter, delivery status), workflows
   D7.2 = 3 (Flowable, JsonLogic branching) and a cron-scheduled workflow trigger that sweeps
   entities periodically (`periodicBatchEntityTrigger.json:12-75`,
   `PeriodicBatchEntityTrigger.java:114-124`; the research missed it and said schedulers run
   ingestion only). **What is missing is a per-record due date with escalation, not an engine or a
   scheduler.** One due-date field feeds D14.1, D14.3, D14.5, D14.6 and D14.7.
3. **No document object in the rubric or product.** Privacy notice, retention-destruction policy,
   destruction minutes, standard contract and Board permits are documents; OpenMetadata's
   `docStore` Document has versions but no effective date and cannot link to assets. D14.10 = 1 is
   the visible tip.

---

## D14.1 — Processing inventory derived from the catalog

**Score 2.** Evidence: `openmetadata-spec/src/main/resources/json/schema/type/customProperty.json:1-71`;
`openmetadata-service/src/main/resources/json/data/entity/entityCsvDocumentation.json:5-183`.
**Law:** VERBİS Reg. Art. 4/1(h), 5/1(ç), 9/2, 13 (**7 days**); Law Art. 16/3. Scope may narrow
under Law Art. 28/2 (Art. 16 not applied for exempt activities).

- **Have:** data category, data-subject group, recipient group as enum / entityReferenceList custom
  properties on table and column; recursive CSV export carries them in the `extension` column;
  max retention is the built-in `retentionPeriod`.
- **Missing:** fields for foreign transfer and technical/administrative measures; auto-fill from
  classification/ownership; VERBİS-ready output; dated inventory diff.
- **Route:** *Tier 1 (config, easy → level 3 on fields):* add the two missing properties; write a
  small CSV → VERBİS-layout converter. *Tier 2 (LLM):* suggest data category and subject group from
  column name, type, sample, path, dbt docs and existing PII tag — base is D6.1 = 3 (41 PII types,
  confidence threshold, false-positive feedback). *Tier 3 (agentic):* scheduled diff over entity
  version history (D1.4 = 3) between two dates → the 7-day change notification list.
- **First step:** add the two properties and produce a VERBİS-format CSV end to end for one schema.

## D14.2 — Purpose and legal basis recorded per asset

**Score 2.** Evidence: `.../schema/type/customProperties/enumConfig.json:9-19`; `.../schema/type/basic.json:242-305`.
**Law:** Law Art. 4/2(c), 5, 6/3; Information Comm. Art. 5/1(h); VERBİS Reg. Art. 4/1(h) (legal
basis mandatory in the inventory since the 28/4/2019 amendment); Law Art. 28/1 (out-of-scope).

- **Have:** purpose and legal basis as organisation-defined enum properties at table (and column)
  level; "out of scope — Art. 28/1(b)" carried as a separate value of the same list.
- **Missing:** a ready non-conformity list of personal-data assets with empty legal basis. The
  evidence note says it **can be built** with an `entitySemantics`/JsonLogic rule or a
  `checkEntityAttributesTask` workflow node — configuration, not code.
- **Route (no code):** (1) legal-basis enum with Art. 5 and Art. 6 conditions + Art. 28/1 values;
  (2) purpose enum; (3) rule "personal-data tag AND legal basis empty" → task to steward;
  (4) mandatory filling needs D1.2 — plan together. Level 4 (several purpose/basis pairs per asset,
  each with its own retention) needs a composite value OM does not have.
- **Comparison:** DataHub = 3, the only `meets` source in the matrix (closed-list at column level,
  empties surfaced via filters/forms). Atlas can make the attribute mandatory but cannot assign the
  gap to an owner.
- **First step:** draft the legal-basis list **with counsel**; the technical work is a day.

## D14.3 — Destruction method and periodic destruction

**Score 2.** Evidence: `enumConfig.json:9-19`; `.../entity/data/table.json:1210-1213`;
`openmetadata-service/src/main/java/org/openmetadata/service/apps/bundles/dataRetention/DataRetention.java:139-206`.
**Law:** Destruction Reg. Art. 4/1(c), 8, 9, 10 (three methods), 7/4 (state method), 6/1(g),(ğ)
(table + periods), 11/2 (**≤ 6 months**), 7/3 (records **≥ 3 years**).

- **Have:** destruction-method enum (erasure / destruction / anonymisation); `retentionPeriod`
  (ISO 8601, inherited) on table, database, schema, container; both exportable as CSV — this is the
  Art. 6/1(g) table.
- **Critical nuance:** the `DataRetention` app is recurring and logs each run, but it **only
  cleans catalog-internal records** (orphan relationships, tag usages, test cases, pipelines,
  time-series rows). It has nothing to do with destroying asset data.
- **Missing:** periodic destruction job for asset data, 6-month bound, destruction minutes,
  any action on expiry (D6.5 = 2).
- **Route:** method enum + CSV table now; run the periodic job **in the orchestrator** (e.g. an
  Airflow DAG) that triggers the storage layer (object-store lifecycle, lakehouse `DELETE`,
  warehouse TTL) and writes a dated result (date, scope, method) back to the catalog; store minutes
  immutably for ≥ 3 years. Requires the storage tables to be catalogued — see
  `openmetadata-platform-integration` for lakehouse connector gaps.
- **First step:** add the method field and export the retention-destruction table.

## D14.4 — Consent status and withdrawal propagated downstream

**Score 2.** Evidence: `customProperty.json:1-71`;
`openmetadata-spec/src/main/resources/json/schema/security/client/oidcClientConfig.json:92`.
**Law:** Law Art. 3/1(a), 5/1; Information Comm. Art. 5/1(f) (inform and obtain consent
separately); Law Art. 7/1 + Destruction Reg. Art. 7/1 (destroy when **all** conditions cease).

- **Finding:** the product has **no consent concept** — the only `consent` in the spec is the OIDC
  login `prompt` description (`oidcClientConfig.json:92`).
- **Have:** consent status / text version / date as enum, string, date properties.
- **Missing:** read from an external consent registry; withdrawal → list of downstream assets.
- **Route:** (1) properties; (2) integration with the consent system of record (lowers ease to 3);
  (3) on withdrawal, walk lineage (D3.4 = 3: depth + filter downstream query, CSV export, downstream
  owner notification; D3.2 = 3 column lineage) and open one destruction task per derived asset;
  (4) if another legal basis (D14.2) exists, exclude with a recorded reason.
- **First step:** flip one table to "withdrawn", run the downstream query by hand, verify the list.

## D14.5 — Data subject request: locate data and run the request

**Score 2.** Evidence: `openmetadata-spec/src/main/resources/elasticsearch/en/column_index_mapping.json`;
`.../schema/entity/feed/thread.json:12-21,49-101`.
**Law:** Law Art. 11 (rights a–ğ), 13/2 + Application Comm. Art. 6/5 (**30 days**), Application
Comm. Art. 5/1 (application via purpose-built software is explicitly allowed). Scope may narrow
under Art. 28/2.

- **Have (works well):** find which assets/columns hold a person identifier (e.g. national ID
  column) via the column index and PII tags; export the hit list as CSV. OM is the best of the
  three products here (T9 rows 19–21: OM `partial`, DataHub `weak`, Atlas `none`).
- **Missing:** request-type field mapped to Art. 11/1 (a)–(ğ), application date, remaining-time
  indicator. Task types are fixed — `RequestDescription`, `UpdateDescription`, `RequestTag`,
  `UpdateTag`, `RequestApproval`, `RequestTestCaseFailureResolution`, `RecognizerFeedbackApproval`,
  `Generic` (`thread.json:12-21`) — and no task carries a due date.
- **Route:** (1) add a due-date field to `thread.json` (spec change, Apache-2.0 tree) — the single
  highest-leverage step, it makes all six durations modellable; (2) a "data subject request" task
  type + Art. 11 request-type enum; (3) application date → remaining time; (4) near-deadline
  alerts on existing D11.4; (5) review flow on D7.2; (6) level 4: auto-expand affected assets via
  lineage up- and downstream, including transfer recipients.
- **First step:** read `thread.json:12-21` and design the due-date field together with D14.6.

## D14.6 — Breach scope extraction

**Score 2.** Evidence: `entityCsvDocumentation.json:5-183`;
`openmetadata-service/src/main/resources/json/data/tags/personalDataTags.json`.
**Law:** Law Art. 12/5; BD 2019/10 (**72 h from learning**, record facts/effects/measures, explain
delays); BD 2019/271 (notice by category, **personal vs special-category split**).

- **Have:** export a database/schema subtree with tags and properties as one CSV → personal-data
  categories and subject groups in scope.
- **Missing:** automatic addition of derived copies via lineage; personal vs special-category split;
  a breach record with a separate **learned-at** field (the 72 h runs from learning, not from the
  breach or from record creation). "Incident" in OM is a **data-quality** incident only.
- **Route:** split report depends on D14.8 tier 1 (separate category tags); breach record as a
  convention on a task/thread or a schema addition (no new entity types); due date shared with
  D14.5; call the downstream lineage query automatically on record creation; keep record and
  attachments immutable (D6.6 = 3 audit log is append-only but not cryptographically signed).
  Access logs from the policy engine of the data plane (e.g. Ranger audit) + catalog lineage are the
  raw inputs; the missing part is the inference on top.
- **First step:** for one schema, produce "personal-data categories in this subtree" split by
  special category.

## D14.7 — Cross-border nature and legal mechanism of a transfer

**Score 1** (lowest group). Evidence: `.../schema/entity/data/dataContract.json:57-127`;
`.../schema/entity/services/connections/database/`.
**Law:** Law Art. 9 (as amended by Law No. 7499) + Cross-border Reg. Art. 6 (three tiers), Art. 10
(four safeguards), Art. 4/1(e) ("making accessible" is a transfer — a read grant to a foreign party
counts), Art. 9/5 + Reg. Art. 14/5 (**5 business days**), Art. 9/2 + Reg. Art. 9/1 (**4 years**),
Art. 9/8 + Reg. Art. 5/2 (onward transfers); Circular 2019/12 Art. 1, 3 (in-country storage).
Art. 9/7: the regime does not apply to public-law activities of public institutions — scope
narrows, does not vanish.

- **Finding:** no transfer object at all. Recipient only as free-text "Consumer AD Groups" in a data
  contract or a description/property. Country exists only as a technical connection parameter
  (e.g. cloud region), not as an asset-level domestic/foreign field.
- **Route (ease 2):** modelling is the blocker — (a) properties on `dataContract` (a contract is not
  a transfer), (b) per-asset "transferred abroad" property (cannot express several recipients),
  (c) glossary terms as transfer records (not clean, but gets ontology support). The mechanism is a
  concept scheme (three tiers, four safeguards) — a natural fit for the RDF/SKOS layer (D4.4 = 4:
  OWL, SKOS, term URIs, SPARQL), which is **off by default** (`RDF_ENABLED=false`) and needs Fuseki.
  Two durations need the due date from D14.5; onward transfers need lineage.
- **First step (cheap, high value):** record "country where data physically resides" and report that
  no asset is outside the country — evidence for Circular 2019/12. Note: the research suggested
  putting this on each **service**; in 1.13.3 service schemas have no `extension` field, so put it
  on `database` / `container` (or schema) instead. Build the three-tier model only when a real
  foreign transfer exists.

## D14.8 — Special-category taxonomy and extra measures

**Score 2.** Evidence: `personalDataTags.json`;
`openmetadata-service/src/main/java/org/openmetadata/service/security/policyevaluator/PolicyEvaluator.java:44-79`.
**Law:** Law Art. 6/1 (closed list), 6/4 (Board-determined adequate measures); BD 2018/10 (separate
policy, scoped/timed access, periodic access review, cryptographic storage, logging of all actions,
2FA for remote access); Law Art. 6/3(a) + Cross-border Reg. Art. 10/1(c).

- **Have:** built-in `PersonalData.SpecialCategory` — single, undivided. The organisation can define
  the 11 categories as its own classification, assign them to columns (D6.1 = 3 feeds this) and
  write a separate policy per category with `matchAnyTag`.
- **Missing:** precedence — conflict resolution is fixed **deny-first**, so a category policy cannot
  override the generic sensitivity policy; masking is hard-wired to `PII.Sensitive`
  (`PIIMasker.java:45-48`), so new categories are not masked. Of the six BD 2018/10 measures only
  three are measurable in a catalog (separate policy, logging, access scope); encryption and 2FA
  belong to source systems. T9 row 36 is `weak` because masking D6.3 = 1.
- **Route:** *Tier 1 (config):* 11-tag classification, assignment, per-category policies.
  *Tier 2 (code):* change precedence in `PolicyEvaluator` so the category policy wins and fix it
  in product (Atlas is the reference: every classification on an entity must be permitted); remove
  the `PII.Sensitive` hard-wiring in `PIIMasker`.
- **First step:** define the 11 tags, write a health-data policy, observe the conflict with the
  generic one.

## D14.9 — Processor relationship and contractual basis

**Score 1.** Evidence: `dataContract.json:76-127,222-234`.
**Law:** Law Art. 12/2 (joint liability for measures), Destruction Reg. Art. 12/1(b) (notify
recipients of destroyed data), Cross-border Reg. Art. 4/1(ğ),(h); Public Guide §2.3 (in public bodies
the institution is the controller; units that do not set purposes/means are not), §3.3.1 (domestic
transfers need an explicit prior legal basis).

- **Finding:** counterparty is not an object; data contract has free-text consumer groups and a
  versioned but unstructured markdown `termsOfUse`. No legal-role or contract reference/date/validity
  field. Owner ≠ legal role: catalog "owner" never encodes controller status.
- **Route (ease 3):** model counterparties as objects. The research proposed **Team** + a "legal role"
  property; in 1.13.3 `team.json` has no `extension`, and the research's own D1.2 note excludes
  teams — so use **glossary terms** (one per party, with role enum, contract link, date, validity)
  and reference them from assets. Rule check: processor with no/expired contract → task.
  Level 4: processor's asset set from lineage; on contract end, list data to destroy/return.
- **First step:** plan with D9.6 (external data sharing) — both need the same counterparty object.

## D14.10 — Versioned privacy notice linked to assets

**Score 1.** Evidence: `.../schema/entity/docStore/document.json`;
`.../schema/entity/policies/policy.json:18-119`.
**Law:** Law Art. 10; Information Comm. Art. 5/1(e) (burden of proof on the controller), 5/1(ç)
(consistent with registry), 5/1(b) (re-inform before a purpose change). Scope may narrow under
Art. 28/2.

- **Finding:** notice is not an object; only a link from description or a hyperlink property.
  `docStore` Document has versions but **no effective date and cannot link to assets**; `policy` is an
  access rule. "Which notice version was linked to this table on date X" cannot be queried.
- **Route A (schema, ease 3):** add effective date to Document, make Document linkable to assets
  (relationship-model change), non-conformity rule, as-of query over version history (D1.4 = 3),
  drift report (LLM text comparison, last stage).
- **Route B (cheaper, ease ≈ 4, research-recommended to try first):** a notice as a **glossary term**
  (versioned, approval workflow, linkable) + effective-date property.
- **First step:** one notice as a glossary term, linked to one table; judge within a week.

## D14.11 — Publicly published personal data

**Score 2.** Evidence: `.../schema/type/customProperties/complexTypes.json:136-157`;
`.../schema/entity/domains/dataProduct.json:36-41`.
**Law:** PD 2026/1301 (publishing is processing and needs an Art. 5/6 condition; minimum data with
masking/anonymisation; set publication periods and remove/destroy at expiry; **promptly review
already-published content**), Law Art. 4/2(ç),(d).

- **Have:** publication status (enum), URL (`hyperlink-cp`), date (`date-cp`) as properties, listed
  through advanced search; data products have a built-in `visibility` enum
  (`PRIVATE`, `INVITATION`, `ORGANISATION`, `DATASPACE`, `PUBLIC`; default `PRIVATE`) — an advantage
  over the other products.
- **Missing:** masking/anonymisation-method field, publication-expiry field and expiry list,
  retrospective scan. T9 row 47 is `none` because anonymisation D6.8 = 0.
- **Route:** two properties + saved search for expired items assigned to an owner (covers the first
  three elements of PD 2026/1301); retrospective periodic scan needs a scheduled job (same gap as
  D14.3/D14.5).
- **First step:** a one-off inventory of what is already published — the decision says "promptly",
  and it is manual work that can start today.

## Suggested order

1. **D14.2** (config only) together with the D1.2 required-field question.
2. **D14.8 tier 1**, **D14.1 tier 1**, **D14.11 tier 1** — properties, tags, CSV.
3. **Due-date field** once, then **D14.5 + D14.6** together (shared due date and lineage-based
   scope), then D14.3: a `periodicBatchEntityTrigger` workflow flags overdue assets, the
   orchestrator performs the destruction and writes the record back.
4. **D14.4** after lakehouse tables are catalogued with lineage (otherwise the chain breaks).
5. **D14.9 + D9.6** (shared counterparty object), **D14.10** route B.
6. **D14.7** full model only when an actual foreign transfer exists; the residency check first.

For the overall 24-gap prioritisation see `openmetadata-gap-closing`; for designing new LLM /
agentic capabilities with a KVKK impact assessment see `openmetadata-capability-design`.
