# OpenMetadata 1.13.3 Gap Catalog (24 gaps)

## Where the 24 come from

1. **Gap set (45 of 90).** A rubric capability counts as a gap when at least one of two
   conditions holds. Either OpenMetadata scores **≤ 2** (an absolute weakness), or at least one
   competitor (DataHub v1.7.0, Atlas 2.5.0) scores **≥ 2 points higher** (a competitive gap).
   Only **4 of the 45** are competitive gaps: D12.8 backup/DR, D6.7 external authorizer, D1.2
   custom properties and D13.2 AI lineage. The other **41** are absolute weaknesses shared
   across the product category. So the case for building them is to do what no catalog does,
   not to catch up with a rival.
2. **Classes.** Rules are applied in this order and the first match wins.

   | Order | Class | Machine rule |
   |---|---|---|
   | 1 | **S5** KVKK/regulatory | capability is in domain D14 |
   | 2 | **S2** competitor has it | `max(competitor) − OM ≥ 2` |
   | 3 | **S1** no product has it | `max(all products) ≤ 1` (strict, so the claim stays defensible) |
   | 4 | **S3** OM has it but weakly | the remainder |
   | — | **S4** platform-specific | a separate axis: invisible to the 90-capability rubric, visible only when OM is compared with the target stack |

   Every S1 candidate was also searched in the four products' issue trackers and roadmaps. That
   search moved one capability (D9.4, available in DataHub Cloud) out of S1 and added "planned"
   or "commercial" caveats to others. Only D6.8, D8.4, D8.3 and D12.5 remain with neither a
   feature nor a plan in any product.
3. **The 24.** Eleven general gaps (Part A), all eleven D14 KVKK capabilities (Part B), and two
   platform gaps (Part C). D12.8 backup/DR, D6.7 and D13.2 are in the 45 but outside this
   selection. D6.7 is handled through gap 4.3.

Low scores need a second look. Several low DataHub scores reflect DataHub Cloud gating
(`saasOnly`), not a missing feature. Unknown scores (`null`) are not zeros.

## Full table (priority order, corrected)

OM = OpenMetadata 1.13.3 rubric score · I = impact · E = ease · P = I × E.

| # | ID | Gap | OM | Class | I | E | P | Theme | One-line route | Owner skill |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **D1.2** | Custom properties cannot be marked mandatory | 2 | S2 | 5 | 5 | **25** | O | Platform `entitySemantics` rule now; `required` flag in `customProperty.json` and a write-path check later | this skill |
| 2 | **D14.2** | Empty purpose/legal-basis fields not reported | 2 | S5 | 5 | 5 | **25** | L, O | Enum custom properties + JsonLogic nonconformity rule + `checkEntityAttributesTask` | `openmetadata-kvkk-compliance` |
| 3 | **4.1** | No Iceberg connector, so the lake is not catalogued first-class | — | S4 | 5 | 4 | **20** | — | Measure what Trino/Spark already show, then write a connector on the ingestion framework (DataHub's as reference) | `openmetadata-platform-integration` |
| 4 | **D14.1** | VERBİS (Data Controllers' Registry) inventory not derivable from the catalog | 2 | S5 | 5 | 4 | **20** | L, A | Missing inventory fields as custom properties, CSV `extension` export, small converter | `openmetadata-kvkk-compliance` |
| 5 | **D14.5** | No data-subject request type or 30-day countdown | 2 | S5 | 5 | 4 | **20** | A, L | Due-date field on `thread.json`, request-type enum, notifications, approval workflow | `openmetadata-kvkk-compliance` |
| 6 | **D14.8** | Special categories of personal data not separated | 2 | S5 | 5 | 4 | **20** | O, L | Eleven special categories as classifications, one `matchAnyTag` policy each | `openmetadata-kvkk-compliance` |
| 7 | **4.3** | No policy link between the catalog and Ranger | — | S4 | 5 | **4** | **20** | A | Fill the `securityService` scaffold or run an external job: tags → Ranger masking policies + drift report | `openmetadata-platform-integration` |
| 8 | **D14.11** | Lifecycle of publicly published personal data not tracked | 2 | S5 | 4 | 4 | **16** | A, L | Custom properties for publication period and masking method; list of expired items | `openmetadata-kvkk-compliance` |
| 9 | **D14.3** | No periodic destruction job and no destruction record | 2 | S5 | 5 | 3 | **15** | A, O | Destruction-method enum + `retentionPeriod` CSV; scheduled job ≤ 6 months that writes immutable records | `openmetadata-kvkk-compliance` |
| 10 | **D14.6** | No breach scoping and no 72-hour countdown | 2 | S5 | 5 | 3 | **15** | A, L | Breach record on task/thread + due date; downstream lineage for scope | `openmetadata-kvkk-compliance` |
| 11 | **D14.4** | No consent concept; withdrawal not propagated downstream | 2 | S5 | 5 | 3 | **15** | A | Consent custom properties; on withdrawal, walk lineage and open deletion tasks | `openmetadata-kvkk-compliance` |
| 12 | **D14.9** | Counterparty legal role and contract basis not recorded | 1 | S5 | 4 | 3 | **12** | O | Counterparty model + contract fields + nonconformity rule (plan with D9.6) | `openmetadata-kvkk-compliance` |
| 13 | **D14.10** | Privacy notice is not a versioned object | 1 | S5 | 4 | 3 | **12** | O, A, L | Effective date + link on `docStore` Document (schema change), or a versioned glossary term (cheaper) | `openmetadata-kvkk-compliance` |
| 14 | **D6.3** | Masking fixed to one tag and one method | 1 | S1 | 3 | 4 | **12** | — | Configurable tag list and mask method in `PIIMasker`; role-aware masking | this skill |
| 15 | **D8.1** | No dedicated reference-data (code list) object | 2 | S3 | 3 | 4 | **12** | O | One glossary per code list, code as a term custom property, CSV import, SKOS export | this skill |
| 16 | **D8.2** | No value-level code mapping object | 2 | S3 | 3 | 4 | **12** | O, L | `conceptMappings` between code-set glossaries; extend CSV export to include mappings | this skill |
| 17 | **D14.7** | No transfer record; cross-border regime cannot be modelled | 1 | S5 | 5 | 2 | **10** | O, A | Borrow `dataContract` or a glossary term + a transfer-mechanism concept scheme; deadlines need a due date | `openmetadata-kvkk-compliance` |
| 18 | **D2.3** | Natural-language search layer left empty | 2 | S3 | 3 | 3 | **9** | L, O | Implement `NLQService`, set `providerClass`, use a local LLM; turn on semantic search first | this skill |
| 19 | **D9.6** | No external sharing agreement object | 1 | S1 | 3 | 3 | **9** | O, A | Party + agreement fields, scheduled expiry sweep, revoke in Trino/Ranger | this skill |
| 20 | **D6.4** | No row/column data access control | 0 | S1 | 3 | 2 | **6** | — | Don't build. Ranger enforces; follow the 2.0.0 policy agent; invest in 4.3 | this skill |
| 21 | **D5.6** | Anomaly detection is a flag with no model behind it | 1 | S1 | 3 | 2 | **6** | A | Confirm OSS status first; then a single-metric drift test on existing profile series | this skill |
| 22 | **D6.8** | No anonymisation concept (all four products score 0) | 0 | S1 | 5 | 1 | **5** | A | Narrow scope to published datasets; record technique and k; run the algorithm outside OM | this skill |
| 23 | **D8.3** | No MDM integration | 1 | S1 | 1 | 3 | **3** | O | Don't start; revisit when an MDM system exists | this skill |
| 24 | **D8.4** | No golden-record or entity-resolution visibility | 0 | S1 | 1 | 1 | **1** | O, L | Don't start; entity resolution belongs in a dedicated component | this skill |

**How impact was set for D14 rows.** All eleven D14 rows rest on a legal obligation. Those
rated "High" in the research's impact assessment get **5**. The three rated "Medium" (D14.9,
D14.10, D14.11) get **4**.

**Two deliberate deviations from the ease scale.** D2.3 would normally score 2, because it needs
LLM inference infrastructure. It gets 3 because the product leaves a ready extension point.
D14.1 is scored on its first tier only. LLM auto-fill on its own would score 2.

**Corrected score.** Gap 4.3 ease went from 3 to 4 after the `securityService` scaffold was
found (`securityService.json:5,21`, `rangerConnection.json`,
`securityServiceMetadataPipeline.json`, `security_service.py:68`). Its priority went from 15
to 20 and the ranking was re-sorted.

## Reading the table

1. **Five of the top six are "add a field" or "configure an existing mechanism".** Most
   high-impact work is cheap.
2. **D1.2 unlocks the most.** In the KVKK obligation matrix, D1.2 is the weakest link for more
   than 20 obligations, because a legal-basis field that can be left empty answers no question.
   Until D1.2 is closed, no KVKK item rises above "partial".
3. **Iceberg (4.1) is the first link of a chain.** Without it, D14.3, D14.4 and D14.5 cannot be
   traced on the platform: lineage breaks at the Silver and Gold layers, and deletion evidence
   is invisible.
4. **D8.3 and D8.4 have no counterpart in a stack without MDM.** They are recorded, not
   scheduled.

## Bands

| Band | Rule | Members |
|---|---|---|
| Quick wins | E ≥ 4 and I ≥ 3 | D1.2, D14.2, 4.1, D14.1, D14.5, D14.8, 4.3, D14.11, D6.3, D8.1, D8.2 |
| High impact, hard | I ≥ 4 and E ≤ 2 | D14.7, D6.8. Run each as a separate workstream and keep it on the list |
| Low priority (reason) | — | D8.4 (1, no MDM) · D8.3 (3, no MDM) · D6.4 (6, Ranger enforces) · D5.6 (6, Flink + Great Expectations already detect) · D2.3 (9, ANSI SQL already serves analysts) · D9.6 (9, add once an external-sharing programme exists) |

Why D6.8 and D14.7 stay on the list:

- Both map directly to law. Anonymisation is one of three valid destruction methods (deletion,
  destruction, anonymisation) under the KVKK by-law on erasure, destruction and anonymisation
  of personal data, art. 4/1(c) and art. 10. The product can *record* that a dataset was
  anonymised but cannot *perform* it.
- No product has D6.8. A solution would be unique.
- D6.8 = 0 is the reason for the one KVKK obligation where OM scores "none": publication with
  masking or anonymisation (KVKK Board principle decision 2026/1301 + Law art. 4/2(ç)).
- Narrowing the scope lifts D6.8 from ease 1 towards 3. Use an existing k-anonymity or
  differential-privacy library, record "technique" and "k achieved" per dataset, and add a
  periodic scan.

## Themes

| Theme | Count | IDs |
|---|---|---|
| **A** agentic | 12 | D5.6, D6.8, D9.6, D14.1, D14.3, D14.4, D14.5, D14.6, D14.7, D14.10, D14.11, 4.3 |
| **O** ontology | 13 | D1.2, D2.3, D8.1, D8.2, D8.3, D8.4, D9.6, D14.2, D14.3, D14.7, D14.8, D14.9, D14.10 |
| **L** LLM | 10 | D2.3, D8.2, D8.4, D14.1, D14.2, D14.5, D14.6, D14.8, D14.10, D14.11 |
| none | 3 | D6.3, D6.4, 4.1 |

- **Agentic:** the gaps share one pattern: the value can be recorded, but nothing happens when
  a deadline passes. That needs a scheduled, state-aware component. Note that 1.13.3 does have
  a cron-scheduled governance workflow trigger; see
  [extending-openmetadata.md](extending-openmetadata.md#schedulers).
- **Ontology:** this is OM's strongest area (D4.4 = 4, K: `openmetadata.ttl:26-33,223-254`,
  `RdfResource.java:392-451,792-861`): term URIs, SKOS/OWL/DCAT/PROV import, a SPARQL
  endpoint. It is **off by default** (`RDF_ENABLED=false`) and needs Fuseki.
- **LLM:** extracting structured fields from unstructured text. OM's PII detection is pattern
  and NER based (D6.1 = 3, K: `ner_scanner.py:57-79`) and has no LLM-based context analysis.
  Use open-weight models served locally only. See `openmetadata-capability-design`.
