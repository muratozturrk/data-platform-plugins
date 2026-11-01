# DMBOK2 Knowledge-Area Coverage

How to project rubric scores onto the DAMA-DMBOK2 knowledge areas, what the projection showed
for three open-source catalogs, and the two areas closest to "governance" in the narrow sense:
**D7 governance processes and organisation** (KA1) and **D8 master and reference data** (KA8).

## Numbering used here

DMBOK2 has **11 knowledge areas**, chapters 3-13 of the book. This skill numbers them
**ordinally, KA1-KA11, in chapter order** (KA *n* = chapter *n* + 2), the same keys as the
research source registry (`dmbok2-ka1` ... `dmbok2-ka11`). The registry entries cite DAMA's
published context-diagram summaries, whose figure numbers rise in the same order (Figure 14 Data
Governance ... Figure 91 Data Quality), which confirms the order.

| KA | Knowledge area | Book chapter | Rubric capabilities citing it |
|---|---|---|---|
| KA1 | Data Governance and Stewardship | 3 | 9 |
| KA2 | Data Architecture | 4 | 3 |
| KA3 | Data Modeling and Design | 5 | 5 |
| KA4 | Data Storage and Operations | 6 | 15 |
| KA5 | Data Security | 7 | 10 |
| KA6 | Data Integration and Interoperability | 8 | 17 |
| KA7 | Documents and Content | 9 | **0** |
| KA8 | Reference and Master Data | 10 | 6 |
| KA9 | Data Warehousing and Business Intelligence | 11 | 2 |
| KA10 | Metadata | 12 | 21 |
| KA11 | Data Quality | 13 | 7 |

Total 95 mappings over 90 capabilities: some cite two areas (D6.4 → KA4 + KA5), and **18 cite
none** (all 11 D14 capabilities plus D2.4, D6.8, D9.6, D12.5, D12.6, D13.3, D13.5).

**Numbering conflicts in the research (do not propagate).** The research design spec labels
domains with a mix of chapter numbers and ordinals (D1 "KA11 Metadata", D5 "KA13", D6 "KA7 Data
Security", D7 "KA1", D4 "KA3 Data Modeling, KA9 Reference & Master Data"), which matches neither
scheme consistently. The research plan calls Documents and Content "KA12" and DW/BI "KA10"; the
coverage table explains these as book-chapter numbers, but in the book they are chapters 9 and 11.
The rubric itself, its README domain table and the coverage table use the ordinal keys
consistently; this skill follows them.

## Method

1. For every capability, read its `sources` list and collect each `ref:dmbok2-kaN`.
   Map at **capability level**, not domain level: a domain's headline area hides capabilities
   that cite another (D7.4 and D7.6 also cite KA10; D8.4 cites KA9).
2. Cell = mean score of the capabilities mapped to that area, printed as `mean (known/mapped)`.
3. An area with zero mapped capabilities is printed as **"not covered"**, never 0.0. A zero
   would say "products are bad at this"; the truth is "this study did not measure it".
4. Flag areas resting on <= 3 capabilities as thin.

## Measured coverage (released versions, inspected 2026-08-11)

OpenMetadata `1.13.3-release` (`255f6694913b`), DataHub `v1.7.0` (`7f81ccbfe27b`), Apache Atlas
`release-2.5.0` (`234608315f29`). All cells cover every mapped capability (no `null`s). Values
recomputed from the evidence files; they match the research table.

| KA | Area | Mapped | Rubric domains | OpenMetadata | DataHub | Atlas |
|---|---|---|---|---|---|---|
| KA1 | Governance and Stewardship | 9 | D6, D7, D9, D13 | **2.6** | 2.1 | 0.8 |
| KA2 | Data Architecture | 3 (thin) | D12 | 2.7 | 2.7 | 1.7 |
| KA3 | Data Modeling and Design | 5 | D4 | **3.4** | 2.4 | 1.8 |
| KA4 | Data Storage and Operations | 15 | D6, D10, D11, D12 | 2.2 | 2.2 | 1.2 |
| KA5 | Data Security | 10 | D6, D9, D12 | **1.9** | 1.6 | 1.3 |
| KA6 | Data Integration and Interoperability | 17 | D3, D5, D6, D8, D9, D10, D11, D13 | 2.6 | **2.7** | 1.6 |
| KA7 | Documents and Content | **0** | none | not covered | not covered | not covered |
| KA8 | Reference and Master Data | 6 | D4, D8 | 1.8 | 1.7 | 1.3 |
| KA9 | DW and BI | 2 (thin) | D8, D13 | 1.0 | **1.5** | 0.5 |
| KA10 | Metadata | 21 | D1, D2, D7, D9, D10, D11, D13 | 2.9 | 2.9 | 1.8 |
| KA11 | Data Quality | 7 | D5 | **2.4** | 1.4 | 0.3 |

Reading the table:

- **Metadata (KA10) is the centre of gravity**: 21 capabilities, and the two leading products tie
  at 2.9. These are metadata-management products with governance built on top.
- **Data Quality (KA11, spread 2.1) and Governance/Stewardship (KA1, spread 1.8) separate the
  products most.** DataHub's lower KA11 and KA1 come largely from features gated to its commercial
  cloud edition (assertion monitors, anomaly detection, approval workflows; OSS/Cloud matrix
  `docs/managed-datahub/managed-datahub-overview.md:30-48`); Atlas's 0.3 is real absence (no
  rules, profiler, test runs, SLA, score or anomaly detection).
- **Data Security (KA5) tops out at 1.9.** None of the three sits in the data path; access
  control governs catalog metadata, not data reads. Row/column filtering, masking and
  anonymisation are absent or nominal in all three. This is a structural limit of the catalog
  category, not a gap one release will close.
- **Reference and Master Data (KA8) is weak everywhere (1.3-1.8).** Catalogs look *at* an MDM
  system; they are not one (see D8 below).
- **DW/BI (KA9) is the lowest area (0.5-1.5)** but rests on two capabilities (D8.4, D13.1);
  dashboards are measured inside D1.1 entity coverage. Do not draw strong conclusions from it.
- **Leaders by area (three products):** OpenMetadata leads KA1, KA3, KA5, KA11 and leads KA8 by
  0.1; DataHub leads KA6 and KA9; OpenMetadata and DataHub tie on KA2, KA4 and KA10; Atlas leads
  none.

## The KA7 gap: Documents and Content

No rubric capability cites `dmbok2-ka7`. The source entry exists and was verified, but nothing
consumes it; no other area is in this state (`nist-ai-rmf` is the only other unused source,
because D13 cites the EU AI Act instead).

- **Meaning:** the study says **nothing** about how catalogs handle documents and content. The
  expectation "catalogs are weak at Documents and Content" is *not confirmed*; there is no data.
  (The parallel expectation for DW/BI was confirmed, with the thinness caveat above.)
- **Why it matters:** many compliance artefacts are documents, not tables: privacy notices,
  retention-and-destruction policy, destruction minutes, standard contracts, regulator permits.
  D14.10 (versioned privacy notice linked to assets) and the destruction-record part of D14.3 are
  the regulatory edge of this gap, and all three products score 1-2 there.
- **Why it was not fixed:** adding capabilities would break the freeze; the gap was reported
  instead.
- **Proposed for the next rubric version:** (a) a document/content object kept as a versioned,
  first-class catalog asset; (b) two-way links between documents and data assets, with the
  document inside the retention/destruction lifecycle.

## Post-DMBOK domains

D9 (data products/mesh), D13 (AI/ML) and D14 (local law) have no primary DMBOK2 area. Some of
their capabilities cite DMBOK2 secondarily (D9.1 → KA1, D9.3 → KA6, D13.1 → KA9/KA10, D13.2 →
KA6, D13.4 → KA1), so they appear indirectly above. **None of D14's 11 capabilities maps to any
DMBOK2 area**; regulatory coverage needs its own matrix (law article → capability → product).

## D7 Governance processes and organisation (KA1)

Scores OpenMetadata / DataHub / Atlas, evidence type K unless marked; paths relative to each repo.

| Cap | OM | DH | Atlas | What decides the score |
|---|---|---|---|---|
| D7.1 Ownership | 3 | 3 | 1 | OM: several owners, inheritance from parent, CSV bulk, ownerless report (`EntityRepository.java:7105-7145`). DH: several ownership types per asset, bulk resolver (`Ownership.pdl:9-33`). Atlas: `owner` is a single free-text string (`addons/models/0000-Area0/0010-base_model.json:37-47`). |
| D7.2 Approval workflow | 3 | 1 | 0 (İ) | OM: Flowable 7.2.0 engine (`openmetadata-service/pom.xml:918-928`), user-defined multi-step flows with JsonLogic branching; level 4 missed on timeouts/escalation. DH OSS: no pre-publish gate; the vendor's OSS/Cloud matrix marks approval workflows Cloud-only (`managed-datahub-overview.md:48`). Atlas: no workflow engine in code; changes publish directly. |
| D7.3 Policy lifecycle | 1 | 2 | 0 | OM `policy.json` carries access rules with only an `enabled` boolean (no draft/effective/review dates). DH policy has ACTIVE/INACTIVE status (`DataHubPolicyInfo.pdl:25-78`). Atlas: policy is a static JSON file read at start-up. |
| D7.4 Certification | 3 | 2 | 1 | OM: certification with expiry, admin-set allowed classes, workflow node, search filter (`assetCertification.json:1-23`). DH: verification forms limited to assigned actors, no expiry. Atlas: free labels only. |
| D7.5 Campaigns | 2 | 2 | 1 | OM: KPI object with target/dates, coverage dashboards; no auto-created campaign. DH: forms auto-assigned by filter with completion facets (`DynamicFormAssignment.pdl:13-20`); progress analytics Cloud-only. Atlas: `IS_NULL` search + CSV only. |
| D7.6 Collaboration | 3 | 1 | 0 | OM: threads, @mentions, tasks, subscriptions to e-mail/Slack/Teams/GChat/webhook (`thread.json`, `eventSubscription.json:98-124`). DH OSS: no comments or @mentions found in GraphQL schema. Atlas: none; "Notifications" means Kafka metadata events. |
| **Mean** | **2.5** | **1.8** | **0.5** | |

Take-aways: process support is where open-source catalogs differ most after data quality.
Policy **lifecycle** (D7.3) is weak in all three: none manages a governance policy as a document
with effective and review dates; "policy" in these products means access rule. If governance
policy management matters, plan it outside the catalog or as a custom object.

## D8 Master and reference data (KA8, KA9)

| Cap | OM | DH | Atlas | What decides the score |
|---|---|---|---|---|
| D8.1 Reference data sets | 2 | 2 | 2 | No dedicated reference-data entity in any. Stand-ins: OM classifications/glossary + enum custom properties (`enumConfig.json:9-19`); DH structured-property `allowedValues` (`StructuredPropertyDefinition.pdl:52-64`); Atlas glossary category with `validValues` (`0011-glossary_model.json:261-277`). No value-level versioning. |
| D8.2 Code lists / crosswalks | 2 | 1 | 2 | OM `conceptMappings` with EXACT/CLOSE/BROAD/NARROW/RELATED match quality (`glossaryTerm.json:26-63`). Atlas many-to-many term relations (`translationTerms`). DH only untyped `relatedTerms`. None maps at code-value level. |
| D8.3 MDM integration | 1 (D) | 1 | 0 | No MDM connector in any. OM connector catalogue has no MDM category (docs); DH's Informatica source is for the ETL/integration cloud, not MDM; Atlas's nine bridges include no MDM. |
| D8.4 Golden record visibility | 0 (D) | 0 | 0 | No golden-record, entity-resolution or survivorship concept in any model (re-checked: zero matches in all three repos). |
| **Mean** | **1.2** | **1.0** | **1.0** | Lowest-spread domain (0.2): it does not separate the products. |

Take-aways: D8 is outside the catalog category. Budget an MDM system or reference-data service
and integrate it (lineage from MDM entities to physical tables is D8.3 level 3). D8.4 is a
shared zero that the cross-check should have flagged as all-zero (see
[assessment-procedure.md](assessment-procedure.md), cross-check limits).
