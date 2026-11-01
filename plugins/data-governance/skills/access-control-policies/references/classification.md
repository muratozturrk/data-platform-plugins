# Sensitive-Data Classification

Classification is the input to every access-control decision: a masking policy that targets the
tag `Sensitive` is only as good as the tagging. This file covers taxonomy design, automatic
detection (rubric D6.1), propagation through lineage, and the review loop. Product facts are from
OpenMetadata `1.13.3-release` (`255f6694`), DataHub `v1.7.0` (`7f81ccbf`) and Apache Atlas
`release-2.5.0` (`2346083`); evidence tags as in [SKILL.md](../SKILL.md).

## 1. Taxonomy: two axes, not one

| Axis | Purpose | Example values | Drives |
|---|---|---|---|
| **Sensitivity level** | Default control | `Public`, `Internal`, `Confidential`, `Restricted` (or `NonSensitive` / `Sensitive`) | Deny vs mask vs allow; who may request access |
| **Data type** | Method choice | `EMAIL`, `PHONE`, `NATIONAL_ID`, `IBAN`, `PERSON_NAME`, `LOCATION` | Mask format (show last 4, hash, null, year-only) |
| **Regulatory category** (optional third) | Legal obligations | Special categories (health, biometric, criminal record, ...) | Extra measures, shorter retention, separate review chain |

Design rules:

- Keep the sensitivity axis **mutually exclusive** (a column has exactly one level). OpenMetadata's
  shipped `PII` classification is `mutuallyExclusive: true` with tags `None`, `NonSensitive`,
  `Sensitive` (`openmetadata-service/src/main/resources/json/data/tags/piiTagsWithRecognizers.json:6,23,112`) (K).
- Keep the data-type axis **non-exclusive** — a free-text column can hold names and phones.
- Model each legally distinct special category as **its own tag**; one undivided "special
  category" tag cannot carry per-category policies. (For a jurisdiction-specific list see the
  `kvkk-compliance` skill.)
- Decide the type -> level mapping explicitly and review it with counsel. OpenMetadata ships an
  opinionated default that maps `DATE_TIME`, `NRP`, `LOCATION`, `PHONE_NUMBER` and `URL` to
  `NonSensitive` (`ingestion/src/metadata/pii/algorithms/tags.py:95-111`) (K) — phone numbers and
  locations are personal data under most privacy laws.
- Version the taxonomy; a tag rename silently breaks every tag-based policy keyed on the old name.

## 2. Detection levels (rubric D6.1)

| Level | What the detector does |
|---|---|
| 0 | No detection; tags are manual only |
| 1 | Fixed patterns on **column names** only |
| 2 | Column names **and sampled content** with patterns; tags auto-applied |
| 3 | Statistical/ML classifiers over samples, **≥ 10 sensitive types**, confidence scores, accept/reject **feedback loop** |
| 4 | LLM-based contextual analysis (name + description + samples together), adaptable to custom taxonomies, bulk tagging under an approval workflow |

Method trade-offs:

| Method | Catches | Misses / risks |
|---|---|---|
| Name regex (`*_email`, `tckn`, `ssn`) | Well-named columns, cheap, no data read | `col_17`, free text, abbreviations in another language |
| Content regex + checksum (Luhn, IBAN, national-ID check digits) | Structured identifiers with high precision | Names, addresses, health text |
| NER / ML on samples | Names, locations, mixed free text | Language coverage, sampling bias, confidence calibration |
| LLM over name + description + samples | Context ("this `code` is a diagnosis code") | Cost per column, needs samples sent to a model — itself a data-protection decision |
| Import from source system | Tags already set in the warehouse (policy tags) | Only as good as the source |

Operational guidance:

- **Scan samples, not tables.** Detection needs a few hundred rows per column; sample inside the
  trust boundary and never store raw samples in the catalog unmasked (see §5).
- **Threshold + review, not auto-apply.** Apply above a high confidence, queue the middle band for a
  steward, drop the rest. OpenMetadata's `PII` classification ships `autoClassificationConfig`
  with `minimumConfidence: 0.6` and `conflictResolution: highest_priority`
  (`piiTagsWithRecognizers.json:7-10`) (K).
- **Record rejections.** A rejected suggestion must not reappear on the next scan; that is the
  "feedback loop" of level 3.
- **Re-scan on schema change**, not only on a calendar: new columns are the usual leak.

## 3. What the measured products do

| Product | D6.1 | Detection | Evidence |
|---|---|---|---|
| OpenMetadata 1.13.3 | **3** | Presidio `AnalyzerEngine` on a spaCy NLP engine over sample data and column names; `PIITag` enum of 38 Presidio entity types; 43 default recognizers (39 attached to `Sensitive`, 4 to `NonSensitive`); confidence threshold; false-positive feedback via `recognizerFeedback` with approval. No LLM analysis. | `ingestion/src/metadata/pii/scanners/ner_scanner.py:57-79`; `ingestion/src/metadata/pii/algorithms/tags.py:25-86`; `openmetadata-spec/.../type/recognizerFeedback.json:21-105` (K) |
| DataHub v1.7.0 | 1 | Built-in column classifier **removed** in this release (dependency on an unmaintained package); the registry stays for custom classifiers; tags can be imported from source systems (warehouse policy tags) | `metadata-ingestion/src/datahub/ingestion/glossary/classifier_registry.py:4-9` (K) |
| Atlas 2.5.0 | 0 | No content- or name-based detection; classifications are assigned by hand or by hooks, then **propagated** | `repository/.../store/graph/v2/tasks/ClassificationPropagateTaskFactory.java:37-80` (K) |

Note on counts: the research summarises OpenMetadata as detecting "41 PII types". The 1.13.3 code
shows 38 entity types in `PIITag` and 43 shipped recognizers; quote those figures, not 41.

DataHub's removal message points users who need the old classifier to pin `acryl-datahub==1.6.0.5`
(`classifier_registry.py:11-18`) (K) — a reminder to re-check classification after every upgrade.

## 4. Propagation through lineage

Propagation copies a tag from a source asset to its downstream copies so that a sensitive column
stays sensitive after `CREATE TABLE AS SELECT`. It is the only way classification keeps up with a
large pipeline estate, and also the easiest way to over-classify.

### How the products do it

| Product | Mechanism | Granularity | Evidence |
|---|---|---|---|
| Atlas | Per-classification `propagate` flag; relationship types declare direction `NONE / ONE_TO_TWO / TWO_TO_ONE / BOTH`; `removePropagationsOnEntityDelete`; time-bounded `validityPeriods`; propagation runs as async tasks | Entity (incl. column entities) along lineage/process relationships | `intg/.../model/instance/AtlasClassification.java:56-58`; `intg/.../model/typedef/AtlasRelationshipDef.java:373-375`; `ClassificationPropagateTaskFactory.java:37-80` (K) |
| DataHub | `tag_propagation` action in DataHub Actions: listens to change events on Kafka, finds downstreams, applies or removes tags filtered by prefix | **Datasets only** — not dashboards/charts (README caveat) | `datahub-actions/src/datahub_actions/plugin/action/tag/tag_propagation_action.py:139-150`; `.../tag/README.md` (K) |
| OpenMetadata | A `LineagePropagationAction` schema (propagate description, tags, glossary terms, tier; `SOURCE`/`TARGET` filter mode) exists under `applications/configuration/external/automator/` | — | `openmetadata-spec/.../external/automator/lineagePropagationAction.json:1-114` (K: schema only). Executor **not found** in the OSS repo; treat as not verified |

### Rules for safe propagation

1. **Propagate along column lineage, not table lineage.** Table-level propagation tags every column
   of the target.
2. **Stop at identity-destroying transforms.** `COUNT(*)`, `AVG(salary)` grouped by department, or
   a k-anonymised extract should not inherit `Sensitive` automatically — but deciding that an
   aggregate is safe is an anonymisation question (see `data-anonymization`), so mark it for review
   rather than silently dropping the tag.
3. **Keep hashing and tokenisation as `Sensitive` (pseudonymised)** unless the key domain is
   destroyed; a deterministic hash is still linkable.
4. **Handle removals.** When the source tag is removed, downstream propagated tags must go too, or
   policies keep firing on stale tags. Atlas and DataHub's action both handle removal; a home-grown
   job must as well.
5. **Mark provenance.** Store "propagated from X" on the tag so stewards can tell a reviewed tag
   from an inherited one.
6. **Propagation is only as complete as lineage.** Lineage coverage gaps (ad-hoc notebooks,
   extracts) become classification gaps; see `data-catalog-lineage`.

## 5. The catalog's own exposure

A catalog stores samples, profiles (min/max, top values) and query text — all copies of the data.
Protect them like the data:

- Restrict sample/profile collection for `Sensitive` columns or disable it per source.
  DataHub can simply not collect sample values (`include_field_sample_values`,
  `metadata-ingestion/src/datahub/ingestion/source/ge_profiling_config.py:99`) (K).
- OpenMetadata masks its stored samples, profiles and query text with a fixed `********` when the
  column or entity carries `PII.Sensitive` (`openmetadata-service/.../security/mask/PIIMasker.java:45-48,54-120`) (K).
  The unmasked view is granted only to admins, bots and asset owners
  (`openmetadata-service/.../security/DefaultAuthorizer.java:190-193`) (K) — it is not
  policy-configurable, and other tags are not masked.
- Gate preview operations with metadata policies (e.g. deny `ViewSampleData` where
  `matchAnyTag('PII.Sensitive')`). That protects the catalog, not the warehouse.

## 6. Classification-to-control matrix (template)

Fill this once with the data owner and security; every row becomes an enforcer policy
(see [policy-enforcement.md](policy-enforcement.md)).

| Level | Type example | Default for general analysts | Privileged group | Requires |
|---|---|---|---|---|
| Restricted | National ID, health code | Column denied or fully masked | Unmasked via approved, time-boxed grant | Justification, periodic review, access log |
| Confidential | Email, phone | Partial mask (show last 4 / domain only) | Unmasked | Owner approval |
| Confidential | Birth date | Year only | Full date | Owner approval |
| Internal | Customer segment | Visible | Visible | Domain membership |
| Public | Country code | Visible | Visible | — |

## 7. Checklist

- [ ] Two axes (level, type) defined; level is mutually exclusive.
- [ ] Type -> level mapping reviewed; defaults of the tool overridden where they disagree with law.
- [ ] Detection runs on new/changed columns; confidence band routed to stewards; rejections stored.
- [ ] Propagation along column lineage with removal handling and provenance.
- [ ] Catalog samples/profiles of sensitive columns masked or not collected.
- [ ] Every level/type row in the matrix maps to an enforcer policy or a recorded exception.
