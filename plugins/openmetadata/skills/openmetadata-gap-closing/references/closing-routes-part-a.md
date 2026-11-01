# Closing Routes: Metadata, Search, Quality and Privacy Gaps (Part A)

This file covers six of the eleven general (non-KVKK) gaps: D1.2, D2.3, D5.6, D6.3, D6.4 and D6.8.
The other five (D8.1–D8.4, D9.6) are in
[closing-routes-reference-data-sharing.md](closing-routes-reference-data-sharing.md). Each gap
lists: what exists in 1.13.3 (with evidence), what is missing, how competitors compare, the
route on OM structures, effort and risk, how to verify, and where to start. Lines marked **[verified here]** were confirmed in the
1.13.3 source while writing this skill and are not part of the original research.

---

<a id="d12"></a>
## D1.2 Mandatory custom properties: I5 × E5 = 25 · OM 2 (K) · S2 · O

**Exists:** typed custom properties (string, number, date, enum, entity reference, table) can
be defined from the UI and the API and are validated against JSON Schema
(`openmetadata-spec/.../type/customProperty.json:1-70`, `TypeRepository.java:164-192`). Enum
config is `{multiSelect, values: [string]}` (`enumConfig.json:9-19`).

**Missing:**
- There is no per-property mandatory flag. `customPropertyConfig` allows only enum, format,
  entityTypes or table config (`customProperty.json:20-44`). The `required` list at `:69`
  applies to the *definition* (name, description, propertyType), not to entity values.
- Custom properties apply only to entity-category types. User, team, service and policy are
  excluded. **[verified here]** `team.json`, `user.json` and `policy.json` have no `extension`
  field.

**Competitors (both OSS, so there is a reference to copy):**
- DataHub = 4: structured properties apply to every entity type including schema fields, with
  `allowedValues`, `cardinality` and `immutable`, versioned
  (`StructuredPropertyDefinition.pdl:104-150`).
- Atlas = 4: typedef updates that remove attributes or add mandatory ones are rejected
  (`AtlasStructDefStoreV2.java:104-165`).

**Route:**
0. **No-code interim [verified here]: platform semantics rules.** `entityRulesSettings` holds
   a list of JsonLogic `semanticsRule` entries with `name`, `rule`, `enabled`, `entityType`
   and `ignoredEntities` (`configuration/entityRulesSettings.json:7-17`,
   `type/basic.json:242+`).
   - The rules are evaluated on create (`EntityRepository.copy()` → `:1357`,
     `EntityMapper.java:39`) and on patch (`EntityRepository.java:3898`). A failed rule throws
     `RuleValidationException` (`RuleEngine.java:184-192`).
   - The facts are the full entity map (`JsonUtils.getMap(entity)`), which includes
     `extension` (custom property values) and `tags`.
   - On update, a write is rejected only if it does not reduce the number of failing rules
     (`RuleEngine.java:63-71`). Existing non-compliant records can still be fixed step by step.
   - Manage the rules through `PUT /v1/system/settings` (`SystemResource.java:555-556`). Read
     them per type with `GET /v1/system/settings/entityRulesSettings/{entityType}` (`:397-398`).
   - Shipped rules show the syntax, for example
     `{"<=":[{"length":{"var":"domains"}},1]}` (`json/data/settings/entityRulesSettings.json`).
   - Example intent: "if the table has a PII tag, then `extension.legalBasis` is non-empty".
     Test it on a staging instance. Ingestion-created entities go through the same create path
     and would be rejected, so scope the rule tightly with `entityType` and a tag condition.
1. **Durable fix (Apache-2.0 tree):** add a boolean `required` (and optionally `cardinality` or
   `immutable`, after DataHub) to `customPropertyConfig` in `customProperty.json`.
2. Add a validation hook in the entity write path (`EntityRepository`). A required property
   that is empty either rejects the write or raises a nonconformity.
3. Add a "required" checkbox to the UI property-definition form.
4. **Backward compatibility:** existing entities with the property empty go to a
   **nonconformity list**. Do not reject them, or the catalog locks up. Handle the bulk CSV
   import path explicitly.

**Effort/risk:**
- Step 0 is configuration only.
- Steps 1–4 add one concept in one place, but the check must hold on every write path (REST,
  PATCH, CSV import, ingestion).
- Upstreaming steps 1–4 avoids carrying a fork.

**Verify:** create a table with the PII tag and no legal basis → `RuleValidationException`.
PATCH the property in → accepted. A CSV import row without it → rejected or listed.

**Start:** read DataHub's `cardinality`/`immutable` modelling, then prove it end to end on one
entity type (table).

---

<a id="d23"></a>
## D2.3 Natural-language search: I3 × E3 = 9 · OM 2 (K) · S3 · L, O

**Exists:**
- Semantic search: embedding kNN plus BM25, combined by hybrid RRF
  (`search/vector/OpenSearchVectorService.java:37,88,149-156`).
- Embeddings can run locally (DJL).
- It is **off by default** and works only on OpenSearch.

**Missing:** the layer that turns a sentence into a structured search query. In 1.13.3 this is
`NoOpNLQService`, whose `transformNaturalLanguageQuery` returns `null`
(`search/nlq/NoOpNLQService.java:22-30`).

**Competitors:**
- DataHub = 2: OSS vector search is off by default and covers documents by default. The NL
  interface is in DataHub Cloud.
- Atlas = 1: Solr synonyms only.
- This is a category gap.

**Route:**
1. **[verified here] It is pluggable, so no fork is needed.** `NLQServiceFactory` instantiates
   `naturalLanguageSearch.providerClass` when `naturalLanguageSearch.enabled` is true. The class
   must implement `NLQService` and have a constructor taking `ElasticSearchConfiguration`
   (`NLQServiceFactory.java:15-41`). Config keys are in `conf/openmetadata.yaml:494-499`
   (`NATURAL_LANGUAGE_SEARCH_ENABLED`, `SEMANTIC_SEARCH_ENABLED`, `EMBEDDING_PROVIDER` with
   options openai/bedrock/google/djl, `NATURAL_LANGUAGE_SEARCH_PROVIDER_CLASS`).
2. Implement `transformNaturalLanguageQuery(SearchRequest, context)` so it returns an
   OpenSearch query JSON. Call an **on-prem, open-weight LLM** (new infrastructure, which is
   the main cost). Use `cacheQuery` for repeats.
3. Prompt the model to emit filters and terms that use the catalog's own field names.
4. Optionally expand queries with SKOS broader/narrower/related relations from the glossary.
   This is where OM can differentiate, since D4.4 = 4 and no competitor has ontology-driven
   expansion.
5. Prerequisites: OpenSearch, semantic search enabled, and DJL embeddings for air-gapped
   setups.

**Effort/risk:** the main costs are LLM serving, prompt evaluation, and hallucinated filters. Keep
a fallback to keyword search.

**Verify:** a fixed question set with expected assets. Measure recall against plain keyword and
semantic search.

**Start:** turn on semantic search alone and measure it. Build NLQ only on top of a baseline
that works.

---

<a id="d56"></a>
## D5.6 Anomaly/drift detection: I3 × E2 = 6 · OM 1 (K) · S1 · A

**Exists:** a `useDynamicAssertion` boolean (default false) on test cases
(`tests/testCase.json:150-153`).

**Missing:** a model behind the flag. The research found no anomaly or seasonality model, no
`prophet`/`statsmodels` dependency (`ingestion/setup.py`) and no `AnomalyDetection` schema in
the repo or docs. It appears to be specific to the commercial (Collate) edition.

**Ambiguity, do not hide it:** the OSS docs publish
`data-quality-observability/anomaly-detection` with no "commercial only" badge. **Ask the
vendor before building**, so you don't rewrite something that already exists.

**Competitors:** DataHub = 0 (Cloud only, `saasOnly`). Atlas = 0. OM leads in OSS.

**Route:**
1. Add an anomaly test type to `testCase.json` with window, seasonality and sensitivity
   parameters.
2. Add a time-series model component. A moving average is not enough; weekly and holiday
   seasonality matter.
3. Wire it into test execution and train it on the stored profile history.
4. Add a feedback loop to suppress false alarms.

**Do not duplicate the platform.** Flink (stateful real-time comparison) and Great Expectations
(statistical checks between layers) already detect anomalies. The catalog's job is to
**collect their results in one place**.

**Effort/risk:** this needs a new model component. Bad seasonality handling floods the list
with false alarms and people stop looking at it.

**Verify:** replay history containing a known incident, for example a sudden row-count drop,
and measure false alarms over a holiday window.

**Start:** confirm OSS status. If it is absent, run a single-metric (row count) drift test on
the profile time series the catalog already stores.

---

<a id="d63"></a>
## D6.3 Masking and pseudonymisation: I3 × E4 = 12 · OM 1 (K) · S1 · —

**Exists:**
- `PIIMasker` masks only data the catalog stores itself: sample data, profile results, query
  text and e-mail addresses.
- It has hard-coded constants: `SENSITIVE_PII_TAG = "PII.Sensitive"`,
  `MASKED_VALUE = "********"`, `MASKED_NAME = "[MASKED]"`, `MASKED_MAIL`
  (`security/mask/PIIMasker.java:45-48,54-120`).

**Missing:**
- Live query results are not masked (the catalog is not in the data path).
- The masking method cannot be chosen.
- Masking is bound to one tag.
- There is no reversible pseudonymisation.

**Competitors:**
- DataHub = 1: it hides samples only by not collecting them.
- Atlas = 0: it leaves masking to Ranger, which is the right architectural answer.

**Upstream caveat (İ):** `supportsMaskedAccess` and PR #28595 (masked access for Databricks)
are part of the **2.0.0** policy agent work. Neither is in 1.13.3 **[verified here: no
`supportsMaskedAccess` in the 1.13.3 spec]**. Read the 2.0.0 notes first.

**Route (one Java class plus config):**
1. Replace the hard-coded `PII.Sensitive` check with a configurable tag list. Special-category
   tags from D14.8 then get masked too.
2. Replace the fixed `********` with a choice of method: partial (last 4 visible), hash, or
   constant.
3. Pass role context into the masking call so different roles see different masks.
4. *(Optional, drops ease to 3.)* For reversible pseudonymisation, call an existing tokenisation
   service such as Vault Transit instead of building crypto into the catalog.

**Real risk this addresses:** if a column misses the tag, raw identifiers appear in catalog
sample data. The catalog usually has a wider audience than the data itself.

**Verify:**
- A column tagged with a configured non-default tag is masked in sample data and profile.
- Each method produces the expected format.
- Untagged columns are unchanged.

**Start:** read `PIIMasker.java:45-48` and move the tag check into configuration.

---

<a id="d64"></a>
## D6.4 Row/column-level access: I3 × E2 = 6 · OM 0 (K) · S1 · —

**Exists:**
- Nothing that controls data access. The `MetadataOperation` enum has no column-scoped or
  row-scoped operation
  (`entity/policies/accessControl/resourceDescriptor.json:13-71`).
- The docs define access control as metadata visibility only.

**Trap:** `dataContract.rowFilters` ("Row Data Filters", `dataContract.json:94-98`) documents a
filter but enforces nothing. A field existing does not mean the work is done.

**Competitors:** DataHub = 1 and Atlas = 1. Both offer metadata visibility only. No product
has row-level filtering.

**Upstream caveat (İ):**
- PR #27396 "Policy agent feature implementation" (merged) adds hierarchical scope
  (`database → schema → table → column`) and real GRANT/REVOKE in Databricks, Snowflake and
  Unity Catalog. `supportsColumnAccess` arrives in follow-up PRs. All of this targets 2.0.0.
  **[verified here]** `policyAgentPipeline.json` is absent in 1.13.3.
- So D6.4 = 0 will be out of date in 2.0.0, but **for columns only**. Row level is still empty.

**Route: do not build it in the catalog.** It goes against the architecture (the catalog is
not in the query path), and a policy engine such as Ranger already enforces row filters and
column masks. Instead:
1. Track the 2.0.0 release notes and whether the policy agent supports your engines (Trino,
   ClickHouse).
2. Put the effort into **gap 4.3**: derive Ranger policies from catalog tags and report drift
   (`openmetadata-platform-integration`).
3. If you must enforce `rowFilters`, generate the rule in the source system or in Ranger. Never
   in the catalog.

---

<a id="d68"></a>
## D6.8 Anonymisation and privacy-enhancing techniques: I5 × E1 = 5 · OM 0 (K) · S1 · A

**Exists:**
- Only fixed masking (`PIIMasker.java:45-48`).
- No k-anonymity, differential privacy, synthetic data, generalisation or aggregation
  threshold in code or docs.

**Competitors:**
- DataHub = 0 and Atlas = 0. All four products score 0.
- The only related OM issue, #23135 (İ), is column shuffling for previews.
- This is the purest category gap: neither a feature nor a plan anywhere.

**Why it matters:**
- Anonymisation is a legal destruction method: by-law on erasure, destruction and
  anonymisation, art. 4/1(c) and art. 10.
- Publishing personal data requires masking or anonymisation (Board principle decision
  2026/1301).
- D6.8 = 0 is the sole "none" cell for OM in the KVKK matrix.

**Route: shrink the scope.**
1. **Easy part (custom properties):** mark quasi-identifier columns, and record per dataset
   the "technique applied" and the "k achieved". This merges with D14.11 at ease 4.
2. **Hard part:** compute k on the data. The catalog does not hold the data, and exact
   computation at petabyte scale is expensive, so sample. Run an existing k-anonymity or
   differential-privacy library in ETL or the query engine.
3. Define and **enforce** generalisation and small-cell suppression (for example, minimum group
   size 5) in the query engine (Trino), not the catalog.
4. Differential-privacy budget management is a separate specialism. Defer it.

**Start:** limit the first scope to **datasets that will be published**. Volume is small and the
legal driver is direct. Open the two custom properties first; the algorithm is phase two.

---

