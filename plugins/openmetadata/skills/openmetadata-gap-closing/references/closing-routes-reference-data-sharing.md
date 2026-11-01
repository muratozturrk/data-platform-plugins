# Closing Routes: Reference Data, MDM and External Sharing (Part A, cont.)

Second half of the eleven general gaps: D8.1–D8.4 and D9.6. Same layout and conventions as
[closing-routes-part-a.md](closing-routes-part-a.md). Items marked **[verified here]** were
confirmed in the 1.13.3 source while writing this skill and are not part of the original research.

---

<a id="d81"></a>
## D8.1 Reference data sets: I3 × E4 = 12 · OM 2 (K) · S3 · O

**Exists:** no dedicated reference-data object, but two substitutes:
- Classification/Tag (`entity/classification/tag.json`): versioned and attachable to columns.
- Enum custom property config (`enumConfig.json:9-19`): an allowed-value set of plain strings.

**Missing:**
- Code–label pair semantics, for example "06 → a province name".
- Constraining column **values** to the list.
- Bulk CSV for classifications.

**Competitors:** DataHub = 2 (`allowedValues` pairs, no value history). Atlas = 2 (glossary
category with `validValues`, no versioning). This is a category gap.

**Route (new entity type impossible, so use a glossary):**
1. Model each code list as a **Glossary**. Glossaries are versioned, have an approval workflow
   and export to SKOS.
2. Each code becomes a term. Store the code itself in a term custom property (for example
   `codeValue: "06"`). Glossary terms carry `extension`.
3. Bind terms to columns with the existing term–asset tagging (D4.5, K:
   `GlossaryTermResource.java:872-970`).
4. Use the glossary CSV path for bulk load and export. It has an `extension` column
   (`json/data/glossary/glossaryCsvDocumentation.json:128-130`).
5. Turn on RDF (`RDF_ENABLED`, Fuseki) to publish the lists as SKOS/OWL and query them over
   SPARQL.

**Effort/risk:** the main risk is the modelling choice, glossary versus classification, because
changing it later means migrating data. Value-level enforcement in the data still belongs in
the data quality layer.

**Verify:** export one list as SKOS and check codes, labels and URIs. A column bound to the list
shows the term.

**Start:** pilot one list (for example province codes) end to end, then roll the decision out to
the other lists.

---

<a id="d82"></a>
## D8.2 Code lists and cross-code mapping: I3 × E4 = 12 · OM 2 (K) · S3 · O, L

**Exists, and it is stronger than expected:**
- `conceptMappings` on glossary terms maps them to external concepts with a mapping quality.
  **[verified here]** the enum is `EXACT_MATCH`, `CLOSE_MATCH`, `BROAD_MATCH`, `NARROW_MATCH`,
  `RELATED_MATCH`, `SAME_AS` (`glossaryTerm.json:25-36,123-130`).
- Custom term–term relation types are also available
  (`configuration/glossaryTermRelationSettings.json:21-94`).

**Missing:**
- A mapping object at code **value** level.
- CSV export does not carry relation types or `conceptMappings`. **[verified here]** the
  glossary CSV columns are parent, name, displayName, description, synonyms, relatedTerms
  (untyped), references, tags, reviewers, owner, glossaryStatus, color, iconURL, domains and
  extension.

**Competitors:** OM leads. DataHub = 1 (only untyped related terms). Atlas = 2
(`translationTerms`/`isA`, no confidence, no bulk export).

**Route:**
1. Build each code set as a glossary (the D8.1 output, so do D8.1 and D8.2 together).
2. Map between sets with `conceptMappings`, choosing the quality value. **This is
   configuration, not development.**
3. Extend the CSV export to include `conceptMappings` and relation types. That means a field
   in the glossary CSV documentation and the export code.
4. Optionally add a thin translation API ("system A code X → system B code?") on top of the
   REST API, outside OM.
5. Optionally (L theme), use an LLM to *suggest* mappings for a human to approve.

**Becomes priority 1** as soon as cross-system joins depend on codes from subsystems with
different coding. Without a recorded mapping, a join silently uses one engineer's private
knowledge.

**Start:** pick two code sets, map them by hand in the UI with no code, and judge fitness within
a week.

---

<a id="d83"></a>
## D8.3 MDM integration: I1 × E3 = 3 · OM 1 (D) · S1 · O

**Exists:** no MDM connector category. The only metadata sources are Alation, Amundsen and Atlas
(`ingestion/src/metadata/ingestion/source/metadata/`; docs.open-metadata.org/latest/connectors).

**Competitors:** DataHub = 1 (its Informatica connector is IDMC/ETL, not MDM). Atlas = 0. OM
issue #12558 (İ) is closed. No product has a plan.

**Route:** **do not start without an MDM system.** If one arrives, write a connector on the
ingestion framework (the same kind of work as the Iceberg connector). Model master entities as
tables or data products, since no new entity type is possible. The ease of 3 is misleading: the
low priority comes from impact, not effort.

<a id="d84"></a>
## D8.4 Golden record / entity-resolution visibility: I1 × E1 = 1 · OM 0 (D) · S1 · O, L

**Exists:** none. No schema or code for "golden record", "entity resolution" or "survivorship".
DataHub = 0 (`Siblings` is entity-level aliasing). Atlas = 0 (ATLAS-3874 is metadata dedup).

**Route:** **do not start.** It combines a modelling block (no new entity type) with a research
problem (match scoring, survivorship rules). If entity resolution is ever needed, it belongs in a
dedicated component. The catalog only **displays** its results.

---

<a id="d96"></a>
## D9.6 External sharing and sharing agreements: I3 × E3 = 9 · OM 1 (D) · S1 · O, A

**Exists:**
- No Delta Sharing, no sharing-agreement object and no external-consumer object.
- Only data-product `visibility` and free-text `termsOfUse` on the contract.
- No end date, no automatic expiry, no auditable sharing history.

**Competitors:**
- DataHub = 1 and Atlas = 1 (Atlas export audit is Atlas-to-Atlas replication).
- DataHub RFC #17242 "Bilateral Data Contracts" (İ, open) proposes a
  PENDING→ACTIVE→EXPIRED→ARCHIVED lifecycle, but for *internal* parties.
- DataHub Delta Sharing #4716 was closed without merge.

**Legal driver:** domestic transfers are also regulated (Law art. 8), and the legal basis must be
stated before transfer.

**Route:**
1. **Model the party.** The research suggests a Team flagged as "external party".
   **[verified here] Caution:** Team has no `extension`, so it cannot carry custom properties.
   The UI entity-reference picker also offers only table, topic, dashboard, pipeline, mlmodel,
   container, searchIndex, storedProcedure and glossaryTerm (`openmetadata-ui/.../constants/
   constants.ts:399-436`). A workable variant: an "External parties" **glossary** with one term
   per party, carrying custom properties for legal capacity and contract. Reference it from
   data products or assets with an `entityReference` custom property
   (`customProperties/complexTypes.json:6`).
2. Put the agreement fields on the data product: recipient (reference), purpose, start and end
   date, basis document.
3. **Expiry:** a scheduled job that lists expired agreements and triggers revocation in the
   system that actually grants access (Trino/Ranger). Candidates are the governance workflow
   `periodicBatchEntityTrigger` (see [extending-openmetadata.md](extending-openmetadata.md#schedulers))
   or an Airflow DAG over the REST API. Revocation itself always runs outside OM.
4. Sharing history comes from entity version history (D1.4 = 3, `entityHistory.json:9-104`).

**Effort/risk:** touches the catalog model, a scheduler, and Ranger/Trino. There is no clean
model because new entity types are not possible.

**Start:** plan it together with **D14.9** (counterparty legal role and contract), since both need
the same party object. Building them separately models the party twice.
