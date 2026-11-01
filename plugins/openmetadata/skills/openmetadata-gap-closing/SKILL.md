---
name: openmetadata-gap-closing
description: "Prioritised capability-gap catalog for OpenMetadata 1.13.3 and the route to close each gap on its real structures: 24 gaps (11 general, 11 KVKK/D14, 2 platform-specific) scored impact x ease, gap classes, LLM/agentic/ontology themes, quick wins vs separate projects vs do-not-build items; per-gap routes for mandatory custom properties (D1.2), natural-language search (D2.3), anomaly detection (D5.6), masking (D6.3), row/column access (D6.4), anonymisation (D6.8), reference data and code mapping (D8.1-D8.4) and external sharing (D9.6); and a playbook for choosing between configuration, external automation, plug-in classes, connectors and schema forks, with path:line evidence. Use when planning an OpenMetadata extension roadmap, deciding whether a missing capability is worth building, estimating effort, picking an extension point, or checking a gap is not already closed upstream."
---

# OpenMetadata Gap Closing

A capability assessment of OpenMetadata found 45 gaps out of 90 rubric capabilities. This
skill covers the 24 that matter most for a typical on-prem, KVKK-regulated data platform. For
each one it gives a priority score and the cheapest route to close it on structures
OpenMetadata already has. Everything was checked against **OpenMetadata 1.13.3**
(`1.13.3-release`, commit `255f6694913b`), with DataHub v1.7.0 and Apache Atlas 2.5.0 as
comparators. The rubric and scoring rules are in `openmetadata-governance-assessment`. The KVKK
gaps are covered in depth in `openmetadata-kvkk-compliance`, and the two platform gaps in
`openmetadata-platform-integration`.

Evidence codes: **K** = checked in the source code at `path:line` · **D** = official
documentation · **İ** = issue, PR, RFC or roadmap item. Paths are relative to the OpenMetadata
repository root. Where a feature is listed as missing, it means it was **not found in 1.13.3**.
That is weaker than proof that it does not exist, so check again on newer versions.

## When to Use This Skill

- Building an extension roadmap for OpenMetadata and deciding what to do first.
- Deciding whether a missing capability is worth building, buying, or leaving to another
  platform component (Ranger, Flink, Great Expectations).
- Choosing an extension mechanism: custom property, governance workflow, external job, a
  pluggable class, an ingestion connector, or a schema change (fork).
- Estimating effort and risk for a gap, and setting up verification once it is closed.
- Checking whether upstream already has, or is shipping, the feature (2.0.0 policy agent work)
  before starting.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| See all 24 gaps with scores, class, theme and owner skill | Priority = Impact × Ease | [gap-catalog.md](references/gap-catalog.md) |
| Make a custom property mandatory | Platform `entitySemantics` JsonLogic rule now; `required` flag in `customProperty.json` later | [closing-routes-part-a.md](references/closing-routes-part-a.md#d12) |
| Natural-language search | Implement `NLQService`, plug it in with `naturalLanguageSearch.providerClass`, use a local LLM | [closing-routes-part-a.md](references/closing-routes-part-a.md#d23) |
| Masking beyond `PII.Sensitive` / `********` | Make the tag list and mask method in `PIIMasker` configurable | [closing-routes-part-a.md](references/closing-routes-part-a.md#d63) |
| Row/column data access | Don't build it in the catalog. Ranger enforces; the catalog derives and checks (gap 4.3) | [closing-routes-part-a.md](references/closing-routes-part-a.md#d64) |
| Code lists and cross-system code mapping | One glossary per code list, `conceptMappings` between lists | [closing-routes-reference-data-sharing.md](references/closing-routes-reference-data-sharing.md#d81) |
| Anonymisation (k-anonymity, differential privacy) | Narrow the scope: record technique and k in custom properties, run the algorithm outside OM | [closing-routes-part-a.md](references/closing-routes-part-a.md#d68) |
| External sharing agreements | Party as a glossary term, agreement fields on the data product, scheduled expiry sweep | [closing-routes-reference-data-sharing.md](references/closing-routes-reference-data-sharing.md#d96) |
| Which extension point fits | Escalation ladder: configure → automate outside → plug in → connector → fork | [extending-openmetadata.md](references/extending-openmetadata.md) |
| KVKK gaps (D14.1–D14.11) | Owned by sibling skill | `openmetadata-kvkk-compliance` |
| Iceberg connector (4.1), Ranger link (4.3) | Owned by sibling skill | `openmetadata-platform-integration` |

## Scoring Model

- **OM score (0–4)** is the frozen rubric maturity score. 0 = concept absent, 1 = free text or
  an indirect path, 2 = structured field but no automation, 3 = the product does the job,
  4 = auditable and extensible.
- **Impact (1–5).** 5 = directly meets a legal obligation, or the platform cannot work without
  it. 4 = closes a major risk. 3 = valuable. 2 = improvement. 1 = theoretical. The research
  scored impact for a typical on-prem, KVKK-regulated platform, so **re-score impact for your
  own context.**
- **Ease (1–5, higher = easier).** 5 = add a field or extension to an existing structure.
  4 = a new entity-like construct or workflow inside the existing framework. 3 = touches
  several components or needs an external integration. 2 = new infrastructure or LLM
  inference. 1 = a research problem that needs a new algorithm.
- **Priority = Impact × Ease** (1–25). When scores tie, the higher-impact gap comes first.
- **Classes:** S1 = no product has it · S2 = a competitor has it and OM does not (there is a
  reference to copy) · S3 = OM has it but weakly · S4 = specific to the target platform ·
  S5 = KVKK/regulatory.
- **Themes:** L = LLM · A = agentic (scheduled or autonomous job) · O = ontology/semantic layer.
  A theme tag means the gap is open to that approach, not that the approach will work.

**One constraint drives many ease scores:** users cannot define new entity types in 1.13.3
(`TypeResource.java:78-85`; D1.1 = 3, K). They can only add custom properties to existing
types. Any gap that needs a new kind of object, such as a transfer record, a request, a party or
a golden record, either has to borrow an existing type or requires a fork.

## Priority Bands (corrected numbers)

| Band | Gaps (priority) |
|---|---|
| **Quick wins** (Ease ≥ 4, Impact ≥ 3) | D1.2 (25), D14.2 (25), 4.1 Iceberg (20), D14.1 (20), D14.5 (20), D14.8 (20), 4.3 Ranger (20), D14.11 (16), D6.3 (12), D8.1 (12), D8.2 (12) |
| **High impact but hard** (Impact ≥ 4, Ease ≤ 2): run as separate projects, keep them on the list | D14.7 (10), D6.8 (5) |
| **Medium** | D14.3, D14.6, D14.4 (15) · D14.9, D14.10 (12) · D2.3, D9.6 (9) |
| **Low, with stated reasons** | D6.4 (6, Ranger enforces it), D5.6 (6, Flink/GE already detect anomalies), D8.3 (3), D8.4 (1) |

Gap 4.3 was originally scored ease 3. It was raised to **4** (priority 20) after the
`securityService` scaffold was found (`securityService.json:5,21`, K). The body text of the
source guide still says "3" in one place. The header score, 4, is the correct one.

The four D8 items do not belong together. D8.1 and D8.2 are real needs for any organisation
that keeps shared code lists across subsystems, and D8.2 becomes top priority the moment
cross-system joins depend on mapped codes. D8.3 and D8.4 only make sense if an MDM system
exists. In a stack without MDM they are theoretical: record them, don't build them.

## Core Rules for Closing Gaps

1. **Check whether upstream already has it.** D6.3 and D6.4 (and D9.4) carry a "vendor may
   already be doing it" caveat. The 2.0.0 policy agent work (PR #27396, merged; PR #28595
   `supportsMaskedAccess`, İ) is not in 1.13.3. Read the 2.0.0 release notes first. D5.6
   is ambiguous: the OSS docs publish an anomaly-detection page without a "commercial only"
   badge, but no model was found in the code. Ask the vendor before building.
2. **Having a field is not the same as enforcing it.** `dataContract.rowFilters`
   (`dataContract.json:94-98`) documents a filter but enforces nothing. A custom property with no
   mandatory rule answers no compliance question.
3. **The catalog is not in the data path.** Masking, row filtering and anonymisation of live
   queries belong to the query/policy layer (Ranger, Trino). The catalog's job is to *derive*
   policies from classifications and *verify* drift.
4. **Go up the extension ladder only as far as you need.** Configuration first, then external
   automation through the REST API or SDK (for example an Airflow DAG), then pluggable classes,
   then connectors, then schema forks. See
   [extending-openmetadata.md](references/extending-openmetadata.md).
5. **Where the code goes matters for licensing.** `openmetadata-spec`, `-service` and `-ui` are
   Apache-2.0 (`LICENSE:1-3`). `ingestion/` is under the Collate Community License v1.0
   (`ingestion/LICENSE:1-5`). Internal use is free, but publishing derivatives of ingestion
   code is restricted.
6. **Plan gaps that share an object together:** D9.6 with D14.9 (counterparty), D8.1 with D8.2
   (code sets), D14.5 with D14.6 (due-date field), D6.3 with D14.8 (masking of new categories),
   and D1.2 with D14.2 (mandatory legal basis).

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Building row/column enforcement inside the catalog | Goes against the architecture; Ranger already enforces | Derive Ranger policies from tags, report drift (4.3) |
| Starting D6.3/D6.4 without reading the 2.0.0 notes | Duplicates vendor work | Check PR #27396/#28595 lineage first |
| Making a new custom property "mandatory" by rejecting old records | Locks the existing catalog | Grandfather existing records into a nonconformity list; enforce on new writes |
| Treating `rowFilters` or `termsOfUse` as controls | Documentation, not enforcement | Enforce in the source system; keep the catalog as the record |
| Building MDM/golden-record features without an MDM system | Bridge to nothing | Record as theoretical; revisit when MDM exists |
| Running anonymisation algorithms in the catalog | The catalog does not hold the data | Run in ETL or the query engine; store technique and k in the catalog |
| Forking the spec for something a custom property plus rule can do | Upgrade debt every release | Use the configuration level; fork only for real type or engine changes |
| Writing the Ranger bridge inside `ingestion/` when it does not need to be there | Licence constraint, upgrade coupling | Run it as an external Airflow job over the REST APIs |

## References

- [references/gap-catalog.md](references/gap-catalog.md): all 24 gaps with scores, class, theme,
  one-line route and owner skill, plus gap-class rules and the competitive-versus-absolute split.
- [references/closing-routes-part-a.md](references/closing-routes-part-a.md): D1.2, D2.3, D5.6,
  D6.3, D6.4, D6.8. For each: what exists (with evidence), what is missing, the route on OM
  structures, effort/risk, and how to verify.
- [references/closing-routes-reference-data-sharing.md](references/closing-routes-reference-data-sharing.md):
  the same for D8.1–D8.4 (reference data, code mapping, MDM, golden record) and D9.6 (external
  sharing).
- [references/extending-openmetadata.md](references/extending-openmetadata.md): the extension
  ladder, when a custom property is enough, verified pluggable points, schedulers, licensing and
  upgrade safety.
- [references/sources.md](references/sources.md): where the material comes from.
- Related skills: `openmetadata-architecture` (entity model, APIs, extension points, licensing),
  `openmetadata-platform-integration` (gaps 4.1, 4.3, connectors),
  `openmetadata-kvkk-compliance` (D14 gaps, T9 matrix), `openmetadata-governance-assessment`
  (rubric, scoring rules), `openmetadata-capability-design` (designing new L/A/O capabilities
  that come out of these gaps).
