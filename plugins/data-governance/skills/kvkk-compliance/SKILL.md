---
name: kvkk-compliance
description: "Platform-neutral KVKK (Turkish Law No. 6698, incl. Law No. 7499 amendments) compliance for a data platform and its catalog: 49 verified obligations from the Law, the VERBİS and destruction regulations, the communiqués and Board decisions mapped to the catalog capability each needs (rubric D14.1-D14.11, levels 0-4); verified deadlines (30-day applications, 72 h from learning of a breach, 6-month periodic destruction, 3-year destruction records, 7-day VERBİS changes); per-activity Article 28 exemptions; the post-7499 cross-border regime; a tool-neutral per-asset metadata model; lineage-based request and breach scoping; destruction evidence; and the measured market state in OpenMetadata 1.13.3, DataHub 1.7.0 and Atlas 2.5.0. Use when building a processing inventory or VERBİS export, designing KVKK metadata fields, scoping a data subject request or breach, planning periodic destruction, judging what a catalog can prove to an auditor, or choosing a catalog for KVKK. Not legal advice."
---

# KVKK Compliance for a Data Platform

KVKK (Law No. 6698 on the Protection of Personal Data) is enforced on data you can find, explain
and destroy on time. On a data platform that work starts in the **catalog**. The catalog holds
purpose, legal basis, retention, destruction method, transfers and processors for each asset, and
lineage shows where copies went. This skill maps every verified KVKK obligation to the catalog
capability it needs, gives a tool-neutral metadata model, and describes the processes that
produce evidence.

**Bottom line.** Every obligation can be *recorded* in today's open-source catalogs, but almost
none is *enforced*. Measured on 49 obligations with a weakest-link rule, OpenMetadata 1.13.3 meets
0, DataHub v1.7.0 meets 4 and Apache Atlas 2.5.0 meets 0. None of the 11 KVKK capabilities
reaches level 3 in any product, except Atlas on special categories (D14.8). No product can count
down a verified deadline on a single request or record. Plan for custom fields, rule checks and
an external orchestrator. Do not wait for a product feature.

> **Not legal advice.** Article numbers and durations were read from the official texts (see
> [sources.md](references/sources.md)). Applying them is counsel's job: which legal basis fits, how
> long to retain, whether an exemption applies. Compliance depends on the organisation's processes
> as much as on any tool.

Evidence tags: **K** = verified in source code, **D** = official docs, **İ** = issue/roadmap,
**B** = vendor claim. All product scores here are **K** (frozen evidence, inspected 2026-08-11).

## When to Use This Skill

- Building the Personal Data Processing Inventory (VERBİS registry input) from catalog metadata
  instead of a spreadsheet.
- Designing the per-asset KVKK fields: purpose, legal basis, retention, destruction method,
  consent, special categories, transfers, processors, privacy notice, publication.
- Scoping a data subject request (DSR) or a breach with lineage and classification.
- Designing periodic destruction (at most 6 months apart) and the destruction records behind it.
- Telling an auditor what the catalog proves and what only a process can prove.
- Comparing catalogs on KVKK, or scoring one with the D14 rubric.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Which article and deadline drives which capability | 49-row obligation map, verified durations | [obligations-map.md](references/obligations-map.md) |
| Is this activity in scope at all (Art. 28) | Exemption applies per processing activity; record it as a legal-basis value | [obligations-map.md](references/obligations-map.md) |
| Transfer abroad after Law No. 7499 | Three tiers; consent is only an incidental exception | [obligations-map.md](references/obligations-map.md) |
| Score a catalog on KVKK | D14.1–D14.11 levels 0–4 + measured scores | [d14-rubric.md](references/d14-rubric.md) |
| Which fields to keep per asset | Neutral data model, enforced vs convention, example YAML | [metadata-model.md](references/metadata-model.md) |
| VERBİS inventory export | Field mapping VERBİS Reg. Art. 4/1(h) → catalog fields | [metadata-model.md](references/metadata-model.md) |
| DSR, consent withdrawal, breach scope | Identifier search → lineage downstream → category report | [processes-and-evidence.md](references/processes-and-evidence.md) |
| Periodic destruction and its minutes | Retention sweep → engine job → record written back | [processes-and-evidence.md](references/processes-and-evidence.md) |
| Auditor questions | Catalog-provable vs process-provable list | [processes-and-evidence.md](references/processes-and-evidence.md) |
| Anonymisation as a destruction method | Destruction Reg. Art. 4/1(c), 10; technique lives elsewhere | `data-anonymization` skill |

## Verified Deadlines

| Duration | Obligation | Source |
|---|---|---|
| 30 days | Conclude a data subject application, free of charge | Law Art. 13/2; Application Comm. Art. 6/5 |
| 72 hours | Breach notice to the Board, counted **from learning** of it | Board Decision 2019/10 (Law Art. 12/5 says only "as soon as possible") |
| ≤ 6 months | Periodic destruction interval | Destruction Reg. Art. 11/2 |
| 3 months | Destruction deadline for a controller without a retention-destruction policy | Destruction Reg. Art. 11/3 |
| ≥ 3 years | Keep records of all destruction operations | Destruction Reg. Art. 7/3 |
| 7 days | Notify changes to registered information via VERBİS | VERBİS Reg. Art. 13 |
| 30 days | Register after becoming obliged to register | VERBİS Reg. Art. 8/2 |
| 5 business days | Notify a standard contract, from completion of signatures | Law Art. 9/5; Cross-border Reg. Art. 14/5 |
| ≤ 4 years | Re-evaluate an adequacy decision | Law Art. 9/2; Cross-border Reg. Art. 9/1 |

Every deadline needs a **start timestamp kept in its own field**. Examples are the application
date, the breach *learned-at* time (not occurred-at, not record-created-at), the date retention
ended and the date signatures were completed. In the three measured products, no request, task or
incident record has a due date with escalation. Run deadline sweeps in an orchestrator or in the
catalog's scheduled workflows, and keep the clock fields in the catalog.

## Core Model: Record → Detect → Act → Prove

1. **Record** per asset (and column where it differs): purpose, legal basis (closed list, including
   "out of scope: Art. 28/1(x)"), data category, data-subject group, recipient group, maximum
   retention, destruction method, consent state, special category (11 separate values), transfers,
   processors, linked privacy-notice version, publication state.
2. **Detect** non-conformities with rules, not with a mandatory flag. Examples: personal-data tag
   with an empty legal basis, retention ended but no destruction record, a transfer abroad with no
   mechanism, a special-category column without its category policy. Route each one to a steward.
3. **Act** outside the catalog. The query engine, storage and orchestrator block reads, mask,
   delete and anonymise. A catalog policy usually governs metadata, not data reads.
4. **Prove** with append-only records written back to the catalog: destruction minutes kept at
   least 3 years, a breach record with learned-at, a DSR record with application date, and the
   dated inventory diff behind the 7-day VERBİS rule.

## Market State (D14, frozen evidence)

| | D14.1 | .2 | .3 | .4 | .5 | .6 | .7 | .8 | .9 | .10 | .11 | T9: meets / partial / weak / none |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| OpenMetadata 1.13.3 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 1 | 1 | 2 | 0 / 30 / 18 / 1 |
| DataHub v1.7.0 | 2 | **3** | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 4 / 37 / 7 / 1 |
| Apache Atlas 2.5.0 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | **3** | 1 | 1 | 2 | 0 / 21 / 13 / 15 |

Almost every `partial` comes from one mechanism: a typed custom field that the organisation
defines, fills and polices. The weakest clusters in all
three products are the same: transfers abroad, processors, versioned privacy notices, and
anonymisation and masking. Product detail and code evidence are in
[d14-rubric.md](references/d14-rubric.md).

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| One "sensitive" or "special category" tag | Law Art. 6/1 is a closed list of separate categories; breach notices must split special from ordinary | 11 separate special-category values |
| Free-text legal basis | Not auditable, cannot be checked for emptiness | Closed list bound to Law Art. 5/6 conditions + "out of scope" values |
| Breach clock starts at record creation | Board Decision 2019/10 counts 72 h from **learning** | Separate `learned_at` field |
| "Explicit consent" as a transfer-abroad basis | After Law No. 7499 it is only an incidental exception (Art. 9/6-a), not for public-law activities (Art. 9/7) | Three-tier mechanism field |
| Treating Art. 28 as an organisation-wide exemption | Exemptions apply per processing activity | Exemption as a legal-basis value per asset |
| Catalog retention job = destruction | Catalog retention apps clean catalog records, not the data | Engine job + destruction record |
| Logical `DELETE` reported as destruction | Table formats keep old snapshots and files for days | Close the record when physical deletion completes |
| "Anonymised" written as a legal fact | A measurement (e.g. k = 7) is a fact; "anonymous" is a legal judgement | Store the measurement + policy version; see `data-anonymization` |
| Catalog access policy taken as data protection | Most catalog policies govern metadata, not data reads | Enforce in engine/storage; record the control in the catalog |
| `partial` read as compliant | `partial` means the organisation must build, fill and police the field | Add rule checks and owners before claiming coverage |

## References

- [references/obligations-map.md](references/obligations-map.md): abbreviations, verified durations, 49-obligation map with three-product cells and weakest link, Art. 28, Law No. 7499.
- [references/d14-rubric.md](references/d14-rubric.md): D14.1–D14.11 level definitions, measured scores and code evidence per product, double-counting rule.
- [references/metadata-model.md](references/metadata-model.md): neutral field set, enforced vs convention, VERBİS mapping, example YAML, platform mapping.
- [references/processes-and-evidence.md](references/processes-and-evidence.md): DSR, consent withdrawal, breach, periodic destruction, VERBİS change tracking, auditor checklist.
- [references/sources.md](references/sources.md): legal texts with Official Gazette references, research files, product versions.
- Related: `data-anonymization` (anonymisation as a destruction method, privacy models),
  `data-catalog-lineage`, `access-control-policies`; OpenMetadata-specific build-out:
  `openmetadata-kvkk-compliance` in the `openmetadata` plugin.
