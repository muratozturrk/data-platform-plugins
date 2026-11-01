---
name: openmetadata-kvkk-compliance
description: "Using OpenMetadata 1.13.3 as the metadata backbone for KVKK (Turkish Law No. 6698) compliance, with source-code evidence: 49 verified obligations from the Law, secondary regulations and Board decisions mapped to the catalog capability each needs and its OpenMetadata status; verified deadlines (30-day applications, 72-hour breach notice from learning, 6-month periodic destruction, 7-day VERBİS changes and others); the Law No. 7499 cross-border regime and Article 28 exemptions; modelling purpose, legal basis, retention, destruction, consent, special categories, transfers, processors, privacy notices and public data with existing structures, enforced vs convention marked; and the D14.1-D14.11 gaps with closing routes. Use when building a processing inventory or VERBİS export from OpenMetadata, designing KVKK custom properties or tags, scoping a data subject request or breach with lineage, judging what OpenMetadata can prove to an auditor, or planning KVKK work. Not legal advice."
---

# OpenMetadata KVKK Compliance

KVKK (Law No. 6698 on the Protection of Personal Data) obligations are recorded, evidenced and
checked in a data catalog long before any data is deleted. This skill maps each verified obligation
to the catalog capability it needs and states what **OpenMetadata 1.13.3** (`1.13.3-release`,
commit `255f6694913b`) does today, with source evidence. The bottom line: every obligation can be
modelled to at least `partial` with structures OpenMetadata already has (custom properties,
classifications, glossary, lineage, workflows), but **nothing is enforced** — no mandatory fields,
no per-record due dates or escalation, no transfer/breach/notice objects. DataHub v1.7.0 and Apache
Atlas 2.5.0 are no better overall: none of the 11 KVKK capabilities reaches 3 in any of them
(except Atlas on special categories).

> **Not legal advice.** Article numbers and durations were verified against official texts, but
> applying them (legal-basis lists, retention periods, exemptions) is counsel's job.
> Compliance depends on the organisation's processes, not only on the product.

Evidence tags: **K** = verified in source code, **D** = official docs, **İ** = issue/roadmap,
**B** = vendor claim. All D14 findings here are **K**. Paths are relative to the OM repo root.

## When to Use This Skill

- Building the Personal Data Processing Inventory (VERBİS, the Data Controllers' Registry) from
  catalog metadata instead of a spreadsheet.
- Designing custom properties, classifications and glossaries for purpose, legal basis, retention,
  destruction method, consent, special categories, transfers, processors and privacy notices.
- Finding all copies of a person's data for a data subject request, or the scope of a breach.
- Telling an auditor what the catalog proves today and what it does not.
- Prioritising KVKK feature work on OpenMetadata (D14.1–D14.11).

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Which article/deadline drives which capability | 49-row obligation map + verified durations | [obligations-map.md](references/obligations-map.md) |
| Is this activity even in scope (Art. 28) | Exemption is per activity; value of the legal-basis field | [obligations-map.md](references/obligations-map.md) |
| Cross-border basis after Law No. 7499 | Three-tier enum; consent only an incidental exception | [obligations-map.md](references/obligations-map.md) |
| VERBİS inventory | Enum custom properties + recursive CSV export → converter script | [modelling-in-openmetadata.md](references/modelling-in-openmetadata.md) |
| Legal basis, empties flagged | Enum property + `checkEntityAttributesTask` / `entitySemantics` rule | [modelling-in-openmetadata.md](references/modelling-in-openmetadata.md) |
| Special categories (Art. 6/1) | Own 11-tag classification, not `PersonalData.SpecialCategory` | [modelling-in-openmetadata.md](references/modelling-in-openmetadata.md) |
| Retention / destruction table | `retentionPeriod` + destruction-method enum, CSV | [modelling-in-openmetadata.md](references/modelling-in-openmetadata.md) |
| Privacy notice, counterparties | Glossary terms with custom properties | [modelling-in-openmetadata.md](references/modelling-in-openmetadata.md) |
| Data subject request / consent withdrawal / breach scope | Column search by PII tag → lineage downstream → export | [modelling-in-openmetadata.md](references/modelling-in-openmetadata.md) |
| What is missing and how to close it | D14.1–D14.11 detail, priority, first steps | [d14-gaps-and-routes.md](references/d14-gaps-and-routes.md) |

## Verified Deadlines

| Duration | Obligation | Source | Modellable in OM 1.13.3? |
|---|---|---|---|
| 30 days | Conclude a data subject application (free) | Law Art. 13/2; Application Communiqué Art. 6/5 | No due date on tasks |
| 72 hours | Breach notice to the Board, **from learning** | Board Decision 2019/10 (Law Art. 12/5 says only "as soon as possible") | No breach object, no due date |
| ≤ 6 months | Periodic destruction interval | Destruction Regulation Art. 11/2 | Periodic workflow trigger can sweep for overdue assets; destruction itself runs outside |
| ≥ 3 years | Keep destruction records | Destruction Regulation Art. 7/3 | No destruction minutes |
| 5 business days | Notify a standard contract (from signatures) | Law Art. 9/5; Cross-border Regulation Art. 14/5 | No transfer object |
| ≤ 4 years | Adequacy decision re-evaluation | Law Art. 9/2; Cross-border Regulation Art. 9/1 | No transfer object |
| 7 days | Notify changes to VERBİS | VERBİS Regulation Art. 13 | Version history exists; no diff job |

Root cause for every row: `thread.json` and `userApprovalTask` have no `dueDate`/`timeout`, so no
single request or task can count down or escalate. A cron-scheduled governance workflow trigger
does exist (`periodicBatchEntityTrigger.json:12-75`, `PeriodicBatchEntityTrigger.java:114-124`:
schedule + entity types + search filter + batch size), so periodic sweeps over entities ("assets
whose retention ended", "personal-data tables with no legal basis") can run inside OpenMetadata.
Alerting (D11.4 = 4) and workflows (D7.2 = 3) are strong; the missing piece is a per-record due
date with escalation.

## What OpenMetadata 1.13.3 Gives You (K)

| Capability | Status | Evidence |
|---|---|---|
| PII detection: Presidio/spaCy, 41 types, confidence, false-positive feedback | 3 | `ingestion/src/metadata/pii/scanners/ner_scanner.py:57-79` |
| Typed custom properties (enum, date, duration, hyperlink, entity refs) on tables **and columns** | 2 — **no required flag** | `spec/.../type/customProperty.json:1-71` |
| `retentionPeriod` (ISO 8601, inherited) on table/database/schema/container | 2 — no action on expiry | `table.json:1210-1213` |
| Lineage incl. column level; downstream query, CSV export, owner notification | 3 | `LineageResource.java:190-259,402-486` |
| Version history with field diffs | 3 | `entityHistory.json:9-104` |
| ABAC policies with `matchAnyTag`, deny-first | 4 — metadata only, not data reads | `RuleEvaluator.java:55-398`, `PolicyEvaluator.java:44-79` |
| Masking | 1 — catalog sample data only, hard-wired to `PII.Sensitive` | `PIIMasker.java:45-48` |
| Anonymisation | 0 — not found (searched: k-anonymity, differential privacy, generalisation) | `PIIMasker.java:45-48` |
| Data product `visibility` incl. `PUBLIC` | ready flag for publication | `dataProduct.json:36-41` |
| Consent concept | not found (only OIDC `prompt` text) | `oidcClientConfig.json:92` |

## Modelling Today: Convention vs Enforced

- **Enforced by the product:** enum value lists, date/duration formats, tag vocabularies,
  version history, lineage edges, audit log (append-only).
- **Convention only:** that a field is filled, that it is correct, that retention is acted on,
  that the 6-month bound holds, that a notice is linked. Turn conventions into **detected**
  non-conformities with governance workflows (`checkEntityAttributesTask`, JsonLogic) routed to a
  steward, and change alerts via event subscriptions.
- **Cannot be objects:** transfer record, breach record, data subject request, counterparty,
  privacy notice — no new entity types (`TypeResource.java:78-85`). Use glossary terms (versioned,
  approval-gated, linkable, accept custom properties) as stand-ins.
- **Out of the catalog's reach:** blocking data reads, masking query results, deleting data.
  Do them in the engine/storage layer and orchestrator; record method and result in the catalog.

## D14 Gaps at a Glance

| ID | Capability | OM | Priority (I×E) | Cheapest first step |
|---|---|---|---|---|
| D14.2 | Purpose + legal basis per asset | 2 | 25 | Legal-basis enum (with counsel) + empty-field rule |
| D14.1 | Inventory from catalog | 2 | 20 | Add transfer + measures fields; VERBİS CSV for one schema |
| D14.5 | Data subject request | 2 | 20 | Due-date field in `thread.json` |
| D14.8 | Special categories | 2 | 20 | 11-tag classification + per-category policy |
| D14.11 | Public publication | 2 | 16 | Masking-method + expiry fields; inventory of what is live |
| D14.3 | Destruction method + periodic job | 2 | 15 | Method enum; job in orchestrator writes results back |
| D14.4 | Consent + withdrawal downstream | 2 | 15 | Consent fields; manual lineage run on one table |
| D14.6 | Breach scope | 2 | 15 | Category report split by special category |
| D14.9 | Processor relationship | 1 | 12 | Counterparty glossary with role + contract |
| D14.10 | Versioned privacy notice | 1 | 12 | Notice as glossary term + effective date |
| D14.7 | Cross-border mechanism | 1 | 10 | Data-residency country on database/container |

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Using built-in `PersonalData.SpecialCategory` as the KVKK list | One undivided GDPR-worded tag, mutually exclusive with `Personal` | 11 separate tags per Law Art. 6/1 |
| Expecting OM policies to protect special-category data | Policies govern metadata; deny-first means a category policy cannot override | Enforce in the data plane; OM records the control |
| Treating `DataRetention` as destruction | It cleans catalog-internal records only | External destruction job + recorded minutes |
| Calling a DQ "incident" a breach record | Different concept; no learned-at field | Separate breach record with learned-at |
| Starting the 72 h clock at breach or record creation | Board Decision 2019/10 counts from learning | Store learned-at separately |
| Enum value "explicit consent" as a transfer basis | Law No. 7499: only an incidental exception; not for public-law activities (Art. 9/7) | Three-tier mechanism enum |
| Free-text legal basis | Not auditable | Closed enum + rule check |
| Assuming Art. 28 exempts the whole organisation | Exemptions apply per processing activity | Out-of-scope as a legal-basis value per asset |
| Putting properties on teams or services | No `extension` field in 1.13.3 | Glossary terms; database/container |

## References

- [references/obligations-map.md](references/obligations-map.md): verified durations, 49-obligation map with OM status and weakest link, Art. 28, Law No. 7499.
- [references/modelling-in-openmetadata.md](references/modelling-in-openmetadata.md): constraints, built-ins, attribute set, checks, anti-patterns.
- [references/d14-gaps-and-routes.md](references/d14-gaps-and-routes.md): D14.1–D14.11 status, gaps, routes, order.
- [references/sources.md](references/sources.md): legal texts, OM files, items checked beyond the research.
- Related skills: `openmetadata-architecture` (custom properties, entity model, versioning, APIs),
  `openmetadata-platform-integration` (lineage coverage for lakehouse/warehouse stores),
  `openmetadata-gap-closing` (24-gap catalog), `openmetadata-governance-assessment` (D14 rubric
  and scoring), `openmetadata-capability-design` (LLM/agentic KVKK features with impact assessment).
