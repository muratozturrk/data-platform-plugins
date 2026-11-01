---
name: governance-assessment
description: "Product-neutral, evidence-based assessment of data governance tools and of a platform's or organisation's governance coverage: a frozen Capability Reference Model (14 domains, 90 capabilities from DAMA-DMBOK2, DCAM, ISO, data mesh and AI frameworks plus a KVKK local-law domain) with 0-4 anchors per capability; scale and level-writing rules; evidence types (K code, D docs, İ issue, B vendor claim) and 'absence is a weak claim' (null is not 0); evidence YAML pinned to tag and commit, with a template; blind parallel assessors, validation and cross-checks (all-zero, all-unknown, spread >= 4, weak zeros) and their blind spots; DMBOK2 coverage incl. the unmeasured Documents and Content area; D7 governance-process and D8 master-data findings; community metrics; a worked OpenMetadata/DataHub/Atlas example. Use when comparing or shortlisting catalog/governance tools, auditing platform coverage, writing or reviewing a rubric, defending a score with evidence, or mapping tools to DMBOK."
---

# Governance Assessment

Measuring a governance tool honestly is mostly discipline: freeze a product-neutral yardstick
**before** looking at any product, pin the exact release and commit, give every capability one
0-4 score backed by one traceable piece of evidence, write `null` when absence is unproven, and
cross-check before publishing. This skill carries the yardstick (90 capabilities in 14 domains),
the procedure, and one worked example. **Bottom line from that example:** open-source catalogs
separate on data quality, governance workflow, discovery and AI/ML; they converge, and all stay
low, on security enforcement, master data, platform operations and regulatory compliance. That is
the ceiling of the catalog category, not of any one product: plan those capabilities in the data
plane, MDM and orchestration layers whatever catalog you pick.

Evidence tags: **K** = verified in source code (`path:line`), **D** = official documentation,
**İ** = issue/RFC/roadmap, **B** = vendor claim. Product results are illustrative; the method is
the point. Score definitions are the rubric's, not the vendors'.

## When to Use This Skill

- Shortlisting or comparing data catalog / governance products on evidence instead of demos.
- Auditing which governance capabilities a platform (several tools together) actually covers.
- Writing, reviewing or extending a capability rubric (levels, sources, local-law slice).
- Defending or challenging a single score: which level clause is met, with what evidence.
- Mapping tools or a programme to DAMA-DMBOK2 knowledge areas and finding unmeasured areas.
- Judging open-source project health (D12.6) without being misled by stars.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| The 90 capabilities and their 0-4 anchors | Compact rubric, per domain, with DMBOK KA | [rubric.md](references/rubric.md) |
| Run an assessment end to end | Freeze → pin → blind parallel assessors → score → validate → cross-check → aggregate | [assessment-procedure.md](references/assessment-procedure.md) |
| Record evidence | One YAML per product, pinned header, `arandı:` for nulls | [evidence-template.yaml](assets/evidence-template.yaml) |
| Decide 0 vs `null` | Two search places for 0; otherwise `null` with where you searched | [assessment-procedure.md](references/assessment-procedure.md) |
| Check consistency across assessors | All-zero, all-unknown, spread >= 4, weak-zero flags + manual decision | [assessment-procedure.md](references/assessment-procedure.md) |
| Assess a platform or organisation, not one tool | Composed score per capability + separate practice overlay | [assessment-procedure.md](references/assessment-procedure.md) |
| Project scores onto DMBOK2 | Capability-level `ref:dmbok2-kaN` mapping; "not covered" ≠ 0 | [dmbok-coverage.md](references/dmbok-coverage.md) |
| Governance processes, master/reference data | D7 and D8 per-capability findings | [dmbok-coverage.md](references/dmbok-coverage.md) |
| Community sustainability | API endpoints, mirror caveats, `?` not 0 | [assessment-procedure.md](references/assessment-procedure.md) |
| See what results look like | Three-product T1/T2, cross-check outcome, T5 | [measured-results.md](references/measured-results.md) |

## The Scale

| Score | Label | Meaning |
|---|---|---|
| 0 | Absent | The concept is not in the product model; done outside or by hand |
| 1 | Experimental / add-on | Free text, manual step, unofficial plug-in, or one narrow case |
| 2 | Basic | Structured and part of the product, but fed by hand; no automatic enforcement |
| 3 | Mature | Level 2 + automatic, fine-grained, reported or wired into a workflow |
| 4 | Differentiator | Level 3 + rare behaviour: auditable, immutable, cross-system inference, automatic non-conformity detection, closed loop |
| `null` | Unknown | Not proven either way; note says where you searched |

Levels are cumulative and conjunctive: score the highest level whose **every** clause holds.
Level texts must be observable (no "good", "comprehensive", "mature"), product-neutral and
monotonic; numbers in them are verified against the source.

## The Model at a Glance

| Domains | Capabilities | Anchor framework |
|---|---|---|
| D1 metadata model (7), D2 discovery (6) | 13 | DMBOK2 Metadata |
| D3 lineage (5), D11 integration (6) | 11 | Data Integration and Interoperability |
| D4 glossary/semantics (5), D8 master/reference data (4) | 9 | Data Modeling; Reference and Master Data |
| D5 data quality (7) | 7 | Data Quality; ISO 8000-61 |
| D6 security/privacy (8) | 8 | Data Security; ISO/IEC 20889 |
| D7 governance processes (6) | 6 | Data Governance and Stewardship; DCAM; DGI |
| D10 observability (6), D12 platform (8) | 14 | Storage and Operations; Data Architecture; ISO 22301 |
| D9 data products/mesh (6), D13 AI/ML (5) | 11 | Data mesh governance; EU AI Act Art. 10 |
| D14 KVKK compliance (11) | 11 | Turkish Law No. 6698 (regulatory delta over D6/D9 only) |

Total **14 domains, 90 capabilities, 48 sources**, frozen 2026-08-11. DMBOK2 knowledge areas
are numbered ordinally here, KA1 Governance ... KA11 Data Quality, in book chapter order
(chapters 3-13). **KA7 Documents and Content is cited by no capability**: report it as "not
covered", never as 0.

## Worked Example in One Table

OpenMetadata `1.13.3-release` (`255f6694913b`), DataHub `v1.7.0` (`7f81ccbfe27b`), Apache Atlas
`release-2.5.0` (`234608315f29`); 90/90 scored for each, B ratio 0 %, 84-87 rows type K.

| | OpenMetadata | DataHub | Atlas |
|---|---|---|---|
| Mean of 90 | 2.33 | 2.22 | 1.38 |
| Count of 0 / 4 | 4 / 6 | 4 / 7 | 21 / 2 |
| Leads (domain means; D12 tied) | D4-D11 | D1, D2, D3, D13, D14 | none |
| Widest gaps | D5 quality 2.4/1.4/0.3; D7 processes 2.5/1.8/0.5 | | |
| Shared zeros | D6.8 anonymisation, D8.4 golden record | | |

DataHub's lower D5/D7 scores are partly edition effects: approval workflows and assertion
monitors are Cloud-only in the vendor's own matrix (`managed-datahub-overview.md:30-48`, K).
Detail, full matrix and community metrics: [measured-results.md](references/measured-results.md).

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Building the rubric while looking at a favourite product | Yardstick shaped around it | Freeze first; changelog + full re-score on change |
| Writing 0 for "did not find" | Absence is the hardest claim | Two search places, or `null` + `arandı:` note |
| Averages without coverage | One known cell poses as a domain mean | Print `mean (known/total)` and B ratio |
| Scoring the commercial cloud for an on-prem decision | Overstates what you can run | Score the edition you will deploy; name gated features |
| Trusting the cross-check tool's silence | All-zero is suppressed by any `null`; weak-zero is a keyword heuristic | Re-run on full-coverage subsets; read every zero of a low scorer |
| Reading "not covered" DMBOK areas as weak | Reports a result that does not exist | Separate "no data" row with the reason |
| Mixing tool capability with organisational practice | A feature nobody uses scores like one in use | Practice as a second column |
| Comparing GitHub stars of a mirror repository | Different history, no issues there | Foundation indicators (JIRA, releases, PMC) |
| Reusing the KVKK domain under another law | Different obligations | Write a local slice, version the rubric |
| Expecting a catalog to enforce security, master data or destruction | Category ceiling (D6, D8, D12, D14 < 2.2 everywhere) | Engine/MDM/orchestrator do it; catalog records and evidences |

## Known Limitations

- One assessor per product; consistency rests on the frozen rubric, shared brief, cross-check and
  manual audit, not on an inter-rater statistic. Means are unweighted.
- The research validator does **not** scan level texts for adjectives or product names, although
  the research README says it does; scan manually.
- DMBOK2 book text was not available; areas come from DAMA's public context-diagram summaries.
- DW/BI (2 capabilities) and Data Architecture (3) are thinly measured.
- Snapshot of 2026-08-11; products ship often. Re-pin before reuse.

## References

- [references/rubric.md](references/rubric.md): all 90 capabilities with 1-4 anchors, DMBOK KA, D14 slice.
- [references/assessment-procedure.md](references/assessment-procedure.md): freeze, pin, assessors, evidence, validation, cross-check, aggregation, platform/organisation adaptation, community metrics.
- [references/dmbok-coverage.md](references/dmbok-coverage.md): KA numbering, coverage method and results, KA7 gap, D7 and D8 findings.
- [references/measured-results.md](references/measured-results.md): three-product worked example (T1, T2, cross-check, T5, limitations).
- [references/sources.md](references/sources.md): research files, pins, re-verified code claims, frameworks, papers, legal texts.
- [assets/evidence-template.yaml](assets/evidence-template.yaml): evidence file template.
- Related skills: `kvkk-compliance` (D14 obligations in depth), `data-catalog-lineage` (D1-D3),
  `data-quality-slas` (D5), `access-control-policies` (D6), `data-contracts` (D9.3),
  `data-anonymization` (D6.8).
