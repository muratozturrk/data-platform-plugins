---
name: openmetadata-governance-assessment
description: "Evidence-based assessment of data governance products with a frozen, product-neutral Capability Reference Model (14 domains, 90 capabilities, DAMA-DMBOK2/DCAM/ISO-derived plus a KVKK domain): the 0-4 maturity scale and level-writing rules, evidence types (K code, D docs, İ issue, B vendor claim), the 'absence is a weak claim' rule with null vs 0, evidence YAML per product pinned to version and commit, parallel assessors, automated validation and cross-checks (all-zero, all-unknown, spread >= 4, weak zeros), DMBOK knowledge-area coverage, community sustainability metrics, and the measured results for OpenMetadata 1.13.3 versus DataHub 1.7.0 and Apache Atlas 2.5.0. Use when evaluating or comparing catalog/governance tools, auditing an OpenMetadata installation's governance maturity, writing or reviewing a capability rubric, defending a score with path:line evidence, or explaining where OpenMetadata is strong or weak."
---

# OpenMetadata Governance Assessment

This skill teaches how to measure a data governance product honestly: freeze a product-neutral
rubric first, pin the exact release and commit, score every capability 0-4 with one piece of
traceable evidence, write `null` instead of 0 when you have not proven absence, and cross-check
before publishing. It also carries the measured outcome of one such study: **OpenMetadata
1.13.3** (`1.13.3-release`, commit `255f6694913b`) against **DataHub v1.7.0** (`7f81ccbfe27b`)
and **Apache Atlas 2.5.0** (`234608315f29`), all inspected on 2026-08-11 from source clones.
Findings are dated; re-verify against newer releases. KVKK (Law No. 6698 on the Protection of
Personal Data) content is a mapping of obligations to product features, not legal advice.

## When to Use This Skill

- Choosing between catalog/governance products, or justifying a choice to architects or auditors.
- Assessing what an existing OpenMetadata installation can and cannot do for governance.
- Writing, freezing or reviewing a capability rubric; turning vague claims into observable levels.
- Filling an evidence file for a new product or a newer OpenMetadata release.
- Reviewing someone else's comparison table for bias, missing evidence or "absent" overclaims.
- Explaining OpenMetadata 1.13.3 strengths and weaknesses with `path:line` evidence.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Know what is measured | 14 domains, 90 capabilities, level-2 and level-4 anchors | [rubric.md](references/rubric.md) |
| Score one capability | 0-4 scale + evidence type + note; `null` when unsure | [assessment-procedure.md](references/assessment-procedure.md) |
| Claim a feature is absent | Search two different places, say where, or write `null` | [assessment-procedure.md](references/assessment-procedure.md) |
| Assess several products in parallel | Same frozen rubric, same prompt, same YAML format, then cross-check | [assessment-procedure.md](references/assessment-procedure.md) |
| Catch inconsistent scoring | Flag all-zero, all-unknown, spread >= 4, weak-zero rows; verify by hand | [assessment-procedure.md](references/assessment-procedure.md) |
| Publish results credibly | Per-domain mean with coverage `(n/total)`, evidence-type ratio, version/commit | [assessment-procedure.md](references/assessment-procedure.md) |
| OpenMetadata 1.13.3 scores and evidence | Domain means, 4s, 0s, comparison, DMBOK and community tables | [openmetadata-results.md](references/openmetadata-results.md) |
| Map results to DAMA-DMBOK2 | Capability-level `ref:dmbok2-kaN` mapping (95 mappings) | [openmetadata-results.md](references/openmetadata-results.md) |

## The Scoring Scale

| Score | Label | Observable meaning |
|---|---|---|
| 0 | Absent | Concept not in the product model; done outside the product or by hand |
| 1 | Experimental / add-on | Indirect, unstructured or partial: free text, manual step, unofficial plugin, one narrow scenario |
| 2 | Basic | Core, structured part of the product, but fed by hand; no automatic enforcement or feedback |
| 3 | Mature | Level 2 + runs automatically, fine-grained configuration, results reported or wired into a workflow |
| 4 | Differentiator | Level 3 + rare behaviour: auditability, immutability, cross-system inference, automatic non-compliance detection, closed-loop enforcement |

Levels are cumulative (level *n* contains level *n-1*). A level 4 is expected to be rare; a product
with 4 in every domain means the rubric is too easy. `null` (shown as `?`) means *unknown*, never 0.

## Evidence Types

| Type | Meaning | Format | Trust |
|---|---|---|---|
| `K` | Verified in source code | `repo/path:line` at the pinned commit | Highest; independent of marketing |
| `D` | Official documentation | URL (record version/date) | High |
| `İ` | Issue, RFC, design discussion, roadmap | URL | Medium; usually proves "not yet" or "planned" |
| `B` | Vendor claim | URL or quote | Lowest; report the B ratio per product, never mix silently with K |

Unreleased code on the main branch is scored as `İ`, not `K`: assess what can actually be
installed from the release.

## Honesty Rules (non-negotiable)

1. **Freeze the rubric before looking at any product.** Changing a level after inspecting a product
   shapes the yardstick to that product. Every post-freeze change goes to a changelog and every
   affected evidence file is re-filled.
2. **Absence is a weak claim.** A 0 needs searches in at least two different places (source + docs,
   or docs + issue tracker), named in the note. If unsure, score `null` and start the note with
   "searched:" listing where you looked.
3. **Every non-null score carries evidence.** No evidence, no score.
4. **Pin the version.** Product, release tag, full commit hash and inspection date in every file.
5. **Never hide the evidence type.** Publish the vendor-claim (B) ratio next to each product.
6. **Report coverage with every average.** `2.4 (7/7)` is a measurement; `3.0 (1/6)` is an anecdote.
7. **Novelty needs three sources.** Before saying "no product does X", search product docs, the issue
   tracker and the literature.
8. **Unmeasured is not zero.** If the rubric does not cover an area (e.g. DMBOK2 Documents and
   Content), say "not measured", never print 0.0.

## OpenMetadata 1.13.3 at a Glance

Domain means over 90 capabilities, all scored (no `null`), 87 `K` + 3 `D`, B ratio 0%:

| Domain | OM | DataHub | Atlas | Domain | OM | DataHub | Atlas |
|---|---|---|---|---|---|---|---|
| D1 Metadata & model | 2.7 | **3.3** | 3.0 | D8 Master/reference | **1.2** | 1.0 | 1.0 |
| D2 Discovery | 3.0 | **3.2** | 1.5 | D9 Products & mesh | **2.2** | 1.8 | 0.7 |
| D3 Lineage | 2.8 | **3.0** | 2.2 | D10 Observability | **2.7** | 2.3 | 1.0 |
| D4 Glossary & semantics | **3.4** | 2.4 | 1.8 | D11 Integration | **2.8** | 2.7 | 1.5 |
| D5 Data quality | **2.4** | 1.4 | 0.3 | D12 Platform | 1.9 | 1.9 | 1.4 |
| D6 Security & privacy | **1.8** | 1.6 | 1.2 | D13 AI/ML | 2.2 | **2.8** | 1.0 |
| D7 Governance process | **2.5** | 1.8 | 0.5 | D14 KVKK | 1.7 | **2.1** | 1.8 |

- **Level 4 (differentiators):** search relevance tuning (D2.1), entity page (D2.6), user-defined
  glossary relation types (D4.2), RDF/OWL/SKOS + SPARQL (D4.4, off by default), RBAC+ABAC with a
  decision-explaining debug endpoint (D6.2), alert routing with retry and delivery status (D11.4).
- **Level 0:** row/column access control (D6.4), anonymisation (D6.8), golden record (D8.4),
  built-in backup/restore (D12.8). OpenMetadata is a metadata plane: it does not sit in the data path.
- **Behind DataHub:** typed custom properties on every entity type (D1.2), ML lineage (D13.2),
  connector breadth (D11.1), as-code governance (D11.6), Kafka/schema-registry integration (D11.3).

Full table with evidence: [openmetadata-results.md](references/openmetadata-results.md).

## Pitfalls

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Writing the rubric while reading the product | Yardstick fits the product, comparison void | Freeze first; changelog + re-fill on change |
| Subjective level text ("good", "comprehensive", "rich") | Two assessors disagree | Observable behaviour per level; grep the rubric for adjectives and product names |
| Scoring 0 after one grep | Feature may live in another module or the docs | Two places searched, or `null` with "searched:" note |
| Mixing vendor claims with code-verified scores | Brochure beats code in the table | Separate B ratio; do not rank B-heavy products alongside K-heavy ones |
| Averaging over a handful of scored rows | One cell becomes a domain verdict | Print `(scored/total)`; treat low coverage as anecdotal |
| Scoring main-branch code as released | Customer cannot install it | `İ` for unreleased, `K` only at the release tag |
| Counting the same feature in two domains | Inflates the regulatory domain | Generic mechanism in D6/D9; D14 scores only the legal delta |
| Trusting GitHub stars for an ASF project | Mirror repo, issues in JIRA, releases on ASF dist | Use JIRA, release cadence, commit flow |
| Treating a cross-check flag as a verdict | Heuristics miss and over-flag | Read the evidence for each flagged row and record the decision |

## Related Skills

- `openmetadata-architecture` — what sits behind D1/D11/D12 scores (entity model, APIs, events, deployment).
- `openmetadata-platform-integration` — connector and lineage coverage behind D3/D11.1.
- `openmetadata-gap-closing` — turning the low scores here into a prioritised gap list and fixes.
- `openmetadata-kvkk-compliance` — D14 in depth: obligations, T9 matrix, how to model compliance metadata today.
- `openmetadata-capability-design` — designing new capabilities where every product scores low.

## References

- [references/rubric.md](references/rubric.md): domains, DMBOK mapping, 90 capabilities with level-2 and level-4 anchors.
- [references/assessment-procedure.md](references/assessment-procedure.md): step-by-step method, evidence YAML template, validation and cross-check rules.
- [references/openmetadata-results.md](references/openmetadata-results.md): full OpenMetadata 1.13.3 results, comparison, DMBOK coverage, community, cross-check findings.
- [references/sources.md](references/sources.md): research basis, frameworks, what was omitted or recomputed.
