---
name: data-anonymization
description: "Vendor-neutral anonymisation of structured data and documents as a governance control, with code and literature evidence: direct identifiers vs quasi-identifiers vs sensitive attributes; automatic discovery (fuzzy name and checksum-validated value matchers, Turkish-locale gaps, sampled QI search vs ARX's 2^n power set); what masking, pseudonymisation, k-anonymity, l-diversity, t-closeness and differential privacy guarantee; ARX-style re-identification risk reports (prosecutor/journalist/marketer, population models); decomposable vs global information-loss metrics; ARX internals and single-machine limits (commit 4e0a5f5); distributed and approximate methods and what is unproven at PB scale; document redaction, OCR and Turkish NER accuracy; KVKK anonymisation-as-destruction and an evidence ledger. Use when choosing a privacy model, measuring k or re-identification risk on large tables, scoping a distributed anonymiser, anonymising documents, or judging what an anonymisation claim proves. Not legal advice."
---

# Data Anonymization

Masking hides a value; anonymisation is a measurable property of the **whole released dataset**.
Removing the national ID column does not help when age + district + a date still single out one
person. This skill covers how to find what identifies people, which privacy model gives which
guarantee, how to measure re-identification risk at scale, and what can honestly be claimed.

**Bottom line.** Exact measurement of k (`GROUP BY QI HAVING count(*) < k`) and the risk report
derived from the class-size histogram are cheap and provable on GB–TB data. The **search** for a
good generalisation is what does not scale: ARX is single-threaded, in-memory and capped at
2³¹−1 cells, and no peer-reviewed work enforces k-anonymity anywhere near a petabyte (largest
verified: 30 GB / 1.2 × 10⁹ records). Approximate methods (sketches, sampling, catalog statistics)
are for triage, never for evidence. For documents only metadata stripping and regeneration are
exact; detection yields a recall estimate, not a guarantee.

> **Not legal advice.** KVKK article numbers come from the research's verified legal table; whether
> a dataset counts as anonymous is a legal qualification for counsel, not an output of any tool.

Evidence tags: **K** = verified in source code (ARX `4e0a5f5`, paths under
`src/main/org/deidentifier/arx/`), **D** = official docs, **L** = peer-reviewed literature,
**B** = vendor claim, **(est.)** = estimate, never measured. "Not verified" ≠ absent; "not
computable" ≠ 0.

## When to Use This Skill

- Classifying columns as direct identifiers, quasi-identifiers (QIs) or sensitive attributes.
- Choosing between masking, pseudonymisation, k-anonymity, ℓ-diversity, t-closeness and DP.
- Producing a re-identification risk report or measuring min k on relational or lakehouse tables.
- Judging whether ARX (or any single-machine tool) fits a dataset, or scoping a distributed design.
- Anonymising PDFs, office files or scans, especially in Turkish.
- Deciding what an anonymisation record proves to an auditor, and what it does not.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| What each model guarantees and costs | Guarantee/limit table; locality + monotonicity of 21 ARX criteria | [privacy-models-and-risk.md](references/privacy-models-and-risk.md) |
| Risk report (prosecutor/journalist/marketer) | Class-size histogram φ_j → four measures; population input or "not computable" | [privacy-models-and-risk.md](references/privacy-models-and-risk.md) |
| Information loss across partitions | Loss/NDS or DM\* (decomposable); avoid NUE, KL, classification | [privacy-models-and-risk.md](references/privacy-models-and-risk.md) |
| KVKK link, evidence ledger, claim limits | Destruction method + ≥ 3-year immutable record | [privacy-models-and-risk.md](references/privacy-models-and-risk.md) |
| Find direct identifiers | Fuzzy name match + value pattern + **check-digit validation** | [discovery-and-risk-report.md](references/discovery-and-risk-report.md) |
| Find quasi-identifiers | α-distinct + α-separation; sampled greedy set cover, O(m²) space | [discovery-and-risk-report.md](references/discovery-and-risk-report.md) |
| Cheap pre-screen without reading data | One-sided NDV test from engine statistics | [discovery-and-risk-report.md](references/discovery-and-risk-report.md) |
| Exact k on PostgreSQL/MySQL/Oracle/SQL Server/Spark/Flink | `GROUP BY … HAVING`, plan recorded | [discovery-and-risk-report.md](references/discovery-and-risk-report.md) |
| Does ARX scale? What parallelises? | Lattice, roll-up state machine, D1–D9 limits | [arx-and-scaling.md](references/arx-and-scaling.md) |
| Distributed algorithm choice and evidence | Distributed Mondrian, Spark subtree, P4, SKALD; DP tools; streaming | [arx-and-scaling.md](references/arx-and-scaling.md) |
| Documents | Metadata strip → OCR → two-engine detection → regenerate | [document-anonymization.md](references/document-anonymization.md) |

## Attribute Roles

| Role | Examples | Treatment |
|---|---|---|
| Direct identifier | National ID, name, e-mail, phone, IBAN | Remove or pseudonymise |
| Quasi-identifier | Age, district, dates, occupation | Generalise / suppress to groups ≥ k |
| Sensitive attribute | Diagnosis, salary, special-category data | Control per-group distribution (ℓ, t) |

A **missing** QI makes measured k look larger than it is — the error is always in the unsafe
direction. Version the QI set, record who approved it, and re-measure when it changes.

## Privacy Models at a Glance

| Model | Guarantees | Check | Distributes? (K) |
|---|---|---|---|
| Masking / pseudonymisation | Field unreadable / consistently replaced | Per record | ✅ trivially; no group guarantee |
| k-anonymity | Each QI group ≥ k records | `count ≥ k` per class | ✅ class-local, monotone |
| ℓ-diversity | ≥ ℓ well-represented sensitive values per group | Per-class distribution | ✅ local; recursive/entropy not monotone under suppression |
| t-closeness | Group distribution within t of global | Class vs **global** distribution | ⚠️ needs a broadcast global pre-table |
| Average risk / uniqueness | Dataset-level risk bounds | Global class histogram | ❌ in ARX; ✅ once the histogram is aggregated |
| Differential privacy | Mechanism-level bound e^ε (+δ) | Noise/sampling + budget | ⚠️ compute yes, budget ledger central |

ARX's (ε,δ)-DP (SafePub) adds no noise: it samples records with probability β and then enforces
k-anonymity; optimal search and local recoding are then unsupported (K,
`criteria/EDDifferentialPrivacy.java:216,259-272`).

## Exact vs Approximate

| Method | Allowed for | Not allowed for |
|---|---|---|
| Exact `GROUP BY … HAVING count(*) < k` | Evidence | — |
| Class-size histogram measures | Risk report | — |
| Catalog statistics (NDV) | One-sided "certainly not k-anonymous" triage | Any "is k-anonymous" claim |
| HLL / KHLL sketches | Uniqueness profile, cost of a k choice | Proving k (minimum ≠ cardinality) |
| Count-Min sketch | Rough profiling | Violation detection (over-estimates → missed violations) |
| Sampling | Mondrian split points, QI discovery | Estimating min k (small groups vanish) |

Sketches are as sensitive as raw data (Desfontaines, Lochbihler, Basin 2019) — protect them so.

## Scale Facts

| Fact | Evidence |
|---|---|
| Lattice size = Π levels per QI; `OPTIMAL` refused above 100,000 nodes by default | K: `framework/lattice/SolutionSpace.java:78-85`, `ARXConfiguration.java:387`, `ARXAnonymizer.java:654-658` |
| Whole table in one `int[]`: rows × columns ≤ 2³¹−1 | K: `framework/data/DataMatrix.java:57-66` |
| No threads/executors in `src/main`; GUI only | K: grep, see arx-and-scaling.md |
| Node cost depends on the previously checked node (roll-up/snapshot) | K: `framework/check/TransformationCheckerStateMachine.java:56-66` |
| QI discovery in ARX enumerates the power set (2ⁿ−1 groupings) | K: `risk/RiskModelAttributes.java:176-187` |
| Largest verified distributed k-anonymity: 30 GB / 1.2 × 10⁹ records, 6 Spark nodes | L: Bazai et al. 2021 |
| Distributed Mondrian reference tested on 1.5 M records | L: De Capitani di Vimercati et al. 2023 |
| Only PB-scale claim is risk **measurement** (KHLL), not enforcement | L: Chia et al. 2019 (production statement) |
| PostgreSQL avoids parallel aggregation for high-cardinality `GROUP BY`; ~1 TB practical ceiling | D; ceiling (est.) |

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Treating masking as anonymisation | Records stay linkable through QIs | Measure k on the QI set |
| Reading PostgreSQL `n_distinct = -1` as "1 value" | The unique column looks safest | Negative = ratio × `reltuples` |
| Iceberg `record_count` as N | Ignores deletes → groups look bigger | Use delete-aware counts or exact `count(*)` |
| "NDV ≤ N/k so it's fine" | Average hides a singleton | Only the one-sided test is valid |
| Reporting α-separation alone | Looks lower-risk than α-distinction | Report both, explain the gap |
| Population measures with a default size | Fabricated precision | "Not computable" until supplied |
| Copying ARX's `0` for empty histograms | "No data" shown as "no risk" | Distinguish null from 0 |
| Non-Uniform Entropy in a distributed run | Needs global tables per node | Loss/NDS or DM\* |
| Locale-default lower-casing of Turkish headers | `TITLE` → `tıtle`, `İL` → `i̇l` | Explicit locale + ASCII-folded match |
| Black boxes over PDF text; editing the source file | Text layer and old revisions remain | Regenerate from scratch |
| Promising published Turkish NER F1 (~94) | Out-of-domain BERTurk fell to 61.91 | Measure on your own documents |
| Silent loss of parallelism (PG planner, `MAXDOP`, Flink streaming mode, Spark SPJ flags) | Job runs, result slow or provisional | Record the plan; fail on wrong shape |
| "Anonymised" ledger kept in a mutable DB | DBA can rewrite evidence | WORM or signed export |

## References

- [references/privacy-models-and-risk.md](references/privacy-models-and-risk.md): roles, model guarantees, ARX criteria table, DP in ARX, risk report and histogram SQL, metric decomposability, KVKK link, evidence ledger.
- [references/discovery-and-risk-report.md](references/discovery-and-risk-report.md): identifier matchers, Turkish locale, QI discovery, catalog-statistics triage per engine, exact measurement, violating-row display.
- [references/arx-and-scaling.md](references/arx-and-scaling.md): Flash, pruning, memory model, single-machine limits, parallelisability, distributed and approximate literature, DP tools, streaming, PB honesty.
- [references/document-anonymization.md](references/document-anonymization.md): fake redaction, incremental save, metadata, OCR, Turkish NER, two-engine detection, statements you can and cannot make.
- [references/sources.md](references/sources.md): research files, ARX commit, papers, docs and legal texts.
- Related skills: `access-control-policies` (masking and row filters in the data plane),
  `data-catalog-lineage` (where QI and sensitivity tags live), `kvkk-compliance` (full KVKK obligation map).
