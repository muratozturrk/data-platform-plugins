---
name: data-quality-slas
description: "Vendor-neutral guide to data quality rules, service level objectives (SLOs) and quality incidents for datasets and data products, backed by a source-code comparison of three open-source catalogs. Covers the 0-4 maturity levels of rubric domains D5 Data Quality (rule authoring, profiling, test execution and scheduling, incidents and SLA, quality score, anomaly detection, external tools such as Great Expectations and dbt tests) and D10 Observability (freshness, volume, schema drift, pipeline runs, usage, cost); writing freshness, completeness, accuracy, uniqueness, validity and volume SLOs with SLI formulas, windows, error budgets and owners; an incident lifecycle with severity, response targets and escalation; and how OpenMetadata 1.13.3, DataHub 1.7.0 and Apache Atlas 2.5.0 scored, with path:line evidence. Use when setting quality expectations for a dataset or data product, deciding where checks run, designing quality alerting and incident handling, or judging what a catalog can really monitor and enforce."
---

# Data Quality SLAs

A quality SLA is a promise about data (fresh by 06:00, no null keys, row count within the usual
range) plus a process for when the promise breaks. This skill gives a vendor-neutral way to write
those promises as measurable SLOs, wire the checks, and run quality incidents. It also shows what
three open-source catalogs actually do, measured on a frozen 0–4 rubric (14 domains, 90
capabilities) against their source code.

**Bottom line.** Catalogs are good at *recording and showing* quality: profiles, test results,
dimension scores, incident records. None of the three measured catalogs *enforces* an SLO from end
to end:

- No incident model has a resolution-time target or escalation. D5.4 is at most 2 in all three.
- OpenMetadata stores `refreshFrequency`/`maxLatency` in a data contract but never checks them
  (D10.1 = 1).
- DataHub's open-source edition stores results from outside tools but does not run checks itself
  (D5.3 = 0). Its native assertions are Cloud-only.

So run the checks in the pipeline or orchestrator, push results to the catalog, and keep the SLO
evaluation, the pipeline gate (circuit breaker) and the escalation in your own process.

Evidence tags: **K** = verified in source code, **D** = official docs, **İ** = issue/roadmap,
**B** = vendor claim. A statement without a tag is general practice, not a research finding.
`?`/`null` means "not determined"; it is not the same as 0.

## When to Use This Skill

- Defining the quality promise of a new dataset or data product (what, how measured, by whom).
- Choosing where a check runs: an inline pipeline gate, a scheduled suite, or catalog monitoring.
- Designing quality alerting, incident triage, severity and escalation.
- Assessing or comparing catalog products on data quality and observability (D5, D10).
- Bringing Great Expectations or dbt test results into a catalog.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| What "level 3 data quality" means, how to probe a product | D5.1–D5.7 and D10.1–D10.6 levels with probes | [quality-capabilities.md](references/quality-capabilities.md) |
| Write an SLO for freshness / completeness / uniqueness / validity / accuracy / volume | SLI formula + target + window + owner + action | [slo-and-incidents.md](references/slo-and-incidents.md) |
| Stop bad data before it reaches consumers | Pipeline gate (circuit breaker) with warn/error tiers | [slo-and-incidents.md](references/slo-and-incidents.md) |
| Run quality incidents | State machine, severity matrix, response targets, escalation | [slo-and-incidents.md](references/slo-and-incidents.md) |
| What OpenMetadata / DataHub / Atlas really do | Per-capability scores with `path:line` | [d5-d10-comparison.md](references/d5-d10-comparison.md) |
| Push Great Expectations / dbt results to a catalog | Integration parameters and limits | [d5-d10-comparison.md](references/d5-d10-comparison.md) |
| Cite a source | Research files, versions, standards | [sources.md](references/sources.md) |

## SLO Dimensions at a Glance

| Dimension | SLI (what you measure) | Typical target form | Where to run |
|---|---|---|---|
| Freshness | `now − max(event_or_load_time)` | ≤ 2 h, 99 % of hours | Scheduler sensor + catalog alert |
| Completeness | `1 − nulls(col) / rows` for required columns | ≥ 99.5 % per load | Pipeline gate |
| Uniqueness | `distinct(key) / rows` | = 100 % per load | Pipeline gate |
| Validity | rows passing a domain rule / rows (regex, range, set) | ≥ 99.9 % per load | Pipeline gate or suite |
| Accuracy | matches against a trusted reference / sampled rows | ≥ 98 % weekly | Scheduled reconciliation |
| Volume | rows loaded vs a trailing median for the same weekday | within ±30 % | Suite + anomaly check |
| Schema | contract columns present, types unchanged | no breaking change | CI + change event (see `data-contracts`) |

The numbers are examples. Set your own with the consumer, and write down the measurement window
and the owner for each one.

## Measured Scorecard (0–4, all evidence K)

| Domain | OpenMetadata 1.13.3 | DataHub v1.7.0 (OSS) | Apache Atlas 2.5.0 |
|---|---|---|---|
| D5 Data quality (mean of 7) | **2.4** | 1.4 | 0.3 |
| D10 Observability (mean of 6) | **2.7** | 2.3 | 1.0 |
| Strongest | D5.1 rules 3, D5.5 score 3, D10.2/10.3 3 | D5.2 profiling 3, D5.7 external 3, D10.3 drift 3 | D10.3 drift 2 |
| Weakest | D5.6 anomaly 1, D10.1 freshness 1 | D5.3 execution 0, D5.6 anomaly 0 | D5.1/5.3/5.4/5.5/5.6 = 0 |

DataHub's low D5 comes mostly from *Cloud gating*: native and inferred assertions are marked
"DataHub Cloud only" (`AssertionSource.pdl:17-32`, K). That does not mean the feature does not
exist. It means it is not in the open-source edition. Full matrix and caveats are in
[d5-d10-comparison.md](references/d5-d10-comparison.md).

## Core Practice

1. **Name the asset, the consumer and the owner first.** An SLO without an owner just sends
   alerts nobody acts on. Every measured catalog can attach owners (D7.1 ≥ 3 in OM and DH).
2. **Write SLIs as queries.** If an SLI cannot be computed by a query or a job, it is not an SLI.
3. **Split checks into tiers.** `error` blocks publication (pipeline gate). `warn` opens an
   incident but lets data through. `info` is only recorded. Great Expectations
   (`critical/warning/info`) and dbt freshness (`warn_after/error_after`) already use these tiers.
4. **Gate in the pipeline, not in the catalog.** None of the measured catalogs offers a
   load-triggered test with a pipeline stop (D5.3 ≤ 2).
5. **Send every result to the catalog** so consumers see quality next to the asset. Then fill the
   gaps the product leaves: SLA timers, escalation, freshness evaluation.
6. **Measure compliance over a window** (for example a rolling 28 days), not a single run. Report
   the error budget left over.

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Writing the SLA in a contract field and assuming it is checked | OM stores `refreshFrequency`/`maxLatency`; no code evaluates them (K) | Run a freshness check yourself; alert on breach |
| Treating catalog incident severity as business impact | OM's classifier rates *asset importance* (tier, owner, followers, votes) using coefficients fitted to synthetic data, and gives no rating when the asset has no tier (K) | Set severity from impact × urgency yourself |
| Expecting "incident" to mean an SLA timer | OM: New/Ack/Assigned/Resolved; DH: ACTIVE/RESOLVED + stage; neither has a due date (K) | Track time-to-ack/resolve outside, or add a periodic sweep |
| Counting DataHub OSS as having native assertions or anomaly detection | Marked Cloud-only in the model (K) | Run GX/dbt and push results; or budget for the commercial edition |
| Static thresholds on seasonal data | Weekend/holiday dips raise false alarms | Compare to the same weekday; use dynamic bounds |
| One global "quality score" | Hides which dimension failed; weights not configurable in OM (D5.5 = 3, not 4) | Report per dimension, weight critical assets separately |
| Checks that only run nightly | Bad data is served for hours | Gate at load time; nightly suite only for slow checks |
| Scoring an unsearched capability 0 | "Absent" is the hardest claim to prove | Search code **and** docs; else write `?` |

## References

- [references/quality-capabilities.md](references/quality-capabilities.md): D5/D10 rubric levels, probes, scoring rules.
- [references/slo-and-incidents.md](references/slo-and-incidents.md): SLO spec template, SLI queries, gates, incident lifecycle.
- [references/d5-d10-comparison.md](references/d5-d10-comparison.md): three-product scores with evidence, integration details, caveats.
- [references/sources.md](references/sources.md): research files, product versions, standards.
- Related skills: `data-contracts` (schema and SLA promises between producer and consumer),
  `data-catalog-lineage` (downstream impact of a breach), `access-control-policies`.
