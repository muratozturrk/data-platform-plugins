# D5 and D10 Capability Levels

The research rubric measures data governance software on **14 domains and 90 capabilities**, each
scored 0–4. It was frozen before any product was assessed, so the criteria were not shaped to fit
a product. This file covers the quality domain (D5, 7 capabilities, derived from DAMA-DMBOK2 KA11
Data Quality) and the observability domain (D10, 6 capabilities, derived from DMBOK2 KA4 Data
Storage and Operations). Level texts are condensed English renderings of the frozen Turkish
originals in `research/rubric/capabilities.yaml`. Each capability ends with a **probe**, a hands-on
test that shows which level a product reaches.

## Scoring rules (apply to every capability)

| Score | Label | Meaning |
|---|---|---|
| 0 | Absent | Not represented in the product; done by hand or by another tool. |
| 1 | Experimental | Indirect or unstructured: free text, a manual step, one narrow scenario. |
| 2 | Basic | A structured, first-class part of the product, but fed by hand or not acted on automatically. |
| 3 | Mature | Runs automatically, can be configured in detail, results are reported or wired into a workflow. Includes all of level 2. |
| 4 | Differentiating | Rare behaviour: auditability, closed-loop enforcement, cross-system inference. Includes all of level 3. |

- **Monotonic:** level *n* includes everything in level *n−1*.
- **"Absent" is a weak claim:** a 0 needs a search in at least two different places (source code
  **and** official docs, or docs **and** issue tracker). If you are unsure, write `null`/`?`, not 0.
- **Commercial-only features** are scored for the open-source edition and marked separately. A
  feature that exists only in a paid edition is "not in OSS", not "absent".

## D5 — Data Quality

### D5.1 Rule authoring
How rules (not-null, unique, range) are defined and how much skill that takes.
- 0 no rule mechanism · 1 only custom code/SQL · 2 parameterised templates (not-null, unique,
  range) in the UI · 3 templates **plus** free SQL/expression rules in the UI and cross-entity
  rules (referential integrity, cross-table) · 4 rules from natural language or suggested from
  history; shareable, versioned rule libraries.
- **Probe:** can a data owner who does not write code add a range rule in the UI? Can you write a
  rule that compares two tables?

### D5.2 Profiling
Automatic statistics: distribution, null ratio, distinct count, type consistency.
- 0 none · 1 row/column counts only · 2 per-column null %, distinct, min/max · 3 histograms,
  pattern detection (e-mail/phone format), sample values, scheduled profiling · 4 trend
  comparison, cross-column correlation, candidate-key detection, profiles feed rule suggestions.
- **Probe:** profile the same table twice a day apart. Is there a time series? Is a pattern
  metric (e.g. "% matching regex") available?

### D5.3 Test execution and scheduling
When and how rules run.
- 0 stored only · 1 manual, one-off · 2 periodic (cron) · 3 triggered by a pipeline step or load
  event, with a **circuit breaker** that stops the pipeline on failure · 4 orchestrated through an
  API with external orchestrators, incremental execution, and run history and performance visible.
- **Probe:** can a failed test stop the next pipeline task? Can a test run only on the new
  partition?

### D5.4 Quality incidents and SLA
Turning a failed rule into a tracked incident with an owner and a deadline.
- 0 pass/fail only · 1 failures listed as a log, no status or assignee · 2 each failure becomes an
  incident record with status and assignee · 3 priority/severity and a **resolution-time target
  (SLA)**; automatic notification or escalation when the SLA is exceeded · 4 two-way integration
  with an external incident/service-management tool; automatic root-cause and recurrence reports.
- **Probe:** create an incident, set it to "assigned", wait past a target. Does anything happen?

### D5.5 Quality score and reporting
Rolling rule results up into a score.
- 0 raw results only · 1 one pass/fail ratio per asset · 2 multi-dimensional score per asset
  (completeness, accuracy, consistency separately) · 3 aggregated per asset, domain and
  organisation on a dashboard over time; exportable by API · 4 user-configurable dimension
  weights, business-impact weighting (critical assets), scheduled report distribution.
- **Probe:** can you see "domain X completeness over 90 days" and pull it from the API?

### D5.6 Anomaly / drift detection
Finding deviations without a fixed rule.
- 0 fixed thresholds only · 1 one fixed statistical threshold (mean ± x %) set by hand ·
  2 dynamic threshold from a moving average/stddev (no seasonality) · 3 seasonality- and
  trend-aware models on value distributions; user feedback tunes the model · 4 thresholds learned
  from past runs, new rules suggested with a stated reason, feedback tunes rule parameters.
- **Probe:** feed a weekly-seasonal series with one Monday dip. Is the dip flagged and the normal
  weekend low not?

### D5.7 External quality tool integration
Bringing in results from open-source test frameworks (Great Expectations, dbt tests, etc.).
- 0 only the product's own engine · 1 manual file upload · 2 at least one tool's results imported
  automatically by an official plugin and linked to the asset · 3 several tools through a standard
  API/format; external results are handled **in the same way** as native ones in the score and in
  incident handling · 4 two-way: catalog-defined rules are exported to the tool and run there,
  results come back; plugin architecture for your own tool.
- **Probe:** a failed dbt test. Does it show on the table page, count in the quality score and
  open an incident?

## D10 — Observability and Operations

### D10.1 Freshness monitoring
- 0 no freshness data · 1 static last-updated timestamp · 2 expected frequency set by hand, with
  a delay indicator on the asset page · 3 continuous monitoring, an automatic alert on breach, and
  a freshness history chart · 4 expected frequency learned from history with a seasonality-aware
  threshold, plus automatic downstream impact (which dashboards are affected).
- **Probe:** declare "daily by 06:00", skip a load. Is there an alert at 06:01?

### D10.2 Volume / row-count monitoring
- 0 none · 1 on-demand count, no history · 2 periodic counts on a time-series chart ·
  3 fixed or percentage-change threshold per asset with automatic alert · 4 seasonality/trend
  model with an explanation (expected range vs observed value).

### D10.3 Schema drift detection
- 0 none · 1 manual comparison of two points in time · 2 periodic scan logs added, removed and
  retyped columns · 3 automatic alert on change, with severity by change type (drop vs add) ·
  4 drift reported with downstream lineage impact; breaking changes stop the pipeline or need
  approval.
- **Probe:** drop a column at the source and re-ingest. Is the change classed as breaking?

### D10.4 Pipeline run status
- 0 not visible · 1 producing job named statically · 2 last run status and time from the
  orchestrator · 3 run history (duration, status, error) and failure notifications · 4 duration
  and resource trends with anomaly detection; one health view of the whole upstream chain.

### D10.5 Usage and popularity analytics
- 0 none · 1 catalog page views only · 2 query counts / last queried from source query logs ·
  3 per user/team breakdown; most and least used assets on a dashboard · 4 usage joined with
  lineage for "who breaks if this is removed"; sudden drops or spikes flagged.

### D10.6 Cost and resource analytics
- 0 none · 1 raw size (GB) · 2 platform cost/usage per asset, periodically · 3 cost aggregated
  per asset/domain/team over time; top-cost assets and queries · 4 cost alerts and optimisation
  suggestions (low usage + high cost) exportable by API.

## Turning levels into requirements

Use the levels as acceptance criteria. Do not use them as a wish list. For a data product with a
published SLA, a sensible minimum is:

| Capability | Minimum | Reason |
|---|---|---|
| D5.1 | 2 | Owners must be able to add simple rules without code. |
| D5.3 | 3 | A breaking rule must be able to stop publication. This is usually done in the orchestrator. |
| D5.4 | 3 | Without a resolution-time target, "SLA" is only a word. |
| D5.7 | 2 | Most teams already run GX or dbt tests. |
| D10.1 | 3 | Freshness is the SLA consumers notice first. |
| D10.3 | 3 | Schema drift is the most common silent breakage. |

Where a product stops below the minimum, close the gap outside it: a pipeline gate, an SLA timer
job, a freshness sensor. Then record in the catalog that the control exists. What the three
measured products reach is in [d5-d10-comparison.md](d5-d10-comparison.md).
