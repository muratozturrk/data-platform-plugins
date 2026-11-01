# Writing Quality SLOs and Running Quality Incidents

Most of this file is **general practice**, taken from site reliability engineering and data
quality literature, and has no evidence tag. Where it says what a measured catalog does, the
statement carries a tag (K = verified in source code) and the details are in
[d5-d10-comparison.md](d5-d10-comparison.md).

## Terms

| Term | Meaning | Example |
|---|---|---|
| **SLI** (indicator) | A number computed by a query or job | Minutes since the latest `loaded_at` |
| **SLO** (objective) | A target for an SLI over a window | Freshness ≤ 120 min in 99 % of hourly checks over 28 days |
| **SLA** (agreement) | An SLO promised to a consumer, with a consequence or response duty | "If breached, owner acknowledges within 1 h and posts an ETA" |
| **Error budget** | Allowed failure in the window: `1 − SLO` | 1 % of 672 hourly checks = about 6 breaching hours per 28 days |
| **Check tier** | What a failing check does | `error` blocks · `warn` opens an incident · `info` records only |

Promise only what you measure. Keep the SLO a little stricter than the SLA, so the producer gets
a warning before the consumer is affected.

## SLO specification template

Keep this next to the dataset (in the repo or in a data contract; see `data-contracts`) and
register the result in the catalog.

```yaml
# quality-slo.yaml — one block per dataset (illustrative)
dataset: sales.orders_daily
owner: team-orders            # accountable for the SLA
consumers: [finance-reporting, demand-forecast]
window: 28d                   # compliance is measured over this window
objectives:
  - name: freshness
    sli: "minutes_since(max(loaded_at))"
    target: "<= 120"
    evaluated: hourly
    slo: 99.0                 # % of evaluations that must pass
    tier: warn
  - name: order_id_unique
    sli: "count(distinct order_id) / count(*)"
    target: "== 1.0"
    evaluated: per_load
    tier: error               # blocks publication
  - name: customer_id_complete
    sli: "1 - count_if(customer_id is null) / count(*)"
    target: ">= 0.995"
    evaluated: per_load
    tier: error
  - name: amount_valid
    sli: "count_if(amount between 0 and 1e7) / count(*)"
    target: ">= 0.999"
    evaluated: per_load
    tier: warn
  - name: daily_volume
    sli: "rows_loaded / median(rows_loaded, same weekday, last 8 weeks)"
    target: "between 0.7 and 1.3"
    evaluated: per_load
    tier: warn
  - name: revenue_reconciles
    sli: "abs(sum(amount) - ledger_total) / ledger_total"
    target: "<= 0.002"
    evaluated: daily
    tier: warn
incident:
  ack_within: 1h              # for tier=error; 4h for warn (business hours)
  resolve_within: 1d
  escalate_to: domain-data-lead
```

## SLI queries per dimension

Write these in the engine's SQL dialect. The dimension names match the ones catalogs use
(OpenMetadata's test dimensions are Completeness, Accuracy, Consistency, Validity, Uniqueness,
Integrity and SQL, `testDefinition.json:108-120`, K).

```sql
-- Freshness (minutes)
SELECT EXTRACT(EPOCH FROM (now() - max(loaded_at))) / 60 AS freshness_min FROM sales.orders_daily;

-- Completeness of a required column, on the newest partition only
SELECT 1 - AVG(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS completeness
FROM sales.orders_daily WHERE load_date = :run_date;

-- Uniqueness of the key
SELECT COUNT(DISTINCT order_id) * 1.0 / COUNT(*) AS uniqueness
FROM sales.orders_daily WHERE load_date = :run_date;

-- Validity (domain rule)
SELECT AVG(CASE WHEN amount BETWEEN 0 AND 1e7 THEN 1 ELSE 0 END) AS validity
FROM sales.orders_daily WHERE load_date = :run_date;

-- Volume vs same weekday baseline
WITH hist AS (
  SELECT load_date, COUNT(*) AS n FROM sales.orders_daily
  WHERE load_date >= :run_date - INTERVAL '56 days'
    AND EXTRACT(DOW FROM load_date) = EXTRACT(DOW FROM DATE :run_date)
  GROUP BY load_date)
SELECT (SELECT n FROM hist WHERE load_date = :run_date)
     / PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n) AS volume_ratio
FROM hist WHERE load_date < :run_date;
```

Accuracy needs a trusted reference: a ledger total, a master record, or a manually verified
sample. Without one, you are measuring validity, not accuracy. Say so in the SLO.

## Where checks run

| Placement | Use for | Strength | Gap to close |
|---|---|---|---|
| **Inline gate** (orchestrator task between load and publish) | `error` tier: keys, required columns, schema | Bad data never reaches consumers | Must write results to the catalog yourself |
| **Write-audit-publish** (load into staging/branch, check, then swap) | Tables with many consumers | Atomic: consumers see old or new, never half-bad | Needs a table format or engine that supports it |
| **Scheduled suite** | `warn` tier, slow checks, reconciliation | Cheap, does not block loads | Detection delay = schedule interval |
| **Catalog monitoring** | Volume and schema drift alerts, freshness display | Consumers see status on the asset page | Measured catalogs do not stop pipelines (D5.3 ≤ 2) |

None of the three measured catalogs offers a load-triggered test with a pipeline stop. In
OpenMetadata, tests run as a `TestSuite` ingestion pipeline on a cron schedule
(`ingestionPipeline.json:14`, K). DataHub OSS does not execute tests (D5.3 = 0, K). The gate
belongs in the orchestrator.

### Gate outline (orchestrator-agnostic)

```text
load_to_staging  ->  run_checks(tier=error)  --fail-->  stop; open incident (sev from matrix)
                              |
                            pass
                              v
                     run_checks(tier=warn)    --fail-->  open incident, continue
                              v
                     publish (swap/merge)  ->  push all results to catalog  ->  emit "published" event
```

Tools already have these tiers. Great Expectations has `critical`/`warning`/`info`; DataHub's GX
plugin maps them to HIGH/MEDIUM/LOW (`gx-plugin/.../common.py:190-206`, K). dbt source freshness
has `warn_after`/`error_after`; DataHub ingests these as assertions (`dbt_core.py:354`, K).

## Freshness needs special care

Freshness is the SLA consumers notice first, and the one catalogs handle worst:

- OpenMetadata 1.13.3 shows a last-updated time (`lifeCycle.updated`) but has **no freshness
  test** among its 25 built-in test definitions. `tableDataToBeFresh` appears only in a 1.12.0
  migration script. A data contract's `refreshFrequency`/`maxLatency` is not evaluated. (K)
- DataHub OSS records the last update (`Operation` aspect) and shows dbt freshness results. The
  native freshness monitor is Cloud-only. (K)

Practical pattern: compute freshness in a scheduler sensor or a small job every N minutes. Compare
it to a per-dataset expected arrival time (for example "by 06:00 local, Mon–Fri"). Write the
result to the catalog as a custom test result or assertion. Alert on breach. Expected-arrival
rules beat "max age" rules for batch data that legitimately does not change at weekends.

## Incident lifecycle

A vendor-neutral state machine. The measured products cover the first part:

```text
 detected -> new -> acknowledged -> assigned -> investigating -> mitigated -> resolved -> reviewed
               \______________________ duplicate / false positive ______________/
```

| Step | What must be recorded | OM 1.13.3 (K) | DataHub v1.7.0 OSS (K) |
|---|---|---|---|
| Detected / new | Failed check, asset, time, tier | `New` | `ACTIVE` |
| Acknowledged | Who, when (starts response timer) | `Ack` | stage `TRIAGE` |
| Assigned | Assignee | `Assigned` | `assignees` |
| Investigating | Notes, suspected cause | thread/task | stage `INVESTIGATION`, `WORK_IN_PROGRESS` |
| Resolved | Reason, comment, by whom | `Resolved` + reason `FalsePositive / MissingData / Duplicates / OutOfBounds / Other` | `RESOLVED`, stage `FIXED` / `NO_ACTION_REQUIRED` |
| Severity | Impact × urgency | `Severity1–5`, auto-classified (see below) | `priority` 0–3 (CRITICAL–LOW) |
| Due date / SLA timer | Ack and resolve deadlines | **none** | **none** |
| Escalation | Who is next if the timer runs out | **none** | **none** |

Sources: `testCaseResolutionStatus.json:14,26`, `resolved.json:13`; `IncidentStatus.pdl:19-57`,
`IncidentInfo.pdl:70-93`.

**About OpenMetadata's automatic severity.** `LogisticRegressionIncidentSeverityClassifier`
predicts Severity1–5 from four *asset* features: tier, has-owner, follower count and votes. The
code comment says the coefficients came from synthetic data. If the asset has no tier, it returns
no severity (`LogisticRegressionIncidentSeverityClassifier.java:21-27,98-105`, K). It measures
how important the asset is, not how bad the failure is. Use it as a hint at most.

### Severity matrix (general practice)

| | Many consumers / regulatory or financial report | One team | Exploratory only |
|---|---|---|---|
| **Wrong data published** | SEV1: ack 15 min, resolve/mitigate 4 h | SEV2: ack 1 h, 1 d | SEV3: ack 1 bd, 5 bd |
| **Data late, correct** | SEV2 | SEV3 | SEV4: next sprint |
| **Warn-tier deviation** | SEV3 | SEV4 | record only |

("bd" = business day.) Start the clock at detection, not at the time the bad data was loaded.

### Closing the SLA-timer gap

Neither product stores a due date on an incident, so add it outside:

1. When an incident is opened, store `ack_due` and `resolve_due` computed from the matrix, in your
   incident tool or as structured fields on a linked ticket.
2. A periodic job (every 5–15 min) lists open incidents whose due time has passed and escalates.
   It can page, reassign or post to the domain channel.
3. Optional two-way sync with an IT service-management tool. This is rubric D5.4 level 4, which no
   measured product reaches.
4. After resolution, write a short review for SEV1/SEV2 incidents. Add a check for the root cause
   if none existed.

## Reporting SLO compliance

- Per dataset: % of evaluations passing per objective over the window, error budget left, number
  of incidents by severity, median time to acknowledge and to resolve.
- Per domain: the same figures weighted by asset criticality. OM computes per-dimension scores and
  exposes `/dataQualityReport` (`TestSuiteResource.java:506`, K), but dimension weights are not
  user-configurable (D5.5 = 3, not 4). Do criticality weighting in your report layer.
- When a budget is used up, freeze non-urgent changes to that dataset's pipeline until it
  recovers. This is the usual error-budget policy.
