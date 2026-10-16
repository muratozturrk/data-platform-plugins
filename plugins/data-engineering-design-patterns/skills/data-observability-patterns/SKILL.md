---
name: data-observability-patterns
description: Data observability design patterns (detectors and lineage trackers) - Flow Interruption Detector, Skew Detector, Lag Detector, SLA Misses Detector, Dataset Tracker and Fine-Grained Tracker - with Spark, Kafka and Airflow examples. Use when monitoring pipelines for missing or stale data, data freshness alerts, volume/partition skew, consumer lag, SLA misses, OpenLineage/Marquez lineage, column-level or row-level lineage, or when asking "why did no data arrive", "is my streaming job falling behind", or "who produces this dataset".
---

# Data Observability Patterns

Data quality patterns protect a dataset against the rules you defined. They do not
tell you that a job silently stopped writing, processed half a dataset, fell behind its
producer, or finished too late. They also do not tell you which team to contact when the
problem is upstream. Observability patterns fill these gaps with two pillars:
**detection** (data detectors and time detectors that monitor and alert) and
**tracking** (data lineage at dataset, column and row level). They are data engineering
work, not "something ops does".

## When to Use This Skill
- A pipeline "succeeds" but downstream reports missing or stale data.
- Batch jobs sometimes process partial or abnormally large inputs.
- Kafka partitions or table partitions are unbalanced.
- A streaming consumer must keep up with its producer, and you need lag metrics.
- A job has a completion deadline (SLA) that consumers depend on.
- You need to know the upstream and downstream dependencies (teams, tables, topics) of a dataset.
- You need to explain which source columns build a column, or which job version wrote a row.
- Designing Prometheus/Grafana alerts, Airflow SLAs, or OpenLineage/Marquez integration.

## Pattern Selection Guide

| Problem / symptom | Pattern | Reference |
|-------------------|---------|-----------|
| Job runs but no new data lands; consumers notice before you do | Flow Interruption Detector | [references/flow_interruption_detector.md](references/flow_interruption_detector.md) |
| Run processed a half-empty (or 3x bigger) dataset; partitions unbalanced | Skew Detector | [references/skew_detector.md](references/skew_detector.md) |
| Streaming consumer is slower than the producer; delivery gets slower | Lag Detector | [references/lag_detector.md](references/lag_detector.md) |
| Job must finish within N minutes, or per-record latency must stay under X | SLA Misses Detector | [references/sla_misses_detector.md](references/sla_misses_detector.md) |
| "Which team/table/topic does this dataset come from?" | Dataset Tracker | [references/dataset_tracker.md](references/dataset_tracker.md) |
| "Which input columns build this column?" / "Which job wrote this row?" | Fine-Grained Tracker | [references/fine_grained_tracker.md](references/fine_grained_tracker.md) |

## Patterns

### Flow Interruption Detector (data detector)
Detects **data unavailability**: no new data for longer than an accepted threshold.
- Streaming: *continuous* (alert on one empty period) or *irregular* (alert when N
  consecutive periods are empty, which avoids alarm fatigue).
- Batch / data at rest: freshness from the **metadata** layer (commit or modification time,
  cheapest), the **data** layer (modification column or row-count deltas), or the
  **storage** layer (last file written).
- Gotchas: thresholds based on past volume break on campaigns; metadata timestamps also
  move on schema changes; compaction writes files without adding data.
- See [reference](references/flow_interruption_detector.md) and [examples](examples/flow_interruption_detector/).

### Skew Detector (data detector)
Guards against processing incomplete or abnormal volumes. Steps: define the comparison
window, set a tolerance (for example ±50%), compute the skew: a window-to-window
`current/previous` ratio, or `STDDEV(x)/AVG(x)` over partitions.
- Put it in the first Audit step of AWAP.
- Gotchas: seasonality and marketing campaigns (coordinate with the business, add
  exceptions); **fatality loop** (compare with the last *successful* run, not just the
  previous one).
- See [reference](references/skew_detector.md) and [examples](examples/skew_detector/).

### Lag Detector (time detector)
Measures how far a consumer is behind: `lag = last_available_unit - last_processed_unit`.
The unit is the Kafka offset or append time, the Delta/Iceberg version, or the partition time.
- Partitioned stores: report **MAX** (worst case) and **P90/P95** (overall). Never the
  average (10, 5, 30, 2, 3, 5, 3 gives avg 8 but P90 18).
- Gotchas: a high MAX lag can be a hot partition (fix the write distribution); Spark keeps
  offsets in its checkpoint, not in the consumer group.
- See [reference](references/lag_detector.md) and [examples](examples/lag_detector/).

### SLA Misses Detector (time detector)
Compares the processing time with the maximum allowed time: batch `end - start`,
micro-batch per iteration, per-record `write - read` aggregated with MAX and percentiles.
- Keep the **processing-time** SLA and the **event-time** (end-to-end) SLA separate. Late
  data breaks the second without any fault in your job.
- It complements lag but does not replace it: a throttled job meets its SLA while lag grows.
- Gotcha: the Airflow 2.x `sla` counts from the DAG run start, not the task start. It is
  removed in 3.0, and Deadline Alerts arrive in 3.1.
- See [reference](references/sla_misses_detector.md) and [examples](examples/sla_misses_detector/).

### Dataset Tracker (lineage)
Builds the dependency tree of tables, topics and folders, with the owning teams. It can be
**managed** (Unity Catalog, Dataplex, Purview) or **custom**: capture inputs and outputs at
the orchestration or database (query-log) layer, send them to a lineage service
(OpenLineage -> Marquez), then visualize.
- Gotchas: vendor lock gives partial graphs across clouds or open source stores; custom
  tasks need manual input/output declaration; dataset naming must be consistent across tools.
- See [reference](references/dataset_tracker.md) and [examples](examples/dataset_tracker/).

### Fine-Grained Tracker (lineage)
Lineage below the dataset level. **Column level**: walk the execution plan or SQL tree from
each output column to its source columns (OpenLineage `columnLineage`, Unity Catalog
`column_lineage`). **Row level**: decorate each row with `job_name`, `job_version`,
`batch_version` and `parent_lineage` (headers or columns).
- Gotchas: UDFs and map functions are opaque to plan-based lineage; row lineage needs its
  own query layer; keep the history of column definitions (evolution).
- See [reference](references/fine_grained_tracker.md) and [examples](examples/fine_grained_tracker/).

## Cross-cutting Best Practices
- Monitor **outputs and data**, not only job liveness. A green job can still be a broken pipeline.
- Alert on percentiles plus MAX. Averages hide problems.
- Every threshold needs an owner and a review cadence. Seasonality turns static thresholds
  into noise. Agree on exception calendars with the business.
- Prefer cheap metadata signals first (commit logs, catalog stats, watermarks), and full
  data scans last.
- Push metrics from inside jobs (listener + Pushgateway) when external tools cannot see
  internal state (for example Spark checkpoints).
- Never let a failed run become a baseline or a "last success" marker.
- Detectors tell you *that* something is wrong. Trackers tell you *whom* to talk to. Deploy both.
- Name datasets consistently (namespace per system, then per team) so lineage from Spark,
  Airflow and custom emitters joins into one graph.
