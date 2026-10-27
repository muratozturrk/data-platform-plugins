# Ingestion Tool Selection and Requirements Evaluator

Scope: which tool writes into Iceberg for which source and requirement (Spark, Flink, Kafka-native,
managed ELT, cloud services), and a rule-based evaluator that turns requirements into a first
recommendation for an architecture review.

## Problem
An architecture review needs a consistent first answer per new data source: model, commit interval,
tools, write pattern, delivery guarantee, guardrails. Starting from a tool leads to mismatches;
starting from requirements and constraints leads to maintainable pipelines.

## Tool landscape
| Tool | Type | Iceberg integration | Best fit |
|---|---|---|---|
| Apache Spark | Distributed engine | Native runtime; `append`, `overwritePartitions`, `MERGE`, Structured Streaming with checkpoints, maintenance procedures | Batch, micro-batch, large transformations, compaction |
| Apache Flink | Stateful stream processor | Iceberg sink; checkpoints + two-phase commit; equality deletes for upserts | Low latency, event time, late data, CDC |
| Kafka Connect (Iceberg sink) | Connector framework | Commits topics to tables with exactly-once coordination | Topic-to-table without a Spark/Flink job |
| Apache NiFi | Visual data flow | `PutIceberg` in 1.19.0-1.23.1, **removed in 2.0.0**; cannot create or compact tables | Edge ingestion, routing, provenance |
| Fivetran | Managed ELT | Managed Data Lake Service, own Iceberg REST catalog, automated maintenance | SaaS/database syncs, little engineering |
| Qlik (Upsolver) | Managed streaming ingestion | Automatic compaction and partition optimization | High-volume streams, hundreds of tables |
| Airbyte | Open-source ELT | S3 Data Lake destination; append, overwrite, MOR dedupe via equality deletes; nested fields as JSON strings | Periodic connector-based syncs |
| Confluent Tableflow | Topics as tables | Append-only; Schema Registry; external catalogs only with Bring Your Own Storage; AWS | Teams on Confluent Cloud |
| Redpanda Iceberg topics | Kafka-compatible broker | Topics as Iceberg v2 tables; modes `key_value`, `value_schema_id_prefix`, `value_schema_latest`; DLQ table | Simple real-time access to topic data |
| AWS Glue / EMR, Amazon Data Firehose | Cloud | Glue: Spark-based Iceberg writes; Firehose: direct to Iceberg | AWS-standardized teams |
| Azure Data Factory | Cloud | Iceberg sink on ADLS Gen2 | Azure teams |
| Google Cloud Dataflow | Cloud | Lands Parquet; register files with Spark/Trino later | GCP teams |

## Selection considerations
| Consideration | Question |
|---|---|
| Ingestion model | Does the tool match the latency need (stream engine vs scheduled ETL)? |
| Write semantics | Upserts needed? Position or equality deletes? Append-only acceptable? |
| Catalog integration | Does it support your catalog (REST, Glue, ...)? |
| Schema evolution | Drift handling, ideally with a schema registry? |
| Transformations | Joins, enrichment, stateful logic -> Flink or Spark only |
| Operational maturity | Deployment, monitoring, scaling vs customization |
| Cloud | Fits existing cloud, IAM, governance? |

## Decision rules
| Condition | Choice | Reason |
|---|---|---|
| Large transformations, backfills, upserts, compaction | Spark | Broad API, `MERGE`, procedures |
| Event-time logic, late data, low latency, CDC at scale | Flink | Stateful streaming, exactly-once checkpoints |
| Topic data must simply become a table | Redpanda, Tableflow, Kafka Connect sink | No custom pipeline; mind append-only/cloud limits |
| Upserts from a Kafka-native tool | **Not** Tableflow | Append-only tables |
| Small team, SaaS sources | Fivetran or Airbyte, incremental syncs | Managed connectors, schema mapping |
| Locked into one cloud | Glue/EMR, ADF, Dataflow, Firehose | IAM, governance, support |
| Non-replayable source (webhooks) | Any tool, at-least-once + idempotent MERGE | Exactly-once impossible end to end |

Freshness -> model (rule thresholds):
```
< 5 s          -> OLTP store or stream state; Iceberg as analytical sink (commit floor 5 s)
<= 60 s        -> streaming (Flink/Spark), commit every max(5, freshness/2) s
<= 1 h         -> micro-batch (availableNow or processingTime), commit every min(freshness/2, 900) s
> 1 h          -> batch, bounded input + overwritePartitions
```

## Scenario archetypes
| Dominant requirement | Example | Direction | Main risk -> mitigation |
|---|---|---|---|
| Low latency | Fintech, transactions queryable within 5 s | Flink from Kafka, commits every few seconds; Redpanda option | Metadata bloat -> commit control, file sizing, monitoring |
| Throughput | Media, ~100 billion events/day | Flink with checkpointing, dedupe, partitioning; Qlik/Redpanda across many topics | File/snapshot explosion -> batched commits, compaction, expiration |
| Complex transformations | Healthcare device streams joined with history | Flink SQL/DataStream joins in flight; bucketing | Failure points, state tuning -> testing, experienced team |
| Schema evolution | Mobile analytics, frequent schema change | Schema registry; append or MOR; alerts on incompatible changes | Static-schema tools -> registry-aware tools, DLQ |
| Low operational overhead | Growth-stage, SaaS data, small team | Fivetran/Airbyte incremental syncs | Less flexibility, cost at scale -> use for routine sources |
| Cloud alignment | AWS-only team on Glue Catalog | Glue, Athena, EMR; Fivetran/Tableflow with Glue | Lock-in -> keep catalog open (REST) |

## Evaluator (pure Python, no I/O)
```python
from requirements_evaluator.evaluator import Requirements, evaluate

rec = evaluate(Requirements("cdc", freshness_s=30, events_per_day=10**7, source="kafka",
                            needs_upserts=True, cloud="aws"))
rec.model, rec.commit_interval_s   # ('streaming', 15)
rec.write_pattern                  # 'MERGE INTO per micro-batch, merge-on-read (Spark: position deletes, Flink: equality deletes)'
"Confluent Tableflow" in rec.tools # False: upserts exclude append-only Tableflow
```
Inputs are validated (`source` in kafka/database/saas/files/api, `transformations` none/light/complex,
`schema_changes`, `team`, `cloud`); unknown values raise `ValueError` instead of a confident wrong answer.
Warnings are kept separate from recommendations.

| Scenario input | Model, interval | Tools |
|---|---|---|
| fintech, 5 s, kafka | streaming, 5 s (+ "aggressive" warning) | Flink, Spark Structured Streaming, Redpanda |
| media, 300 s, 100 B/day | micro-batch, 150 s | + Qlik (Upsolver); file-sizing guardrail |
| healthcare, 60 s, complex | streaming, 30 s | Flink, Spark only; checkpoint-tuning guardrail |
| product analytics, 900 s, frequent schema change | micro-batch, 450 s | + Schema Registry; DLQ guardrail |
| SaaS, 4 h, small team | batch, 4 h | Fivetran, Airbyte; `overwritePartitions` |
| AWS database, 24 h | batch, 24 h | Spark, Airbyte, Glue, EMR (no Firehose for batch) |
| ticker, 0.5 s | streaming, 5 s | OLTP-store warning |
| webhook API, not replayable | micro-batch | at-least-once + idempotency warning |

Interactions the rules surface: needing upserts removes Tableflow from a Kafka scenario; a
non-replayable source caps the guarantee at at-least-once regardless of engine.

## Gotchas
- NiFi 2.x has no Iceberg processor: stay on 1.x or land files and let Spark/Flink commit.
- Airbyte MOR dedupe assumes in-order, consistent change emission; many APIs do not guarantee that.
- Tableflow tables are read-only for external engines; Redpanda schema modes need the Schema Registry
  wire format, plain JSON only via `key_value`.
- Most platforms combine a small set of tools per source; one tool for everything is a smell.
