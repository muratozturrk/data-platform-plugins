# Passthrough Replicator

Category: data replication. One-liner: copy a dataset **as is**, metadata included, from one environment or
storage to another of the same type.

## Replication versus loading
Replication moves data between the **same type of storage** and ideally preserves all its metadata attributes
(primary keys in a database, event positions in a broker). Loading is more flexible and has no homogeneous
environment constraint. Replication's goal is a copy; real life often adds alterations (privacy), which is
the Transformation Replicator.

## Problem
The deployment process has development, staging and production. Many jobs use a reference dataset of device
parameters loaded daily in production from an external API. For a better development experience and easier
bug detection, that dataset must exist in the other environments too. But the third-party API is **not
idempotent**: the same call returns different results during the day, so replaying the loading pipeline in dev
and staging would produce different data. You need **the same data as production**.

## Solution / Mechanics
A non-idempotent provider plus required cross-environment consistency calls for the Passthrough Replicator,
set up at compute or infrastructure level.
- **Compute level**: an EL job with only two phases, read and write, copying files or rows as is. No
  transformation: even type conversion (string → date) or float rounding introduces quality issues.
- **Infrastructure level**: a *replication policy* document declaring input and output; the storage provider
  replicates records for you (S3 replication rule, MirrorMaker for Kafka).

```
 PRODUCTION (data owner, push)                                    STAGING / DEV
 ┌───────────────────────────┐   1. wait for _SUCCESS              ┌───────────────────────────┐
 │ devices/date=2024-01-01/  │──────────────────────┐              │ devices/date=2024-01-01/  │
 │   part-*.json, _SUCCESS   │   2. copy as is      ▼              │   same files, same bytes  │
 │   (_delta_log for tables) │──► replicator ──► tmp dir ──swap──► │   _REPLICATED marker      │
 └───────────────────────────┘   3. verify count/bytes             └───────────────────────────┘
     infra alternative: S3 replication rule / MirrorMaker 2 (same direction: from the owner)
```

Step by step (compute level):
1. Run in the data-owning environment, after the source is complete (readiness marker / `_SUCCESS`).
2. Choose the dumbest copy: native command > file copy (`FileUtil.copy`, `aws s3 sync`) > raw text API
   (`read.text`/`write.text`) > structured API (never for passthrough).
3. Write to a temporary location, verify (lines, or files + bytes), swap into place.
4. Copy metadata with the data: Delta `_delta_log`, Kafka key/headers/partition/order.
5. Mark the copy ready in the target environment.

## Consequences & trade-offs
- **Keep it simple**. Prefer the store's own copy command. If you must use a framework on JSON,
  use the raw **text** API, not the JSON reader/writer. If file count or file names matter, avoid distributed
  frameworks that don't let you control them (Spark decides the output layout).
- **Security and isolation**. Cross-environment jobs can destabilise the target. Use **push**,
  not pull: the environment owning the dataset copies it out and controls frequency and throughput. Push still
  shares resources: a replication job can take the last free IP of a subnet and block other jobs.
- **PII data**. If the dataset holds PII or anything that can't leave production, use the
  Transformation Replicator.
- **Latency**. Infrastructure replication adds provider-defined lag; check the SLA, especially if you
  reuse the pattern for time-sensitive scenarios.
- **Metadata**. Copying only the Parquet files of a Delta table isn't enough. For Kafka, keys and values
  aren't enough either: keep headers and the order of events within each partition.

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Level | infrastructure policy · compute job | Infrastructure when available (less code, less risk); compute for custom scheduling/verification |
| Direction | push from owner · pull from target | Push |
| File copy granularity | byte copy (files mode) · line copy (text mode) | Byte copy for table formats and when names/counts matter |
| Kafka replication | MirrorMaker 2 · Spark foreachBatch | MM2 for whole topics/clusters; Spark when you need custom filtering of topics or verification |
| Kafka topic names | `DefaultReplicationPolicy` (prefix) · `IdentityReplicationPolicy` | Identity for one-way env copies; never with active/active flows (no cycle detection) |
| Target partitions | same count as source | Required to keep the per-partition order with the same partition numbers |
| Isolation | dedicated pool/queue, quotas | Cap concurrency so the copy can't starve production |

## When to use / when not
| Situation | Use Passthrough? | Alternative |
|---|---|---|
| Prod reference data needed in dev/staging, loader not replayable | Yes | - |
| Kafka topic mirrored to another cluster (env copy, DR) | Yes (MM2) | - |
| Dataset contains PII or regulated attributes | No | Transformation Replicator |
| Target is a different storage type / needs reshaping | No | Full or Incremental Loader (ETL) |
| Consumers need typed, cleaned data | No | Downstream transformation jobs |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Staging table unreadable / "not a Delta table" | Only Parquet files copied, `_delta_log` missing | Copy the whole directory (files mode) |
| Timestamps/decimals differ from prod | Structured reader + writer re-serialised values | Raw text or byte copy |
| File count differs, tests on file names fail | Spark repartitioned the output | Files mode / native copy |
| Kafka replica out of order per key | Different partition counts, `minPartitions`, no offset sort | Same partition count, write `partition` column, `sortWithinPartitions("offset")` |
| Kafka headers lost | `includeHeaders` not enabled on the source | `includeHeaders=true` and select the `headers` column |
| Topics renamed `source.xxx` on target | MM2 default replication policy | `replication.policy.class=...IdentityReplicationPolicy` |
| Prod jobs slowed during the copy | Pull from the target or unbounded parallelism | Push from prod, dedicated pool, throughput caps |
| Stale files remain in target | Copy without delete semantics | Temp dir + swap, or `aws s3 sync --delete` |

## Implementation notes per stack

### Spark
`examples/passthrough_replicator/spark/spark_passthrough_file_replicator.py`:
```python
def replicate_text(spark, source, tmp):
    lines = spark.read.text(source)              # no JSON parsing
    lines.write.mode("overwrite").text(tmp)
...
fs.copy_tree(source, tmp)                        # files mode: FileUtil.copy, _delta_log included
if not summaries_match(fs.summary(source), fs.summary(tmp)): raise RuntimeError(...)
```
`choose_mode()` refuses text mode when the source has a `_delta_log`/`metadata` directory.

`examples/passthrough_replicator/spark/spark_passthrough_kafka_replicator.py`:
```python
events_to_replicate = input_data_stream.selectExpr("key", "value", "partition", "headers", "offset")
def write_sorted_events(events, batch_id):
    (events.sortWithinPartitions("offset", ascending=True).drop("offset")
     .write.format("kafka").option("kafka.bootstrap.servers", target_bootstrap)
     .option("topic", target_topic).option("includeHeaders", "true").save())
```
It checks equal partition counts first and uses `failOnDataLoss=true`.

### Kafka (infrastructure)
`examples/passthrough_replicator/kafka/mirrormaker2_passthrough.properties` + `register_mirrormaker2.py`:
MirrorSource (data), MirrorCheckpoint (consumer offset translation), MirrorHeartbeat connectors;
`replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy`; no SMTs.

### Files (infrastructure)
```hcl
resource "aws_s3_bucket_replication_configuration" "replication" {
  role   = aws_iam_role.replication.arn
  bucket = aws_s3_bucket.devices_production.id
  rule { id = "devices"  status = "Enabled"
         destination { bucket = aws_s3_bucket.devices_staging.arn  storage_class = "STANDARD" } }
}
```
S3 replication only copies new objects after the rule exists; use S3 Batch Replication for existing ones.

### Airflow
`examples/passthrough_replicator/airflow/passthrough_replicator_dag.py`: lives in the **prod** Airflow;
`FileSensor` on `date={{ ds }}/_SUCCESS` → one `replicate_to_<env>` SparkSubmit per environment in pool
`cross_env_replication` → `mark_replicated_<env>` written last.

## Related patterns
- **Transformation Replicator** - the same copy with PII removed or altered in between.
- **Full Loader** - same EL mechanics without the exact-copy/metadata requirement.
- **Readiness Marker** - source side: copy only complete datasets; target side: announce the copy.
- **Compactor** - replicating a small-files dataset copies the problem; compact at the source first.
