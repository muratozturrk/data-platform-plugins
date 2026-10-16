# In-Place Overwriter

Category: Data Removal · Layer: storage rewrite

## Problem
You inherited a legacy system: terabytes in time-based horizontal partitions, no personal
data strategy. It is still widely used and must comply with new privacy rules requiring
personal data removal on user request. There is no budget to re-layout the data.

## Solution / Mechanics
Rewrite the existing storage without the removed entities. The mechanics depend on the store:

1. **Native in-place delete** (Delta Lake, Iceberg, databases): `DELETE ... WHERE user_id IN (...)`,
   then reclaim old data blocks (`VACUUM`, snapshot expiry). With time travel, a delete that
   doesn't reclaim blocks leaves the PII readable.
2. **Flat files without DELETE** (JSON, CSV): simulate it.
   - Read the whole dataset, filter out removed users, write the survivors to a **staging** area.
   - Keep staging private until the job fully succeeds.
   - **Promote** staging to the public location in a separate step.
3. **Compacted Kafka topic** with key-based records:
   - Read the topic up to the current end offsets.
   - For every record of a removed entity, produce `(key, null)`; compaction keeps only the latest
     value per key, so earlier PII versions disappear.

```
 removal requests (batched)
          |
          v
  +-----------------+   filter out users    +-----------+  validate   +-----------+
  | public dataset  | --------------------> |  staging  | ----------> |  promote  |
  | s3://b/output/  |   (read as text)      | s3://b/stg|  no leaks,  | rm output |
  +-----------------+                       +-----------+  not empty  | mv stg -> |
          ^                                                           |  output   |
          +-----------------------------------------------------------+-----------+
     a failed/retried rewrite never touches output; a failed promotion can be re-run from staging
```

Deletion vectors: table formats either write a small side file listing removed
rows (readers filter at read time; cheap writes) or rewrite files without them (heavy writes,
simple reads). Both still need `VACUUM`/expiry to physically drop the original files.

Retention shortcut: if retention is shorter than the legal delay, expiry acts as the
removal. Validate with the CDO and legal before relying on it.

## Consequences & trade-offs
- **I/O overhead**: reading and rewriting everything; storage can nearly double
  during the rewrite. Parquet statistics (min/max per row group) let Delta/Iceberg skip files
  with no matching users, which reduces this.
- **Cost**: 2,000 records for one user means 2,000 rows rewritten vs one with the
  Vertical Partitioner. Mitigate by **batching** requests into one run.
- **Impossible rollback**: after promotion the original is gone. A buggy removal can't
  be replayed unless you use the Proxy pattern or object-store versioning.
- **Non-atomic promotion**: object-store renames are copy + delete. A failure can leave a
  half-promoted output; keep staging until the copy completes and re-run promotion.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Legacy partitioned data, no reorganization budget | Yes | - |
| Table format with native DELETE | Yes (cheap path) | - |
| New pipeline you design now | No | Vertical Partitioner |
| Compacted topic, one key per entity | Yes (tombstones) | - |
| Append-only event topic, many events per key | No | Retention-based expiry, or rewrite into a new topic |
| Frequent single-user requests on TB data | Batch first | Weekly/daily batched run |

## Key design decisions
- Batch window: daily/weekly runs that collect all pending requests (legal delay is the bound).
- Staging location and its access control: staging holds a full copy, treat it as production PII.
- Validation before promotion: zero rows of removed users, non-empty output, row-count sanity.
- Rollback strategy: bucket versioning (with its own lifecycle/expiry!) or Proxy-based versions.
- Physical cleanup: `VACUUM` retention, Iceberg `expire_snapshots`, versioned-object expiry.

## Gotchas & best practices
- Read flat files with the **text** API and parse only the filter attribute: lines stay
  byte-identical and you avoid schema drift.
- Never `mode("overwrite")` onto the path you are reading: Spark is lazy and transactionless, a
  failure destroys data.
- Object-store versioning keeps the deleted PII as noncurrent versions: add an expiry rule.
- Pass user IDs as column expressions (`isin`) rather than string-formatted SQL.
- Run with `max_active_runs=1`; two concurrent rewrites of the same dataset lose requests.
- Kafka: the consumer must read up to end offsets captured at start, otherwise it never ends.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Output empty after a failed run | Overwrote input in place | Staging + promotion |
| Deleted rows readable via time travel | No VACUUM / snapshot expiry | Run cleanup with retention <= legal delay |
| Partial output after promotion crash | Non-atomic S3 copy | Re-run promotion from staging; delete staging last |
| Job cost explodes | One run per request | Batch requests |
| Tombstones have no effect | Topic not compacted, or key mismatch | `cleanup.policy=compact`, reuse the record's exact key bytes |
| Removed data back after restore | Versioned objects / backups not purged | Expire noncurrent versions, purge backups |

## Implementation notes per stack
**Spark** — `examples/in_place_overwriter/spark/in_place_overwriter_job.py`:
```python
raw = spark.read.text(INPUT_DIR)
with_user = raw.withColumn("user", F.from_json("value", "user_id STRING"))
survivors = with_user.filter(~F.col("user.user_id").isin(user_ids)).select("value")
survivors.write.mode("overwrite").format("text").save(STAGING_DIR)
staged = spark.read.text(STAGING_DIR).withColumn("user", F.from_json("value", "user_id STRING"))
if staged.filter(F.col("user.user_id").isin(user_ids)).count():
    raise RuntimeError("rows of removed users still in staging")
```
`delta` mode: `DeltaTable.delete(F.col("user_id").isin(ids))` + `vacuum(hours)`.

**Airflow** — `examples/in_place_overwriter/airflow/in_place_overwriter_dag.py`:
`collect_requests >> rewrite_to_staging (spark-submit, PROMOTE=false) >> validate_staging >> promote`.
```python
bucket.objects.filter(Prefix=OUTPUT_PREFIX).delete()
for obj in bucket.objects.filter(Prefix=STAGING_PREFIX):
    bucket.copy({"Bucket": BUCKET, "Key": obj.key}, OUTPUT_PREFIX + obj.key[len(STAGING_PREFIX):])
bucket.objects.filter(Prefix=STAGING_PREFIX).delete()  # only after the full copy
```

**Kafka** — `examples/in_place_overwriter/kafka/tombstone_overwriter.py`: captures watermarks,
scans from offset 0 to the end, collects keys of removed users, sends `value=None` per key.

## Related patterns
- Vertical Partitioner: cheaper removal when the layout can change.
- Proxy: keeps previous versions so a buggy removal can be rolled back.
- Fast Metadata Cleaner / Data Overwrite: the same overwrite mechanics for idempotency.
- Single Runner: avoids concurrent rewrites.
- Audit-Write-Audit-Publish: the staging + validate + promote shape.
