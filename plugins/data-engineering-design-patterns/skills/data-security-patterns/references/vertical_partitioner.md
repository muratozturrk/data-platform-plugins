# Vertical Partitioner

Category: Data Removal · Layer: ingestion / storage layout

## Problem
A personal-data removal pipeline is being designed. Reviewers point out a storage overhead:
several columns are **immutable** (birthday, personal ID number, login) yet repeated in every
event. They ask that each immutable property be stored **once**, so a removal request has far
less data to delete.

## Solution / Mechanics
Split each row **vertically**: mutable/behavioral attributes go to one store, immutable and
PII attributes to another, linked by a join key such as `user_id`. Horizontal
partitioning (date partitions) groups whole records; vertical partitioning splits
one record across stores. The two combine: split parts can still be date-partitioned.

Steps:
1. Identify the columns to split off (immutable + PII) and the attribute that re-joins them.
2. Add the split to the **ingestion** job: one projection (`SELECT`) per target store.
3. Apply per-store business rules, typically **deduplication** of the PII part to one row/user.
4. Serve removals from the small PII store: one row (`DELETE` + `VACUUM`) or one tombstone.
5. Optionally apply different access and retention rules to each store.

```
                      visits_raw (Kafka)
                             |
                 foreachBatch: persist()
                  /                     \
   drop context.user                  select context.user.*, user_id
   key = visit_id                     key = user_id
        |                                   |
   visits_no_pii topic             user_context topic (compacted)
   (many rows per user,                     |
    only user_id inside)          MERGE dedup -> Delta users table
                                            |
              removal request ----> DELETE user_id = X + VACUUM
                                    tombstone (X, null) on user_context
```

## Consequences & trade-offs
- **Query performance**: it is a form of normalization. Writes shrink, but every
  reader that needs PII must join the parts, adding network shuffles that a single row avoided.
- **Querying complexity**: consumers must know attributes live elsewhere. Mitigate with
  a single entry point (view), catalog documentation, or lineage (Dataset Tracker).
- **Polyglot persistence**: when the same record lives in a search engine, a KV
  store and a warehouse, the split must happen everywhere, with one removal pipeline per store.
  Pattern: one splitting job, then dedicated consumers write each part to its store.
- **Raw data**: the pattern applies from the first transformation onward. Unsplit raw
  data needs another removal method; a short retention works only if it fits the legal delay
  and you accept less backfill history.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| New pipeline, PII repeated in every event | Yes | - |
| You control the ingestion job and can migrate consumers | Yes | - |
| Legacy terabytes with no budget to reorganize | No | In-Place Overwriter |
| Readers constantly need PII + facts together at low latency | Weigh it | Expose a materialized join view, keep removal on the PII store |
| Only raw immutable files, no transformation step | No | Short retention or In-Place Overwriter |
| Topic where many events share a key | Not with compaction | Keep it append-only; only the PII topic is compacted |

## Key design decisions
- **Split key**: stable, non-PII surrogate (`user_id`), never the email or login.
- **What goes to the PII side**: immutable attributes + anything personal; mutable behavior stays.
- **Dedup policy on the PII side**: most recent value per key (MERGE update-all) vs first-seen.
- **Removal mechanics per store**: Delta `DELETE` + `VACUUM`; Kafka tombstone on a compacted
  topic; KV store delete; search index delete-by-id.
- **Retention**: `VACUUM` retention and Kafka `delete.retention.ms` must fit the legal delay.

## Gotchas & best practices
- `DELETE` in Delta only removes rows from the current version. Old files stay readable through
  time travel until `VACUUM` removes files older than the retention.
- Tombstones work only on a topic with `cleanup.policy=compact` and **one logical record per
  key**. Compacting the visits topic would keep only the last visit per key.
- `segment.ms` and `min.cleanable.dirty.ratio` decide when compaction actually runs; the active
  segment is never compacted.
- `persist()` the micro-batch before projecting twice, and `unpersist()` afterwards.
- Filter rows without a user context before writing the non-PII side, or define a fallback.
- Batch removal requests and run them with a Single Runner so rewrites don't overlap.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Deleted user still visible with `VERSION AS OF` | No `VACUUM`, or retention longer than legal delay | `VACUUM` with retention <= legal delay |
| `VACUUM` refuses retention < 168h | Delta safety check | Disable `retentionDurationCheck` knowingly, or plan delay around 7 days |
| Tombstoned key still in topic after days | Active segment not rolled, dirty ratio not reached | Lower `segment.ms`, `min.cleanable.dirty.ratio` |
| Consumers see only last visit per key | Compaction enabled on events topic | Revert to `cleanup.policy=delete` |
| PII duplicated in Delta table | MERGE input not deduplicated per batch | `dropDuplicates(["user_id"])` before MERGE |
| Downstream copy still has PII | Copies outside the partitioned stores | Use lineage to find copies; delete there too |

## Implementation notes per stack
**Spark** — `examples/vertical_partitioner/spark/vertical_partitioner_job.py`:
```python
def split_visit_attributes(visits: DataFrame, batch_id: int) -> None:
    visits.persist()  # read once, project twice
    without_user = (visits.filter("user_id IS NOT NULL AND context.user.login IS NOT NULL")
                    .withColumn("context", F.col("context").dropFields("user"))
                    .select(F.col("visit_id").alias("key"), F.to_json(F.struct("*")).alias("value")))
    write_topic(without_user, "visits_no_pii")
    user_context = (visits.selectExpr("context.user.*", "user_id")
                    .select(F.col("user_id").alias("key"), F.to_json(F.struct("*")).alias("value")))
    write_topic(user_context, "user_context")
    visits.unpersist()
```
The `merge` mode MERGEs `user_context` into Delta; `delete <user_id>` runs `delete` + `vacuum`.

**Kafka** — `examples/vertical_partitioner/kafka/compacted_user_context.py`:
```python
config={"cleanup.policy": "compact",
        "delete.retention.ms": str(24 * 3600 * 1000),
        "segment.ms": str(6 * 3600 * 1000),
        "min.cleanable.dirty.ratio": "0.1"}
...
producer.produce(TOPIC, key=user_id.encode("utf-8"), value=None, on_delivery=on_delivery)
```

**Airflow** — `examples/vertical_partitioner/airflow/user_removal_dag.py`: daily
`collect_requests >> delete_from_delta (+VACUUM) >> send_tombstones >> mark_done`, with
`max_active_runs=1` and batched user IDs.

## Related patterns
- In-Place Overwriter: the fallback when the data can't be reorganized.
- Horizontal Partitioner: combines with vertical splits; partitions each part by date.
- Fine-Grained Accessor for Tables: the split PII store gets its own, stricter grants.
- Dataset Tracker: shows consumers where the split attributes live.
- Single Runner: serializes removal runs.
- Merger / Stateful Merger: MERGE-based dedup of the PII store.
