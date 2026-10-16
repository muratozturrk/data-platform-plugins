# Vertical Partitioner

> Category: data storage, partitioning. Batch and streaming, table formats, RDBMS, Kafka.
> The Vertical Partitioner in `data-security-patterns` is the security specialization of the same idea.

## Problem

You track website visits. The dataset has two kinds of attributes:

- **mutable**: change at every event (visit time, visited page);
- **immutable**: stay the same for the whole visit (IP address, browser, device).

Every event row repeats the immutable part. You want to store it **once per visit**.

## Solution / Mechanics

The Horizontal Partitioner moves *whole rows*. The Vertical Partitioner *splits each row* and
writes the parts to different places.

1. **Classify attributes** into groups of related columns: mutable vs immutable, or user context
   vs technical context.
2. **Pick the join key** shared by all groups, the one used to recombine them. Here: `visit_id`.
3. **Write each group to its own location** (table, directory, topic). Immutable groups are
   deduplicated on the key, so each is stored once.
4. **Apply per-group policies**: different retention, access control or storage class. That is
   hard on an undivided row.
5. **Expose the full row** where consumers need it, through a view or a materialized dataset.

```
 input row: visit_id | event_time | page | user_id | ip | login | browser | device
                 |
       +---------+-------------------+----------------------+
       v                             v                      v
 visits (mutable)          users (immutable)        technical (immutable)
 visit_id,event_time,page  visit_id,user_id,ip,     visit_id,browser,device
 one row per event         login  dedup on visit_id dedup on visit_id
       \_____________________________|______________________/
                      JOIN ON visit_id  (view / Dataset Materializer)
```

Horizontal vs vertical: horizontal applies the rule to the whole row and
moves it; vertical cuts the row and writes the pieces to separate locations, each piece carrying
the key needed to rebuild the row.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Domain split | Logically related attributes live in separate places and are hard to find | Document the split; name tables by domain; publish a catalog entry |
| Querying | Getting the full picture needs joins, unlike a horizontally partitioned dataset | Expose a view over all parts, or materialize it (Dataset Materializer) |
| Data producer | Producers can't just write a row; they implement the split and do several writes, with more network cost | Read once and `persist()` (Spark), one transaction for all writes (RDBMS), idempotent per-part writes |
| Storage | Immutable data stored once instead of per event | This is the gain; the key column is the only duplicated part |

## Key design decisions

- **Grouping rule**: by mutability (storage), by domain (readability) or by sensitivity (security,
  see data security patterns). Don't mix rules without documenting them.
- **Join key**: stable, present in every part, and unique in the immutable parts.
- **Dedup strategy** for immutable parts: `dropDuplicates(["visit_id"])`, `DISTINCT ON`,
  `ON CONFLICT DO NOTHING`, compacted topic.
- **Consistency between parts**: one transaction (RDBMS), or idempotent writes plus a readiness
  marker (lakes, Kafka) so readers don't join half-written parts.
- **Consumer access**: raw parts only, or a recombining view as the public interface.

## When to use / when not

| Situation | Use Vertical Partitioner? | Alternative |
| --- | --- | --- |
| Immutable attributes repeated on every event | Yes | - |
| Column groups need different retention or access | Yes (data security version for PII) | Fine-Grained Accessor for column masking |
| Most queries need all columns together | No, joins everywhere | Keep the row, or Denormalizer |
| Need to skip rows by time or key | Wrong axis | Horizontal Partitioner |
| Columnar format already skips unread columns | Maybe not needed for read speed | Parquet column pruning |
| Full normalized model with many entities | Goes beyond this pattern | Normalizer |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Input read twice, job slow | No caching before the multiple writes | `visits.persist()` ... `unpersist()` |
| Immutable table grows per event | Deduplication missing or on the wrong columns | Dedup on the join key; `ON CONFLICT DO NOTHING` |
| Join returns fewer rows than events | Some parts not written (partial failure) | Write all parts in one transaction or idempotently; LEFT JOIN in the view |
| "Immutable" attribute differs inside one visit | Classification wrong (e.g. IP changes mid-visit) | Re-classify; keep first/last value explicitly |
| Kafka immutable topic has duplicates | At-least-once delivery | Compacted topic keyed by `visit_id` absorbs them |
| Users can't find a column | Domain split undocumented | Docs plus a full-row view |

## Implementation notes per stack

**Spark** - `examples/vertical_partitioner/spark/spark_vertical_partitioner.py`: one read, `persist()`,
then three Delta tables built with `drop`/`dropFields` and `selectExpr` + `dropDuplicates`, and a
recombination by `visit_id`.

```python
visits.persist()
(visits.drop("user_id")
 .withColumn("context", F.col("context").dropFields("user", "technical"))
 .write.mode("overwrite").format("delta").save(f"{base_dir}/visits"))
(visits.selectExpr("visit_id", "user_id", "context.user.*").dropDuplicates(["visit_id"])
 .write.mode("overwrite").format("delta").save(f"{base_dir}/users"))
(visits.selectExpr("visit_id", "context.technical.*").dropDuplicates(["visit_id"])
 .write.mode("overwrite").format("delta").save(f"{base_dir}/technical"))
visits.unpersist()
```

**Kafka** - `examples/vertical_partitioner/kafka/kafka_vertical_splitter.py`: a consume-split-produce
loop. Mutable fields go to `visits_events`, immutable context goes to a compacted
`visits_technical` keyed by `visit_id`. One input message becomes up to two writes, which is the
producer cost described above.

```python
mutable, technical = split(json.loads(msg.value()))
key = mutable["visit_id"]
producer.produce("visits_events", key=key, value=json.dumps(mutable))
if key not in seen_visits:
    producer.produce("visits_technical", key=key, value=json.dumps(technical))
    seen_visits.add(key)
producer.flush()
consumer.commit(msg, asynchronous=False)  # at-least-once; compaction absorbs dups
```

**Airflow** - `examples/vertical_partitioner/airflow/vertical_partitioner_dag.py`: PostgreSQL
`INSERT INTO ... SELECT DISTINCT` from the JSONB `visits_all` into per-group tables (CTAS is the alternative), plus a recombining view.

```python
INSERT INTO dp.technical (visit_id, browser, browser_version, device_type)
SELECT DISTINCT ON (visit_id) visit_id, context->'technical'->>'browser',
       context->'technical'->>'browser_version', context->'technical'->>'device_type'
FROM dp.visits_all WHERE event_time::date = %(ds)s
ON CONFLICT (visit_id) DO NOTHING
```

## Related patterns

- **Horizontal Partitioner**: the row-wise counterpart; the two combine (vertical parts, each partitioned by date).
- **Vertical Partitioner (data security patterns)**: same split, applied to isolate sensitive columns for removal or access.
- **Dataset Materializer**: rebuilds the full row once for readers, instead of joining on every query.
- **Normalizer**: the generalization; vertical partitioning is a first step toward entity tables.
- **Denormalizer**: the opposite trade-off, when joins hurt more than duplication.
- **Fine-Grained Accessor** (data security patterns): column-level access control when splitting tables isn't wanted.
