# Static Joiner

> Category: data enrichment, enrichment dataset at rest. Batch and streaming.

## Problem

Business stakeholders want to understand how a user's registration date relates to their
day-to-day activity. The raw visits dataset has no user context: it only carries `user_id`.
The context lives in a **static user reference dataset**. Raw events are good at
time and space attributes but rarely carry extended context such as the last connection date
or a profile score. The job has to bring the reference attributes to each visit.

"Static" means the enrichment dataset is at rest and evolves much more slowly than the events:
a dimension table, a lookup table, or data behind an API.

## Solution / Mechanics

1. **Pick the join attributes** on both sides, e.g. `visits.user_id = users.id`, or a composite
   key `(device_type, device_version)`.
2. **Decide whether the join is time-sensitive.** If the reference changes and you need the
   value valid at a given time, model it as a slowly changing dimension:
   - **SCD type 2**: one table with validity dates. The current row has an empty or far-future
     end date (`'9999-12-31'`).
   - **SCD type 4**: a current-values table plus a history table. Same join query, different
     storage.
3. **Express the join in SQL or the DataFrame API.** It works in data warehouses and in
   processing frameworks, and for both batch and streaming (stream-to-batch join).
4. **If the reference sits behind an API**, either call it from the job (real-time API
   enrichment) or **materialize** it into a table in a pre-processing step and join that table
. Materializing into an SCD table is the idempotent choice.
5. **Call APIs in bulk**: buffer rows, request the distinct missing keys once, cache the answers.
   The network call is usually the most expensive part of the enrichment.

```
 Keyed join (batch or stream-to-static)        SCD2 time-sensitive join

 visits (moving) ----+                          visits v           users u (SCD2)
                     |  ON user_id = id         user_id=1 -------> id=1 a@ 06-01..06-21
 users (at rest) ----+--> enriched visits       ds=2024-06-30       id=1 b@ 06-21..07-05  <- match
   broadcast to every                                               id=1 c@ 07-05..9999
   task, no shuffle                             AND ds BETWEEN start_date AND end_date

 API reference, two options:
   real-time:     process --(HTTP, bulk)--> API            replays see today's answers
   materialized:  sync API -> SCD table -> process (JOIN)  replays see the same answers
```

SCD2 query. `NOW()` fits an ad hoc query; pipelines should use the
orchestrator's execution time:

```sql
SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email
FROM dp.visits v JOIN dp.users u ON u.id = v.user_id
  AND '{{ ds }}' BETWEEN u.start_date AND u.end_date;
```

## Consequences & trade-offs

| Consequence | What happens | Mitigation |
| --- | --- | --- |
| Late data and consistency | A user changes the profile and visits right after. The visit is processed before the change arrives, so it gets the old value or no match | Batch: wait for the reference with the Readiness Marker. Streaming: switch to the Dynamic Joiner |
| Idempotency | A backfill joins against today's reference, so the output differs from the first run | Time-based query on an SCD table with the run's execution date. If the provider cannot answer time-based queries, copy the data into your own SCD store first |
| External API | APIs rarely support "as of" queries | Write every API answer into your own SCD table |
| Separate lifecycles in streaming | The stream does not wait for the reference refresh: join misses, or a join against an empty table during a full rewrite | Store the reference in a table format with atomic commits (Delta, Iceberg); avoid raw JSON/CSV rewrites |
| API network cost | One request per record makes the job network-bound | Bulk requests over a buffer of distinct keys, per-task cache |

Cost: a broadcast copies the reference to every executor, so its size is bounded by executor
memory. A larger reference falls back to a shuffle join (see Distributed Aggregator).

## Key design decisions

- **Join type**: `left` by default. The primary dataset is the source of truth: keep unmatched
  rows with NULLs and a `_ref_miss` flag instead of dropping them.
- **Time reference for SCD joins**: event time (value valid when the event happened) or
  execution date (value valid for the run). Both are replayable; `NOW()` is not.
- **Interval semantics**: SQL `BETWEEN` is inclusive on both ends. If one row's `end_date`
  equals the next row's `start_date`, a boundary timestamp matches two rows. Use a half-open
  interval (`>= start AND < end`) or non-touching dates.
- **Reference uniqueness**: one row per key (keyed join) or per `(key, start_date)` (SCD2).
  Check it before the join, since duplicates multiply primary rows.
- **Projection**: carry only the join key and needed columns into the broadcast.
- **Refresh in streaming**: reload the reference on a cadence or on a version change, not on
  every micro-batch. Log the reference version used with each run or batch.

## When to use / when not

| Situation | Static Joiner? | Alternative |
| --- | --- | --- |
| Reference is a dimension or lookup table that changes slowly | Yes | none |
| Value must be the one valid at event time or run time | Yes, SCD2/SCD4 variant | none |
| Reference is behind an API and replays must be reproducible | Yes, materialize the API first | none |
| Reference is a stream with frequent changes (CDC of user profiles) | No | Dynamic Joiner |
| Both datasets are huge and share a partitioning key | Shuffle or bucketed join | Distributed / Local Aggregator ideas |
| Enrichment must be exactly right even for late reference updates | Partially | Add late data handling |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Output has more rows than the input | Duplicate reference keys or overlapping SCD intervals | Uniqueness check on the reference, assert `output_rows == primary_rows` |
| Backfill output differs from the original run | `NOW()` or "latest" reference in the join | Join on `{{ ds }}` or event time against an SCD table |
| Spike of NULL enrichment in streaming | Reference rewritten non-atomically, or reference lags the stream | Table format with atomic commits; monitor miss rate per batch |
| Executors OOM on the join | Reference too big for broadcast | Project columns, raise the limit carefully, or use a shuffle join |
| Job is slow and the API rate-limits it | One API call per row | Buffer and send distinct keys in bulk, cache per task, partition by key |
| Streaming job never sees reference updates | Reference DataFrame cached forever | Reload when the Delta version changes |
| Boundary events match two SCD rows | Inclusive `BETWEEN` on touching intervals | Half-open interval |

## Implementation notes per stack

**Spark batch**: `examples/static_joiner/spark/spark_static_joiner_batch.py`. `--mode scd2` builds
the time-sensitive condition. Writes use `replaceWhere`, and the per-date audit is overwritten:

```python
as_of = "p.event_time" if cfg.as_of == "event_time" else f"CAST('{processing_date}' AS TIMESTAMP)"
cond = F.expr(f"p.user_id = r.id AND {as_of} >= r.start_date AND {as_of} < r.end_date")
joined = p.join(F.broadcast(r), cond, "left")
if output_rows != primary_rows:
    raise ValueError("overlapping SCD intervals or duplicate keys")
```

**Spark streaming**: `examples/static_joiner/spark/spark_streaming_static_joiner.py`. The
stream-to-batch join is a plain join between the streaming and the static DataFrame:

```python
devices = spark.read.format("delta").load(devices_path)
visits = spark.readStream.format("kafka")...
visits.join(devices, [visits.device_type == devices.type,
                      visits.device_version == devices.version], "left_outer")
```

The example does it in `foreachBatch` with a cached reference that is reloaded only when the
Delta version changes.

**Spark + API**: `examples/static_joiner/spark/spark_api_bulk_enricher.py`.
Each task buffers 100 rows and requests only the distinct uncached IPs:

```python
missing = sorted({r["ip"] for r in self.buffer} - self.enriched_ips.keys())
mapped = self.fetch(missing)            # GET /geolocation/fetch?ips=a,b,c
self.enriched_ips.update({ip: mapped.get(ip, UNKNOWN) for ip in missing})
```

**Kafka**: `examples/static_joiner/kafka/kafka_static_joiner_consumer.py`. The reference is a
compacted topic loaded into a dict and refreshed by a background thread under a lock.
Tombstones delete keys. The consumer commits only after `producer.flush()`.

**Airflow**: `examples/static_joiner/airflow/static_joiner_dag.py`. Two `FileSensor`s (visits
and reference readiness), a freshness warning read from `_delta_log`, the SCD2 job with
`--as-of execution_date`, then audit validation of the row count and miss rate.

**Warehouse**: the same SQL works in PostgreSQL, BigQuery or Redshift. For SCD4, join the
history table for past dates and the current table for today.

## Related patterns

- **Dynamic Joiner**: use it when the enrichment side is also a stream and latency differs.
- **Readiness Marker**: makes a batch job wait for the reference partition.
- **Change Data Capture**: the usual source of reference changes, and the trigger to move
  to the Dynamic Joiner.
- **Late data patterns**: neither joiner guarantees 100% matches when data is late.
- **Idempotency patterns**: SCD joins on the execution date and `replaceWhere` writes keep
  backfills deterministic.
- **Wrapper**: keeps the enriched attributes apart from the raw record when that matters.
- **Distributed Aggregator**: shuffle join when the reference is too large to broadcast.
