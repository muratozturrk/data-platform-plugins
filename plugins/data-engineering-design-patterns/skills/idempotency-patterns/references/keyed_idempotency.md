# Keyed Idempotency

Family: **Database**. Generate the same key for the same logical record on every run. A key-based store then writes it exactly once: a second write with that key overwrites the row instead of adding one. Idempotency lives in the **key generation logic** on the processing side.

## Problem
A streaming pipeline buffers visit events per user for a time window and writes an updated session to a key-value store. When a task retries, it must not create a second session for the same visits.

## Solution / Mechanics
1. **Look for immutable properties** to build the key from. If the dataset already has a fitting unique attribute, use it. `user_id` is enough for "latest activity per user".
2. **Check the key's semantics.** A session cannot use `user_id` alone, because each new session would replace the previous one. You need `user_id` + something that marks the session start.
3. **Never anchor on a mutable value.** `user_id` + first *event time* looks right, but late data breaks it. The job emits `1_09:56`, restarts, a late `09:55` event arrives, and the same visits now get `1_09:55`, so there are two sessions.
4. **Anchor on the append time**: the time the broker or store physically wrote the record. It never changes and only grows, so late events cannot move the first one.
   - Kafka: the `timestamp` attribute with `log.message.timestamp.type=LogAppendTime`.
   - Kinesis Data Streams: `approximateArrivalTimestamp`.
   - Data at rest: an `added`/`ingestion`/`insertion` time column, often `DEFAULT now()`.
5. **Pick the first row in ascending order** on that time. `SELECT ... OVER (PARTITION BY user_id ORDER BY ingestion_time ASC, visit_time ASC)`. New rows only go to the end, so they never change the first one.
6. **Containers count too**. Name a file, partition or table after the execution time: the 20/11/2024 run writes `20_11_2024`, the next day `21_11_2024`. A replay rewrites the same object.

Session flow:
```
 Kafka visits (LogAppendTime)          stateful job                 key-based store
 ----------------------------          -----------------------      ----------------------------
 u1 page=a  event 09:57 append 09:57 ->  state(u1):                  sessions
 u1 page=b  event 09:56 append 09:58 ->   min_append = 09:57  ---->  (sha(u1|09:57), u1) [a,b]
 -- crash, restart, replay from checkpoint --
 u1 page=a  event 09:57 append 09:57 ->   min_append = 09:57
 u1 page=c  event 09:55 append 09:59 ->   (late event, later append)
                                          min_append = 09:57  ---->  (sha(u1|09:57), u1) [a,b,c]
                                                                     same key -> row overwritten
```
Partial results share the final ID, so a store can serve them early and still stay consistent.

Typical key sources:

| Source | Immutable key material |
|---|---|
| Streaming broker record | user/entity id + first broker append time; or topic + partition + offset |
| Batch row with a natural id | `sha256(concat_ws('|', source, partition_date, pk...))` |
| Table without stable ids | group key + first `ingestion_time` (ascending window) |
| Output container | logical/execution date (`visits_20_11_2024`, `dt=2024-11-20`) |

## Consequences & trade-offs
- **Database dependent**.
  - The pattern fits key-based NoSQL stores (Cassandra, ScyllaDB, HBase), where writing an existing key overwrites it.
  - In a relational database, inserting an existing primary key raises an error. The write becomes `MERGE` / `INSERT ... ON CONFLICT DO UPDATE`, which brings back the Merger's complexity.
  - Kafka has keys but is an append-only log: it does not dedup at write time. Asynchronous compaction removes old versions later, so consumers see duplicates for a while. Those duplicates share the key, though, so they are easy to identify.
- **Mutable data source**. If compaction or retention deletes the first event used for the key, a restarted job takes the next record and derives a different key. Strict idempotency breaks, but since the data really changed, a different key is logically defensible.

## Key design decisions

| Decision | Guidance |
|---|---|
| Key fields | Natural ids and append/ingestion time only. Never run ids, runtime UUIDs, `now()`, or event time used as the "first" anchor. |
| Hash function | A stable hash: SHA-256 (truncate to 63 bits for a BIGINT column). **Not** Python `hash()` on strings: it is salted per process (`PYTHONHASHSEED`), so keys change after every restart. A `hash(str(min_append_time))` key has this flaw. |
| Separator | `concat_ws('|', ...)` avoids collisions such as `"12"+"3"` vs `"1"+"23"`. |
| Timestamp source | Broker-controlled `LogAppendTime`. A producer-set `CreateTime` is riskier. Check `log.message.timestamp.type`. |
| Store write | Native upsert (Cassandra/ScyllaDB append), `MERGE`/`ON CONFLICT` (RDBMS, Delta), compacted topic (Kafka). |
| Matched behavior | Update (late data extends the record) vs do nothing (first write wins). |

## When to use / when not

| Situation | Use Keyed Idempotency? | Alternative |
|---|---|---|
| Streaming job, no orchestrator to clean up before reruns | Yes | Transactional Writer for visibility |
| Key-value / wide-column sink | Yes, natural fit | - |
| RDBMS / lakehouse sink | Yes, with MERGE / ON CONFLICT | Merger |
| Batch output per day | Yes, as a container key (file or partition named after the date) | Data Overwrite, Fast Metadata Cleaner |
| No immutable attribute exists or can be added | No | Windowed Deduplicator downstream |
| Consumers must never see partial output | Not enough on its own | Transactional Writer |

## Gotchas & best practices
- Validate the key inputs. A null component makes unrelated rows share one hash.
- Dedup on the key inside a batch before MERGE. Delta fails when several source rows match one target row.
- Kafka's `enable.idempotence=true` covers a producer's internal retries within one session. Replays after a restart still depend on the deterministic key.
- When the session state lives in memory, commit consumer offsets only up to the first record of the oldest open session. A restart must re-read the session's first event to rebuild the same key.
- Spread across Kafka partitions, a user's records have independent append clocks. Key the input topic by the entity, or take `min()` across partitions.
- A DAG-level "already processed" skip is an optimisation. It does not replace a deterministic key.

## Implementation notes per stack

### Spark
[`spark/spark_streaming_keyed_idempotency.py`](../examples/keyed_idempotency/spark/spark_streaming_keyed_idempotency.py), the session example with a stable hash:
```python
visits = (input_data.selectExpr("CAST(value AS STRING) AS value", "timestamp")
          .select(F.from_json("value", "user_id LONG, page STRING, event_time TIMESTAMP")
                  .alias("visit"), "timestamp")
          .select("visit.*", F.expr("unix_millis(timestamp)").alias("append_time"))
          .withWatermark("event_time", "10 seconds"))
sessions = visits.groupBy("user_id").applyInPandasWithState(
    map_visits_to_session, OUTPUT_SCHEMA, STATE_SCHEMA, "append",
    GroupStateTimeout.EventTimeTimeout)
# on timeout: session_id = sha256(f"{user_id}|{min_append}")[:63 bits]
```
`foreachBatch` appends into ScyllaDB/Cassandra `PRIMARY KEY(session_id, user_id)`, so a replayed micro-batch rewrites the same rows.

[`spark/spark_keyed_idempotency_batch.py`](../examples/keyed_idempotency/spark/spark_keyed_idempotency_batch.py): batch keys plus a Delta MERGE, because Delta does not overwrite on key collision:
```python
w = Window.partitionBy("user_id").orderBy(F.col("ingestion_time").asc(), F.col("visit_time").asc())
df.withColumn("_idem_key", F.sha2(F.concat_ws("|", "user_id", first_ingestion), 256))
DeltaTable.forPath(spark, out).alias("target").merge(keyed.alias("source"),
    "target._idem_key = source._idem_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Kafka
[`kafka/kafka_append_time_session_keys.py`](../examples/keyed_idempotency/kafka/kafka_append_time_session_keys.py): confluent-kafka. It refuses topics without `LogAppendTime`, builds gap-based sessions on the broker clock and writes them to a compacted topic keyed by session id:
```python
ts_type, append_ms = msg.timestamp()
if ts_type != TIMESTAMP_LOG_APPEND_TIME and not ALLOW_CREATE_TIME:
    raise RuntimeError(...)
producer.produce(SESSIONS_TOPIC, key=session_key(user_id, first_append_ms), value=...)
producer.flush(30)
consumer.commit(offsets=[TopicPartition(VISITS_TOPIC, p, o)
                         for p, o in builder.safe_offsets().items()], asynchronous=False)
```

### Airflow
[`airflow/keyed_idempotency_dag.py`](../examples/keyed_idempotency/airflow/keyed_idempotency_dag.py) covers the container variant and the RDBMS consequence:
```python
def output_name(logical_date): return f"visits_{logical_date.strftime('%d_%m_%Y')}.csv"
write_atomically(path, header, cursor)     # tmp file + os.replace, same name on every retry
INSERT INTO dp.sessions (...) SELECT md5(user_id || '|' || first_ingestion), ...
ON CONFLICT (session_id, user_id) DO UPDATE SET pages = EXCLUDED.pages
```

## Common mistakes / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Two sessions for the same visits after a restart | Key anchored on event time; a late event became "first" | Anchor on append/ingestion time |
| Every restart produces new session ids | Python `hash()` (salted) or `uuid4()` in the key | SHA-256 over the immutable fields |
| Keys change after replays from an old offset | Retention/compaction deleted the session's first event | Accept it (data changed), or retain the input longer than the replay window |
| `duplicate key value violates unique constraint` | RDBMS sink with plain INSERT | `INSERT ... ON CONFLICT DO UPDATE` or MERGE |
| Consumers see several versions of a session | Kafka compaction is asynchronous | Keep the latest per key downstream; tune `min.cleanable.dirty.ratio` |
| Append times jump backwards | Topic uses `CreateTime` set by producers | Switch the topic to `LogAppendTime` |
| Delta MERGE fails with multiple matches | Duplicates of a key in the source batch | `dropDuplicates([key])` before MERGE |

## Related patterns
- **Merger**: the write path on stores that reject duplicate keys (RDBMS, Delta).
- **Transactional Writer**: the other database-level option. It hides partial output, but by itself it does not survive reruns.
- **Windowed Deduplicator**: consumer-side cleanup when duplicates with the same key still reach readers (Kafka before compaction).
- **Stateful Sessionizer**: the session-building logic these keys are attached to.
- **Data Overwrite**: the container variant (one file or partition per date) is an overwrite keyed by the date.
