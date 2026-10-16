# Merger

Family: **Updates**. Makes an *incremental* dataset idempotent: changes are MERGEd (UPSERTed) on an immutable identity key, so replaying a change set updates rows instead of duplicating them.

## Problem
A pipeline receives a stream of changes synchronised from Kafka with the Change Data Capture pattern. A new batch job must replicate all of these changes into an existing Delta Lake table. The table has to reflect the source at any given moment, so it cannot contain duplicates.

Overwriting is not an option: each run only brings the modified or new rows, not the whole dataset. To rebuild the dataset you would first have to reconstruct the latest version of every entity.

The pattern applies when the dataset identity is **static** (row identities never change) and the dataset supports updates or inserts.

## Solution / Mechanics
1. **Define the merge key**. Use a single attribute when it is unique across the dataset (`user_id`). Otherwise use a composite key, such as `(visit_id, visit_time)` or `(type, version)` for devices.
2. **Pick the combine operation.** Data processing frameworks, table file formats and warehouses all offer `MERGE` (aka `UPSERT`), which is the best way to implement the pattern.
3. **Define the behaviour per scenario**:
   - *Insert*: the key is absent from the target, so add the row.
   - *Update*: the key exists, so apply the new version.
   - *Delete*: the trickiest case. A row that is simply missing from the increment changes nothing, so deletes must arrive as **soft deletes** (an `is_deleted` attribute). They can then be applied as a hard or a soft delete.

```
 increment (CDC)                      target table
 +-----------------------+            +------------------+
 | key  | value | is_del |  dedup per | key  | value     |
 |------+-------+--------|  key, keep |------+-----------|
 | A    | a2    | false  |  latest    | A    | a1        |
 | B    | -     | true   | ---------> | B    | b1        |
 | C    | c1    | false  |   MERGE    |                  |
 | D    | -     | true   |   ON key   |                  |
 +-----------------------+            +------------------+
   A: MATCHED, not deleted     -> UPDATE        => A a2
   B: MATCHED, deleted         -> DELETE        => (gone)
   C: NOT MATCHED, not deleted -> INSERT        => C c1
   D: NOT MATCHED, deleted     -> skip (guard)  => never inserted
```

The MERGE statement:
```sql
MERGE INTO dp.devices_output AS target USING dp.devices_input AS input
  ON target.type = input.type AND target.version = input.version
WHEN MATCHED AND input.is_deleted = true THEN DELETE
WHEN MATCHED AND input.is_deleted = false THEN UPDATE SET full_name = input.full_name
WHEN NOT MATCHED AND input.is_deleted = false THEN
  INSERT (full_name, version, type) VALUES (input.full_name, input.version, input.type)
```

The `is_deleted = false` condition on INSERT is essential. Without it, the first run (or any run on an empty target) inserts records that are already deleted, and later runs never remove them.

SQL loading idiom: `CREATE TEMPORARY TABLE changed_devices (LIKE dp.devices)`, then `COPY` the file into it, then `MERGE`.
- `LIKE` avoids declaring the schema twice and keeps the temporary table in sync with the target.
- The temporary table disappears at the end of the session.

## Consequences & trade-offs
- **Uniqueness**. This is the first and most important requirement. The provider or the generating job must define immutable attributes that identify each record. If they don't, a backfill inserts a new row instead of updating the old one, which produces inconsistent duplicates.
- **I/O**. Unlike the Fast Metadata Cleaner, Merger is a data-level pattern that works on data blocks, so it is more compute-intensive. Modern databases and table formats first look up the impacted records in the metadata layer (statistics, file pruning), which skips irrelevant files.
- **Incremental datasets with backfilling**. A replay starts from the *latest* table version, not from the state at the replayed time.
  - A consistent load:

    | Time | Increment | Output |
    |---|---|---|
    | 07:00 | A | A |
    | 08:00 | A-U, B | A-U, B |
    | 09:00 | B-D, C | A-U, C |
    | 10:00 | M, N, O | A-U, C, M, N, O |

  - Replaying from 08:00 applies `A-U, B` to the current state, which gives `A-U, B, C, M, N, O`. Consumers see B, which was deleted at 09:00, alongside M, N and O, which arrived only at 10:00. The table only becomes correct again after the last period is replayed.
  - The fix is a restore step outside the merge, which turns the pattern into the **Stateful Merger**.
- **Provider mistakes need no replay**. If the provider shipped wrong values, ask for a corrected dataset and process it as a new increment. MERGE overwrites the invalid rows, provided the row identities did not change.

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Merge key | natural ID / composite / hashed natural IDs | Only immutable attributes. Never processing time or run IDs. |
| Duplicate keys inside one increment | dedup first / let MERGE fail | Keep the latest change per key, ordered by `(change_ts, log position)`. The tie-breaker makes the result deterministic. |
| Delete handling in target | hard `DELETE` / soft `is_deleted` column | Hard delete mirrors the source. A soft column keeps an audit trail, but every reader must then filter on it. |
| Matched behaviour | `UPDATE` / `DO NOTHING` | `UPDATE` accepts corrections. `DO NOTHING` means the first write wins (insert-only feeds). |
| Offset or checkpoint commit | before / after the merge | Always after the data commit. Replaying a merged batch is harmless, losing one is not. |
| Concurrency | parallel runs / serialised | Serialise runs (`max_active_runs=1`). Concurrent MERGEs on the same files conflict or interleave. |

### When to use / when not to use
| Situation | Use Merger? | Alternative |
|---|---|---|
| CDC or incremental feed, stable keys | Yes | - |
| Full snapshot delivered every run | No | Data Overwrite / Fast Metadata Cleaner |
| Backfills must show point-in-time state | Not alone | Stateful Merger |
| No stable key exists | No | Fix the key first (Keyed Idempotency), or overwrite |
| Key-value store, streaming writer | Often unnecessary | Keyed Idempotency (a PUT is already an upsert) |
| Several sources that must land atomically | Partly | Transactional Writer around the MERGEs |

## Gotchas & best practices
- Validate the key before merging: a null key component cannot match anything and is inserted on every run.
- Dedup the increment per key. Delta raises `multiple source rows matched`, and PostgreSQL raises `MERGE command cannot affect row a second time`.
- Partition or cluster the target on the merge key, or on a correlated column (date, type), so file pruning keeps the I/O bounded.
- Run only when the increment is complete (Readiness Marker or `_SUCCESS` sensor). Merging a half-written file creates partial state.
- Deletes arriving before their insert are skipped by the guard. Make sure the source never emits "delete then insert" out of order for the same key in different batches.

| Symptom | Likely cause | Fix |
|---|---|---|
| Duplicates after a backfill | Key not unique, or mutable (e.g. `updated_at` in the key) | Rebuild with a key made only of immutable attributes |
| Deleted rows present after the first run | INSERT branch lacks `is_deleted = false` | Add the guard |
| `multiple source rows matched` | Several changes per key in one increment | Window dedup on `(change_ts, lsn)` |
| Deleted rows reappear during a replay, then vanish | Replay starts from the latest state | Stateful Merger, restore before the replay |
| MERGE slows down each day | No pruning: target not clustered on the key | Partition, Z-order or cluster, and add a selective predicate |
| Lost changes after a crash | Kafka offsets committed before the DB commit | Commit offsets after the MERGE transaction |

## Implementation notes per stack
**Spark / Delta**: [`spark_cdc_merge_soft_delete.py`](../examples/merger/spark/spark_cdc_merge_soft_delete.py). The script dedups the increment, creates an empty target on the first run, then runs the MERGE builder. With `--soft-delete-target`, the DELETE branch becomes an `UPDATE SET is_deleted = true`.
```python
w = Window.partitionBy(*key).orderBy(F.col("change_ts").desc(), F.col("change_lsn").desc())
changes = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
(DeltaTable.forPath(spark, target).alias("t")
 .merge(changes.alias("c"), "t.type = c.type AND t.version = c.version")
 .whenMatchedDelete(condition="c.is_deleted = true")
 .whenMatchedUpdate(condition="c.is_deleted = false", set={"full_name": "c.full_name"})
 .whenNotMatchedInsert(condition="c.is_deleted = false",
                       values={"type": "c.type", "version": "c.version", "full_name": "c.full_name"})
 .execute())
```

**Kafka -> PostgreSQL**: [`kafka_cdc_merge_postgres.py`](../examples/merger/kafka/kafka_cdc_merge_postgres.py). The consumer maps Debezium `op=d` to `is_deleted=true` and keeps the latest change per key. In one transaction it loads the batch into `changed_devices (LIKE dp.devices)` and runs `MERGE`. It commits the Kafka offsets only after that transaction commits.
```python
with conn:                                   # temp load + MERGE: one transaction
    with conn.cursor() as cur:
        cur.execute(CREATE_TEMP_SQL)         # ... (LIKE dp.devices) ON COMMIT DELETE ROWS
        execute_values(cur, "INSERT INTO changed_devices (...) VALUES %s", rows)
        cur.execute(MERGE_SQL)
consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in next_offsets.items()],
                asynchronous=False)
```

**Airflow**: [`merger_dag.py`](../examples/merger/airflow/merger_dag.py) implements the SQL loading idiom above.
- A FileSensor waits for the day's CSV.
- One `SQLExecuteQueryOperator` (`autocommit=False`, `split_statements=False`) runs the temp table, `COPY`, the dedup and the MERGE as a single transaction.
- A `SQLCheckOperator` then asserts that no key is duplicated.
```python
merge_devices = SQLExecuteQueryOperator(
    task_id="merge_devices", conn_id="dp_postgres",
    sql=MERGE_SQL, autocommit=False, split_statements=False)
wait_for_increment >> merge_devices >> check_no_duplicate_keys
```

## Related patterns
- [Stateful Merger](stateful_merger.md): adds a state table and a restore step, so backfills stay consistent.
- [Fast Metadata Cleaner](fast_metadata_cleaner.md) and [Data Overwrite](data_overwrite.md): simpler idempotency when the full dataset is available on each run.
- [Keyed Idempotency](keyed_idempotency.md): the same reliance on immutable keys. Relational stores implement it with MERGE.
- [Transactional Writer](transactional_writer.md): wraps several MERGEs so they commit or fail together.
- Change Data Capture / Incremental Loader: produce the increments, including the soft-delete flag.
- Windowed Deduplicator: removes duplicate changes per key before the MERGE.
