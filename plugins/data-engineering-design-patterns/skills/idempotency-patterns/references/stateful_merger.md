# Stateful Merger

Family: **Updates**. A Merger plus a **state table** that maps each execution time to the table version its MERGE produced. Before a backfill merges, the table is restored to the state that the replayed execution originally saw.

## Problem
Changes between two Delta Lake tables are synchronised with the Merger pattern. A week later an issue is found in the merged dataset, and business users ask for a backfill.

They care about consistency: the dataset must be restored to the last valid version *before* any backfill starts. The plain Merger replays on top of the latest state, so it cannot meet this demand.

## Solution / Mechanics
The workflow gains one step before the merge and one after it:

```
 +----------------------+     +------------------+     +----------------------+
 | restore the table    | --> | run the MERGE    | --> | update the state     |
 | if required          |     | statement        |     | table                |
 +----------------------+     +------------------+     +----------------------+
        ^  reads                                              | writes
        |                                                     v
        +--------------- versions(execution_time, delta_table_version) ----+
```

**State table**: `versions(execution_time STRING NOT NULL, delta_table_version INT NOT NULL)`.
- After each MERGE, read `MAX(version)` from `DESCRIBE HISTORY` and **MERGE** it into the state for the current execution time.
- It must be a MERGE because a backfill replaces an existing entry, while a normal run inserts a new one.
- Example: runs at 08:00, 09:00 and 10:00 created versions 4, 5 and 6.

**Restore step.** It runs only in backfill mode.
- If the orchestrator exposes the execution mode (backfill or normal) in its context, use that.
- Otherwise derive the mode from the state table:
  1. Read the version created by the **previous** execution. If none exists, this is the first run or a backfill of the first run. `TRUNCATE` the table and go straight to the MERGE.
  2. Compare that version with the table's latest MERGE version:
     - **Equal**: normal run, nothing to restore. In the example, the 11:00 run sees 6 = 6.
     - **Different**: backfill. Restore the table first. In the example, re-running 09:00 sees 4 vs 6.

**Walkthrough**. State: 10-05→1, 10-06→2, 10-07→3, 10-08→4.

| Run | Previous execution's version | Last MERGE | Action |
|---|---|---|---|
| 10-09 (new) | 4 (10-08) | 4 | Normal merge |
| 10-05 again | none (10-04) | 4 | TRUNCATE, then merge |
| 10-07 again | 2 (10-06) | 4 | Restore v2, merge, state becomes 10-07→5 |
| 10-08 again (follows) | 5 (10-07) | 5 | Normal merge. The workflow is back in normal mode |

**Which version to restore**. No-data commits such as compaction or OPTIMIZE also create versions. Restoring "the previous run's version" would drop them. Assuming versions increase by 1, restore `version_for_current_execution_time - 1` instead.

Compaction example:
- State: 05→5, 06→7, 07→9, 08→10.
- History: 5 MERGE, 6 COMPACTION, 7 MERGE, 8 COMPACTION, 9 MERGE, 10 MERGE.
- Backfilling 10-07 restores **8**, the compacted table. Backfilling 10-08 restores **9**.

**Stores without versioning**. Load every increment into a raw `devices_history` table with an `execution_time` column, then:
1. Detect a backfill: `SELECT COUNT(*) > 0 FROM devices_history WHERE execution_time > '{{ ts }}'`.
2. If it returns true, delete the history rows with `execution_time >= ts`. The history then holds exactly the last valid version.
3. Rebuild the main table from the history with a Windowed Deduplicator (latest per key).
4. Load the current input into the history and merge it into the rebuilt table.

## Consequences & trade-offs
- **Versioned data stores**. The Delta implementation needs a store in which every write creates a new table version (Delta, Iceberg, Hudi, warehouse time travel). Otherwise use the history-table adaptation, which costs extra storage and a full rebuild on every backfill.
- **Vacuum operations**. VACUUM removes the files that old versions point to once the retention period has passed, so those versions can no longer be restored. You can either pay for a longer retention, or accept that the pipeline cannot be backfilled beyond it.
- **Metadata operations**. Compaction (see the Compactor pattern) changes no data but still creates a version. Use the `current - 1` rule. Otherwise a restore silently skips commits made between two merges.
- **A restore is table-wide.** It rolls back *every* commit after the target version, including other writers' commits. Keep one writer per table and serialise runs.
- **Sequential replays.** Each backfilled run rewrites its state entry, so the next execution falls back into normal mode only if executions replay in order.

## Key design decisions
| Decision | Guidance |
|---|---|
| Backfill signal | Orchestrator context if it is reliable, otherwise the state-table comparison. The comparison also catches manual reruns. |
| Restore target | `version(current execution) - 1` when the execution ran before. Otherwise the previous execution's version. |
| "Previous execution" | Derive it from the schedule (`ds - 1 day`), not from "last successful run", which skips failed days. |
| Empty the table on the first-run backfill | `TRUNCATE`. On OSS Delta, use `DELETE FROM t` without a predicate: also metadata-only, because the files are only unlinked from the log. |
| State write | MERGE on `execution_time`, right after the data MERGE. Fail if any other commit landed in between. |
| Retention | `delta.deletedFileRetentionDuration` and `logRetentionDuration` of at least the backfill window you promise. |

### When to use / when not to use
| Situation | Use Stateful Merger? | Alternative |
|---|---|---|
| Incremental CDC; backfills must be point-in-time correct | Yes | - |
| Backfills rare, or a temporary inconsistency is acceptable | No | Merger |
| Full dataset available each run | No | Data Overwrite / Fast Metadata Cleaner |
| Store without time travel | Yes, history-table variant | Proxy (keep every version) if storage allows |
| Several independent writers on the target | No | Isolate the writers first. A restore would drop their commits |

## Gotchas & best practices
- Filter `DESCRIBE HISTORY` on `operation = 'MERGE'` to get the last merge version. Don't use the overall last version for the comparison.
- Check that the restore target is still in the history (`MIN(version)`) before calling `restoreToVersion`, so the run fails with a clear error. RESTORE also fails when the files were vacuumed even though the log survived.
- Set `max_active_runs=1` and `depends_on_past=True`. Restore, merge and record of two executions must never interleave.
- Every step must be safe to retry. A retried restore picks the same target, and a retried MERGE is idempotent.

| Symptom | Likely cause | Fix |
|---|---|---|
| Restore fails with missing files | VACUUM retention is shorter than the backfill horizon | Extend retention, or accept the limit and rebuild from source |
| Compaction work lost after a backfill | Restored the previous run's version | Restore `current - 1` |
| Normal run restores the table | State not updated after the last MERGE, or another job committed | Upsert the state right after the MERGE and enforce a single writer |
| Backfill of day N+1 not in normal mode | Days replayed out of order or in parallel | Serialise with `depends_on_past` |
| History-table rebuild keeps deleted rows | The dedup ignored `is_deleted` | Keep the latest change per key, then drop the deleted ones |

## Implementation notes per stack
**Spark / Delta**: [`delta_version_restore_merger.py`](../examples/stateful_merger/spark/delta_version_restore_merger.py). `decide_restore()` is pure, and its self-test covers the normal-run, backfill and compaction scenarios. Steps: `--step restore|merge|update-state|all`.
```python
if previous_job_version is None:
    return RestoreDecision("truncate")
if previous_job_version >= last_merge_version:
    return RestoreDecision("none")
target = current_execution_version - 1 if current_execution_version is not None \
    else previous_job_version
if target < earliest_available_version:
    raise RuntimeError("cannot restore beyond VACUUM / log retention")
return RestoreDecision("restore", target)
# executed as: DELETE FROM default.devices  |  DeltaTable.forName(...).restoreToVersion(target)
```

**Airflow**: [`stateful_merger_dag.py`](../examples/stateful_merger/airflow/stateful_merger_dag.py). The three tasks (restore, merge, update state) are separate `SparkSubmitOperator`s that call the same script with different `--step` values.
```python
COMMON_ARGS = ["--execution-time", "{{ ds }}",
               "--previous-execution-time", "{{ macros.ds_add(ds, -1) }}"]
restore_if_required = spark_step("restore_if_required", "restore")
merge_increment = spark_step("merge_increment", "merge", ["--input", INPUT])
update_state_table = spark_step("update_state_table", "update-state")
wait_for_increment >> restore_if_required >> merge_increment >> update_state_table
```

**Kafka -> PostgreSQL (non-versioned variant)**: [`kafka_history_table_stateful_merger.py`](../examples/stateful_merger/kafka/kafka_history_table_stateful_merger.py).
- `offsets_for_times` turns the execution window into a deterministic offset slice. Use a `LogAppendTime` topic.
- One transaction then detects a backfill, trims and rebuilds from the history, inserts the slice and MERGEs it.
```python
cur.execute("SELECT COUNT(*) > 0 FROM dp.devices_history WHERE execution_time > %s", (start,))
if cur.fetchone()[0]:
    cur.execute("DELETE FROM dp.devices_history WHERE execution_time >= %s", (start,))
    cur.execute(REBUILD_SQL)    # TRUNCATE + DISTINCT ON (type, version) latest, not deleted
execute_values(cur, "INSERT INTO dp.devices_history (...) VALUES %s", slice_rows)
cur.execute(MERGE_SQL, (start,))
```

## Related patterns
- [Merger](merger.md): the stateless base. Its backfill inconsistency motivates this pattern.
- [Data Overwrite](data_overwrite.md) / [Fast Metadata Cleaner](fast_metadata_cleaner.md): no state needed when each run carries the full dataset.
- [Proxy](proxy.md): also relies on versions, but exposes them through a view instead of restoring.
- Windowed Deduplicator: rebuilds the table from the history table in the non-versioned variant.
- Compactor: creates the no-data versions that the `current - 1` rule accounts for.
