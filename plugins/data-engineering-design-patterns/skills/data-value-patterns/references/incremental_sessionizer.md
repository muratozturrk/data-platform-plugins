# Incremental Sessionizer

Category: Sessionization (batch).

## Problem
The ingestion team stores visit events in an **hourly partitioned** location. A session starts with the first visit and ends when the user sends no new visit for **two hours**. Sessions last from minutes to three hours, so one session spreads across up to three partitions. Analysts struggle because each session needs many consecutive partitions per user. Records of one session live in several consecutive partitions, so the problem belongs to the incremental processing family and builds on the Incremental Loader.

## Solution / Mechanics
Three **storage spaces**:
| Space | Visibility | Content and rules |
|---|---|---|
| Input dataset | upstream-owned | Raw partitioned events (the hourly visits). |
| Completed sessions | **public** | Finished sessions. Ongoing sessions may be written here too, but must be distinguished (`is_final` / `is_completed = false`). |
| Pending sessions | **private** | Sessions that will close in a later run. The format is free and evolves with your logic, and may carry technical fields (execution id, expiration batch) that help guarantee idempotency. |

Workflow per run:
1. **Combine** the input with the pending sessions of the **previous run**, per session entity (user, product, visit). Each entity becomes one of:
   - a **new** session (no pending session for the entity);
   - a **restored session with new data**;
   - a **restored session without new data** (it will probably expire in this run or the next).
2. **Session logic** in three states:
   - **Initialization**: a session starts, with the first event or a start event (e.g. the home page).
   - **Accumulation**: the session is live, so fold in new data (e.g. append the visited pages in order).
   - **Finalization**: a session stops, on an end event (e.g. logout) or after an inactivity period.
3. **Write**: finished sessions go to public storage, and still-open sessions go to pending storage for the next run.

```
 run H-1                        run H                                   run H+1
 pending[H-1] ───────┐
                     ├── FULL OUTER JOIN on session_id ──┐
 input/hour=H ───────┘   (new | restored+data |          │
                          restored, no data)             ├── expiration_batch_id > H  ──> pending[H] ──> (input of H+1)
                          initialize/accumulate/finalize │
                                                         └── expiration_batch_id <= H ──> sessions[H]  (public)
 cleanup first: DELETE sessions, pending WHERE execution_time_id >= H   (this and later runs)
```

Expressing the logic: a WINDOW function or GROUP BY plus a custom mapping. A programmatic API helps once rules get complex.

## Reference implementation (PostgreSQL + Airflow)
- Task graph: `[clean_previous_runs_sessions, clean_previous_runs_pending_sessions] >> generate_sessions`.
- Idempotency relies on the immutable `ds`, and deletes this **and all subsequent** executions:
```sql
DELETE FROM dp.sessions         WHERE execution_time_id >= '{{ ds }}';
DELETE FROM dp.pending_sessions WHERE execution_time_id >= '{{ ds }}';
```
- Load the input once into a session-scoped temp table, so a failing session query doesn't reload it.
- Combine: pending rows come from `execution_time_id = '{{ prev_ds }}'`. The query uses COALESCE, LEAST, GREATEST and ARRAY_CAT, and sets the expiration only when the session has new data:
```sql
CASE WHEN n.user_id IS NULL THEN p.expiration_batch_id
     ELSE '{{ macros.ds_add(ds, 2) }}' END AS expiration_batch_id
```
- Split: `expiration_batch_id != ds` goes to `pending_sessions`, and `= ds` goes to `sessions`.

## Consequences & trade-offs
| Consequence | Detail | Handling |
|---|---|---|
| Inactivity period | A longer period includes more late data, but costs more compute and storage and keeps sessions hidden longer. | Pick it from business needs: there's no balance that maximizes both. |
| Partial sessions | You can emit them for freshness, but they may change later. A fraud score can flip from "not at risk" to "risky" in the next partition. | Flag them (`is_completed: false`) so consumers can ignore them. |
| Data freshness | Batch-bound: hourly partitions give at least 1 h latency. | Use partial sessions, or switch to the **Stateful Sessionizer**. |
| Late data with event-time partitions | Late events for already-processed partitions miss their session. | Accept the loss, or reprocess (see backfilling). |
| Forward dependency / backfilling | The 09:00 session impacts 10:00, which impacts 11:00. Rerunning one partition forces rerunning **all later ones**. | Simple and costly: replay everything after the backfilled run. Cheap and complex: detect the affected entities and rerun only them in a dedicated backfill pipeline. |

## When to use / when not
| Situation | Use | Alternative |
|---|---|---|
| Sessions span consecutive batch partitions; hourly or daily latency is fine | Incremental Sessionizer | - |
| Sessions needed within seconds or minutes | No | **Stateful Sessionizer** |
| Sessions never cross a partition boundary (e.g. daily, with sessions closing at midnight) | No | Plain GROUP BY per partition |
| Heavy late data, frequent backfills of old partitions | Carefully | Reprocess windows, or a full recompute over a bounded range |
| Many small partitions (minutes) | Costly | Micro-batch streaming with state |

## Key design decisions
| Decision | Guidance |
|---|---|
| Session entity and id | Use a deterministic id: the visit id or entity + first event time. Never a UUID or wall-clock value. |
| Expiration rule | In **run ids** (`expiration_batch_id = current + N`, where N = gap / partition length) or in event time (`last_event + gap < partition_end`). Run ids are simpler and idempotent. |
| Run id | The orchestrator's immutable interval start (`ds`, `data_interval_start`). Fixed-width ISO strings sort like time. |
| Pending format | Private and free to evolve: keep timestamps per page and technical columns. The public format is a contract. |
| Partial sessions | Off by default. If on, keep `is_completed` in the public schema and document "latest row per session_id wins". |
| Empty partitions | Still run: pending sessions must expire even when no visits arrived. |

## Implementation notes per stack
**Airflow + PostgreSQL** (reference form): `examples/incremental_sessionizer/airflow/incremental_sessionizer_sql_dag.py`. It adds idempotent DDL, primary keys on `(execution_time_id, session_id)`, a single transaction with `ON COMMIT DROP` temp tables, closing-page finalization and `<=` for overdue sessions.

**Spark + Delta**: `examples/incremental_sessionizer/spark/spark_incremental_sessionizer.py`, hourly. Actions mirror the SQL version's tasks (`clean-sessions`, `clean-pending`, `generate`).
```python
joined = n.join(p, F.col("n.session_id") == F.col("p.session_id"), "full_outer")
F.when(F.col("n.session_id").isNull(), F.col("p.expiration_batch_id"))   # restored, no data
 .when(F.col("n.has_closing_page"), F.lit(run.current))                  # finalized by event
 .otherwise(F.lit(run.expiration_if_active))                             # new or extended
completed  = classified.filter(F.col("expiration_batch_id") <= run.current)
still_open = classified.filter(F.col("expiration_batch_id") >  run.current)
(df.write.format("delta").mode("overwrite")
   .option("replaceWhere", f"execution_time_id = '{run.current}'").save(path))   # retry-safe
```
Pending sessions are read with `execution_time_id = previous` only. Pages are merged with `array_sort(array_distinct(concat(...)))`.

**Airflow orchestrating Spark**: `examples/incremental_sessionizer/airflow/incremental_sessionizer_dag.py`. The graph is `wait_for_input` (readiness marker) `>> [clean x2] >> generate_sessions`, with `catchup=True`, `depends_on_past=True` and `max_active_runs=1`.
```python
wait_for_input >> [clean_sessions, clean_pending] >> generate_sessions
# backfill hour H: airflow tasks clear incremental_sessionizer_spark -s H -e <now> -y
```

**Kafka**: no batch counterpart. A broker-fed sessionizer is the Stateful Sessionizer (`../stateful_sessionizer/`).

## Gotchas & best practices
1. Read the pending sessions of exactly the previous run. "All pending" double counts after any rerun.
2. Cleanup must delete `>= current`, not `= current`, because later runs were built on this one.
3. Serialize the chain: `max_active_runs=1` + `depends_on_past=True`. A skipped run strands its pending sessions.
4. Use `<=` when splitting on expiration, so a session that is overdue (after a gap in runs) still completes instead of staying pending forever.
5. A late event for an already-completed session creates a second session with the same visit id. Detect it (anti-join with the public sessions) or include the start time in the id.
6. Deduplicate input rows before accumulation. Duplicated events inflate page arrays.
7. Keep pending storage private (separate schema or ACL). Consumers who read it couple themselves to your internals.

## Troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Sessions duplicated after a retry | Append writes without cleanup | Delete `>= current` first, and use `replaceWhere` / PK on `(execution_time_id, session_id)` |
| Sessions never complete | An empty hour was skipped, or the expiration compare is `=` while a run was missed | Always run on empty input; split on `<=` |
| Backfilled hour fixed, but later sessions are still wrong | Forward dependency not replayed | Clear the backfilled run and every later run |
| Pending storage keeps growing | Expiration never set for sessions without new data, or a bot visit id never stops | Check the CASE logic; cap the session length or the page count |
| Session starts in the "wrong" partition | Partitioned by ingestion time while the logic uses event time | Align the partitioning with the event time, or accept the drift and document it |
| Consumers see changing numbers | Partial sessions without a flag | Add `is_completed` and tell consumers to filter on it |

## Related patterns
- **Incremental Loader**: the base mechanism of processing one new partition per run.
- **Stateful Sessionizer**: the streaming counterpart, where the state store replaces pending storage.
- **Readiness Marker**: the input partition must be complete before generation starts.
- **Fast Metadata Cleaner**: the delete or overwrite-by-run-id idempotency used for both outputs.
- **Late data handling**: late events break forward-dependent sessions.
