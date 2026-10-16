# Aligned Fan-In

Category: Fan-In · Scope: many parent branches -> one child (orchestration or processing layer)

## Problem
Your pipeline produces a daily aggregate of blog visits from raw visit events. The dataset is
partitioned by hour because that fits most use cases in the organization. Consumers only want
the full daily view, but you want to leverage the hourly partitioning so a single job doesn't
process too much data at once.

## Solution / Mechanics
The child task depends on several parents and runs only when **all** direct parents succeed.

Orchestration layer: declare separate branches merging into a common task -- e.g. 24 hourly
branches computing partial aggregates, then a merge task computing the daily output.

Processing layer: you still define branches, but also how they interact:
- **vertical alignment = UNION**: one dataset with more rows, same columns;
- **horizontal alignment = JOIN**: rows combined, extra columns, fewer rows than a UNION.

Step by step:
1. Choose the branch unit (hour, source, region) by **backfill granularity**: what should be
   replayable alone?
2. Add a context-clearing parent if the merge reads a shared staging area.
3. Generate branches (loop or static list) that each write one idempotent partition.
4. Wire all branches to the merge task with the default `all_success` rule.
5. In code, merge by name (`unionByName`) or by key (`join`), then compute the final output.
6. Cap concurrency to what the infrastructure absorbs.

```
                   +-> wait_00 -> load_00 --+
 clear_context ----+-> wait_01 -> load_01 --+--(all_success)--> generate_trends
                   +-> ...                  |
                   +-> wait_23 -> load_23 --+
 failure of load_23 => fix + replay load_23 only, then generate_trends
```

Benefits:
- **Faster feedback loop**: 24 small jobs run simultaneously and surface an error sooner than
  one day-long job that fails near the end.
- **Cheaper failure recovery**: if only the last hour fails, you replay that hour, not 24.

## Consequences & trade-offs
- **Infrastructure spikes**: 24 jobs at once. Fine with elastic capacity;
  otherwise reduce allowed concurrency, or run branches incrementally (each hourly branch runs
  hourly and only the day's last run aggregates).
- **Scheduling skew**: unbalanced parents make the child wait for the slowest
  one; trigger time = longest parent.
- **Scheduling overhead**: too granular pipelines make the orchestrator spend its
  resources scheduling and coordinating tasks -- the price of decoupling.
- **Complexity**: more decoupling = longer pipelines and reduced readability.
  Ask "What tasks belong to a single unit of execution?" and "What operations should be
  backfilled individually?"
- **Position-based UNION**: `SELECT a,b,c UNION SELECT c,b,a` combines a+c, b+b,
  c+a; use `unionByName`.

## Key design decisions
1. Branch granularity (hour vs day vs source) and how many branches run concurrently.
2. Static list (`[a, b] >> c`, explicit and readable) vs loop-generated branches.
3. UNION vs JOIN semantics in the merge; schema alignment rules.
4. Is a missing branch an error (aligned) or acceptable (switch to Unaligned Fan-In)?
5. Incremental branches (run as data arrives) vs all at once at day end.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Output needs complete input, parts are independently computable | Yes | -- |
| Partial output is acceptable / preferred to none | No | Unaligned Fan-In |
| Branches need very different hardware | Yes, branches as separate jobs | -- |
| Engine already parallelizes the work (Spark partitions) and no individual retries needed | No | One job |
| Limited, non-elastic capacity | With caps (`max_active_tasks`, pools) | Incremental hourly branches |
| Several streams merged continuously | Processing-layer variant | Stream union/join with watermarks |

## Gotchas & best practices
- Each branch overwrites its own partition (delete+insert per hour) so replays don't double count.
- Use `unionByName`; set `allowMissingColumns=True` only deliberately (it hides schema drift).
- A `clear_context` parent prevents mixing old and new loads in a shared staging table.
- Fan out where you need independent retries, not where the engine parallelizes for free.
- Put the trigger rule (even the default) in a comment/doc so readers know the merge semantics.

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Daily aggregate doubles after a replay | Branch appends without clearing its hour | Delete+insert / overwrite per hour |
| Columns contain each other's values | Position-based `union` | `unionByName` |
| Merge task starts late every day | Scheduling skew, one slow hour | Balance branches, run them incrementally |
| Cluster saturated at midnight | 24 branches start together | `max_active_tasks`, pools, incremental runs |
| Scheduler lag across the platform | Too many tiny tasks | Coarser branches (e.g. 4 x 6h) |
| Merge never runs after one failure | Aligned semantics as designed | Fix + clear the failed branch, or Unaligned Fan-In |

## Implementation notes per stack
**Airflow** -- `examples/aligned_fan_in/airflow/aligned_fan_in_dag.py`:
loop-generated sensor+loader per hour, all wired to `generate_trends` (default `all_success`),
`max_active_tasks=8` caps the spike, loaders delete their hour before `COPY`.
```python
for hour_to_load in [f"{hour:02d}" for hour in range(24)]:
    file_sensor = FileSensor(task_id=f"wait_for_{hour_to_load}", filepath=..., mode="reschedule")
    visits_loader = SQLExecuteQueryOperator(task_id=f"load_hourly_visits_{hour_to_load}",
                                            params={"hour": hour_to_load}, sql=...)
    clear_context >> file_sensor >> visits_loader >> generate_trends
```
**Spark** -- `examples/aligned_fan_in/spark/aligned_fan_in_job.py`: fails
if any hourly partial is missing, then `unionByName` (vertical) and a device `join` (horizontal).
```python
missing = [p for p in paths if not os.path.exists(p)]
if missing:
    raise RuntimeError(f"{len(missing)} hourly partials missing, e.g. {missing[0]}")
day = functools.reduce(lambda l, r: l.unionByName(r), [spark.read.parquet(p) for p in paths])
enriched = day.join(devices, "device_id", "left")
```
**Kafka** -- `examples/aligned_fan_in/kafka/README.md`: multi-topic subscription = UNION,
stream joins with watermarks = the "all parents" barrier; readiness events per hour for a
batch barrier.

## Related patterns
- Unaligned Fan-In -- relaxes the all-success condition for partial results.
- Local Sequencer -- same backfill-based boundary reasoning.
- Parallel Split -- the opposite shape (one parent, many children).
- Readiness Marker -- per-branch sensors on hourly partitions.
- Distributed Aggregator -- the merge step's computation.
