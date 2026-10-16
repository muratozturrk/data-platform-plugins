# Unaligned Fan-In

Category: Fan-In · Scope: many parents -> one child with relaxed outcome condition

## Problem
The hourly visits aggregation built with the Aligned Fan-In has worked for weeks, but it has no
proper way to manage failed tasks. A few times an hour wasn't processed correctly, so the
aggregated views were not produced for downstream consumers at all. After a meeting you agree it
is better to release even a partial dataset and fill the gaps later; you need to evolve the
Aligned Fan-In pipeline accordingly.

## Solution / Mechanics
Relax the dependency on the parents' outcome: the child can run even when some parents don't
succeed. Two scenarios:
- **some parents succeed, failures acceptable** -- run the child anyway on partial input,
  which creates a partial dataset;
- **all parents fail** -- schedule a task on the failure criteria instead (fallback or
  error-management task).

Implementation: use the trigger conditions of the orchestrator (Airflow `trigger_rule`, e.g.
`ALL_DONE`); in less declarative environments evaluate the parents' outcomes explicitly.

Step by step:
1. Start from the Aligned Fan-In graph (branches + merge).
2. Set the merge task's trigger rule to run after all parents finished regardless of outcome.
3. In the merge, compute **completeness from the data** (which branches actually landed).
4. Annotate the output: `is_approximate` flag, companion completeness table/metadata tags, or
   notification.
5. Decide exposure: publish partial data flagged, or keep it private until 100% complete.
6. When a gap is filled, replay the branch and the merge; the merge overwrites its output.

```
 wait_00 -> load_00 --(ok)-----+
 wait_01 -> load_01 --(failed)-+--(ALL_DONE)--> generate_cube --> visits_cube (is_approximate=true)
 ...                           |                           \--> completeness: 23/24 = 95.8%
 wait_23 -> load_23 --(ok)-----+
 failures -> on_failure_callback (alert), not extra graph branches
```

Step Functions variant: a detector lambda lists partitions, a
Map state runs a processor per partition returning a boolean, and a table-creator lambda sets
`is_partial=True` if any result is `False`.

## Consequences & trade-offs
Shares all Aligned Fan-In drawbacks (spikes, skew, overhead, complexity) plus:
- **Readability**: two kinds of downstream tasks (one for all-success, one for failures) make
  the flow confusing and impossible to understand without reading code. Prefer the
  orchestrator's dedicated hooks: Airflow `on_failure_callback`, Step Functions `Catch`.
- **Hidden rule**: the trigger rule lives in code and isn't visible in the graph.
- **Partial data**: consumers must know the dataset is incomplete or they'll make wrong
  assumptions. Options: completeness companion table (12/24 = 50%), metadata layer (object
  tags), notifications (email), or keep the partial dataset private (internal table/folder)
  until completeness is 100%.

## Key design decisions
1. Which trigger rule: `ALL_DONE` (any outcome), `ONE_SUCCESS`, `NONE_FAILED_MIN_ONE_SUCCESS`,
   `ALL_FAILED` (fallback task)?
2. Minimum completeness to publish (any, 50%, all-but-one) -- a business decision.
3. How completeness is communicated: row flag, companion table, tags, events.
4. Public partial output vs private staging until complete.
5. How gaps get filled (automatic retry window, manual clear, late-data integration).

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Consumers prefer partial, flagged data over nothing | Yes | -- |
| Output must be exact (billing, finance close) | No | Aligned Fan-In |
| All parents failed -> need a fallback path | Yes (`ALL_FAILED`/callback) | Dead-Letter for records |
| Partial data acceptable only internally | Yes, private target | Audit-Write-Audit-Publish |
| Optional enrichment branch that may be skipped | Yes, `NONE_FAILED_MIN_ONE_SUCCESS` | Exclusive Choice |
| Streaming merge where one source is late | Processing-layer variant | Watermark + outer join |

## Gotchas & best practices
- `ALL_DONE` also fires on `upstream_failed`/`skipped` parents: never assume a parent loaded.
- Document the rule in `doc_md` or the task name (e.g. `generate_cube_partial_ok`).
- Keep the merge idempotent so the gap-fill replay overwrites rather than adds.
- Readiness markers downstream should carry completeness, not just existence.
- Keep failures loud: callbacks/alerts even though the pipeline continues.
- Clear failed branch + merge together when filling a gap (`airflow tasks clear -t "load_hourly_visits_13|generate_cube"`).

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Cube marked complete with missing hours | Completeness assumed from task states | Count landed partitions in the data |
| Dashboard numbers drop silently | Consumers unaware of partial data | `is_approximate` + completeness table/tags |
| Gap-filled day has duplicate rows | Branch/merge append on replay | Delete+insert per hour and per day |
| Graph shows success/failure branches tangled | Separate failure tasks | `on_failure_callback` / `Catch` |
| Merge runs with zero data | No-parent-succeeded case unhandled | Fail when nothing landed; fallback task |
| Downstream picks up private partial output | Same path for partial and final | Separate private location |

## Implementation notes per stack
**Airflow** -- `examples/unaligned_fan_in/airflow/unaligned_fan_in_dag.py`: same 24-branch loop, `generate_cube` with `TriggerRule.ALL_DONE`, `is_approximate`
subquery, completeness companion table, `on_failure_callback` alerting; hourly loads are
delete+COPY so retries don't duplicate.
```python
generate_cube = SQLExecuteQueryOperator(task_id="generate_cube", conn_id=CONN_ID,
    trigger_rule=TriggerRule.ALL_DONE,
    doc_md="Runs with **ALL_DONE**: builds a partial cube when some hours failed.", sql="""
    ... (SELECT CASE WHEN hours_subquery.all_hours = 24 THEN false ELSE true END FROM
          (SELECT COUNT(DISTINCT execution_time_hour_id) AS all_hours FROM dp.visits_raw
           WHERE execution_time_id = '{{ ds }}') AS hours_subquery) ...""")
```
**Spark** -- `examples/unaligned_fan_in/spark/unaligned_fan_in_job.py`: the less declarative
flavor -- lists available hours, fails if none, unions what exists, writes `is_approximate`
and `_COMPLETENESS.json`, and `--private-if-partial` routes partial output to a private path.
```python
available = [h for h in EXPECTED_HOURS if os.path.exists(f"{args.partials}/date={args.date}/hour={h}")]
if not available:
    raise RuntimeError("No parent succeeded -- let the fallback/error task handle it")
completeness = len(available) / len(EXPECTED_HOURS)
target_root = args.private if (completeness < 1.0 and args.private_if_partial) else args.output
```
**Kafka** -- `examples/unaligned_fan_in/kafka/README.md`: window/outer join emits when the
window closes, completeness fields on records, late corrections as upserts on compacted topics.

## Related patterns
- Aligned Fan-In -- the strict version this pattern relaxes.
- Exclusive Choice -- merge after branching also needs a relaxed rule (`NONE_FAILED_MIN_ONE_SUCCESS`).
- Dead-Letter -- record-level counterpart for tolerating failures.
- Static / Dynamic Late Data Integrator -- fill the gaps after publishing partial data.
- Audit-Write-Audit-Publish -- keep partial output private until checks pass.
- Metadata Decorator -- carry completeness as metadata/tags.
