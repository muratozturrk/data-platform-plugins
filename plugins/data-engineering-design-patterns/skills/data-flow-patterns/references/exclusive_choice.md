# Exclusive Choice

Category: Fan-Out · Scope: one parent -> exactly one of several children

## Problem
The migration done with the Parallel Split works. Now the pipeline must run the new job version
only from January 1, 2024; backfills for earlier days must still run the previous job. You want
this without creating a new pipeline, to keep the full execution history.

## Solution / Mechanics
Still two child tasks, but only one runs at a time. Declare at least two
downstream processes as in the Parallel Split, then add a **condition evaluator** task one step
before branching that decides which path to follow.

Built-in support:
- Apache Airflow -- branch operator (`BranchPythonOperator` / `@task.branch`);
- Azure Data Factory -- If Condition activity; AWS Step Functions -- Choice state;
- processing layer -- plain `if`/`else` or `switch` of the job's language.

Step by step:
1. Keep the common parent (sensor, read).
2. Add a router whose output is the id of the branch to run; base it on **metadata** (logical
   date, config, schema), not on scanning data.
3. Declare all branches downstream of the router; the orchestrator skips the others.
4. If branches merge later, the merge needs a rule that tolerates skipped parents.
5. In jobs, hide variants behind a Factory or dedicated writer classes; or let the orchestrator
   pick a different entry point so outcomes stay visible.

```
                                   +--> load_job_trigger_delta  (logical_date >= migration)
 wait_for_input --> format_router -+                                   \
                                   +--> load_job_trigger_csv  (skipped) -+--> publish (NONE_FAILED_MIN_ONE_SUCCESS)
```

Processing-layer flavors:
- **external configuration** -- `--output_type delta|csv` + Factory that returns the right
  SparkSession and writer; dedicated classes per type avoid repeating
  conditions in each method;
- **dataset characteristics** -- route by the input schema (>= 3 fields -> new location, else
  legacy) using metadata, which is much cheaper than data.

## Consequences & trade-offs
- **Complexity factory**: branching is flexible; many if/else branches that merge
  or split further make pipelines barely readable. Test: explain the pipeline to an imaginary
  newcomer (rubber duck); many follow-up questions = too complex.
- **Hidden logic**: conditional statements inside a job that produce different
  datasets or write to different stores get forgotten over time. Prefer evaluating the
  condition in the orchestrator and calling different entry points.
- **Heavy conditions**: a condition that processes data adds execution time;
  prefer metadata-based conditions. If data is unavoidable, process it once and incrementally.
- **Alternative to branching**: instead of more routes, create another pipeline
  bounded by `start_date`/`end_date` for time validity.

## Key design decisions
1. Where the condition is evaluated: orchestrator (visible) vs job (cheaper, hidden).
2. Condition input: logical date, configuration, schema/metadata, data (last resort).
3. Default branch vs explicit error branch for unknown values.
4. How a later merge treats the skipped branch.
5. Branching vs separate time-bounded pipelines.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Behavior switches at a cut-over date, backfills need old path | Yes | Two DAGs with `start_date`/`end_date` |
| Output type depends on a parameter | Yes (Factory) | Separate jobs per type |
| Route by input schema version | Yes, metadata-based | Schema Migrator |
| Several outputs needed at once | No | Parallel Split |
| Per-record routing in a stream | Processing-layer variant | Kafka content router |
| More than a handful of branches | Reconsider | Config/strategy objects, separate pipelines |

## Gotchas & best practices
- Skipped branches propagate `skipped` downstream: merge tasks need
  `trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS` (or `NONE_FAILED`).
- Use the **logical date** (`logical_date`, formerly `execution_date`), never `now()`, so
  backfills take the historical path.
- Router must be deterministic and side-effect free (it is re-run on retries).
- Route unknown values to an explicit error/DLQ branch, not to a silent default.
- `provide_context=True` (an Airflow 1 idiom) is unnecessary in Airflow 2.
- Router return values must match existing `task_id`s exactly (typos fail at runtime only).

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Merge task always skipped | Default `all_success` after a branch | `NONE_FAILED_MIN_ONE_SUCCESS` |
| Backfill for 2023 ran the new job | Condition on wall-clock time | Use `logical_date` |
| `Invalid task id` from branch task | Router returns non-existent id | Return constants matching task ids |
| Nobody knows job writes to two stores | In-job branching | Orchestrator chooses entry point |
| Router slow, scans whole table | Data-based condition | Metadata (schema, partition list, tags) |
| Records silently dropped | Unknown route mapped to nothing | DLQ / error branch |

## Implementation notes per stack
**Airflow** -- `examples/exclusive_choice/airflow/exclusive_choice_dag.py`:
`@task.branch` router on `logical_date`, two branches, merge with
`NONE_FAILED_MIN_ONE_SUCCESS`.
```python
@task.branch(task_id="format_router")
def get_output_format_route() -> str:
    if get_current_context()["logical_date"] >= MIGRATION_DATE:
        return "load_job_trigger_delta"
    return "load_job_trigger_csv"
wait_for_input() >> router >> [load_delta(), load_csv()] >> publish_dataset_ready()
```
**Spark** -- `examples/exclusive_choice/spark/exclusive_choice_job.py`: `OutputType` enum argument, factory returning `DeltaWriter`/`CsvWriter` (dedicated
classes instead of repeated if/else), optional `--route-by-schema`.
```python
def output_generation_factory(output_type: OutputType):
    return {OutputType.delta_lake: DeltaWriter, OutputType.csv: CsvWriter}[output_type]()
def route_by_schema(df, output_dir):
    return f"{output_dir}/devices_schema_changed" if len(df.schema.fields) >= 3 \
        else f"{output_dir}/devices_legacy"
```
**Kafka** -- `examples/exclusive_choice/kafka/content_router.py`: each record goes to exactly
one topic chosen from a header (metadata) first, payload fallback, DLQ for unknowns; write and
offset commit in one transaction.

## Related patterns
- Parallel Split -- same fan-out shape, all branches run.
- Unaligned Fan-In -- relaxed trigger rules also needed to merge after a branch.
- Isolated Sequencer -- alternative of separate, time-bounded pipelines.
- Dead-Letter -- destination for records no branch accepts.
- Schema Compatibility Enforcer / Schema Migrator -- formalize schema-driven routing.
