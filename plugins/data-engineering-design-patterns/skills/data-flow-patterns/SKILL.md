---
name: data-flow-patterns
description: Data flow design patterns for coordinating pipeline steps and pipelines - Local Sequencer, Isolated Sequencer, Aligned Fan-In, Unaligned Fan-In, Parallel Split, Exclusive Choice, Single Runner and Concurrent Runner - with Airflow, Spark and Kafka examples. Use when designing DAG/task boundaries, chaining tasks or cross-team pipeline dependencies (ExternalTaskSensor, Datasets, readiness markers), merging or branching tasks (trigger rules, BranchPythonOperator, unionByName, persist), handling partial results, or setting run concurrency (max_active_runs, depends_on_past, pools, backfills).
---

# Data Flow Design Patterns

Data flow patterns design and coordinate all the steps required to generate a dataset: chaining
tasks, creating parallel or exclusive branches, merging them back, managing dependencies between
physically separated pipelines, and controlling how many runs of a pipeline execute at once.
They operate on two levels -- the **data orchestration layer** (tasks and pipelines in Airflow,
Step Functions, Data Factory, EMR steps) and the **data processing layer** (how business logic is
organized inside a Spark/SQL job). The four families are *sequence*, *fan-in*, *fan-out* and
*orchestration*.

## When to Use This Skill

- Splitting a monolithic job into restartable steps, or deciding task vs in-job boundaries.
- Connecting pipelines owned by different teams (sensors, markers, Airflow Datasets, ExternalTaskSensor/Marker, TriggerDagRun).
- Merging many parallel branches (hourly partitions, multiple sources) into one result, strictly or with partial data.
- Writing one dataset to several outputs or running only one of several branches (migrations, cut-over dates).
- Choosing `max_active_runs`, `depends_on_past`, pools, EMR `StepConcurrencyLevel` for incremental vs independent pipelines.
- Reviewing DAGs for readability, scheduling skew, blocked runs, resource spikes or shared-state races.

## Pattern Selection Guide

| Problem / symptom | Pattern | Reference |
|---|---|---|
| Big job always restarts from scratch; retries recompute costly successful steps | Local Sequencer | [local_sequencer.md](references/local_sequencer.md) |
| Your output feeds another team's pipeline; can't merge them into one | Isolated Sequencer | [isolated_sequencer.md](references/isolated_sequencer.md) |
| One step needs ALL of N parallel branches (e.g. 24 hourly loads -> daily aggregate) | Aligned Fan-In | [aligned_fan_in.md](references/aligned_fan_in.md) |
| One failed branch blocks the whole output; consumers accept partial data | Unaligned Fan-In | [unaligned_fan_in.md](references/unaligned_fan_in.md) |
| Same input must feed several independent outputs/teams (dual write during migration) | Parallel Split | [parallel_split.md](references/parallel_split.md) |
| Only one of several branches should run (by date, config, schema) | Exclusive Choice | [exclusive_choice.md](references/exclusive_choice.md) |
| Run N depends on run N-1 (incremental, sessionization); concurrent runs corrupt results | Single Runner | [single_runner.md](references/single_runner.md) |
| Independent runs queue behind a slow one; backfills are too slow | Concurrent Runner | [concurrent_runner.md](references/concurrent_runner.md) |

## Patterns

### Local Sequencer
Decompose one big component into smaller steps run one after another within the same pipeline
or job, ordered by dataset dependencies (B after A if B reads A's output).
- **Use when**: a job is hard to read/maintain, fails often, or retries recompute costly steps.
- **Mechanic**: Airflow `sensor >> load >> expose`; EMR steps with `ActionOnFailure`; in Spark,
  chained DataFrames with a materialized intermediate after the most expensive step.
- **Gotchas**: pick **restart** and **transaction** boundaries (keep processing + state update
  together); too many tiny tasks add scheduler overhead; non-Airflow runners may need explicit
  stop-on-failure config to avoid exposing partial data.
- Ref: [references/local_sequencer.md](references/local_sequencer.md) · Code: [examples/local_sequencer/](examples/local_sequencer/)

### Isolated Sequencer
Sequence physically separated pipelines (usually different teams) as producer -> consumer.
- **Use when**: organizational boundaries prevent one pipeline, or one pipeline gets unreadable.
- **Mechanic**: *data-based* (producer writes data + readiness marker, consumer waits; loose
  coupling) or *task-based* (ExternalTaskMarker/Sensor, TriggerDagRun; explicit, tight coupling).
- **Gotchas**: task-based breaks on consumer renames and needs aligned schedules; data-based
  dependencies are invisible without lineage; the real fix for drift is team communication.
- Ref: [references/isolated_sequencer.md](references/isolated_sequencer.md) · Code: [examples/isolated_sequencer/](examples/isolated_sequencer/)

### Aligned Fan-In
Many parent branches merge into one child that runs only if **all** parents succeed.
- **Use when**: the result needs complete input but you want smaller, parallel, individually
  replayable units (24 hourly loads -> daily trends).
- **Mechanic**: loop-generated branches feeding a common task (`all_success`); in processing,
  UNION (vertical, `unionByName`) or JOIN (horizontal).
- **Gotchas**: infrastructure spikes (cap with `max_active_tasks`/pools), scheduling skew (child
  waits for the slowest parent), position-based UNION silently mixing columns.
- Ref: [references/aligned_fan_in.md](references/aligned_fan_in.md) · Code: [examples/aligned_fan_in/](examples/aligned_fan_in/)

### Unaligned Fan-In
Relax the parents' outcome condition: the child runs on partial success, or a fallback runs when
all failed.
- **Use when**: releasing partial data now and filling gaps later beats publishing nothing.
- **Mechanic**: Airflow `trigger_rule=ALL_DONE`/`ONE_SUCCESS`...; explicit outcome evaluation in
  Step Functions/Lambda; flag output with `is_approximate` + completeness table/tags.
- **Gotchas**: the rule is hidden from the graph (document it, prefer `on_failure_callback`
  over extra failure branches); consumers must be told completeness or keep partial data private.
- Ref: [references/unaligned_fan_in.md](references/unaligned_fan_in.md) · Code: [examples/unaligned_fan_in/](examples/unaligned_fan_in/)

### Parallel Split
One parent feeds two or more isolated children running in parallel.
- **Use when**: the same dataset feeds several outputs/teams (e.g. Delta + legacy CSV during a migration).
- **Mechanic**: branches declared after the common task; in a job, read once and `persist()`,
  then write each branch; make writes retry-safe (Delta `txnAppId`/`txnVersion`).
- **Gotchas**: the slowest/failing branch blocks the next time-dependent run (extract it with an
  Isolated Sequencer); branches in one job share hardware (split jobs for CPU- vs memory-heavy work).
- Ref: [references/parallel_split.md](references/parallel_split.md) · Code: [examples/parallel_split/](examples/parallel_split/)

### Exclusive Choice
A condition evaluator after the common task chooses exactly one downstream branch.
- **Use when**: behavior depends on logical date (cut-overs), configuration or input schema.
- **Mechanic**: Airflow `@task.branch`/BranchPythonOperator, ADF If Condition, Step Functions
  Choice; in jobs, if/else behind a Factory or metadata-based routing.
- **Gotchas**: branch explosion ("complexity factory"), hidden in-job outcomes (prefer separate
  entry points chosen by the orchestrator), data-scanning conditions; merge tasks need
  `NONE_FAILED_MIN_ONE_SUCCESS`.
- Ref: [references/exclusive_choice.md](references/exclusive_choice.md) · Code: [examples/exclusive_choice/](examples/exclusive_choice/)

### Single Runner
At most one execution of a pipeline at a time.
- **Use when**: runs are incremental/stateful and depend on the previous run's output.
- **Mechanic**: Airflow `max_active_runs=1` + `depends_on_past=True`; EMR `StepConcurrencyLevel=1`;
  ADF `Concurrency=1` (queues runs, 429 when full); else previous-run marker + lease lock
  (TTL + heartbeat, stale-lock takeover after a crashed run).
- **Gotchas**: slow sequential backfills (backfill only needed steps), accumulating latency when
  runs get slower than the schedule, a failure halts the chain.
- Ref: [references/single_runner.md](references/single_runner.md) · Code: [examples/single_runner/](examples/single_runner/)

### Concurrent Runner
Allow several runs of the same pipeline simultaneously.
- **Use when**: runs process independent datasets (ingestion, partition-scoped jobs) and you need
  catch-up/backfill speed.
- **Mechanic**: `max_active_runs > 1`, `depends_on_past=False`; bound with workload management
  (pools, queues, quotas).
- **Gotchas**: resource starvation in multitenant orchestrators; shared-state races (shared
  pointers, late-data integrators); output ordering not guaranteed.
- Ref: [references/concurrent_runner.md](references/concurrent_runner.md) · Code: [examples/concurrent_runner/](examples/concurrent_runner/)

## Cross-cutting Best Practices

- Draw boundaries by **restart/backfill granularity** and **transaction scope**, not by code size.
- Make every task and branch **idempotent** (partition overwrite, MERGE, Delta txn options) -- all
  these patterns rely on safe retries and replays.
- Prefer behavior the graph shows: explicit dependencies, documented trigger rules, separate
  entry points over hidden in-job branching.
- Prefer **metadata-based** conditions and markers (cheap) over data scans.
- Write readiness markers/events **after** the data commit; include completeness/schema info.
- Use `mode="reschedule"` sensors, timeouts and `failed_states` so waits fail loudly.
- Cap parallelism with pools/queues and think platform-wide before raising concurrency.
- Add lineage (OpenLineage) when pipelines depend on each other through data.
