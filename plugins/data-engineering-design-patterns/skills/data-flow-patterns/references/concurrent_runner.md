# Concurrent Runner

Category: Orchestration · Scope: several simultaneous runs of one pipeline

## Problem
You're on the data ingestion team bringing data from external at-rest sources to the internal
database as soon as possible. Ingestion runs every 30 minutes to 1 hour, but sometimes a run
takes longer; with a Single Runner every subsequent delivery is delayed. The loaded datasets
don't depend on each other, so you question whether sequential execution is required.

## Solution / Mechanics
Independent datasets can be ingested in any order, so relax the concurrency constraint: define
a concurrency higher than 1. The orchestrator then picks the next pipeline
execution available for scheduling as long as running instances stay below the allowed maximum.
"With great power comes great responsibility": balance the level against other pipelines on
the same infrastructure.

Step by step:
1. Verify independence: run N reads nothing run N-1 writes, and no shared mutable state.
2. Make every run write a run-scoped output (partition = logical date/hour), overwritten on retry.
3. Set run concurrency (`max_active_runs > 1`) and keep `depends_on_past=False` unless a
   specific task needs it.
4. Bound total pressure with workload management (pools, queues, quotas) and per-source limits.
5. Size the level for backfills, not just steady state.

```
 max_active_runs=3, depends_on_past=False
 t ->  run 10:00 [==========]           (slow delivery)
       run 10:30    [====]
       run 11:00         [===]
       run 11:30              [====]    <- starts as soon as a slot frees, not after 10:00 ends
 pool "ingestion" (10 slots) caps all team DAGs together
```

## Consequences & trade-offs
- **Resource starvation**: in a multitenant orchestrator, several pipelines with
  high concurrency backfilling at once can leave the scheduler without capacity for others.
  Handle it with workload management: allocate compute capacity to a group of users so a team
  can't exceed its threshold whatever concurrency it sets. Serverless orchestrators have high
  limits (AWS Step Functions: up to 10,000 parallel child workflow executions).
- **Shared state**: concurrent, nondeterministic runs on a shared component cause
  side effects -- e.g. the Dynamic Late Data Integrator could trigger backfilling several times
  or not at all for some execution dates.
- **Ordering**: completion order isn't the schedule order; consumers must not assume "latest
  partition finished = all earlier finished".
- **Target pressure**: N parallel loads hit the source/target database N times.
- **Flexibility differs by tool**: Airflow lets you combine concurrency with
  task-level `depends_on_past`; Azure Data Factory supports concurrency and trigger-based
  dependency but not task-based dependency.

## Key design decisions
1. Concurrency level: steady-state catch-up need vs platform capacity and source limits.
2. Task-level exceptions: tasks that still need `depends_on_past=True` -- they
   stop the pipeline when their past run failed.
3. Workload management: pools/queues per team, per source system.
4. How outputs stay independent (partition overwrite, keyed upserts).
5. How consumers learn completeness when runs finish out of order (per-partition markers).

Concurrency knobs per tool:
| Tool | Run concurrency | Workload cap |
|---|---|---|
| Airflow | `max_active_runs`, `max_active_tasks` | pools, `max_active_tis_per_dag`, queues |
| AWS EMR | `StepConcurrencyLevel` (1-256) | YARN queues, instance fleets |
| Azure Data Factory | pipeline `Concurrency` | integration runtime capacity |
| AWS Step Functions | Map `MaxConcurrency` | account quotas |
| Kafka consumers | group members (<= partitions) | partitions, quotas per client |

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Independent periodic ingestion, deliveries sometimes slow | Yes | -- |
| Backfill of partition-scoped, stateless logic | Yes | -- |
| Run N needs run N-1's output | No | Single Runner |
| Shared "last processed" pointer updated by every run | No (until removed) | Compare-and-set, Single Runner |
| Shared multitenant scheduler with tight capacity | With pools/quotas | Lower level, serverless orchestrator |
| Streaming consumers on independent keys | Yes | Consumer group sized to partitions |

## Gotchas & best practices
- Write run-scoped outputs (`.../hour={{ ts_nodash }}`) and overwrite them; no global "latest"
  pointer updated by every run (or update it with compare-and-set).
- Bound external pressure separately (pool slots for the source DB, API rate limits).
- Raise concurrency for backfills only if the target store can absorb it.
- `depends_on_past=True` on any task partially serializes the pipeline again.
- Per-partition readiness markers instead of a single "dataset updated" flag.
- Kafka analog: group concurrency is capped by partition count; ordering only per partition.

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Other teams' DAGs stop being scheduled | Big backfill with high concurrency | Pools / workload management |
| Same backfill triggered twice | Shared state (late-data integrator) | Single Runner for that DAG, or locking |
| Rows of two runs mixed | Runs write a shared staging table | Run-scoped staging, partition overwrite |
| Source DB overloaded | N runs x M parallel queries | Pool slots per source, rate limits |
| Consumers read a hole in the data | Later run finished before an earlier one | Per-partition markers, completeness checks |
| Extra Kafka workers idle | Workers > partitions | Increase partitions or reduce workers |

## Implementation notes per stack
**Airflow** -- `examples/concurrent_runner/airflow/concurrent_runner_dag.py`:
```python
@dag(dag_id="devices_loader_concurrent", schedule="@hourly", catchup=True,
     max_active_runs=5, max_active_tasks=10,
     default_args={"depends_on_past": False, "pool": "ingestion", "retries": 3})
```
Create the pool first: `airflow pools set ingestion 10 "team ingestion"`; each run writes a
`ts_nodash`-scoped location.

**Spark** -- `examples/concurrent_runner/spark/README.md`: concurrency is owned by the
cluster/orchestrator -- EMR `StepConcurrencyLevel` (1-256), FAIR scheduler pools for several
jobs in one application, YARN queues/K8s quotas; jobs overwrite only their own partition
(`partitionOverwriteMode=dynamic`).
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ingestion")
```
**Kafka** -- `examples/concurrent_runner/kafka/concurrent_consumers.py`: N processes in one
consumer group; concurrency ceiling = partition count; per-key order preserved per partition.

## Related patterns
- Single Runner -- the strict counterpart for dependent runs.
- Dynamic Late Data Integrator -- example of shared state unsafe under concurrency.
- Full Loader / Incremental Loader -- typical independent ingestion workloads.
- Fast Metadata Cleaner / Data Overwrite -- idempotent per-run outputs that make
  concurrency safe.
- Aligned Fan-In -- task-level parallelism inside one run, the complement of run-level parallelism.
