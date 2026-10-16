# Single Runner

Category: Orchestration · Scope: run-level concurrency of one pipeline

## Problem
You built a sessionization pipeline with the Incremental Sessionizer as a proof of
concept and ran it manually on demand to validate sessions with business owners. The project is
entering the release cycle, so you need orchestration: you have the pipeline graph but no way to
execute it. The logic is incremental, thus sequential: never more than one run.

## Solution / Mechanics
A runner that guarantees there is always a **single execution** of a given pipeline. Configure the data flow's concurrency level to 1:
- **Airflow** -- `max_active_runs=1` (concurrent DAG runs) + `depends_on_past=True` (each task
  starts only if the same task in the previous run succeeded);
- **AWS EMR** -- `StepConcurrencyLevel=1` controls parallel jobs on a cluster;
- **Azure Data Factory** -- pipeline `Concurrency=1`; extra runs are created and queued in a
  bounded queue (up to 100); when full, scheduling returns HTTP 429. Airflow
  instead doesn't schedule a new run while one is active.
- **No native support** -- implement a Readiness Marker waiting for the previous execution to
  complete. Gotcha: future runs still get triggered and pile up waiting.

Step by step (in-job guard when the runner has no concurrency setting):
1. Check the previous run's commit marker (`_SUCCESS` of run N-1); refuse to start without it.
2. Acquire an exclusive lease lock (atomic create); if it exists and is fresh, exit.
3. If it exists but its heartbeat is older than the TTL, the holder crashed: take it over
   atomically (rename away, then exclusive create).
4. Heartbeat the lock while running so long runs aren't considered stale.
5. Process: previous state + new increment -> overwrite run N's partition.
6. Release the lock only if still the owner.

```
 time ->   run D-1 [=====]                 run D [=====]          run D+1 [=====]
           max_active_runs=1: D waits for D-1 to finish; depends_on_past: D's task waits
           for D-1's same task to SUCCEED (a failure halts the chain)

 in-job lease:  _LOCK {owner, mtime=heartbeat}
   exists & fresh  -> exit "held by ..."
   exists & stale  -> rename(_LOCK, _LOCK.stale.<uuid>) -> O_EXCL create -> run
   release         -> delete only if owner matches
```

## Consequences & trade-offs
- **Backfilling**: reprocessing is very slow because it's sequential; you can't
  parallelize since concurrent runs give wrong results. Check whether the whole pipeline must
  be backfilled -- e.g. if only the loading location changed, re-run the load step and skip
  data processing.
- **Latency**: stragglers delay everything. An hourly flow taking 30 min that
  grows to 1.5 h falls further behind each run, and downstream consumers inherit the delay.
  Mitigate with more compute (scalable infrastructure) or better processing logic.
- **Failure halts the chain** with `depends_on_past`: correct by design, needs alerting.
- **Queued runs** (ADF) or **piling waiters** (marker-based) consume slots/quotas.

## Key design decisions
1. Is sequential execution a real business requirement (state carried between runs)? If the
   datasets are independent, use the Concurrent Runner.
2. Run-level only (`max_active_runs`) or task-level chaining too (`depends_on_past`,
   `wait_for_downstream`)?
3. Behavior on failure: halt (default, correct) vs skip-and-continue (breaks correctness).
4. Enforcement point when the scheduler can't: in-job lease lock, DB row lock, Kafka fencing.
5. Lock TTL: longer than the worst heartbeat gap, shorter than the acceptable recovery delay.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Incremental state (sessionization, running totals, day-over-day diff) | Yes | -- |
| Independent partitions/datasets per run | No | Concurrent Runner |
| Need fast backfill of stateful logic | Partly | Backfill only needed steps; bigger compute |
| Streaming stateful processor | Yes | Kafka transactional fencing / one consumer per partition |
| Only one task must be serialized | Partly | Airflow pool with 1 slot for that task |
| Schedulers without concurrency config (cron) | Yes, in-job guard | DB/ZooKeeper lock |

## Gotchas & best practices
- `max_active_runs=1` alone lets run N+1 start after run N **failed**; add `depends_on_past=True`.
- `depends_on_past`: a newly added task's first run has no past instance and runs; a skipped
  previous instance counts as satisfied.
- A lock without expiry blocks forever after a crash; use a lease (TTL + heartbeat) and
  owner-checked release.
- Heartbeat interval well below the TTL (code enforces `< ttl/2`); clock skew matters on
  shared storage -- keep a margin.
- On object stores use conditional writes (S3 `If-None-Match`, GCS `ifGenerationMatch`) or a
  DB row lock with the same lease semantics; plain "check then write" races.
- In streaming, a stable `transactional.id` fences zombie instances.
- Keep each run small so the latency debt is recoverable.

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Two runs overlap during catchup | `max_active_runs` default (16) | `max_active_runs=1` |
| Run after a failure computed on bad state | Missing `depends_on_past` | `depends_on_past=True` |
| Pipeline stuck "lock held" after OOM | Lock without TTL | Lease lock with stale takeover (`--lock-ttl`) |
| Long run's lock taken over mid-run | TTL shorter than heartbeat gaps | Raise TTL / shorten heartbeat |
| ADF returns 429 Too many requests | Queue of 100 queued runs full | Fewer triggers, faster runs |
| Data increasingly late each hour | Run duration > schedule interval | Scale compute, optimize, smaller increments |

## Implementation notes per stack
**Airflow** -- `examples/single_runner/airflow/single_runner_dag.py`:
```python
@dag(dag_id="visits_trend_generator", schedule="@daily", catchup=True, max_active_runs=1,
     default_args={"depends_on_past": True, "wait_for_downstream": True, "retries": 2})
```
**Spark** -- `examples/single_runner/spark/single_runner_job.py`: previous-run `_SUCCESS`
check + `LeaseLock` (O_EXCL create, heartbeat thread, stale takeover via atomic rename,
owner-checked release); `--self-test` exercises crash/takeover logic without Spark.
```python
if age < self.ttl:
    raise LockHeldError(f"{self.path} held by {self.current_owner()} (heartbeat {age:.0f}s ago)")
tombstone = f"{self.path}.stale.{uuid.uuid4().hex}"
os.rename(self.path, tombstone)          # only one contender can move the stale lock
os.remove(tombstone)
if not self._create():                   # O_CREAT | O_EXCL again
    raise LockHeldError(f"{self.path} created by another run during takeover")
```
**Kafka** -- `examples/single_runner/kafka/single_runner_fencing.py`: shared
`transactional.id`; `init_transactions()` fences the older instance, which exits on the fatal
`PRODUCER_FENCED` error.

## Related patterns
- Concurrent Runner -- relaxed variant for independent runs.
- Incremental Sessionizer / Stateful Sessionizer -- typical workloads needing it.
- Dynamic Late Data Integrator -- shared state that breaks under concurrent runs.
- Readiness Marker -- fallback mechanism without native concurrency settings.
- Local Sequencer -- split steps so backfills re-run only what is needed.
