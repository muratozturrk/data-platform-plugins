# Local Sequencer

Category: Sequence · Scope: one pipeline (orchestration layer) or one job (processing layer)

## Problem
One of the oldest jobs in the analytics department grew from dozens to hundreds of lines and
three times more transformations. It fails often and always restarts from the beginning, so
every incident means a long debugging session and an expensive full re-run. You must simplify
it and improve daily maintenance, but you can't remove any business logic.

## Solution / Mechanics
Decompose the big component into smaller, connected items run **sequentially**, ordered by
**dataset dependencies**: if task B reads what task A generated, B runs after A completes. Typical ingestion: a Readiness Marker task, then a Full Loader task,
either as two dependent tasks (orchestration-layer dependency) or as one task (processing-layer
dependency).

Step by step:
1. List the steps the job performs and the dataset each one reads/writes.
2. Draw the dependency graph from those datasets (not from the code order).
3. Mark **restart boundaries**: steps that must be retryable alone (expensive, paid, external).
4. Mark **transaction boundaries**: steps that must succeed or fail together stay in one unit.
5. Put each restart-bounded unit in its own orchestrator task; keep the rest chained in code.
6. Make every unit idempotent so retrying one step never corrupts the next.

```
 single task (processing-layer sequence)      Local Sequencer (orchestration-layer sequence)
 +-------------------------------------+      +-----------+   +-----------+   +--------+
 | wait input -> load -> expose        |      | wait input|-->| load data |-->| expose |
 +-------------------------------------+      +-----------+   +-----------+   +--------+
  failure in "expose" => rerun all             failure in "expose" => retry "expose" only
```

Three criteria for orchestration vs single-task sequencing:

| Criterion | Split into orchestrator tasks when... |
|---|---|
| Separation of concerns | you struggle to name the task, or the name gets too long |
| Maintainability | retries/backfills would recompute successful steps (e.g. 10 paid API calls in the readiness check, restarted 3 times) |
| Implementation effort | the orchestrator ships ready abstractions (SQL, HTTP, sensors) you would otherwise reinvent |

Failure semantics: Airflow advances a task only when its parent succeeded (default
`trigger_rule=all_success`). AWS EMR steps need `ActionOnFailure=TERMINATE_CLUSTER` (or
`CANCEL_AND_WAIT`), otherwise a later step can expose partial data while the run looks
successful. In the processing layer each variable is the input for the next step
until the write.

## Consequences & trade-offs
- **Boundaries**: wrong boundaries lengthen execution time or impact other
  pipelines if the scheduler can't scale. Too coarse = long restarts; too fine = scheduler
  pressure, inter-task latency and long, less readable graphs.
- **Restart boundaries**: Readiness Marker and Full Loader fail individually; the marker isn't
  re-run when the load fails (assumes immutable source data -- if the producer can remove data,
  re-run the check).
- **Compute boundaries**: inside a job, split after the most compute-expensive operation and
  materialize the intermediate dataset so retries of the final step don't recompute it.
- **Transactions**: operations forming one unit stay together -- e.g. the Dynamic Late Data
  Integrator processes data and updates the last processed version in one task.
- Not a perfect organization from day one: expect to move boundaries as failure patterns emerge.
- CRON remains valid for an isolated use case without dependencies.

## Key design decisions
1. Which steps must restart individually (cost, external side effects, duration)?
2. Which steps form a transaction (data + state/metadata update, write + commit marker)?
3. Orchestration layer (visible, individually retryable) or processing layer (cheaper, no
   scheduling overhead, shared Spark session/cache)?
4. Where to store intermediate datasets (private work area, retention/TTL, format)?
5. What a failed step does to the run (stop, alert, terminate cluster).

## When to use / when not
| Situation | Use Local Sequencer? | Alternative |
|---|---|---|
| Monolithic job, frequent failures, costly full restarts | Yes | -- |
| Steps owned by different teams or pipelines | No | Isolated Sequencer |
| Steps independent of each other on the same input | No | Parallel Split |
| N similar partitions feeding one result | Partly | Aligned / Unaligned Fan-In |
| Two operations must be atomic | Keep them in one unit | Transactional Writer |
| Single isolated script without dependencies | No | CRON / simple scheduler |

## Gotchas & best practices
- Name tasks by business intent; a painful name means the task does too much.
- Make each step idempotent (overwrite partitions, `TRUNCATE`+`COPY`, `CREATE OR REPLACE VIEW`).
- Use `mode="reschedule"` sensors with a timeout so waiting doesn't hold a worker slot forever.
- Materialized intermediates are private: keep them out of consumer paths and expire them.
- Never split inside a transaction: a half-applied state is worse than a slower retry.
- Set explicit failure actions on non-Airflow runners (EMR, Step Functions, ADF).

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Retrying the last step re-runs the paid API check | Check and load in one task | Split at the restart boundary |
| Consumers see partial table after a failed run | Runner continues after failed step | `ActionOnFailure=TERMINATE_CLUSTER` / default `all_success` |
| DAG with 40 tiny tasks is slow and unreadable | Over-splitting | Merge steps that never fail/retry independently |
| Retry duplicates rows | Non-idempotent append step | Overwrite/delete-insert per partition |
| State table says "processed" but data missing | Processing and state update split | Keep them in one transactional unit |
| `--stage final` fails: path not found | Intermediate never materialized/expired | Run `intermediate` stage, check TTL |

## Implementation notes per stack
**Airflow** -- `examples/local_sequencer/airflow/local_sequencer_dag.py`: Readiness Marker
(FileSensor) >> Full Loader (truncate + COPY) >> expose (view swap), each a restart boundary.
```python
input_data_sensor = FileSensor(task_id="input_data_sensor",
    filepath=INPUT_DIR + "/{{ ds_nodash }}/_SUCCESS", mode="reschedule", timeout=3600)
load_data_to_table = SQLExecuteQueryOperator(task_id="load_data_to_table", ...)
expose_new_table = SQLExecuteQueryOperator(task_id="expose_new_table",
    sql="CREATE OR REPLACE VIEW dp.devices AS SELECT * FROM dp.devices_{{ ds_nodash }};")
input_data_sensor >> load_data_to_table >> expose_new_table
```
**AWS EMR**: add each job as a step; `ActionOnFailure=TERMINATE_CLUSTER`
prevents later steps from publishing after a failure.

**Spark** -- `examples/local_sequencer/spark/local_sequencer_job.py`: named step functions
(read -> validate -> build_sessions -> enrich); the expensive sessions dataset is materialized
and `--stage final` replays only the last step.
```python
if args.stage in ("all", "intermediate"):
    sessions = build_sessions(validate(read_input(spark, args.input)))
    sessions.write.mode("overwrite").parquet(intermediate_path)   # restart boundary
if args.stage in ("all", "final"):
    enrich(spark.read.parquet(intermediate_path)).write.mode("overwrite").parquet(args.output)
```
**Kafka** -- `examples/local_sequencer/kafka/README.md`: in-process chain vs chained topics
(each hop = consumer group = restart boundary); Kafka transactions for atomic hops.

## Related patterns
- Readiness Marker, Full Loader -- the canonical first two steps of a sequence.
- Isolated Sequencer -- the same idea when steps live in separate pipelines/teams.
- Aligned Fan-In -- applies the same backfill-granularity reasoning to parallel branches.
- Parallel Split -- uses Local Sequencer splitting to give branches dedicated hardware.
- Dynamic Late Data Integrator -- example of a transaction boundary kept in one task.
- Idempotency patterns -- make each step safely retryable.
