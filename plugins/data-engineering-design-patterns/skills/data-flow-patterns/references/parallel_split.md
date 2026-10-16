# Parallel Split

Category: Fan-Out · Scope: one parent -> two or more children running in parallel

## Problem
You're replacing a legacy C# data processing framework nobody knows anymore; maintainers left
without documentation. After reverse engineering you rewrite the logic with a modern open source
Python library. Because the reverse engineering may be imperfect, you keep the old pipelines
running until consumers switch -- so during the migration the processed dataset must be written
to two different places.

## Solution / Mechanics
Two operations depend on a common parent, so use a fan-out: the Parallel Split breaks the work
into parallel parts whose only shared point is the parent.

Orchestration layer: straightforward with the orchestrator's DSL or functions -- declare the
branches after the common task.

Processing layer -- three rules:
1. **Compute common work once**: the split must not trigger separate reads; materialize the
   intermediate dataset (temporary SQL table or Spark `.persist()`).
2. **Isolate branches**: global/shared variables must be read-only or modified compatibly
   across all writers.
3. **Size the compute**: if execution time matters, allocate dedicated resources or auto-scaling
   for the parallel workload.

Step by step:
1. Identify the common parent (sensor, read, cleaning).
2. List the branches and each one's hardware profile and SLA.
3. Same hardware -> one job, read once + `persist()`, write each branch.
4. Different hardware -> split the job (Local Sequencer): write the intermediate dataset, then
   start each branch on dedicated compute.
5. Make every branch write retry-safe (Delta `txnAppId`/`txnVersion`, or idempotency patterns).
6. If one branch is slow/fragile, move it into its own pipeline (Isolated Sequencer).

```
 orchestration layer                        processing layer (one job)
                 +-> load_delta              read --> persist() --+--> write devices (Delta, txn)
 file_sensor ----+                                                +--> enrich --> write devices_enriched
                 +-> load_csv                (one read; both writes idempotent per batch_id)
 no common child: the sensor is the only shared point
```

## Consequences & trade-offs
- **Blocked execution**: with time-dependent pipelines (each run needs the
  previous one to succeed) the next run waits for the slowest branch; a failed branch stops
  subsequent runs entirely. Mitigation: export the slow branch to a dedicated pipeline linked
  with a data- or task-based dependency (Isolated Sequencer).
- **Hardware**: branches inside one job share infrastructure; a CPU-heavy and a
  memory-heavy branch can't both get adapted hardware. Mitigation: generate the intermediate
  dataset, then run branches as separate jobs on their own clusters.
- **Duplicate writes on retries**: replays can duplicate data; Delta Lake's
  `txnAppId` + `txnVersion` make a replayed write a no-op.
- **Cache pressure**: `MEMORY_ONLY` may not fit; `MEMORY_AND_DISK` spills.

## Key design decisions
1. Split in the orchestrator (isolation, own hardware, own retries) or in one job (single read)?
2. Storage level for the shared dataset (`MEMORY_ONLY` vs `MEMORY_AND_DISK`).
3. Idempotency mechanism per sink (Delta txn options, partition overwrite, MERGE).
4. Is the branch coupling to the next run acceptable, or does a branch get its own pipeline?
5. When does the split end (migration finished -> switch to Exclusive Choice or remove branch)?

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| Same dataset to several outputs/teams simultaneously | Yes | -- |
| Dual write during a migration | Yes | Exclusive Choice once a cut-over date exists |
| Only one branch should run per execution | No | Exclusive Choice |
| One branch is slow and blocks the next run | Partly | Isolated Sequencer for that branch |
| Branches need different hardware | Yes, as separate jobs | Local Sequencer + dedicated clusters |
| Many consumers of a stream | Native in Kafka | Separate consumer groups |

## Gotchas & best practices
- `persist()` is lazy: the first action materializes it; `unpersist()` after the last branch.
- `txnAppId` stable across retries but unique per writer/job version; `txnVersion` monotonic per
  batch (e.g. logical date as int). Different tables can share the same pair.
- Put branch-specific resources (`executor_memory`, pools) on the orchestration task.
- Avoid driver-side shared mutable state between branch writes (accumulators, globals).
- Watch the downstream contract: legacy and new outputs must be compared before cut-over.

## Common mistakes / troubleshooting
| Symptom | Likely cause | Fix |
|---|---|---|
| Source read twice (double S3 cost / time) | No `persist()` before branches | `persist()` the common DataFrame |
| Executors OOM after persist | `MEMORY_ONLY` too large | `MEMORY_AND_DISK`, persist after filtering |
| Duplicates after retry | Plain append on replay | Delta `txnAppId`/`txnVersion` or overwrite |
| Next day's run waits for hours | Slow branch + `depends_on_past` | Move branch to its own DAG |
| Replay writes nothing | `txnVersion` reused for new data | Derive version from the batch/logical date |
| One branch OOMs, other is idle | Shared cluster, different profiles | Split jobs, dedicated hardware |

## Implementation notes per stack
**Airflow** -- `examples/parallel_split/airflow/parallel_split_dag.py`:
loop over `["delta", "csv"]` after the common `file_sensor`; no common child. Uses
`SparkSubmitOperator` (instead of `SparkKubernetesOperator` + sensor) with per-branch memory.
```python
for output_format in ["delta", "csv"]:
    load_job = SparkSubmitOperator(task_id=f"load_job_{output_format}", application=JOB_PATH,
        executor_memory="2g" if output_format == "csv" else "1g",
        application_args=["--format", output_format, "--batch-id", "{{ ds_nodash }}", ...])
    file_sensor >> load_job
```
**Spark** -- `examples/parallel_split/spark/parallel_split_job.py`:
read once, `persist(MEMORY_AND_DISK)`, two idempotent Delta appends; `--format` runs one branch
for the orchestration-layer split.
```python
def idempotent_append(df, path, batch_id):
    (df.write.mode("append").format("delta")
     .option("txnVersion", batch_id).option("txnAppId", APP_ID).save(path))
input_dataset.persist(StorageLevel.MEMORY_AND_DISK)
idempotent_append(input_dataset, f"{args.output}/devices", args.batch_id)
idempotent_append(enriched, f"{args.output}/devices_enriched", args.batch_id)
```
**Kafka** -- `examples/parallel_split/kafka/parallel_split_consumers.py`: one topic, one
consumer group per branch; each branch has its own offsets/lag, so a slow branch doesn't block
the other (sinks must still be idempotent under at-least-once).

## Related patterns
- Exclusive Choice -- same fan-out shape but only one branch runs.
- Aligned Fan-In -- the inverse shape (many parents, one child).
- Isolated Sequencer -- detach a slow branch into a separate pipeline.
- Local Sequencer -- split the job to give branches dedicated hardware.
- Transactional Writer / Keyed Idempotency -- retry-safe writes where Delta txn options don't exist.
