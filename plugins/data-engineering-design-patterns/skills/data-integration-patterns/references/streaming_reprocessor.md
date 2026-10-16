# Streaming Reprocessor

Category: Data Reprocessing.

## Problem
Yesterday you released a new version of the sessionization job with a new session rule. Today
the number of sessions dropped significantly: the release accidentally removed the check that
closes inactive sessions. The bug is fixed, but all data since the failed release
must be reprocessed. The source data is still in the topic.

## Solution / Mechanics
Reprocess with the same streaming job, pointed at the past. It is a regular streaming pipeline;
only the processed data differs.

1. **Identify the starting point**, to avoid consistency issues from overlapping data:
   - *from the data source*: Kafka offsets or timestamps for every partition; a lakehouse
     table timestamp or version;
   - *from the processing framework*: one of the previously checkpointed metadata items.
2. **Configure the job to start from that position**, one of:
   - *pass the starting position to the job*; with checkpoint-based frameworks also use a
     **fresh checkpoint location**, otherwise the old checkpoint wins;
   - *promote the starting-position checkpoint as the most recent*: remove all invalid
     (newer) checkpoint metadata so the job resumes from the last valid point;
   - *restore the starting position from the checkpoint files*: start explicitly from a valid
     checkpoint, which invalidates all fresher ones.

```
 release v2 (bug) at offset 5000            fix v3
 topic:  ...|4000|4500|5000|5500|6000|6500|  now
 checkpoints:  chk-40 (4500)  chk-41 (5000)  chk-42 ... chk-48
                                 ^ last valid
 reprocess: start v3 from chk-41 (or offsets@5000 + fresh checkpoint)
            -> re-emits results for 5000..now -> idempotent sink overwrites v2 output
```

**Checkpoint and deploy**: with explicit checkpoints (Flink savepoints), make them
part of the release: (1) checkpoint the running job and stop it; (2) start the new version from
that checkpoint. After a regression you restart from the checkpoint taken before the release.

Spark Structured Streaming has no "start from checkpoint N" option, so there are three
strategies:
- **New empty checkpoint**: new `checkpointLocation` + `startingOffsets` (or
  `startingTimestamp`) on the Kafka reader. Stateless jobs only; stateful jobs also need the
  state restored, for example by reading the previous state and setting it as the initial state.
- **Restore a new checkpoint**: copy all valid checkpoint files to a new location and use it as
  `checkpointLocation`; delete the old one once the restarted job works.
- **Clean an old checkpoint**: delete invalid files in place. Riskiest: errors corrupt the
  state, and production environments may forbid deletes.

Checkpoint internals you must know for the last two: `offsets/N` and
`commits/N` (+ `.N.crc`) per micro-batch, and state files versioned **N+1** for micro-batch N
(`state/<op>/<partition>/<N+1>.delta|.zip`).

## Consequences & trade-offs
Six points:
- **Reprocessing start time**: easy on append-only logs (offset or ingestion time), harder on
  object stores (last modification time, or names of the last processed file if names are
  incremental). Prefer identifying the start from the framework's checkpoints.
- **Duplicates**: the job processes the same data again and may emit identical rows. Design for
  idempotency.
- **All-or-nothing semantics**: logs replay everything. Fixing only a subset (one tenant of a
  multitenant table) needs custom filters, or the Batch Reprocessor.
- **Lost state or data**: a fresh checkpoint starts with no memory; all state is recomputed.
  To reuse current checkpoint data, extract the state and use it as the initial state.
- **Human error**: manual operations on framework-managed files, such as checkpoint removal,
  can corrupt the application state. Back up first and automate the procedure.
- **Time scope**: works only while the source still holds the data. With aggressive retention,
  use the Batch Reprocessor. Kafka tiered storage (3.6+) keeps older data on cheaper remote
  storage; diskless topics (Aiven, Bufstream, WarpStream) write directly to object stores.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Start position source | offsets/timestamps, table version, checkpoint | checkpoint is simplest when available |
| Restart method | new position + fresh checkpoint, restored copy, cleaned checkpoint | prefer copy over in-place deletion |
| State | rebuild from source, restore from checkpoint, initial state | stateful jobs cannot use an empty checkpoint naively |
| Output handling | overwrite by key, MERGE, versioned output | consumers must tolerate re-emitted rows |
| Release process | savepoint/checkpoint per release, labelled with the version | makes the start point obvious |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| The source still retains the data to reprocess | Retention already removed it | Batch Reprocessor on the lakehouse copy |
| The whole stream since time T must be recomputed | Only a subset (tenant, key range) is wrong | Batch Reprocessor with filters |
| Sinks are idempotent | Sinks append blindly and consumers cannot dedupe | fix sink idempotency first |

## Gotchas & best practices
- Stop the running job before rewinding; two instances writing the same output corrupt it.
- Back up the checkpoint directory before any manual change, and keep the original intact
  (copy strategy).
- `startingOffsets` / `startingTimestamp` apply only to a new checkpoint location; with an
  existing location Spark silently resumes from the checkpoint.
- Consumer-group based jobs (plain consumers, Kafka Streams) can be rewound with
  `offsets_for_times` and an offset commit while the group is inactive.
- Retain enough checkpoints (`state.checkpoints.num-retained`, Spark
  `spark.sql.streaming.minBatchesToRetain`) to reach the release point.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Job ignores the new starting offsets | old checkpoint location reused | fresh checkpoint location |
| Sessions wrong after rewind | stateless strategy on a stateful job | restore a checkpoint copy, or rebuild state |
| "Unknown member" when committing offsets | consumer group still active | stop all consumers first |
| Restart fails after deleting checkpoint files | state version N+1 or `.crc` files inconsistent | restore from backup; prefer the copy strategy |
| Rewind target offsets are missing | retention deleted them | Batch Reprocessor |

## Implementation notes per stack
**Flink**: retained checkpoints and an explicit restart point:

```python
env.enable_checkpointing(30_000)
env.get_checkpoint_config().set_externalized_checkpoint_retention(RETAIN_ON_CANCELLATION)
# state.checkpoints.dir: s3://my_job/checkpoints ; state.checkpoints.num-retained: 20
# flink run -s file:///checkpoints_flink/<job-id>/chk-2 -py stateful_flink_consumer.py
```

**Spark** - `examples/streaming_reprocessor/spark/reprocess_from_past.py`: `new-checkpoint`
(fresh location + `startingTimestamp`, stateless only) and `restore-copy`, which copies the
checkpoint up to a valid batch into a new location and keeps the original as backup:

```python
def _is_newer(directory, name, last_valid_batch):
    stem = name.lstrip(".").split(".")[0]   # "42", ".42.crc", "43.delta", "43.zip"
    ...
    return version > (last_valid_batch + 1 if in_state else last_valid_batch)
```
In-place cleaning of newer checkpoint files is deliberately not shown.

**Kafka** - `examples/streaming_reprocessor/kafka/reset_offsets_to_timestamp.py`: dry-run by
default; resolves offsets with `offsets_for_times` and commits them for a stopped group
(same as `kafka-consumer-groups.sh --reset-offsets --to-datetime`).

**Airflow** - `examples/streaming_reprocessor/airflow/reprocessing_dag.py`: on-demand
`backup_checkpoint >> stop_streaming_job >> restart_from_past`, with a Flink savepoint or Spark
checkpoint strategy, to remove human error from the procedure.

## Related patterns
- [Batch Reprocessor](batch_reprocessor.md): the alternative when retention is short or only a subset must be fixed.
- [Checkpointer](../../error-management-patterns/references/checkpointer.md): the mechanism this pattern rewinds.
- [Keyed Idempotency](../../idempotency-patterns/references/keyed_idempotency.md) / [Merger](../../idempotency-patterns/references/merger.md): make re-emitted results safe.
- [Hybrid Source](hybrid_source.md): uses the same "start from a position" mechanics for backfills.
- [Lag Detector](../../data-observability-patterns/references/lag_detector.md): the rewound job shows a large, decreasing lag; expected, but monitor it.
