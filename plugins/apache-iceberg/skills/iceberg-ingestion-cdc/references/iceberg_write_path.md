# The Iceberg Write Path

Scope: what each write commits (append, overwrite, MERGE/UPDATE/DELETE), copy-on-write vs
merge-on-read, position vs equality deletes, write distribution, and the optimistic commit
protocol (retries, `CommitFailedException`, `ValidationException`).

## Problem
An accounts table receives appends, partition rebuilds, upserts and deletes, sometimes from several
jobs at once (streams, batch jobs, compaction). You need to know what each write produces, which
writes can conflict, and how to avoid lost updates.

## Every write = one snapshot
No data file is ever modified. A write adds files (and/or delete files) and publishes a new snapshot.

| Operation | What changes | Snapshot `operation` |
|---|---|---|
| Append (`INSERT`, `append()`) | Adds data files | `append` |
| Overwrite by filter / `overwritePartitions()` | Replaces files in scope | `overwrite` |
| Row-level change, copy-on-write | Rewrites affected data files | `overwrite` |
| Row-level change, merge-on-read | Adds delete files (+ new rows) | `overwrite` |
| Delete of whole files/partitions | Removes files from metadata | `delete` |

Sequence `append, append, overwritePartitions, MERGE (COW), DELETE WHERE region='eu'` yields operations
`append, append, overwrite, overwrite, delete`; the partition overwrite reports `deleted-records=100`,
`added-records=50`; time travel `VERSION AS OF <first>` still returns 100 rows.

A failing job leaves no trace: `INSERT ... raise_error()` on row 5,000 of 10,000 leaves snapshot count
and row count unchanged. Readers never see partial data.

## Copy-on-write vs merge-on-read
```sql
CREATE TABLE lake.crm.accounts (id BIGINT, region STRING, balance DOUBLE)
USING iceberg PARTITIONED BY (region)
TBLPROPERTIES ('format-version'='2',
  'write.merge.mode'='merge-on-read',    -- or copy-on-write (default)
  'write.update.mode'='merge-on-read',
  'write.delete.mode'='merge-on-read');
```
| | Copy-on-write | Merge-on-read |
|---|---|---|
| Write cost | Rewrites every touched file | Small delete files |
| Read cost | Clean files | Merge deletes at read time |
| Summary of a MERGE | `deleted-data-files` > 0, no delete files | `added-position-deletes`, no `deleted-data-files` |
| Fit | Read-heavy, rare large changes | Frequent small changes, CDC |
| Must have | - | Regular compaction of delete files |

MOR MERGE of 4 staged rows into 1,000: `added-position-deletes=3` (3 updates), base file untouched.

| Delete file type | Identifies rows by | Read cost | Writer |
|---|---|---|---|
| Position delete | file path + row position | Lower; easy to compact | Spark |
| Equality delete | column values | Higher; hard to compact | Flink (streaming, CDC), Airbyte dedupe |

MOR requires `format-version` >= 2.

## MERGE INTO
```sql
MERGE INTO lake.crm.accounts t USING staging s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```
- One commit for conditional insert/update/delete: upserts, SCD, corrections.
- At most **one source row per target row**; reduce the source first (latest per key).
- Scope the `ON` clause / add partition predicates so fewer files are scanned and rewritten.

## Write distribution (partitioned writes)
| Option | Effect | Measured (15 table partitions, 4 Kafka partitions) |
|---|---|---|
| `distribution-mode=hash` | Shuffle rows by partition before writing | 15 files per micro-batch |
| `distribution-mode=none` | Write in arrival order | 60 files (4 tasks x 15) |
| `none` + `fanout-enabled=true` | One open file per partition per task, no sort | 60 files; unsorted input accepted |

```python
(df.writeStream.format("iceberg").outputMode("append")
   .option("distribution-mode", "hash")      # fewer, larger files; costs a shuffle
   .option("checkpointLocation", cp).toTable("lake.web.events"))
```
File count is controlled by distribution mode, not by fanout. Table-level default:
`write.distribution-mode`.

## Commit protocol: optimistic concurrency
```
A, B read metadata S1; both write files
A: swap S1 -> S2   OK
B: swap S1 -> S2'  CommitFailedException (pointer is S2)
B: refresh to S2, re-apply its changes, run validations
   no logical conflict (append, disjoint partition)  -> swap S2 -> S3 OK
   conflicting data in validated scope               -> ValidationException, job fails
```
- No locks. The compare-and-swap of the metadata pointer happens in the catalog (REST, Glue, ...).
- A client that refreshes before committing never conflicts in sequential code; a writer holding
  stale metadata is simply rebased (its snapshot's parent becomes the other writer's snapshot).
  Conflicts occur only when commits race inside the refresh-to-swap window.
- 4 concurrent Spark `INSERT`s into one table: 4 `append` snapshots, 4,000 distinct rows.

### Retries must match writer concurrency
| `commit.retry.num-retries` | 8 writers appending at the same instant |
|---|---|
| 0 | 1 committed, 7 `CommitFailedException`; losers added no files |
| 4 (default) | Non-deterministic: 0, 2 or 3 of 8 failed across runs |
| 20 | 8 of 8 committed |

```sql
ALTER TABLE lake.crm.accounts SET TBLPROPERTIES (
  'commit.retry.num-retries'='20', 'commit.retry.min-wait-ms'='100', 'commit.retry.max-wait-ms'='60000');
```

### Validated overwrites (avoid lost updates)
An overwrite that does not validate silently removes a file another writer just appended to the
same partition: no error, data gone. Validate from the snapshot you read:

```java
table.newOverwrite()
     .overwriteByRowFilter(Expressions.equal("region", "eu"))
     .addFile(rebuiltFile)
     .validateFromSnapshot(baseSnapshotId)                  // snapshot the job planned against
     .conflictDetectionFilter(Expressions.equal("region", "eu"))
     .validateNoConflictingData()
     .commit();                                            // ValidationException if eu changed
```
- Overlapping scope (concurrent `eu` append): `ValidationException`, concurrent file kept, rebuild not committed.
- Disjoint scope (concurrent `us` append): overwrite of `eu` succeeds after the retry, both files live.
- Spark row-level ops validate per `write.merge.isolation-level` / `write.update.isolation-level` /
  `write.delete.isolation-level` (`serializable` default, `snapshot` looser).

## Metadata cost of frequent commits
The commit is cheap; accumulated snapshots and manifests are not. Batch changes per commit, then
expire snapshots, rewrite manifests and compact files on a schedule.

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `CommitFailedException` under load | More racing writers than retries | Raise `commit.retry.num-retries`, stagger writers |
| `ValidationException` on overwrite/MERGE | Concurrent change in validated scope | Disjoint partitions per writer; retry the job |
| Rows vanished after a rebuild | Unvalidated overwrite | `validateFromSnapshot` + `validateNoConflictingData`; Spark `serializable` |
| MERGE fails: multiple source rows matched | Duplicate keys in source | Dedupe to latest per key |
| Slow reads after CDC | Delete files pile up (MOR) | `rewrite_data_files` / `rewrite_position_delete_files` |
| Many small files per partition | `distribution-mode=none` with many tasks | `hash` (or `range` for sorted tables) |
