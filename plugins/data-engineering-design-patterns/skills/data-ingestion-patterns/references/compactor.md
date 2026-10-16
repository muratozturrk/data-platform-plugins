# Compactor

Category: data compaction. One-liner: reduce the storage footprint by **merging many small files into fewer
bigger ones** (or, in key-based logs, keeping only the latest record per key).

## Problem
A real-time ingestion pipeline synchronises events from a streaming broker to an object store; data must be
available to batch jobs within 10 minutes. It's a simple passthrough job and runs fine. After three months,
all the batch jobs suffer from **metadata overhead**: too many small files. They spend **70% of their execution
time listing files** and only 30% processing data, with a serious latency and cost impact on
pay-as-you-go services.

## Solution / Mechanics
Small files are an old problem (Hadoop era) still present in object-store lakehouses: listing takes longer, and
opening/closing files adds I/O. The natural fix is fewer files: the Compactor combines small files into bigger
ones, reducing I/O overhead on reads. Implementations:
- **Open table formats** run a transactional distributed job that writes bigger files and commits them:
  Iceberg *rewrite data files*, Delta Lake `OPTIMIZE`.
- **Hudi merge-on-read** tables write columnar base files plus row-format change logs; compaction merges the row
  logs into the columnar storage to speed reads.
- **Apache Kafka** (key-based append-only log): configuration only. The broker keeps only the most recent entry per
  key; unlike file compaction, it **overwrites the present data**.
- **PostgreSQL/Redshift**: `VACUUM` reclaims space of dead tuples (rows not physically removed).

```
 before (1 partition, 2,880 files/day)          OPTIMIZE (one transaction)             after
 ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐ ... 4 MB each   ──►  add: 3 x ~1 GB files            ┌──────┐┌──────┐┌──────┐
 └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘                       remove (logically): small files  └──────┘└──────┘└──────┘
                                                readers: old OR new snapshot
                       VACUUM (later, after retention) ──► physically deletes the removed small files
```

Step by step (Delta):
1. Select scope: recent or unhealthy partitions (avg file size under a threshold).
2. `OPTIMIZE` (optionally `ZORDER BY` for data skipping): only files present at job start are rewritten, so it
   doesn't conflict with concurrent appends.
3. Check file statistics (count, average size).
4. Periodically `VACUUM` files older than the retention threshold to reclaim storage.

## Consequences & trade-offs
- **Cost versus performance**. Compaction is a regular, possibly compute-intensive job. Running it rarely
  (daily, off-hours, outside the producing pipeline) is cheap, but jobs reading not-yet-compacted data don't
  benefit. Sometimes you'll even put compaction inside ingestion because penalising consumers costs more than
  ingestion throughput. No one-size-fits-all.
- **Consistency**. Compaction rewrites existing data; consumers may not distinguish data in use from data
  being compacted. Much simpler and safer with ACID table formats (Delta, Iceberg) than raw JSON/CSV.
- **Cleaning**. Compaction may keep source files (time travel, running readers), so metadata
  operations still see them. Complete it with a cleaning job (`VACUUM`), but deleted files mean you can't restore
  versions based on them.
- **Kafka**. Log compaction is nondeterministic: you can't expect it on a schedule, and it doesn't
  guarantee a unique record per key at any moment.

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Where | separate scheduled job · inside ingestion · auto-compaction/optimized writes | Separate job by default; inline when consumer latency dominates |
| Scope | whole table · recent partitions · threshold-based | Recent/unhealthy partitions only (`WHERE date >= ...`) |
| Target file size | 128 MB - 1 GB | Delta `optimize.maxFileSize` ~1 GB; smaller for highly selective reads |
| Z-ORDER | none · 1-3 high-cardinality filter columns | Only columns in frequent predicates; costs extra rewrite time |
| VACUUM retention | default 7 days · longer | Never below the longest query + time-travel need; ≥ 7 days |
| Raw formats | coalesce rewrite + swap | Stage then swap; readers may see duplicates mid-swap |
| Kafka | `compact` · `compact,delete` | `compact` for latest-state topics; `compact,delete` to also cap retention |

## When to use / when not
| Situation | Use Compactor? | Alternative |
|---|---|---|
| Listing/opening files dominates job time | Yes | - |
| Streaming or frequent micro-batch ingestion into a lake | Yes | Longer triggers, optimized writes (reduce the cause) |
| Topic must hold only the latest value per key | Yes (Kafka compaction) | - |
| Dataset needs all historical versions per key in Kafka | No (compaction deletes them) | Retention-based topic + lake history |
| Few large files already | No | - |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Storage keeps growing after OPTIMIZE | No VACUUM; old files only logically removed | Scheduled VACUUM (e.g. weekly) |
| `FileNotFoundException` in long queries | VACUUM retention shorter than running queries | Retention ≥ 7 days; don't disable `retentionDurationCheck` |
| Time travel to last week fails | VACUUM removed the files of old versions | Align retention with rollback needs |
| Streaming reader re-processes compacted data | Rewrite commit seen as new data | Compaction commits are `dataChange=false`; use `skipChangeCommits` for update/delete commits |
| OPTIMIZE conflicts | Concurrent UPDATE/DELETE/MERGE on the same files | Schedule apart, or partition-scoped OPTIMIZE |
| Kafka topic still has duplicates per key | Compaction is asynchronous, active segment never compacted | Consumers dedupe; tune `min.cleanable.dirty.ratio`, `segment.ms`, `max.compaction.lag.ms` |
| Deletes reappear on Kafka consumers | Tombstones removed before slow consumers saw them | `delete.retention.ms` > max consumer downtime |
| Compaction job as expensive as ingestion | Compacting all partitions every run | Threshold-based selection, recent partitions only |

## Implementation notes per stack

### Spark + Delta Lake
Examples:
```python
devices_table = DeltaTable.forPath(spark_session, table_dir)
devices_table.optimize().executeCompaction()       # or .executeZOrderBy("col")
devices_table.vacuum()                             # default retention (7 days)
```
`examples/compactor/spark/spark_compactor_delta_optimize.py` runs `OPTIMIZE delta.\`path\` [WHERE ...]
[ZORDER BY (...)]` per target with before/after file stats and an optional
`VACUUM ... RETAIN {hours} HOURS` (minimum 168). `spark_compactor_binpack.py` handles plain Parquet: average file
size per partition, `coalesce(n)` into a staging directory, swap, `--dry-run`.

### Kafka
`examples/compactor/kafka/kafka_topic_compaction_setup.py` (Admin API):
```python
"cleanup.policy": "compact",
"min.compaction.lag.ms": str(24 * 60 * 60 * 1000),     # keep recent history uncompacted
"max.compaction.lag.ms": str(7 * 24 * 60 * 60 * 1000), # upper bound before a record is eligible
"min.cleanable.dirty.ratio": "0.1",
"delete.retention.ms": str(24 * 60 * 60 * 1000),       # how long tombstones survive
```

### Airflow
`examples/compactor/airflow/compactor_dag.py`: nightly OPTIMIZE after ingestion, a `BranchPythonOperator`
that runs VACUUM only on Sundays, then a health check failing when the average file size stays under threshold,
plus an audit row per run.

### Databases
`VACUUM` (PostgreSQL) reclaims dead tuples; it is space reclamation, not file merging.

## Related patterns
- **Passthrough Replicator / Incremental Loader / CDC** - the frequent writers that create small files.
- **Readiness Marker** - with raw formats, don't announce a partition while it's being compacted.
- **Horizontal Partitioner / Bucket / Sorter** - complementary storage layout; over-partitioning causes small files.
- **Full Loader** - time travel used for rollback depends on VACUUM retention.
