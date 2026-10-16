# Transactional Writer

Family: **Database**. Rely on the data store's native transactions, so that in-progress changes stay invisible to readers until the writer commits. A failed attempt rolls back, or leaves orphan data that no reader can see. Consumers get all of the output or none of it.

## Problem
A batch job runs on the cloud provider's unused (spot) capacity and saves 60% of the infrastructure cost. When the provider reclaims a node, its tasks fail and retry on another node, writing the same data again. Consumers complain about duplicates and incomplete records. The job must never expose partial data.

## Solution / Mechanics
Three steps:
1. **Begin** the transaction. Explicit: `START TRANSACTION` / `BEGIN`, or `producer.begin_transaction()`. Implicit: the processing layer opens it for you (a Delta write, psycopg2 with autocommit off).
2. **Write**. The new records sit in the store but are private to the transaction.
3. **Commit** to publish them, or **roll back** to discard them on any error.

```
 producer                         data store                              consumer
 --------                         ----------                              --------
 BEGIN  ------------------------> txn 42 opened
 write batch 1 -----------------> [rows, private to txn 42]
 write batch 2 -----X (error)
 ROLLBACK ----------------------> txn 42 discarded              ------>  sees nothing new
   ... retry ...
 BEGIN  ------------------------> txn 43
 write batch 1, write batch 2 --> [rows, private to txn 43]
 COMMIT ------------------------> txn 43 visible               ------>  sees batch 1 + 2
```

Two implementation families:
- **Standalone / ELT jobs inside the store** (BigQuery, Redshift, Snowflake, PostgreSQL). The transaction is declarative and fully managed by the store, even when the engine distributes the work.
- **Distributed ETL jobs**, where many tasks write the same output in parallel:
  - **Local (task-based) transactions.** Each task commits on its own. This works as long as the job does not retry: a retried job rewrites the tasks that already committed.
  - **Job-level transaction.** It opens before the tasks start and commits once all of them finish. This guarantee is stronger but harder to achieve. With Spark and Delta Lake, the commit is the new file in the `_delta_log` directory. If that step fails, the data files stay behind and must be moved aside or vacuumed.
- **Kafka** works the same way. The producer sends a transaction start marker to the partitions, and the commit writes a marker that makes the pending records visible to `read_committed` consumers. Flink uses `KafkaSink` with `DeliveryGuarantee.EXACTLY_ONCE` and `transaction.timeout.ms`.
- **Multi-statement SQL**: two blocks of `CREATE TEMP TABLE ... LIKE` + `COPY` + `MERGE`, then one `COMMIT`. If the second file has values that are too long, *neither* MERGE is committed, because the runner never reaches COMMIT. (A common slip is MERGEing `changed_devices_file1` twice; the second block must use `_file2`.)
- Supporting stores: Delta Lake, Iceberg and Hudi; Kafka; Redshift and BigQuery; PostgreSQL, MySQL, Oracle and SQL Server.

## Consequences & trade-offs
- **Commit step**. Opening and committing are two extra steps, and data conflicts have to be resolved at both. The data also becomes available later: a JSON or CSV file is readable as soon as it lands, but a Delta file only after the commit, which means after the *slowest* task. This coordination overhead is the price of exposing only complete datasets.
- **Distributed processing**. Framework support for transactions is not universal. Spark has no transactional Kafka producer, whereas Flink has one. Table formats are well supported by both.
- **Idempotency scope**. Idempotency is limited to the transaction itself:
  - With local (task) transactions and no coordination of the committed tasks, a job restart rewrites data from transactions that already committed.
  - A backfill opens a new transaction and adds the same records again.
  - Pair the pattern with an overwrite, a MERGE, a deterministic key or Delta's `txnAppId`/`txnVersion` to make reruns safe.
- **Readers must cooperate**. A reader running at `READ UNCOMMITTED` gets dirty reads, and Kafka consumers at the default `read_uncommitted` see aborted records.
- **No cross-store atomicity.** Delta has no multi-table transactions, and Delta + Kafka + PostgreSQL cannot commit together. Use one database transaction, or a single visibility switch (Proxy).

## Key design decisions

| Decision | Guidance |
|---|---|
| Transaction granularity | Job-level when partial runs are unacceptable. Task-level plus idempotent writes when a job-level commit is not possible. |
| Cross-run idempotency | Delta: `replaceWhere` + `txnAppId`/`txnVersion`. SQL: MERGE or DELETE + INSERT in the transaction. Kafka: `send_offsets_to_transaction`. |
| `txnAppId` / `txnVersion` | Versions must grow per app id. Use one app id per partition (or per streaming query) so that a backfill of an older date is not silently skipped. |
| Kafka `transactional.id` | Stable per logical producer, so a restarted instance fences the zombie. A random id per start defeats fencing. |
| Transaction timeout | Longer than the worst batch or checkpoint duration, and within the broker's `transaction.max.timeout.ms`. |
| Multiple outputs | One transaction when the store supports it (SQL). Otherwise accept per-table atomicity and make each write replay-safe. |

## When to use / when not

| Situation | Use Transactional Writer? | Alternative |
|---|---|---|
| Preemptible/spot workers, retries expose partial files | Yes | - |
| Several loads must appear together (one database) | Yes, one SQL transaction | - |
| Kafka-to-Kafka exactly-once | Yes, native client or Flink | - |
| Spark writing to Kafka | No transactional sink | Keyed Idempotency with compacted topics |
| Backfills must not duplicate | Not alone | Combine with Data Overwrite, Merger or Keyed Idempotency |
| Several tables in different stores must flip together | No | Proxy view switch, single-store design |

## Gotchas & best practices
- **kafka-python has no transactions API.** Use confluent-kafka or the Java client.
- After `abort_transaction()`, seek the consumer back to the first offset of the aborted batch. Its in-memory position has already moved past it, and the next poll would otherwise skip the data.
- A fatal producer error (for example, fenced by a newer instance) cannot be retried in place. Stop and recreate the producer.
- In `foreachBatch`, re-raise errors. A swallowed exception lets the checkpoint advance past a batch that was never written.
- Guard against empty commits: a transactional overwrite of zero rows is still a valid commit.
- Clean up the orphan files of failed commits with VACUUM. Do not delete them by hand while writers are active.
- `max_active_runs=1` avoids conflicting transactions on the same rows or partition.

## Implementation notes per stack

### Spark
[`spark/spark_transactional_writer.py`](../examples/transactional_writer/spark/spark_transactional_writer.py): one DataFrameWriter call is one job-level commit. Delta's idempotent-writer options make a retried job that already committed a no-op:
```python
(df.write.format("delta").mode("overwrite")
   .option("replaceWhere", f"visit_date = '{partition_date}'")
   .option("txnAppId", ids.app_id)          # visits_silver:visit_date=2024-11-20
   .option("txnVersion", ids.version)       # run version, bumped on reprocessing
   .option("userMetadata", ids.user_metadata)
   .partitionBy("visit_date").save(output_path))
```
[`spark/spark_streaming_transactional_writer.py`](../examples/transactional_writer/spark/spark_streaming_transactional_writer.py): one commit per table per micro-batch. A MERGE makes the orders table replay-safe, and the metrics append is deduplicated by `txnVersion=batch_id`:
```python
metrics_df.write.format("delta").mode("append") \
    .option("txnAppId", METRICS_TXN_APP_ID).option("txnVersion", batch_id).save(METRICS_OUTPUT_PATH)
```

### Kafka
[`kafka/kafka_transactional_writer.py`](../examples/transactional_writer/kafka/kafka_transactional_writer.py): read-process-write with consumed offsets inside the transaction:
```python
producer.begin_transaction()
...produce to gold.orders and gold.order_events...
producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata())
producer.commit_transaction()
# except: producer.abort_transaction(); rewind_to_batch_start(consumer, messages)
```
Consumers of the output topics set `isolation.level=read_committed`.

### Airflow
[`airflow/transactional_writer_dag.py`](../examples/transactional_writer/airflow/transactional_writer_dag.py), two loads in one transaction on PostgreSQL 15+:
```python
conn.autocommit = False
for index, path in enumerate(files, start=1):
    cur.execute(f"CREATE TEMPORARY TABLE changed_devices_file{index} (LIKE dp.devices) ON COMMIT DROP")
    cur.copy_expert(f"COPY changed_devices_file{index} FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true)", f)
    cur.execute(merge)          # MERGE INTO dp.devices ... USING changed_devices_file{index}
conn.commit()                   # except: conn.rollback(); raise
```

## Common mistakes / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Records lost after an aborted Kafka transaction | Consumer position not rewound after abort | Seek to the batch's first offsets (or the committed offsets) |
| `ProducerFencedException` / fatal error loop | Another instance took the same `transactional.id` | Exit, let the new instance own the id |
| Flink/Kafka commit fails with an expired transaction | `transaction.timeout.ms` shorter than the checkpoint or batch | Raise the timeout (up to the broker max) |
| Consumers see aborted data | `read_uncommitted` consumer / `READ UNCOMMITTED` isolation | `read_committed` |
| Backfill duplicated a day | Transaction scope = one run; plain append | `replaceWhere`, MERGE, or `txnAppId`/`txnVersion` |
| Delta write silently did nothing | Same `txnAppId` with a version <= the recorded one | Per-partition app id; bump the run version |
| First file visible although the second failed | Each statement auto-committed | `autocommit=False`, a single COMMIT |
| Storage grows after failed jobs | Orphan files of uncommitted Delta writes | VACUUM on a schedule |

## Related patterns
- **Keyed Idempotency / Merger**: give the cross-run idempotency that a single transaction lacks.
- **Data Overwrite**: a `replaceWhere` commit is both an overwrite and a job-level transaction.
- **Proxy**: an alternative visibility switch. Readers move to the new table only when the view changes.
- **Checkpointer**: Kafka/Flink transactions commit together with checkpoints, so their timeouts must be tuned together.
