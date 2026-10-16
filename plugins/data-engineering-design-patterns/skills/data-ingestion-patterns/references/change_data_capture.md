# Change Data Capture (CDC)

Category: data loading. One-liner: continuously stream every committed change from the database **commit log**.

## Problem
The legacy visits integrated with the Incremental Loader are too slow; downstream consumers complain about the
wait. The product manager asks to put these transactional records into the streaming broker: the job must
**capture each change from the table within 30 seconds** and make it available to other consumers from a
central topic.

## Solution / Mechanics
The Incremental Loader cannot meet this latency: job scheduling and query execution overheads are too high
. CDC continuously ingests all modified rows **directly from the internal database commit log**,
a lower-level and faster access path than any query.
- A *commit log* (WAL in PostgreSQL, binlog in MySQL, redo log in Oracle) is append-only: every operation on
  existing rows is appended at the end. The CDC consumer streams those entries to a broker or other output.
- Consumers then do what they want: keep the whole history, or only the latest value per row.
- CDC intercepts **all** operations, including **hard deletes**, so producers don't need soft deletes.

```
 PostgreSQL                 Kafka Connect + Debezium             Kafka                    consumers
 ┌──────────┐  pgoutput    ┌─────────────────────────┐   dp.dp_schema.events   ┌───────────────────┐
 │ table A  │──logical────►│ PostgresConnector       │──► topic per table ────────►│ Spark: dedupe by  │
 │ table B  │  replication │ (slot + publication)    │──►             │ LSN, MERGE/DELETE │
 └──────────┘   slot       └─────────────────────────┘                             └───────────────────┘
       Lake-native alternative: Delta table with delta.enableChangeDataFeed=true ──readChangeFeed──► stream
```

Step by step (Debezium):
1. Prepare the DB: `wal_level=logical`, a replication user, a publication; Debezium creates a slot.
2. Register the connector: connection, `schema.include.list`/`table.include.list`, `topic.prefix`.
   Table `dp_schema.events` lands in topic `dp.dp_schema.events`.
3. Optionally snapshot the existing rows (`snapshot.mode=initial`), then stream changes.
4. Consumers parse the envelope (`op`, `before`, `after`, `source.lsn`, `ts_ms`), dedupe per key by log position,
   apply upserts/deletes.

Step by step (Delta Change Data Feed):
1. Enable per table (`TBLPROPERTIES (delta.enableChangeDataFeed = true)`) or per session
   (`spark.databricks.delta.properties.defaults.enableChangeDataFeed`).
2. `readStream.format("delta").option("readChangeFeed", "true").option("startingVersion", 0)`; throttle with
   `maxFilesPerTrigger`/`maxBytesPerTrigger`.
3. Rows carry `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`), `_commit_version`,
   `_commit_timestamp`.

## Consequences & trade-offs
- **Complexity**. Full and incremental loaders only need compute + orchestration. CDC needs DB-level
  setup (commit log access, logical replication, slots, privileges), often with the operations team.
- **Data scope**. Depending on the client you may only get changes made after the client started.
  If you need history, combine with another pattern (Full Loader snapshot first).
- **Payload**. Records carry extra metadata: operation type, modification time, column types.
  Consumers must adapt and ignore irrelevant attributes (e.g. Debezium `ExtractNewRecordState` unwrap SMT).
- **Data semantics**. Data at rest becomes data in motion. A stream-stream JOIN without a match does
  not mean "no matching data", only "not there *yet*": don't treat CDC output as static data.
- Operational risk: a stalled consumer/connector keeps the replication slot open, the WAL is retained and the
  source disk can fill up.

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Capture technology | Debezium on Kafka Connect · Debezium Server/Embedded · Delta CDF · cloud DMS | Debezium for OLTP sources; CDF when the source is already a Delta table |
| Snapshot | `initial` · `never` · separate Full Loader | Initial snapshot or a full load first if consumers need history |
| Envelope | full (`before`/`after`) · unwrapped (`ExtractNewRecordState`) | Unwrap for simple upserts; keep full envelope for audit/SCD2 |
| Deletes | tombstones · `delete.handling.mode=rewrite` (`__deleted`) | Keep tombstones on compacted topics; rewrite for consumers that can't handle null values |
| Ordering key | Kafka message key = primary key | Per-key order is guaranteed only within a partition |
| Consumer apply | append history · MERGE latest | Dedupe per key by LSN/`_commit_version` inside each micro-batch before MERGE |
| Secrets | inline · config provider | `${env:...}`/`${file:...}` config providers, never plain text in the connector JSON |

## When to use / when not
| Situation | Use CDC? | Alternative |
|---|---|---|
| Seconds-level latency from an OLTP DB | Yes | - |
| Hard deletes must propagate | Yes | Soft deletes + Incremental Loader |
| Every intermediate change matters (audit, SCD2) | Yes | - |
| No access to DB internals / ops can't enable logical replication | No | Incremental Loader (or outbox table) |
| Small, rarely changing reference table | No (overkill) | Full Loader |
| Source is a Delta table | Yes, via CDF | - |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Source disk filling up | Inactive replication slot retaining WAL | Monitor slot lag, alert, drop abandoned slots; heartbeat (`heartbeat.interval.ms`) |
| Old rows missing in the target | Connector started after the data existed, `snapshot.mode=never` | Initial snapshot or Full Loader bootstrap |
| Older value overwrites newer one | Out-of-order application across partitions or batches | Dedupe by LSN/`_commit_version` per key; condition MERGE on version |
| Deleted rows still in target | Tombstones/`op=d` ignored | Handle `op='d'` with `whenMatchedDelete` |
| Updates appear twice in Delta CDF | Both `update_preimage` and `update_postimage` applied | Filter out `update_preimage` |
| JOIN on CDC streams returns nothing | Treating streams as static tables | Stream-stream joins with watermarks or a state store |
| Connector config leaks passwords | Plain text `database.password` | Config providers + secret store |

## Implementation notes per stack

### Kafka (Debezium)
`examples/change_data_capture/kafka/debezium_postgres_connector.json` (plus unwrap):
```json
"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
"database.password": "${env:DEBEZIUM_DB_PASSWORD}",
"plugin.name": "pgoutput", "slot.name": "debezium_slot",
"table.include.list": "public.orders", "topic.prefix": "dbserver1",
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
```
`setup_postgres_for_cdc.sql` prepares the DB; `register_debezium_connector.py` POSTs the config once.

### Spark
`examples/change_data_capture/spark/spark_cdc_streaming_consumer.py`: parse the Debezium envelope, keep the
latest change per key (`row_number()` over `partitionBy(key).orderBy(desc("lsn"))`), `foreachBatch` MERGE for
c/u/r and DELETE for d. `spark_cdc_batch_replay.py` replays a bounded offset range for reconciliation.

`examples/change_data_capture/spark/spark_delta_change_data_feed.py` (with apply logic):
```python
events = (spark.readStream.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", args.starting_version)
          .option("maxFilesPerTrigger", args.max_files_per_trigger)
          .load(args.source))
latest = (changes.where(F.col("_change_type") != "update_preimage")
          .withColumn("_rn", F.row_number().over(
              Window.partitionBy(KEY).orderBy(F.col("_commit_version").desc())))
          .where("_rn = 1"))
```
MERGE conditions compare `_commit_version` with the stored `_source_version`, so replayed batches never
regress a row.

### Airflow
`examples/change_data_capture/airflow/cdc_pipeline_monitor_dag.py`: CDC runs continuously, so Airflow is the
watchdog, not the scheduler: connector status via the Connect REST API, auto-restart of failed tasks,
consumer-lag check, and a reconciliation DAG trigger when lag passes a threshold.

## Related patterns
- **Incremental Loader** - simpler alternative when latency and hard deletes don't matter.
- **Full Loader** - bootstraps history CDC can't see (data scope).
- **Compactor (Kafka log compaction)** - keeps only the latest record per key on CDC topics.
- **Passthrough Replicator** - replicating CDC topics to another cluster keeps keys, headers and order.
- **Dead-Letter** - for change events the consumer cannot parse or apply.
