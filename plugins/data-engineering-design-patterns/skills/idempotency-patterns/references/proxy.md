# Proxy

Family: **Immutable Dataset**. Write every run to a new, write-once location and expose the current one through a single access point: a view, an alias or a manifest. The name comes from "we can solve any problem by introducing an extra level of indirection".

## Problem
A batch job generates a full dataset on every run. Only the latest version mattered, so the job overwrote the previous one. Now the legal department requires a copy of every past version. The mutable approach no longer holds: the pipeline must keep each copy but expose only the most recent one, from a single place.

## Solution / Mechanics
The dataset must be immutable, so written only once:
1. **Write once, to a new location each run.** Use timestamped or versioned tables: regular tables whose name carries a version or timestamp suffix (`devices_internal_20240108_031500`). Remove all write permissions after creation, so each table is writable only once.
2. **On object stores, lock it**: write once read many (WORM) with S3 Object Lock, Azure Blob immutability policies, or GCS object holds and bucket locks.
3. **Create the single access point: the proxy.** Usually a passthrough view with no transformation in the `SELECT`. Works on most warehouses and RDBMSs, plus OpenSearch (aliases), Cassandra, ScyllaDB and MongoDB.
4. **No views?** Build the equivalent yourself: a **manifest** file that references the location or lists the files consumers should read. Generate it outside the data processing job, so a manifest failure does not force reprocessing, which is much slower.
5. **Natively versioned stores** (Delta Lake, Iceberg, BigQuery): an overwrite keeps the old data for time travel, so a regular table plus its versions already gives immutable copies. You cannot remove write permissions here, but the storage keeps the history, so permission management is not required. Retention may be limited and not configurable (BigQuery: seven days at the time of writing).

```
View-based
  process -> load into devices_internal_<run start> -> REVOKE writes -> CREATE OR REPLACE VIEW devices
                                                                         -> revoke reads on previous table

Manifest-based
  process -> write versions/<run start>/ (+ _SUCCESS) -> [separate step] write _manifest.json
                                                         -> remove reads on previous files

Versioned
  process -> overwrite regular table (new version)   readers: latest | VERSION AS OF n

            consumers ---> [ devices view / manifest / table ] ---> newest immutable copy
                                                          \--x--> older copies (kept, hidden)
```

Idempotency comes from immutability: a rerun never mutates an earlier output. It writes a new version and moves the pointer. The DAG is two tasks: `load_data_to_internal_table >> refresh_view`. The second recreates the view; the table name is built from `dag_run.start_date`.

## Consequences & trade-offs
| Consequence | What it means | Mitigation |
|---|---|---|
| **Database support** | Not every store has views to act as an immutable entry point. Manifests replace them, but reading gets more cumbersome. | Ship a small reader helper that resolves the manifest. |
| **Immutability configuration** | Configuring the writing task in the orchestrator is not enough. Enforce immutability in the data store too: object locks, and write permissions removed once a table exists. This usually needs the infrastructure team. | Separate loader and owner roles; WORM buckets; ACLs. |
| **Permissions of the loader** | If the loading user can delete older internal tables, even by accident, the guarantee is gone. | The loader may only create tables. |
| **Storage growth** | Every run adds a full copy. | Lifecycle rules once legal retention allows deletion. |
| **Retention on versioned stores** | Time travel only reaches as far back as log and file retention (BigQuery: seven days). | Raise retention properties, or use explicit versioned tables. |

## Key design decisions
| Decision | Guidance |
|---|---|
| Version naming | Use the **run start time** (`dag_run.start_date`) or a run ID, never the logical date. A rerun of the same interval must not rewrite the previous dataset. |
| Proxy type | View (warehouse/RDBMS) > alias (OpenSearch) > manifest (files, Kafka) > native versions (lakehouse). |
| Permissions | Loader: CREATE only. Owner of frozen tables: a separate role the loader is not a member of. Readers: SELECT on the view only. |
| Repoint step | A separate task after load and validation, so it is cheap to retry. |
| Previous version | Revoke reads on it so consumers cannot bypass the proxy; keep the data. |

## When to use / when not to use
| Situation | Use it? | Alternative |
|---|---|---|
| Legal or audit requires every version of a full dataset | Yes | - |
| Consumers need one stable name and instant rollback | Yes | - |
| Dataset can be mutated and history is not needed | No | Data Overwrite, Fast Metadata Cleaner |
| Only increments arrive | No (each version must be full) | Merger, Stateful Merger |
| Store has native versioning with enough retention | Yes, versioned approach | - |
| Huge dataset rebuilt hourly | Costly | Versioned table format, or Merger |

## Gotchas & best practices
- Validate the new version (counts, schema) **before** moving the pointer. A bad load must never be published.
- Rollback = point the view or manifest back to the previous version. Keep it readable for the owner role.
- `max_active_runs=1`: two runs racing on the view swap can publish an older table last.
- Do not create internal tables with `IF NOT EXISTS`: an existing name means a second writer, so fail.
- In PostgreSQL the owner keeps implicit privileges: transfer ownership to a frozen-owner role through a privileged connection, then revoke `INSERT, UPDATE, DELETE, TRUNCATE`.
- Readers already running against the previous table keep reading it; the swap only affects new queries.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| A rerun overwrote yesterday's internal table | Name derived from the logical date | Suffix from `dag_run.start_date`. |
| Old table modified after publication | Loader still owns it, or `PUBLIC` keeps write grants | Owner transfer + REVOKE in a freeze step. |
| Consumers query `devices_internal_*` directly | Grants on internal tables | Grant SELECT on the view, revoke reads on previous tables. |
| View points to an empty table | Repoint before validation | Validate, then refresh the view. |
| Manifest references a half-written version | Manifest written by the job before its commit | Separate step; only publish versions with a `_SUCCESS` marker. |
| Readers see no manifest for a moment | Rename on object stores is copy + delete | Tolerate a brief miss, or use a store with atomic rename (HDFS). |
| Old versions no longer readable on Delta | VACUUM after default retention | Raise `delta.logRetentionDuration` / `delta.deletedFileRetentionDuration`. |

## Implementation notes per stack

### Airflow + PostgreSQL (main implementation)
[`examples/proxy/airflow/immutable_tables_view_dag.py`](../examples/proxy/airflow/immutable_tables_view_dag.py): load, validate, freeze, refresh the view, revoke reads on the previous table.
```python
def devices_table_name_for(start_date: datetime) -> str:
    return f"dp.devices_internal_{start_date.strftime('%Y%m%d_%H%M%S')}"

@task
def freeze(table: str) -> str:
    hook(ADMIN_CONN_ID).run(f"ALTER TABLE {table} OWNER TO {FROZEN_OWNER_ROLE}", autocommit=True)
    hook(ADMIN_CONN_ID).run(freeze_statements(table), autocommit=True)  # REVOKE writes
    return table

table = freeze(validate(load_data_to_internal_table()))
remove_read_access_from_previous(refresh_view(table))  # CREATE OR REPLACE VIEW dp.devices
```

### Spark + Delta
[`examples/proxy/spark/spark_versioned_tables_proxy.py`](../examples/proxy/spark/spark_versioned_tables_proxy.py): `--mode view | manifest | versioned`, one per implementation scenario.
```python
# view: write-once table, then repoint
df.write.format("delta").mode("errorifexists").saveAsTable(table)
spark.sql(f"CREATE OR REPLACE VIEW dp.devices AS SELECT * FROM {table}")

# manifest: separate step, newest version that has _SUCCESS, temp file + rename(OVERWRITE)
doc = manifest_document(version, location, spark.read.parquet(location).inputFiles())

# versioned: plain overwrite; history via SELECT ... VERSION AS OF n
df.write.format("delta").mode("overwrite").saveAsTable("dp.devices_versioned")
```

### Kafka (adaptation, manifest approach)
[`examples/proxy/kafka/kafka_versioned_topics_proxy.py`](../examples/proxy/kafka/kafka_versioned_topics_proxy.py): one `devices_v<run_start>` topic per snapshot (`retention.ms=-1`), then a pointer record in a compacted topic.
```python
create_snapshot_topic(admin, topic)              # fails if the topic exists
count = produce_all(producer, topic, records)    # idempotent producer, flush + error check
producer.produce(POINTER_TOPIC, key=b"devices", value=pointer_value(topic, count))
...
topic = resolve_pointer()                        # readers: latest pointer -> snapshot topic
```
Enforce immutability with ACLs: deny Write on `devices_v*` for everyone but the publisher, and drop the publisher's Write once the pointer has moved.

## Related patterns
- **Fast Metadata Cleaner**: also puts a view over physical tables, but those tables are truncated and reused.
- **Data Overwrite**: the mutable approach this pattern replaces when history must be kept.
- **Transactional Writer**: another way to make a whole output visible at once; Proxy gets it from the pointer swap.
- **Stateful Merger**: also relies on table versions, there to restore before a backfill.
- **Readiness Marker**: the `_SUCCESS` check before publishing a manifest is that pattern.
