# Zero-ETL Synchronizer

Category: Streaming Data Ingestion.

## Problem
After downsizing, a small data platform team maintains a platform built by a team four times
larger. You must optimize workflows and reduce complexity. The quickest win is
the streaming ingestion applications whose only purpose is to take data out of Kafka and move
it to the lakehouse.

## Solution / Mechanics
Remove the pipeline: let the streaming broker or vendor service synchronize topics into
lakehouse tables. There is no fully standardized open source solution for Kafka
yet; most vendors offer a managed implementation, with varying support for Iceberg, Hudi and
Delta Lake. An open source proposal from Aiven (August 2025) converts Kafka's row format into
Parquet-backed Iceberg tables through the tiered storage remote storage manager.

```
 before:  producers --> Kafka topic --> ingestion job (code, deploy, monitor) --> lakehouse table
 after :  producers --> Kafka topic ==(broker/vendor sync, configuration only)==> lakehouse table
                                               |                                    |
                                    catalog updates (schema, table)        you: monitoring,
                                                                           compaction, tuning
```

No code, but configuration:
1. **Output location** where the topic is synchronized, usually transparently converted into
   Parquet files.
2. **Access permissions** so the synchronizer can write the converted records.
3. **Data catalog** that receives all changes related to written data (schema changes, table name).
4. **Synchronization tuning**, for example size or throughput of the sync process.

Three implementations:

| Implementation | Mechanism | Configuration |
|----------------|-----------|---------------|
| Aiven Iceberg topics (open source proposal) | `RemoteStorageManager` converts **closed segments** into Iceberg tables | broker `remote.log.storage.*` + `rsm.config.iceberg.*` (namespace, REST catalog, S3 warehouse) |
| Bufstream | archive to Iceberg per topic | `iceberg.catalogs` in the broker config + topic configs `bufstream.archive.kind=ICEBERG`, `...iceberg.catalog`, `...iceberg.table` |
| Confluent Tableflow | managed sync to Iceberg or Delta | UI or Terraform (recommended): topic id, S3 bucket/external location, catalog type, credentials |

In Bufstream development environments you trigger the sync with `admin clean topics`; the
managed service syncs on its schedule interval.

## Consequences & trade-offs
Three gotchas plus a warning:
- **Not zero effort**: you no longer write or maintain the pipeline, but you still need
  metrics, monitoring and performance tuning of the created tables: scheduled compaction,
  partition tuning, and observability of whether the sync performs well.
- **Zero transform**: there is no ETL between source and destination. Transformations can only
  run after the records are archived in the lakehouse. If you need them during the copy, use
  the Transformation Replicator pattern instead.
- **Data transformation at the source**: keep the synced raw data as-is. Transforming the
  origin tables in place is not forbidden but risky because changes may not be restorable.
  Write transformed records to a new table.
- **Time travel limits**: do not rely on table time travel as your immutability guarantee.
  Retention and compaction policies eventually remove older states.
- Latency follows the sync mechanism. Segment-based conversion only exports **closed**
  segments, so freshness depends on `segment.ms`/`segment.bytes` and the vendor schedule.
- Vendor lock: the sync is a feature of the broker or vendor, so changing brokers means
  changing the ingestion path.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Vendor / mechanism | Aiven plug-in, Bufstream, Tableflow, other managed offerings | table format support differs (Iceberg, Delta, Hudi) |
| Table format | Iceberg, Delta | match your query engines and catalog |
| Catalog | Iceberg REST, Unity Catalog, Glue | the sync publishes schema changes there |
| Schema source | schema registry (Avro/Protobuf), JSON inference | a registry gives stable column types |
| Freshness | segment roll, sync interval | trade-off between file size and latency |
| Housekeeping owner | your orchestrator, vendor maintenance | check which operations the vendor already runs |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Jobs only copy topics to lakehouse tables 1:1 | Data must be transformed, filtered or anonymized before landing | Transformation Replicator |
| Your broker or vendor supports your table format | You need cross-vendor portability today | Passthrough Replicator job (e.g. Kafka Connect, Spark) |
| Raw copies are needed for BI and reprocessing | You need sub-second freshness in the table | streaming job with small triggers |

## Gotchas & best practices
- Monitor table freshness (latest snapshot or commit time) like any ingestion job; a
  permission error stops the sync silently.
- Run compaction, snapshot expiration and manifest rewrites on a schedule unless the vendor
  does it.
- Keep secrets (Databricks tokens, cloud keys) in a secret store and reference them from
  Terraform variables; never inline them.
- Schema evolution on the topic propagates to the table; protect it with schema compatibility
  rules at the producer side.
- Derived tables read the synced table incrementally (snapshot or version ranges) instead of
  rewriting it.

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Table not updated for hours | permissions, catalog errors, or segments not closing | check sync logs, lower `segment.ms`, alert on snapshot age |
| Many small files, slow queries | frequent sync commits | scheduled `rewrite_data_files` / OPTIMIZE |
| Time travel to last month fails | snapshots expired by retention | keep immutable copies in a separate table if needed |
| Columns typed as strings | no schema registry | register Avro/Protobuf schemas for the topic |
| Need to mask a column | zero transform | derived table, or Transformation Replicator instead |

## Implementation notes per stack
**Kafka (configuration)** - `examples/zero_etl_synchronizer/kafka/README.md` collects the
Aiven, Bufstream and Tableflow settings:

```properties
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=io.aiven.kafka.tieredstorage.RemoteStorageManager
rsm.config.segment.format=iceberg
rsm.config.iceberg.namespace=dp
rsm.config.iceberg.catalog.class=org.apache.iceberg.rest.RESTCatalog
rsm.config.iceberg.catalog.uri=http://localhost:8181
rsm.config.iceberg.catalog.warehouse=s3://dp/iceberg
```

**Spark** - `examples/zero_etl_synchronizer/spark/synced_table_housekeeping.py` implements the
"not zero effort" part on the synced Iceberg table: snapshot freshness, housekeeping, and
transformation into a new table:

```python
last = spark.sql(f"SELECT MAX(committed_at) AS last FROM {TABLE}.snapshots").collect()[0].last
spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'dp.visits', ...)")
spark.sql("CALL lakehouse.system.expire_snapshots(table => 'dp.visits', older_than => ..., retain_last => 50)")
spark.sql(f"INSERT OVERWRITE {DERIVED_TABLE} SELECT * FROM {TABLE} WHERE page IS NOT NULL")
```

**Airflow** - `examples/zero_etl_synchronizer/airflow/housekeeping_dag.py` schedules that job
nightly with `SparkSubmitOperator`.

## Related patterns
- [Transformation Replicator](../../data-ingestion-patterns/references/transformation_replicator.md): the alternative when data must change during the copy.
- [Passthrough Replicator](../../data-ingestion-patterns/references/passthrough_replicator.md): the job-based version of the same raw copy.
- [Compactor](../../data-ingestion-patterns/references/compactor.md): the housekeeping that remains your job.
- [Hybrid Source](hybrid_source.md): synced tables are a typical historical source; beware of interleaving with the topic.
- [Batch Reprocessor](batch_reprocessor.md): the raw lakehouse copy makes reprocessing possible after topic retention expires.
- [Flow Interruption Detector](../../data-observability-patterns/references/flow_interruption_detector.md): detects a stalled sync.
