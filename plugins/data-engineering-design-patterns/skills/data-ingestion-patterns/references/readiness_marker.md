# Readiness Marker

Category: data readiness. One-liner: mark a dataset as **complete** so consumers start processing at the
right moment; consumers **pull** (check) the marker.

## Problem
Every hour a batch job prepares data in the Silver layer: known quality issues fixed, enriched with context from
the users' database and an external API. Other teams build ML models and BI dashboards on it, and they often
complain about **incomplete datasets**. They ask for a mechanism that notifies them, directly or indirectly,
when they can start consuming.

## Solution / Mechanics
The issue shows up between logically dependent but physically isolated pipelines owned by different teams:
your job can't trigger their pipelines directly. Instead you **mark** the dataset as ready.
Two implementations:
1. **Event/flag file**: a file created after successful data generation. Native in many processing layers:
   Spark writes `_SUCCESS` for raw file formats, Delta Lake writes a new commit log entry. Otherwise the
   orchestration layer creates the flag in a separate task after successful processing.
2. **Convention for partitioned data**: an hourly job writes partition 10 at 10:00, 11 at 11:00. A
   consumer that wants partition 10 waits until the producer works on partition 11.

```
 producer DAG                                                consumer DAG (pull)
 delete_dataset ──► generate_dataset ──► create_readiness_file    FileSensor(.../COMPLETED, mode="reschedule")
 (rm -r dir)        (data files)          (COMPLETED, LAST task)          │ exists?
                                               │                          ▼
 dataset_dir/ part-0000.json ... COMPLETED ◄───┘                    process dataset_dir
 Spark: data files first, then _SUCCESS (written by the committer)
 Convention: date=10 is ready when date=11 appears
```

Step by step:
1. (Re)generation starts by removing the previous output and its marker, so a stale marker can't survive.
2. Write all data files.
3. Write the marker as the **very last** step, after the last transformation, atomically (temp + rename).
4. Consumers wait for the marker (sensor in reschedule mode) and only then read; optionally validate marker
   metadata (row count, schema hash, version).

## Consequences & trade-offs
- **Lack of enforcement**. Nothing forces consumers to wait for the flag or the next partition; they can
  start while you're still generating. Communicate the convention and the risks of ignoring it.
- **Reliability for late data**. With event-time partitions, late data for a closed partition
  (e.g. 9 o'clock data detected after 8, 9 and 10 were consumed) is integrated where consumers won't look again.
  Either treat closed partitions as immutable or define and share mutability rules, and notify consumers about new
  data (Late Data).
- **Pull costs.** Consumers poll; poorly configured sensors hold orchestrator slots (use `reschedule`).
- Marker without data validation can still announce a broken dataset: put count/schema metadata in it.

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Marker type | processing-layer native (`_SUCCESS`, Delta commit) · orchestrator flag file · DB row · event | Native first; orchestrator flag when the processing layer can't write one |
| Marker content | empty · JSON metadata | Row count, schema hash, table version, run id: lets consumers validate |
| Atomicity | direct write · temp + rename · DB upsert | Temp + rename on file systems; upsert keyed by (dataset, partition) in DBs |
| Partition convention | flag per partition · "next partition exists" | Next-partition only with strictly sequential, immutable partitions |
| Marker location | inside the dataset dir · separate marker dir/table | Inside for file datasets (deleted with the data); separate for tables shared by many writers |
| Sensor mode | `poke` · `reschedule` · deferrable | `reschedule` (or deferrable) with realistic `timeout` |
| Reprocessing | overwrite in place · delete marker first | Delete marker (or whole dir) first so consumers don't read a mix |

## When to use / when not
| Situation | Use Readiness Marker? | Alternative |
|---|---|---|
| Consumers in other teams/pipelines read your dataset on their own schedule | Yes | - |
| Producer can't (or must not) know its consumers | Yes | - |
| Producer must notify immediately and data arrival is unpredictable | Partly | External Trigger (push) |
| Single DAG owns producer and consumer | No | Plain task dependencies |
| Table format with atomic commits and consumers reading "latest" | Often not needed | Rely on commits; marker only for business completeness |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Consumers read partial data despite the marker | Marker written before the last task, or before data commit | Marker is the last task, after all writes |
| Old marker makes consumers read half-rewritten data | Reprocessing kept the previous marker | `delete_dataset` first, or delete the marker before rewriting |
| Sensor never fires | Templated path not rendered (custom sensor without `template_fields`), wrong partition naming | Declare `template_fields`; log the resolved path |
| Orchestrator out of worker slots | Sensors in `poke` mode waiting for hours | `mode="reschedule"`, deferrable sensors, timeouts |
| `_SUCCESS` missing for Delta | Delta doesn't use the Hadoop committer marker the same way | Use the Delta commit/version or write an explicit marker |
| Late events never reach consumers | Event-time partition closed and consumed | Immutable partitions + separate late-data partitions, or notify consumers |
| Consumers ignore the marker | Convention not documented | Document it in the data contract; validate in consumer sensors |

## Implementation notes per stack

### Spark
`_SUCCESS` for free:
```python
dataset = spark_session.read.schema('...').json(f'{base_dir}/input')
dataset.write.mode('overwrite').format('parquet').save('devices-parquet')   # data files, then _SUCCESS
```
`examples/readiness_marker/spark/spark_readiness_marker_writer.py` writes a richer `_READY` after the Delta
commit: JSON with row count, schema hash and Delta version, to `_READY.tmp` then `fs.rename()` to `_READY`.
`spark_readiness_marker_db.py` stores markers in a table with `UNIQUE(dataset, partition_date)` and
`ON CONFLICT DO UPDATE`.

### Airflow
`examples/readiness_marker/airflow/readiness_marker_flag_file_dag.py`:
```python
@task
def create_readiness_file(dataset_dir: str) -> str:
    tmp, final = f"{dataset_dir}/.COMPLETED.tmp", f"{dataset_dir}/COMPLETED"
    with open(tmp, "w") as marker_file:
        marker_file.write("")
    os.replace(tmp, final)
create_readiness_file(generate_dataset(delete_dataset()))
...
FileSensor(task_id="wait_for_spark_success", filepath=f"{SPARK_OUTPUT_DIR}/_SUCCESS", mode="reschedule")
```
`readiness_marker_dag.py` shows a custom `DatasetReadinessSensor` on the DB marker that also validates minimum
rows and schema hash (`template_fields = ("partition_date",)` so `{{ ds }}` renders).

### Kafka
`examples/readiness_marker/kafka/kafka_readiness_marker.py` publishes a readiness record on a compacted
`pipeline.readiness` topic keyed `dataset:partition_date` (`acks="all"`), and `wait_for_readiness()` reads it.
Consumers still decide when to look (pull); if the record should start work, that's an External Trigger.

## Related patterns
- **External Trigger** - the push counterpart: the producer notifies instead of consumers checking.
- **Incremental Loader** - the partition-based loader waits on the marker or next partition.
- **Passthrough / Transformation Replicator** - copy only complete datasets; announce the copy with a marker.
- **Late Data patterns** - deal with data arriving after a partition was marked ready.
- **Compactor** - don't mark raw-format partitions ready while compaction rewrites them.
