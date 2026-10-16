# Manifest

> Category: read (and load) path optimization. Batch; manifests also appear in streaming file sinks.

## Problem

A Parquet dataset in an object store serves batch jobs well. Then it has to be exposed to data
analysts through a new data warehouse layer, and the first tests are slower than the batch producer.
The slowest operation is **listing the files** to load from the object store, and you want to skip
that costly step. Listing is slow on object stores with many files because it means
many paginated API calls. Parallel listing helps, but there is a better way.

## Solution / Mechanics

List files **only once**, or **not at all** if the producer records filenames as it writes them.
Store that list in a **manifest** that readers and loaders use instead of listing storage.

1. **Automatic manifests (table formats)**: Delta Lake, Iceberg and Hudi write the files added by
   each transaction to the commit log. Readers get data files from commit files without listing
   storage. Those commit logs *are* manifests.
2. **Export for foreign engines**: Delta `generate('symlink_format_manifest')` writes the file list
   of the latest snapshot. A BigQuery external table points at it with
   `file_set_spec_type = 'NEW_LINE_DELIMITED_MANIFEST'`.
3. **Manual manifests**: the producer (or one preparation step) lists the output once and writes the
   manifest. This pays off when many readers share one dataset, as in the Fan-Out patterns.
4. **Manifests for loading**: Redshift `COPY ... MANIFEST` loads exactly the listed files, and
   `"mandatory": true` fails the load if a file is missing. GCP Storage Transfer Service copies from
   manifest listings.
5. **Idempotency**: create the manifest *before* loading, tie it to the job run, and reuse the same
   manifest for every replay of that run.

```
 producer job --writes--> s3://devices/input/date=D/part-*.parquet
      |                                   (no listing by readers)
      +--records file list--> manifest_<run>.json  {"entries":[{"url":..., "mandatory":true,
      |                         (frozen per run)                "meta":{"content_length":N}}]}
      v                              |
 Delta: _delta_log/*.json            +--> Redshift COPY ... MANIFEST   (replay = same files)
 = built-in manifest                 +--> BigQuery external table (symlink manifest)
                                     +--> fan-out readers: spark.read.parquet(*listed_files)
```

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Complexity | Creating the manifest is one more step in the flow | The step is simple (list recent files once) and much cheaper than slow, unpredictable listings on every read |
| Size | Many small files or a continuous streaming producer can push manifests to several GB; some implementations cap manifest size or expire entries | Compact data files (Compactor), set manifest retention, split manifests per run/partition |
| Restart failures | Early Spark Structured Streaming file sinks grew `_spark_metadata` until jobs could not restart (fixed in SPARK-27188) | Recent Spark; prefer table formats for streaming sinks |
| Snapshot semantics | A manifest describes one moment; files written later are invisible | Regenerate after each commit (Delta: `delta.compatibility.symlinkFormatManifest.enabled`) |

## Key design decisions

- **Built-in vs manual**: use a table format when you can. Use manual manifests for plain files,
  cross-system loads and fan-out readers.
- **Granularity**: per run (idempotent replays), per partition, or per table snapshot.
- **Who writes it**: the producer (zero listings) or a separate step (one listing).
- **Content**: absolute URIs, `mandatory` flag, file size (required by Redshift for columnar
  formats), optional row count/checksum for completeness checks.
- **Discovery**: fixed path per run, a `_latest` pointer, or a compacted Kafka topic.

## When to use / when not

| Situation | Use Manifest? | Alternative |
| --- | --- | --- |
| Readers of an object-store dataset spend most time listing | Yes | - |
| Data already in Delta/Iceberg/Hudi read by a native engine | Already built in | - |
| External engine (BigQuery, Athena) must read a Delta table | Yes, symlink manifest | Native connector if available |
| Replayable loads into a warehouse (Redshift COPY) | Yes, frozen per run | Staging table + Data Overwrite |
| Few files, a single reader | No, listing is cheap | Plain listing |
| Slow reads caused by scanning, not listing | No | Metadata Enhancer, Sorter, Horizontal Partitioner |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Replay loads more files than the first run | Manifest regenerated on retry | Create once, reuse if it exists (`check_for_key` in the DAG) |
| Readers load partial output | Manifest written before data files were committed | Write it last, atomically (temp + rename or single PUT); it doubles as a readiness marker |
| Redshift COPY Parquet with MANIFEST fails | Missing `meta.content_length` on entries | Add the object size per entry (`head_object` ContentLength) |
| BigQuery external table shows stale data | Symlink manifest not refreshed after new commits | Regenerate after writes or enable auto-update table property |
| Huge manifest slows readers | Small-files explosion | Compactor; per-run manifests |
| `mandatory: true` load fails | File deleted or moved by retention/compaction | Keep the manifest's files until all its loads finish |

## Implementation notes per stack

**Spark** - `examples/manifest/spark/spark_manifest.py`: Delta symlink manifest export, plus a producer-written JSON manifest for plain Parquet and a reader that loads only
the listed paths.

```python
DeltaTable.forPath(spark, f"{base}/devices_delta").generate("symlink_format_manifest")

def write_with_manifest(df, output_dir: str, run_id: str) -> str:
    run_dir = f"{output_dir}/run={run_id}"
    df.write.mode("overwrite").parquet(run_dir)
    files = sorted(os.path.abspath(p) for p in glob.glob(f"{run_dir}/*.parquet"))  # one listing
    ...
    json.dump({"run_id": run_id, "entries": [{"url": p, "mandatory": True} for p in files]}, f)

def read_from_manifest(spark, manifest_path: str):
    paths = [e["url"] for e in json.load(open(manifest_path))["entries"]]
    return spark.read.parquet(*paths)
```

**Kafka** - `examples/manifest/kafka/kafka_manifest_publisher.py`: one manifest message per finished
run on a compacted `dataset_manifests` topic (key `dataset/run_id`). Fan-out readers consume it and
load the listed files. For very large runs, publish the manifest's URI instead of the list.

```python
manifest = {"dataset": dataset, "run_id": run_id,
            "entries": [{"url": f, "mandatory": True} for f in files]}
producer.produce(TOPIC, key=f"{dataset}/{run_id}", value=json.dumps(manifest))
```

**Airflow** - `examples/manifest/airflow/manifest_dag.py`: the run's S3 prefix is listed once and a
frozen manifest written. Retries and replays reuse it. Redshift then runs `COPY ... MANIFEST` into a
staging table and does delete/insert in one transaction.

```python
if s3.check_for_key(key, bucket_name=BUCKET):
    return f"s3://{BUCKET}/{key}"  # replay: reuse the frozen file list
files = s3.list_keys(bucket_name=BUCKET, prefix=f"input/date={ds}/")
# Redshift requires meta.content_length per entry for columnar (Parquet/ORC) COPY.
body = {"entries": [{"url": f"s3://{BUCKET}/{f}", "mandatory": True,
                     "meta": {"content_length": s3.head_object(f, BUCKET)["ContentLength"]}}
                    for f in files]}
s3.load_string(json.dumps(body), key=key, bucket_name=BUCKET)
```
The COPY uses `DELETE FROM customer_staging`, not `TRUNCATE`, because TRUNCATE commits implicitly
in Redshift and would break the single transaction.

## Related patterns

- **Readiness Marker**: a manifest written last also signals that the dataset is complete.
- **Fan-Out (Parallel Split, Exclusive Choice)**: many readers share one manifest instead of listing.
- **Data Overwrite / Transactional Writer**: frozen manifests make warehouse loads idempotent.
- **Compactor**: fewer, bigger files keep manifests small.
- **Metadata Enhancer**: table-format commit logs carry both the file list and per-file statistics.
- **Incremental Loader**: a per-run manifest defines exactly which new files a run processes.
