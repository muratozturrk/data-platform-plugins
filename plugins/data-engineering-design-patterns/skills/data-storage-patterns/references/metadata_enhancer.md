# Metadata Enhancer

> Category: data storage, read optimization. Columnar files, table formats, RDBMS/warehouses.

## Problem

You partitioned a JSON dataset by event time and batch jobs got faster. Then new data analysts
started querying small subsets of rows inside one partition. Partitions are big, so their queries
are slow, and on a pay-as-you-go engine they're expensive too. The analysis shows each query
**loads the full data and filters afterwards**. You want to filter **before** loading.

## Solution / Mechanics

Skip irrelevant data files before reading them, by collecting and persisting statistics about the
stored records.

1. **Write in a format that carries statistics.** Columnar formats like Apache Parquet store a
   footer per file with per-column metadata (min, max, null count, row count), computed at write
   time by the producer. The statistics describe only that file's values.
2. **The reader evaluates the predicate against footers first.** For `WHERE age > 50`, a file
   whose `max(age) <= 50` is never opened. Footers are small, so reading all of them
   costs far less than reading the data.
3. **Table formats add a second layer.** Delta Lake, Iceberg and Hudi store per-file stats in the
   commit log (`numRecords`, `minValues`, `maxValues`, `nullCount`). Readers skip files without
   opening any footer, and can answer counts or impossible-value filters quickly.
4. **Databases and warehouses** keep table statistics in a separate catalog that the query
   planner uses to pick execution plans.
5. **Keep statistics fresh**: refresh them (`ANALYZE TABLE`) when automatic updates lag.

```
 query: SELECT ... WHERE age > 50
          |
          v
 _delta_log/000..05.json  add.stats per file          (layer 2: commit log)
   part-0  age[0..22]    -> skip
   part-1  age[23..45]   -> skip
   part-2  age[46..67]   -> candidate --+
   part-3  age[68..89]   -> candidate --+
                                        v
 Parquet footer: row groups' min/max     (layer 1: file footer)
   part-2 rg0 age[46..49] -> skip  rg1 age[50..67] -> read
   part-3 rg0 age[68..89] -> read
          |
          v
 only the selected row groups are decoded
```

Statistics skip data only if values are **clustered** per file. Random layouts give every file a
`[min..max]` covering the whole domain, so pair this pattern with the Sorter.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Overhead | Writers compute and keep stats for each column: a slight write-time cost | Limit indexed columns (Delta `dataSkippingNumIndexedCols`) to the ones queried |
| Store must keep stats up to date | RDBMS/warehouse planners use outdated numbers and pick bad plans | Run the refresh command after big loads |
| Out-of-date statistics | Automatic updates fire only past thresholds (rows modified since last update); small repeated changes never trigger them | Manual `ANALYZE TABLE`, which adds temporary read load on the database |
| Footer reading | All footers still have to be read when there is no commit-log layer | Table formats (log stats) or a Manifest avoid listing and opening |

## Key design decisions

- **Format**: Parquet/ORC (footer stats) vs table format (log stats as well) vs raw JSON/CSV (no
  stats; convert first).
- **Which columns get stats**: Delta indexes the first 32 columns by default. Put filter columns early.
- **Layout**: sort or Z-order on the filter columns so min/max ranges are narrow.
- **Row group / file size**: smaller row groups give finer skipping but more footer overhead.
- **RDBMS refresh policy**: rely on autovacuum/auto-analyze thresholds, or analyze explicitly
  after each load when tables are big and changes small.
- **Extended statistics** for correlated columns (PostgreSQL `CREATE STATISTICS`) when planner
  estimates are off.

## When to use / when not

| Situation | Use Metadata Enhancer? | Alternative |
| --- | --- | --- |
| Selective filters inside large partitions | Yes, columnar format + sorted layout | - |
| Pay-per-scan engines (Athena, BigQuery) | Yes, scanned bytes drop | Partitioning for coarse pruning |
| Counting rows / checking existence often | Yes, commit-log stats answer it | Dataset Materializer for complex aggregates |
| Random values across all files | Limited effect | Sorter first |
| Raw JSON/CSV landing zone | No stats available | Convert to Parquet/Delta |
| Slow plans in PostgreSQL after small daily loads | Yes, refresh stats | - |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| No `PushedFilters` in Spark plan | Filter on an expression/UDF, or type mismatch | Filter on the raw column with a literal of its type |
| Files not skipped despite stats | Values not clustered; each file spans the whole range | Sort/Z-order on the filter column |
| Delta skips on column A but not B | B is beyond the first 32 indexed columns | Reorder schema or raise `delta.dataSkippingNumIndexedCols` |
| String stats look truncated | Formats truncate long min/max strings | Expected; skipping still works on prefixes |
| PostgreSQL plan regressed after loads | `n_mod_since_analyze` below auto-analyze threshold | `ANALYZE` after load (DAG below) |
| Query slow on "metadata" step | Millions of small files, millions of footers | Compactor; table-format log stats |

## Implementation notes per stack

**Spark** - `examples/metadata_enhancer/spark/spark_metadata_enhancer.py`: writes Parquet (stats
created under the hood) and prints footer stats with pyarrow, the equivalent of
the `parquet-tools meta` Docker command. Then it writes Delta and prints the
`add.stats` JSON from `_delta_log`, and runs a selective query.

```python
devices = devices.repartitionByRange(4, "age")  # disjoint per-file ranges -> more skipping
devices.write.mode("overwrite").parquet(f"{base_dir}/parquet")
for path in sorted(glob.glob(f"{parquet_dir}/*.parquet"))[:2]:
    meta = pq.ParquetFile(path).metadata
    col = meta.row_group(0).column(col_idx)
    print(f"  {col.path_in_schema}: min={col.statistics.min} max={col.statistics.max}")
query = spark.read.format("delta").load(f"{base_dir}/delta").filter("age > 80")
query.explain()  # PushedFilters: [IsNotNull(age), GreaterThan(age,80)]
```

**Kafka** - `examples/metadata_enhancer/kafka/kafka_time_index_seek.py`: Kafka has no column
statistics, but each segment keeps a time index (timestamp -> offset). `offsets_for_times` jumps
to the first record at or after a timestamp instead of scanning, and producer headers
(`schema_version`) let consumers skip records without deserializing them.

```python
lookups = [TopicPartition(topic, p, since_ms) for p in partitions]
starts = consumer.offsets_for_times(lookups, timeout=10)
consumer.assign([tp for tp in starts if tp.offset >= 0])
headers = dict(msg.headers() or [])
if headers.get("schema_version", b"1") != b"1":
    continue  # header metadata lets us skip without parsing the value
```

**Airflow** - `examples/metadata_enhancer/airflow/metadata_enhancer_dag.py`: after a load, it reads
`pg_stat_user_tables.n_mod_since_analyze` and branches to `ANALYZE` (plus extended statistics)
when more than 2% of rows changed. This is the out-of-date statistics mitigation.

```python
row = PostgresHook(postgres_conn_id=CONN_ID).get_first("""
    SELECT n_mod_since_analyze, GREATEST(n_live_tup, 1), last_analyze, last_autoanalyze
    FROM pg_stat_user_tables WHERE schemaname = 'dp' AND relname = %s""", parameters=(TABLE,))
modified, live, *_ = row
return "refresh_statistics" if modified / live > MAX_STALE_RATIO else "statistics_fresh"
```

## Related patterns

- **Sorter**: makes min/max ranges narrow; without it the statistics rarely exclude a file.
- **Horizontal Partitioner**: directory-level pruning; Metadata Enhancer prunes inside partitions.
- **Manifest**: commit logs are manifests, and the same log holds both the file list and its stats.
- **Compactor** (data ingestion patterns): fewer, bigger files mean fewer footers to read.
- **Schema Migrator** (data quality patterns): wide schemas push filter columns past the 32 indexed Delta columns.
