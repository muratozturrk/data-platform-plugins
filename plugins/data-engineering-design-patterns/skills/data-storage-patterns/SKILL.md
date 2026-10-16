---
name: data-storage-patterns
description: Data storage design patterns for organizing and representing datasets to cut query latency and cost - Horizontal Partitioner, Vertical Partitioner, Bucket, Sorter (Z-order, clustering), Metadata Enhancer, Dataset Materializer, Manifest, Normalizer and Denormalizer (One Big Table, star/snowflake schema) - with Spark, Kafka and Airflow examples. Use when choosing partition keys, bucketing or clustering high-cardinality columns, Z-ordering, enabling data skipping with Parquet/Delta statistics, materializing views, avoiding object-store listing, or deciding between normalized and denormalized models.
---

# Data Storage Patterns

Slow, expensive queries can be fixed reactively with more compute, or proactively with a smart data layout.
These patterns cover four families:
**partitioning** (split whole rows or columns into isolated locations), **records organization** (bucket and
sort records inside storage, for high-cardinality columns where partitions don't work), **read
performance optimization** (metadata statistics, materialized results, manifests instead of listings), and
**data representation** (normalized for consistency vs denormalized for read speed).

## When to Use This Skill
- Designing a table/topic layout: choosing a partition key, granularity, bucket count or sort keys.
- Queries scan whole datasets although they filter on time or on an id.
- Joins on a big table shuffle too much, or a high-cardinality column would explode the partition count.
- Analysts on pay-per-scan engines (Athena, BigQuery) complain about cost/latency.
- The same expensive query (UNION/JOIN/aggregation) runs over and over behind a view.
- Object-store file listing is the slowest step of a reader or a loader.
- Choosing between 3NF/snowflake and star schema/One Big Table, or keeping both in sync.

## Pattern Selection Guide

| Problem / symptom | Pattern | Reference |
|---|---|---|
| Incremental jobs filter old rows on every run; filter on low-cardinality attribute (date, country) | Horizontal Partitioner | [references/horizontal_partitioner.md](references/horizontal_partitioner.md) |
| Immutable attributes repeated on every row; groups of columns need different retention/access | Vertical Partitioner | [references/vertical_partitioner.md](references/vertical_partitioner.md) |
| Frequent filter/join on a high-cardinality column (user_id); join shuffles | Bucket | [references/bucket.md](references/bucket.md) |
| Queries filter/sort on known columns; want block skipping inside partitions | Sorter | [references/sorter.md](references/sorter.md) |
| Engine loads whole files then filters; need file/row-group skipping or better plans | Metadata Enhancer | [references/metadata_enhancer.md](references/metadata_enhancer.md) |
| Expensive query behind a view re-runs on every read | Dataset Materializer | [references/dataset_materializer.md](references/dataset_materializer.md) |
| Listing files in object storage dominates read/load time; need idempotent loads | Manifest | [references/manifest.md](references/manifest.md) |
| Duplicated descriptive data, slow/inconsistent updates | Normalizer | [references/normalizer.md](references/normalizer.md) |
| Too many joins per query, read latency on normalized model | Denormalizer | [references/denormalizer.md](references/denormalizer.md) |

Rule of thumb: partition on low cardinality, bucket/sort on high cardinality, let metadata do the skipping,
and materialize or denormalize only what is read often.

## Patterns

### Horizontal Partitioner
Splits a dataset into physically isolated locations, one per value of a **partition key** (usually event or
execution date; also business keys or nested `date/country`). Readers filtering on it prune partitions.
Declared store-side (`PARTITIONED BY`, PostgreSQL `PARTITION BY RANGE`) or producer-side (Spark
`partitionBy`, Kafka partitioner). Also the base of idempotent partition overwrites.
- Gotchas: high-cardinality keys -> millions of partitions and small files (use Bucket); **skew** (hot
  partition stalls micro-batches; backpressure buffer); changing the key means rewriting data (Iceberg
  partition evolution is metadata-only).
- [Reference](references/horizontal_partitioner.md) | [Examples](examples/horizontal_partitioner/)

### Vertical Partitioner
Splits each **row** into column groups (e.g. mutable events vs immutable context) written to separate
tables/topics, linked by a shared key (`visit_id`). Saves storage and allows per-group retention and access
policies.
- Gotchas: domain split needs documentation; full rows need joins (expose a view / Dataset Materializer);
  producers must split and perform multiple writes (Spark: `persist()` the input first).
- [Reference](references/vertical_partitioner.md) | [Examples](examples/vertical_partitioner/)

### Bucket
Colocates **groups of values** in a fixed number of buckets: `hash(key) % n`. Enables **bucket pruning** on
equality filters and **shuffle-free joins** between identically bucketed tables. Spark `bucketBy`, Athena
`CLUSTERED BY ... INTO n BUCKETS`; Kafka key-hash partitions are the streaming analogue (co-partitioning).
- Gotchas: bucket schema is immutable (change = backfill); sizing for growth is hard; Spark needs
  `saveAsTable`; both join sides need the same column, count and hash.
- [Reference](references/bucket.md) | [Examples](examples/bucket/)

### Sorter
Stores rows ordered by the columns most used in filters/sorts so per-block min/max ranges are narrow and
irrelevant blocks are skipped. Lexicographic sort (BigQuery/Snowflake clustering, Redshift sort keys) or
**curved sorts / Z-order** (Delta `OPTIMIZE ZORDER BY`, Iceberg, Redshift interleaved keys) for multiple
independent predicates.
- Gotchas: composite lexicographic keys help only when leading columns are filtered; appends create
  **unsorted segments** (schedule re-sorts); changing keys may re-sort the whole table; writers pay.
- [Reference](references/sorter.md) | [Examples](examples/sorter/)

### Metadata Enhancer
Collects **statistics** (min/max, null count, row count) next to the data so readers skip irrelevant
files/row groups before loading: Parquet footers, Delta/Iceberg/Hudi commit-log stats, database planner
statistics.
- Gotchas: slight write overhead; database stats go **stale** below auto-analyze thresholds (`ANALYZE`);
  stats only help if data is clustered (pair with Sorter); Delta indexes first 32 columns by default.
- [Reference](references/metadata_enhancer.md) | [Examples](examples/metadata_enhancer/)

### Dataset Materializer
Runs an expensive query once and **stores the result** as a materialized view (auto/manual refresh) or a
table (self-managed refresh, but partitioning/bucketing/sorting allowed). Incremental refresh = Incremental
Loader + Merger for insert-only sources.
- Gotchas: refresh cost competes with users; auto-refresh timing is not guaranteed; access rules of every
  source must carry over; storage duplication (materialize only costly parts); `count + x` merges are not
  idempotent.
- [Reference](references/dataset_materializer.md) | [Examples](examples/dataset_materializer/)

### Manifest
Lists files **once** (or never, when the producer records them) into a manifest that readers/loaders use
instead of listing storage. Built into table formats (commit logs); exported via Delta
`symlink_format_manifest`; used for loading by Redshift `COPY ... MANIFEST` and GCP Storage Transfer.
- Gotchas: extra pipeline step; manifests can grow to GBs (streaming, many small files); freeze a run's
  manifest so replays load the same files (idempotency); write it after data, atomically; Redshift
  Parquet/ORC `COPY ... MANIFEST` needs `meta.content_length` per entry.
- [Reference](references/manifest.md) | [Examples](examples/manifest/)

### Normalizer
Stores each fact **once**: entities, attributes, relationships. Implemented with **normal forms** (1NF atomic
values + PK, 2NF full-key dependency, 3NF no transitive dependency) for transactional workloads, or a
**snowflake schema** (fact + nested dimensions) for analytics. Favors consistency and cheap updates.
- Gotchas: many joins (broadcast small tables, colocate/bucket); complex queries; history needs SCD
  techniques; load parents before children.
- [Reference](references/normalizer.md) | [Examples](examples/normalizer/)

### Denormalizer
Flattens joined tables into the read rows: **One Big Table** (flat columns), **nested STRUCTs**, or a **star
schema** (one dimension level). Join cost paid once at write time. Often built on top of a normalized model
kept as source of truth.
- Gotchas: updates touch many rows (treat as snapshot); storage duplication (dictionary encoding helps);
  OBT without domain logic becomes a "trash bag" (if its name needs many "and"s, split it); check for join
  fan-out.
- [Reference](references/denormalizer.md) | [Examples](examples/denormalizer/)

## Cross-cutting Best Practices
- Start from access patterns: list the top queries' predicates and joins before choosing any layout.
- Layer the techniques: partition (low cardinality) -> bucket or sort (high cardinality) -> metadata
  statistics do the skipping; verify with `EXPLAIN` / bytes scanned, not assumptions.
- Layouts are hard to change: partition keys, bucket counts and sort keys all require rewrites - prefer
  managed/evolvable options (Iceberg partition evolution, Delta liquid clustering) when unsure.
- Watch file sizes: over-partitioning and bucketing both cause small files; schedule compaction.
- Every read optimization costs writes or storage - materialize/denormalize only high-traffic data.
- Keep storage optimizations idempotent: partition-scoped overwrites, frozen manifests, run markers for
  incremental merges.
- Propagate governance (retention, access control) to every derived copy: vertical splits, materializations,
  One Big Tables.
- Secrets (DB passwords, cloud keys) come from Airflow connections / environment variables, never code.
