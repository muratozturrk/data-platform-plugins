# Fine-Grained Tracker

Category: Data Lineage.

## Problem
You implemented the Denormalizer pattern to avoid costly joins. In three years the table grew
to more than 30 columns, and the team composition changes often. The Dataset
Tracker answers most questions about table dependencies, but new colleagues also ask which
upstream columns build each column of the denormalized table. Dataset-level lineage cannot
answer that.

## Solution / Mechanics
Track lineage below the dataset level: column level (which input columns compose each output
column) and row level (which job produced each row).

**Column level**
- *Managed*: Databricks Unity Catalog exposes `system.access.column_lineage`; Azure Purview
  supports it natively too. Both work under constraints (supported jobs and stores only).
- *Custom*: analyze the query execution plan and track the dependencies of each output column.

```
SELECT CONCAT(u.first_name, d.delivery_address) AS user_with_address
FROM users u JOIN addresses d ON d.user_id = u.id

 Project[user_with_address]
        |
   Concat(first_name, delivery_address)
        |                     |
 users.first_name   addresses.delivery_address      <- leaves = source columns
        \                     /
            Join(d.user_id = u.id)
```
`user_with_address` depends on `users.first_name` and `addresses.delivery_address`. Data processing tools such as Spark have native support in lineage frameworks
(OpenLineage emits a `columnLineage` facet).

**Row level**: record which job produced each row, as an extra attribute or
column, using the data decoration patterns (Metadata Decorator, Wrapper). A typical example
puts `job_name`, `job_version`, `batch_version` in Kafka headers, and the
downstream job adds `parent_lineage`, the upstream headers serialized as JSON:

```
visits --> visits_decorator_job --> visits-decorated --> visits_reducer_job --> visits-reduced
           headers:                                      headers:
           job_name=visits_decorator_job                 job_name=visits_reducer_job
           job_version=v1.2                              job_version=v3.0
           batch_version=41                              batch_version=17
                                                         parent_lineage={"job_name":"visits_decorator_job",...}
```

Any output row can be traced back to the job, version and micro-batch that produced it and
to its upstream producer, which gives producers concrete debugging details.

## Consequences & trade-offs
Three limits:
- **Custom code**: plan-based column lineage works with native SQL functions. Programmatic
  mapping functions and UDFs are opaque boxes: the framework sees the output but cannot
  interpret the transformation logic.
- **Row-level visualization**: dataset and column lineage are widely supported by extraction
  and visualization tools; row-level lineage is not. It is mostly debugging value (finding
  jobs that write erroneous rows) and needs its own query layer.
- **Evolution management**: the transformation that builds a column today may change. The
  lineage solution, managed or custom, must keep the history of definitions; otherwise it
  shows wrong origins after upstream columns change.
- Row lineage adds bytes to every record (headers or columns). Nested `parent_lineage` grows
  with each hop; cap the depth or store only the direct parent.

## Key design decisions

| Decision | Options | Notes |
|----------|---------|-------|
| Level | column, row, both | column for "how is it built", row for "who wrote it" |
| Column extraction | managed (Unity Catalog, Purview), OpenLineage facet, SQL parser (sqlglot) | parsers work on SQL only; plans see DataFrame code without UDFs |
| Row carrier | Kafka headers, extra columns, wrapper attributes | headers keep payloads unchanged (Metadata Decorator) |
| Parent depth | direct parent, full chain | full chain grows unbounded |
| History | versioned lineage documents per run or per deployment | needed for evolution management |

**When to use / when not to use**

| Use it when | Do not use it when | Alternative |
|-------------|--------------------|-------------|
| Wide or denormalized tables with many inputs per column | Table-level dependencies are enough | Dataset Tracker |
| You must debug which job version wrote bad rows | Transformations are mostly UDFs (column lineage blind) | document mappings, or row-level lineage only |
| Compliance needs column provenance (PII) | Record size is critical and rows are tiny | lineage at batch level in a side table |

## Gotchas & best practices
- Prefer native expressions over UDFs where lineage matters; isolate unavoidable UDFs and
  document their input columns.
- Store the job version (git SHA or release tag) in row lineage, not just the job name.
- `batch_version` should be the micro-batch id (Spark `foreachBatch` batch id) or the run id,
  so it joins with orchestration and checkpoint logs.
- When forwarding upstream headers, decode them safely (`errors="replace"`): headers are bytes.
- Version column lineage documents with the run date, so you can answer "what did this column
  mean last quarter?".

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Column has no lineage in OpenLineage | built by a Python UDF or `mapInPandas` | rewrite with native functions or document manually |
| Lineage UI does not show rows | row lineage is not a lineage-UI concept | query headers/columns with a separate tool |
| Old lineage looks wrong after a refactor | no history kept | store versioned lineage per run |
| Kafka records grow per hop | full nested `parent_lineage` | keep only the direct parent or a lineage id |
| sqlglot cannot resolve a column | `SELECT *` or unqualified columns without schema | pass the schema to the parser or qualify columns |

## Implementation notes per stack
**Spark** - column lineage comes from the same OpenLineage listener as the Dataset Tracker
(`../dataset_tracker/spark/openlineage_spark_job.py`), so no separate example is given. Row lineage is in
`examples/fine_grained_tracker/spark/row_level_lineage.py`:

```python
def write(batch: DataFrame, batch_number: int) -> None:
    headers = lineage_headers(job_name, batch_number)        # job_version, job_name, batch_version
    if with_parent:
        headers.append(F.struct(F.lit("parent_lineage").alias("key"),
                                F.to_json(F.col("headers")).cast("binary").alias("value")))
    batch.withColumn("headers", F.array(*headers)).select("key", "value", "headers") \
        .write.format("kafka")...
```
The reader needs `includeHeaders=true` to see the upstream headers.

**Kafka** - `examples/fine_grained_tracker/kafka/row_lineage_propagation.py`: the same
propagation with plain clients, committed after each produced batch; `--self-test` checks that
the parent's `batch_version` survives one hop.

**Airflow** - `examples/fine_grained_tracker/airflow/column_lineage_dag.py`: custom column lineage
by parsing the task's SQL with `sqlglot.lineage`, stored as one JSON document per run (history
for evolution management):

```python
for column in output_columns:
    leaves = [n for n in lineage(column, sql).walk() if not n.downstream]
    result[column] = sorted({f"{leaf.source.name}.{leaf.name.split('.')[-1]}" for leaf in leaves})
```

## Related patterns
- [Dataset Tracker](dataset_tracker.md): the coarse-grained level; the column level extends its graph.
- [Denormalizer](../../data-storage-patterns/references/denormalizer.md): the wide tables that motivate column lineage.
- [Metadata Decorator](../../data-value-patterns/references/metadata_decorator.md): how row lineage travels in headers without changing payloads.
- [Wrapper](../../data-value-patterns/references/wrapper.md): alternative carrier when lineage must be part of the record.
- [Offline Observer](../../data-quality-patterns/references/offline_observer.md): row lineage helps route quality findings to the job version that wrote the rows.
