# Metadata Decorator

> Category: data decoration, technical context. Streaming and batch.

## Problem

Your streaming jobs change often, and a new version ships almost every week. Deployments go
fine, but you can't see what a release did to the data it produced. To make maintenance easier
you want each generated record to carry technical context such as the job version, but that
context must not reach the records end users read.

The Wrapper is the wrong tool here because it puts the context inside the record. End users don't
care whether job v1 or v2 wrote a row, and seeing it can mislead them.

## Solution / Mechanics

Write the technical context to the **metadata layer of the data store**, beside the data and
not in it. How you do that depends on what the store supports:

| Store | Metadata carrier | Granularity |
| --- | --- | --- | --- |
| Apache Kafka | record headers (key/value pairs next to key and value) | per record |
| Object store | object tags / user metadata | per file |
| Warehouse / RDBMS / NoSQL (no native metadata) | dedicated column hidden behind a view or permissions | per record |
| Same | separate context table referenced by an id (normalized) | per run |

Steps:

1. **Pick the fields**: job/code version, run or batch id, attempt, processing time, and the source
   position (topic/partition/offset or input file).
2. **Generate them from immutable context**: the micro-batch id, the orchestrator's execution date
   and `try_number`. A retry then writes the same identifiers, with an updated attempt.
3. **Attach them** through the store's native mechanism (headers, tags). Without one, use a hidden
   column or a context table.
4. **Restrict access**: business users read a view without the technical column, and only the
   technical role can read the context table.
5. **Propagate**: every hop that re-produces records keeps upstream headers and replaces only the
   keys it owns.

```
                  Kafka                                       Warehouse (no headers)
 visits --> [ decorator job ] --> visits-decorated     staging --> [ load run ] --+--> dp.visits_2024_23
               |                  key   : unchanged                  |            |    ..., visits_context_
               |                  value : unchanged                  |            |    execution_date_time (FK)
               +--- headers ----> job_version=v1.0.3                 |            |
                                  batch_version=42                   +--> dp.visits_context (1 row per run)
                                  processing_time=...                     execution_date_time | loading_time
                                  source=visits/3/1234                    code_version | loading_attempt
 business consumers: read value only               business users: view dp_public.visits_current_week
 operators: read headers                            (no technical column); context table: technical role only
```

**Wrapper vs Metadata Decorator**: the mechanics are close, but the meaning
differs. Metadata describes the data and is not exposed to business users. The Wrapper decorates
business attributes, and technical ones when needed, in a place where consumers can see them.

## Consequences & trade-offs

| Consequence | What happens | Mitigation |
| --- | --- | --- |
| Implementation: store support | The biggest limit. Some brokers have no metadata at all: Amazon Kinesis Data Streams has no headers | Context table keyed by run, or a field inside the value that consumers are told to ignore (this turns into a Wrapper) |
| Implementation: tables | An extra column plus a view/ACL, or an extra table plus a join. More work than native headers | Generate the view and grants in the same pipeline that creates the table |
| Data | Business attributes in metadata (shipping address, invoice amount) stay invisible, because consumers never query the metadata part | Only data *about* the data goes to metadata; business values belong in the record or in a Wrapper |
| Size | Headers are bytes on every record; per-record columns duplicate the context | Keep the header set small; use the normalized context table for per-run context |
| Visibility | Hidden context is easy to lose: a re-producing hop drops headers, a `SELECT *` view exposes a column | Propagate explicitly; list view columns explicitly |

## Key design decisions

- **Per record or per run?** Use headers or a hidden column when the value changes per record
  (source offset). Use a context table when it is constant for a load (code version, attempt).
- **Header ownership**: decide which keys a job owns. Replace those and keep every other upstream
  header (trace ids, producer metadata).
- **Encoding**: header values are bytes. Use UTF-8 strings for readability, and fix the format of
  times (ISO-8601 or epoch ms).
- **Attempt semantics**: `try_number` (Airflow), restart count (containers), micro-batch id
  (Spark). Together with the version, the attempt tells "new code" apart from "retry of old code".
- **Access model**: a technical schema for context tables, and grants on views for business roles
  (Fine-Grained Accessor).

## When to use / when not

| Situation | Use Metadata Decorator? | Alternative |
| --- | --- | --- |
| Debug which release or run produced a record, hidden from users | Yes | none |
| Store supports headers or tags (Kafka, S3, GCS) | Yes, native and cheap | none |
| Kinesis or another store without metadata | Only with a context table | Wrapper with a clearly named technical struct |
| Consumers need the value for business logic (price tier, segment) | No | Wrapper or Static Joiner |
| Lineage across many jobs and datasets | Partly (per-record breadcrumbs) | Dataset-level lineage (observability patterns) |
| Failed records need their error context | Yes, as part of Dead-Letter | none |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Spark Kafka write fails on `headers` | Header value is STRING, not BINARY, or wrong struct field names | `array<struct<key:string,value:binary>>`, cast with `.cast("binary")` |
| Headers missing downstream | `includeHeaders` set on the writer but not on the reader (or the reverse), or a hop re-produces without headers | Set `includeHeaders=true` on both sides; forward `msg.headers()` in every consumer/producer hop |
| Two `job_version` headers on one record | Upstream headers appended without removing owned keys | Filter owned keys first (`F.filter(headers, ...)`, `build_headers()`) |
| Duplicated records, same `batch_version` | Kafka sink is at-least-once and the micro-batch was replayed | Expected; dedup downstream on key + `batch_version`, or use an idempotent sink |
| `loading_attempt` always 1 after retries | Attempt computed in code instead of from `try_number` | Render `{{ task_instance.try_number }}` |
| FK violation on rerun | Context row deleted before the data rows that reference it | Delete data rows first, then context, in one transaction |
| Business users see the technical column | View defined with `SELECT *`, or grants on the base table | Explicit column list; `REVOKE` on base and context tables |
| S3 `put_object_tagging` fails | More than 10 tags or a value longer than 256 chars | Keep tags few and short (`file_tags()` checks the limits) |

## Implementation notes per stack

**Spark Structured Streaming**: `examples/metadata_decorator/spark/spark_kafka_headers_decorator.py`
. The reader uses `includeHeaders=true`. `foreachBatch` merges headers and
writes to Kafka with `includeHeaders=true`:

```python
ours = F.array(
    F.struct(F.lit("job_version").alias("key"), F.lit(cfg.job_version).cast("binary").alias("value")),
    F.struct(F.lit("batch_version").alias("key"), F.lit(str(batch_id)).cast("binary").alias("value")),
    ...)
upstream = F.filter(F.coalesce(F.col("headers"), empty_headers), lambda h: ~h["key"].isin(*OWNED_HEADER_KEYS))
(batch_df.select("key", "value", F.concat(upstream, ours).alias("headers"))
    .write.format("kafka").option("includeHeaders", "true").option("topic", cfg.target_topic).save())
```

`--sink s3` writes `batch_version=<id>/` with `mode("overwrite")`, so a replay rewrites its own
prefix. It then tags every object with `job_version`, `batch_version` and `processing_time`.

**Kafka**: `examples/metadata_decorator/kafka/kafka_headers_metadata_decorator.py`. It is an
idempotent producer and forwards key and value byte-for-byte:

```python
headers = build_headers(msg.headers(), JOB_VERSION, ATTEMPT, int(time.time() * 1000),
                        msg.topic(), msg.partition(), msg.offset())
producer.produce(TARGET_TOPIC, key=msg.key(), value=msg.value(), headers=headers, on_delivery=...)
# every COMMIT_EVERY records: producer.flush() -> raise on failures -> consumer.commit()
```

`inspect --job-version v1.0.3` reads the decorated topic with a throwaway group and prints the
records with that header. This is the operator's view, which business consumers never need.

**Airflow / warehouse**: `examples/metadata_decorator/airflow/metadata_decorator_dag.py`
. The context and the data load run as one
`SQLExecuteQueryOperator` transaction:

```sql
DELETE FROM {{ weekly_table }} WHERE visits_context_execution_date_time = '{{ execution_date }}';
DELETE FROM dp.visits_context WHERE execution_date_time = '{{ execution_date }}';
INSERT INTO dp.visits_context (execution_date_time, loading_time, code_version, loading_attempt)
VALUES ('{{ execution_date }}', '{{ dag_run.start_date }}', '{{ params.code_version }}',
        {{ task_instance.try_number }});
INSERT INTO {{ weekly_table }} SELECT s.*, '{{ execution_date }}' FROM dp.visits_staging s WHERE ...;
```

A `SQLCheckOperator` asserts that there is one context row for the deployed version and that every
staged row is linked. Then `dp_public.visits_current_week` is refreshed with an explicit column
list and grants.

## Related patterns

- **Wrapper**: same mechanics, but its decorations are meant to be seen; use it for business attributes.
- **Dead-Letter**: failed records carry error context via this pattern.
- **Fine-Grained Accessor for Tables**: hides the metadata column or table from users.
- **Fast Metadata Cleaner / Proxy**: the weekly-table layout and view swap used by the warehouse variant.
- **Static Joiner**: the context table is joined back by id when operators need it.
- **Observability patterns**: consume `job_version` / attempt for lineage and release impact.
