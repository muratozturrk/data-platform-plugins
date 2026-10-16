# Wrapper

> Category: data decoration. Batch and streaming, any structured or semi-structured format.

## Problem

The streaming layer processes visits that come from different data providers, so they arrive in
different output schemas. You must extract the relevant fields and put them in one
place so downstream consumers can rely on it. The computed values must be **clearly separated
from the original ones**, and the **original structure must stay intact** for debugging.

Parsing the row into a new structure doesn't work: you would lose the initial values.

## Solution / Mechanics

In software engineering, wrapping adds behavior or attributes to an object. In data, it also
separates the original parts of a record from the transformed parts.

1. **Add an abstraction at the record level**: a high-level envelope around the original values.
2. **Put the raw record in one section, unchanged.**
3. **Put computed attributes in another section.** They come from the input itself (business
   values such as `is_connected`, `page_referral_key`) or from the execution context (processing
   time, job version, batch number).
4. **Pick a layout.** Streaming formats (Avro, Protobuf, JSON) use a nested envelope. Tables use
   one of four layouts:

```
 Implementation 1   [ orig col 1 | ... | orig col n | decorated struct<computed...> ]
 Implementation 2   [ computed 1 | ... | computed n | raw struct<orig...>           ]
 Implementation 3   [ orig col 1 | ... | orig col n | computed 1 | ... | computed n ]
 Implementation 4   Table 1 [ id | orig col 1..n ]  --join on id--  Table 2 [ id | computed 1..n ]

 Stream envelope    value = {"raw_data": "<original value>",
                             "processing_context": {"job_version": "v1.0.3", "batch_version": 42}}
```

Implementations 1 and 2 are denormalized and faster to read. Implementation 4 is normalized,
slower to read, and fits when you must isolate the datasets logically or can't change the
original structure. In a table the envelope is the row itself, so you don't have to
pack several columns into one nested column.
Implementation 3 keeps everything at one level; users can't tell raw from computed unless the
names say so.

## Consequences & trade-offs

| Consequence | What happens | Mitigation |
| --- | --- | --- |
| Domain split | Attributes of one entity (a user) live in two high-level structures, raw and computed. Retrieval gets harder; consumers must know both locations | Keep wrapped data in internal layers (Silver) and expose flattened data to end users, for whom the split is confusing |
| Size | Decorated values are part of the record, so they add storage and network traffic. This differs from the Metadata Decorator | Use storage with projection (columnar formats, Redshift, BigQuery) so readers fetch only the columns they need |
| Schema management | The envelope schema has to evolve with both sides | Schema consistency patterns; version the envelope |
| Ambiguity in the flat layout | Consumers pick the raw column instead of the cleaned one | Prefix columns (`raw_*`, `computed_*`) or use a nested layout |

## Key design decisions

- **Layout**: who reads the data and whether the raw structure may change. Use nested layouts
  for Silver, layout 4 when the raw table is owned by someone else, and the flat layout only with
  explicit prefixes.
- **Raw representation**: in streams, keep the original value as a string or bytes, not a
  re-serialized object. Re-serialization changes key order, number formatting and drops unknown
  fields.
- **Business vs technical attributes**: the Wrapper decorates business attributes in addition to
  technical ones. If an attribute is purely technical and should be hidden, use the Metadata
  Decorator.
- **Execution-context values**: take job version and batch number from the deployment and the
  orchestrator (`run_id`, `try_number`, micro-batch id), never from wall-clock-derived ids.
- **Join key for layout 4**: a deterministic id (hash of the raw row, or the business key plus
  event time) so a rerun produces the same key.

## When to use / when not

| Situation | Use Wrapper? | Alternative |
| --- | --- | --- |
| Several providers, one consistent shape, originals needed for debugging | Yes | none |
| Silver layer that must stay replayable from its own raw part | Yes | Keep Bronze and re-derive (costs a second dataset) |
| Technical context (job version, run) that users shouldn't see | No | Metadata Decorator |
| Gold or BI tables for business users | No (flatten) | Views or tables exposing only the computed side |
| Raw schema owned by another team and immutable | Yes, layout 4 | Separate computed table joined by key |
| Payload is huge and computed fields are tiny | Maybe | Layout 4 or columnar storage with projection |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Raw part differs from the source | A transformation (cast, trim, timezone) was applied to raw columns | Derive computed fields only into the computed section; compare count + checksum of raw vs input after each run |
| Consumers read wrong values in the flat layout | Raw and computed columns have similar names | Prefixes, or switch to layout 1 or 2 |
| Envelope inside envelope after several stages | Each stage re-wraps the previous output | Wrap once; later stages add fields to the computed section |
| Storage cost jumps after wrapping | Computed section duplicates large raw fields | Keep computed fields small; layout 4; projection pushdown |
| Joins on layout 4 multiply rows | Non-unique wrap id | Hash the full raw row, or the business key plus event time |
| Rerun duplicates rows | Append writes | Overwrite the processed partition (`replaceWhere`) |
| Streaming envelope breaks consumers | Value schema changed from raw to envelope without notice | New topic or schema version; announce via the schema registry |

## Implementation notes per stack

**Spark batch**: `examples/wrapper/spark/spark_record_envelope_wrapper.py` (`--layout`).

```python
def processing_context(job_version, batch_version):
    return F.struct(F.lit(job_version).alias("job_version"),
                    F.lit(batch_version).alias("batch_version"),
                    F.current_timestamp().alias("processing_time"))

# layout raw_nested (implementation 2)
visits.select(*[c.alias(n) for n, c in computed_columns().items()],
              F.struct(*visits.columns).alias("raw"), ctx.alias("processing_context"))
```
`--engine sql` runs the SQL version:
```sql
SELECT *, NAMED_STRUCT('is_connected', CASE WHEN context.user.connected_since IS NULL
         THEN false ELSE true END, 'page_referral_key', CONCAT_WS('-', page, context.referral)) AS decorated
FROM input_visits
```
Output is a Delta partition overwritten with `replaceWhere`. The job writes a metrics JSON with
the count and an `xxhash64` checksum sum (as decimal, no overflow) of the input and of the raw
part re-read from the output.

**Spark streaming**, `--stream` in the same file:
```python
kafka_df.select("key", F.to_json(F.struct(
    F.col("value").cast("string").alias("raw_data"),
    processing_context(job_version, str(batch_id)).alias("processing_context"))).alias("value"))
```

**Kafka**: `examples/wrapper/kafka/kafka_record_wrapper.py`. One extractor per provider topic
(`PROVIDERS` map) fills the same computed fields. `raw` is the undecoded original string.
Unwrappable records go to a DLQ topic with error headers. Produce, flush, then commit.

**Airflow**: `examples/wrapper/airflow/wrapper_dag.py`. `FileSensor` on the Bronze `_SUCCESS`,
then `SparkSubmitOperator` with `--job-version {{ var.value.wrapper_job_version }}` and
`--batch-version {{ run_id }}/try-{{ ti.try_number }}`, then `validate_raw_untouched` fails the
run if the raw count or checksum differs from the input.

## Related patterns

- **Metadata Decorator**: same idea for technical context, hidden from consumers instead of exposed.
- **Static / Dynamic Joiner**: enrichment results are typical computed attributes in the envelope.
- **Dead-Letter**: wrappers and DLQ records share the "raw + error/context" envelope.
- **Schema consistency patterns**: evolve the envelope schema safely.
- **Medallion layers** (use case): wrapped data belongs to Silver, flattened data to Gold.
- **Fine-Grained Accessor for Tables**: expose only the computed side to business users.
