# Constraints Enforcer

> Category: quality enforcement, storage side (declarative rules). Batch and streaming producers.

## Problem

A batch pipeline processes visits and writes them to a table. After several months without
problems, random NULLs start appearing in several required fields. The job is already complex
and you don't want to add validation code to it. You want an approach that **fails the load**
whenever a quality rule such as "required field present" is broken.

## Solution / Mechanics

Delegate validation to the database, the table format or the serialization format. The rules
become part of the dataset definition instead of the pipeline code.

```
 producer (job, app) --write--> [ store / serializer with constraints ] --commit--> table / topic
                                    | type        : value has the declared type
                                    | nullability : NOT NULL / required
                                    | value       : CHECK (expr), allowed sets, ranges
                                    | integrity   : FOREIGN KEY -> parent table
                                    v
                        any violation -> whole write rejected (error names the constraint)
```

1. **Identify the attributes** that need rules. This is business- or legislation-driven (an
   order needs an amount and a billing address); there is no one-size-fits-all list.
2. **Assign constraints** from four categories:
   - **Type**: all values of an attribute have one type. It is part of the schema and the backbone
     of schema consistency.
   - **Nullability**: a non-nullable attribute rejects rows with missing values. A nullable one
     tells consumers they may have to filter NULLs.
   - **Value**: a value or set of values/expressions plus a comparison operator, e.g.
     `x <= NOW()` (never in the future), `x BETWEEN 1901 AND 2000`.
   - **Integrity**: a value must reference an existing row in another table. Typical of models
     built with the Normalizer pattern (a visit referencing a non-existent page is rejected).
3. **Let the store enforce them** on every write.

Where it is implemented: relational databases; Delta Lake (`NOT NULL` columns plus
`CHECK` constraints); serialization formats: Avro and Protobuf cover types natively, and
**protovalidate** adds value rules to Protobuf.

The pattern is **informative** for consumers (it documents the dataset's shape and allowed
values) and **interactive** for producers (they cannot add records that break the rules).

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| All-or-nothing semantics | One invalid row rejects every row of the transaction | Pre-filter in the job with the same predicate and dispatch violators (example's last step) |
| First error only | Databases stop at the first error, so a producer with many issues needs several round trips | Validate on the producer side to list all issues, at the cost of the informative/interactive benefits |
| Data producer shift | Constraints express the producer's contract; a consumer may need a nullable field to be present | Consumers still validate/filter on top, or use AWAP for their own expectations |
| Constraints coverage | Not every rule is expressible; table formats lack integrity constraints | Complete with AWAP rules in the job, where the only limit is your language |

## Key design decisions

- **Which rules live in storage**: stable, universal rules (types, required fields, simple ranges).
  Evolving or dataset-level rules (volume, distribution) belong to AWAP or an observer.
- **Enforcement point**: at write time in the table, or at produce time in the serializer, which
  stops bad data before it leaves the producer.
- **Violation handling**: fail the batch (default), or pre-filter and dispatch rejects.
- **Constraint naming**: names show up in errors (`event_time_not_in_the_future`); treat them as
  part of the data contract and document them.

## When to use / when not

| Situation | Use Constraints Enforcer? | Alternative |
| --- | --- | --- |
| Required fields or ranges must hold for every consumer | Yes | - |
| Job is complex and you don't want validation code in it | Yes (the motivating case) | - |
| Rule compares with previous runs or other datasets | No | AWAP |
| Store doesn't enforce the constraint (BigQuery/Snowflake/Redshift PK/FK are informational) | No, it would be documentation only | AWAP or a CI check |
| Partial loads are acceptable and bad rows must be kept | Partially: storage rejects all | Pre-filter + dispatch, or Dead-Letter |
| Contract is about schema evolution, not values | No | Schema Compatibility Enforcer |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `ALTER TABLE ... ADD CONSTRAINT` fails on Delta | Existing rows already violate the CHECK | Clean the table first; Delta validates existing data |
| Fresh rows rejected as "in the future" | Clock skew between writer and engine | Add a tolerance, for example `NOW() + INTERVAL '1 SECOND'` |
| PostgreSQL CHECK with `NOW()` breaks dump/restore | CHECK expressions should be immutable; old rows are re-checked on restore | Keep time rules loose, or enforce them in a trigger/AWAP |
| Whole daily load rejected for one row | All-or-nothing semantics | Expected. Fix the input or pre-filter + dispatch |
| Retries keep failing | Constraint violation is not transient | `retries=0` on the load task, as in the DAG |
| Avro producer accepts an empty string for "required" | Non-null is not non-empty | Value rule in the serializer hook / protovalidate `min_len` |

## Implementation notes per stack

**Spark** - `examples/constraints_enforcer/spark/spark_delta_constraints.py`:
Delta table with `NOT NULL` columns and an idempotently added CHECK; one bad row makes the whole
append fail; then the same rule is evaluated in the job to dispatch violators.
```python
spark.sql("""CREATE TABLE IF NOT EXISTS default.visits (
  visit_id STRING NOT NULL, event_time TIMESTAMP NOT NULL, page STRING) USING delta""")
if "delta.constraints.event_time_not_in_the_future" not in existing:
    spark.sql("""ALTER TABLE default.visits ADD CONSTRAINT event_time_not_in_the_future
                 CHECK (event_time < NOW() + INTERVAL '1 SECOND')""")
try:
    bad.writeTo("default.visits").append()   # DELTA_VIOLATE_CONSTRAINT_WITH_VALUES
except Exception as error:
    print(f"Rejected whole batch, v2 not written either: ...")
```

**Kafka** - `examples/constraints_enforcer/kafka/kafka_avro_constraints_producer.py`: the Avro
schema enforces types and required fields inside `AvroSerializer`; a `to_dict` hook adds the
value rules that protovalidate would express (`min_len = 5`, `lt_now`, CEL
`!endsWith('html')`).
```python
def value_constraints(visit: dict, ctx) -> dict:
    errors = []
    if len(visit["visit_id"]) < 5:
        errors.append("visit_id shorter than 5")
    if visit["page"].endswith("html"):
        errors.append("page cannot end with an html extension")
    if errors:
        raise ValueError("; ".join(errors))
    return visit

serializer = AvroSerializer(registry, VISIT_SCHEMA, to_dict=value_constraints)
```

**Airflow** - `examples/constraints_enforcer/airflow/constraints_enforcer_dag.py`: PostgreSQL
tables with type, NOT NULL, CHECK, PK and FK constraints. The daily load is one transaction, so
the database rejects it as a whole; the DAG has no validation code.
```sql
CREATE TABLE IF NOT EXISTS dp.visits (
  visit_id   CHAR(36)    NOT NULL,
  event_time TIMESTAMP   NOT NULL CONSTRAINT event_time_not_in_future CHECK (event_time <= NOW()),
  page       VARCHAR(50) NOT NULL REFERENCES dp.pages(page),
  PRIMARY KEY (visit_id, event_time));
```

## Related patterns

- **Audit-Write-Audit-Publish**: imperative complement for the rules storage cannot express.
- **Schema Compatibility Enforcer**: the "implicit with inserts" mode is this pattern applied to schemas.
- **Normalizer** (data storage patterns): integrity constraints come with normalized models.
- **Dead-Letter** (error management patterns): where pre-filtered violators can go instead of failing the load.
- **Transactional Writer** (idempotency patterns): the all-or-nothing semantics rely on it.
