# Transformation Replicator

Category: data replication. One-liner: replicate production data to another environment with a
**transformation layer in between** that removes or alters what must not leave production (PII).

## Problem
Before releasing a new version of a data processing job you want to test it against real data. A synthetic
data generator won't do: the provider often has data quality issues that no tool can simulate. So the data
must be replicated from production to staging, but the dataset contains **PII that is not accessible outside
production**. A simple Passthrough Replicator is not allowed.

This is not a Bronze → Silver cleansing pattern: the output is a test copy for a lower environment, as close to
production as privacy rules permit.

## Solution / Mechanics
If the provider can't guarantee consistent schema and values, production data is unavoidable for testing;
sensitive attributes can't move to environments with looser access policies. The Transformation
Replicator keeps the read and write parts of the Passthrough Replicator and adds a transformation between them.
The transformation is either:
- a **custom mapping function** in a processing framework (Spark, Flink), or
- a **SQL `SELECT`** when the logic is easy to express in SQL.

It either **replaces** attributes that shouldn't be replicated (e.g. Anonymizer pattern) or **removes**
them when the tested job doesn't need them.

```
 PRODUCTION (owner, push)                                                     STAGING
 ┌──────────────┐  read   ┌───────────────────────────────────────────┐ write ┌──────────────┐
 │ devices      │───────► │ PII contract (catalog / data contract tags)│──────►│ devices      │
 │ ip, lat, lon │         │  drop: ip, latitude, longitude  (reduction)│       │ no ip/lat/lon│
 │ full_name    │         │  expr: SUBSTRING(full_name, 2, ...)        │       │ full_name*   │
 │ email, user  │         │  mask: email   hash: user_id (+ salt)      │       │ a***@x.com   │
 └──────────────┘         │  unclassified column -> FAIL (desync guard)│       └──────────────┘
                          └───────────────────────────────────────────┘ verify: no forbidden column
```

Step by step:
1. Wait until the production dataset is complete (readiness marker).
2. Load the PII contract: one action per column, ideally generated from catalog tags.
3. Compare the source schema with the contract; unclassified columns fail the run (or are dropped in allow-list mode).
4. Read (text formats with all values as strings), apply the reduction/alteration, write to staging idempotently.
5. Verify the copy (no forbidden column, row count) and only then publish a staging readiness marker.

Implementation approaches:
- **Data reduction**: `SELECT * EXCEPT (ip, latitude, longitude)` (Databricks, BigQuery) or PySpark
  `.drop('ip', 'latitude', 'longitude')`.
- **Access control instead of copying**: column-level grants, e.g. Redshift
  `GRANT SELECT (visit_id, event_time, user_id) ON TABLE visits TO user_a` (Fine-Grained Accessor).
- **Column-based transformation** when the fields are needed:
  `withColumn('full_name', expr('SUBSTRING(full_name, 2, LENGTH(full_name))'))`.
- **Row-level mapping function** for complex or row-dependent rules (a `map` over a
  case class: devices with version `1.x` get a truncated name and `version = "invalid"`).

## Consequences & trade-offs
- **More risk than passthrough**. Custom logic can break the dataset.
- **Transformation risk for text formats**. Defining a typed schema on JSON/CSV can silently null out
  timestamps whose format differs from the framework's default, so staging fails for the wrong reason. Keep
  it simple: read columns you don't transform as strings.
- **Desynchronisation**. Data evolves: new PII columns appear, existing attributes get reclassified.
  A hardcoded rule list rots. Drive the transformation from a data governance tool (catalog, data contract)
  where sensitive fields are tagged; otherwise maintain the rules yourself and fail on unknown columns.
- Test fidelity drops with every altered column: the tested job must not depend on removed attributes, and
  hashed keys still join only if hashed with the same salt everywhere.

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Reduce vs alter | drop · mask/hash/truncate · nullify | Drop when the tested job doesn't read the column; alter when it does |
| Rule source | data catalog tags · data contract file · hardcoded list | Catalog/contract; hardcoded lists drift (desync) |
| Unknown columns | fail (deny by default) · drop (allow-list) | Fail in strict environments; allow-list when schema changes are frequent |
| Expression vs mapping function | SQL/`expr` · `mapInPandas`/typed `map` | Column rules in SQL; row-dependent rules in a mapping function |
| Hashing | plain SHA · salted SHA · tokenisation service | Salted (secret salt from a vault) so values can't be brute-forced; same salt across datasets keeps joins |
| Text sources | typed schema · all strings | All strings for untouched columns |
| Write | overwrite · `replaceWhere` partition | Idempotent overwrite of what was replicated |
| Direction | push from prod | The transformation must run where PII is allowed: production |

## When to use / when not
| Situation | Use Transformation Replicator? | Alternative |
|---|---|---|
| Realistic staging tests, data contains PII | Yes | - |
| Lower env needs only some columns | Yes (data reduction) | Column-level grants on a shared table |
| No sensitive data at all | No | Passthrough Replicator (less risk) |
| Consumers in the same environment, just need restricted access | No copy needed | Fine-Grained Accessor |
| Need statistically similar but fully fake data | No | Synthetic generation (only if quality issues needn't be reproduced) |
| Raw → cleansed layer inside production | No (different problem) | Data value/quality patterns |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| New PII column leaked to staging | Deny-list rules, schema changed in prod | Contract-driven rules; fail on unclassified columns; post-write leak check |
| Staging job fails on null timestamps | Typed schema on JSON/CSV parsed dates differently | Read as strings, transform only what you must |
| Joins between replicated datasets break | Different salts or hash functions per dataset | One salt per environment pair, shared function |
| Hash values can be reversed | Unsalted hash of low-cardinality values (emails, ids) | Secret salt or tokenisation |
| Replica order/keys lost on Kafka | Transformation rebuilt records without key/headers/partition | Change only the value; keep key, headers, partition, timestamp |
| Rerun duplicates rows | Append mode | Overwrite / `replaceWhere` on the replicated scope |
| Spark rejects `SELECT * EXCEPT` | Not supported by OSS Spark SQL | `df.drop(...)` or explicit column list |
| Undeliverable records lost | Invalid JSON skipped silently | Quarantine topic that stays in production |

## Implementation notes per stack

### Spark
`examples/transformation_replicator/spark/spark_transformation_replicator_batch.py` + `../pii_rules.json`:
```python
df = read_source(spark, args.source_path, args.source_format, args.partition_filter)
plan, unclassified = plan_columns(df.columns, rules)     # raises on unclassified (default_action=fail)
for column, action, spec in plan:
    if action == "hash":
        out.append(F.sha2(F.concat(F.lit(salt), c.cast("string")), 256).alias(column))
    elif action == "expr":
        out.append(F.expr(spec["expr"]).alias(column))
replica = df.select(*out)                                # dropped columns never selected
```
`--row-mapping legacy_devices` runs `transform_device_row` through `mapInPandas` (a row-level `map` in PySpark). `--verify-only` re-reads staging and fails if a `drop` column exists or counts differ.

### Kafka
`examples/transformation_replicator/kafka/kafka_transformation_replicator.py` (confluent-kafka):
```python
value, reason = transform_record(json.loads(msg.value()), rules, salt)
staging.produce(args.target_topic, key=msg.key(), value=json.dumps(value).encode(),
                partition=msg.partition(), headers=msg.headers(), timestamp=msg.timestamp()[1])
...
staging.flush(30); consumer.commit(asynchronous=False)   # flush before commit
```
Records with unclassified fields or invalid JSON go to a quarantine topic in the production cluster.

### Airflow
`examples/transformation_replicator/airflow/transformation_replicator_dag.py` (prod Airflow, push):
`FileSensor` on the prod `_READY` → `validate_pii_contract` → `replicate` (SparkSubmit) →
`verify_staging_copy` (`--verify-only`) → `publish_staging_marker` (temp file + `os.replace`).

### SQL
```sql
SELECT * EXCEPT (ip, latitude, longitude) FROM devices;            -- Databricks / BigQuery
GRANT SELECT (visit_id, event_time, user_id) ON TABLE visits TO user_a;  -- Redshift column grants
```

## Related patterns
- **Passthrough Replicator** - same read/write mechanics without the transformation; use it when no PII exists.
- **Anonymizer / Pseudo-Anonymizer** - the replacement techniques applied inside the transformation.
- **Fine-Grained Accessor** - column-level access control as an alternative to copying.
- **Readiness Marker** - gate the replication on a complete source and announce the staging copy.
- **Dead-Letter** - home for records the transformation can't process safely.
