# Schema Migrator

> Category: schema consistency, producer side. Breaking schema changes with a grace period.

## Problem

From day one you added fields to the visit events without bothering consumers. Domain-related
fields are now spread over a message with up to 60 attributes. Consumers complain that the domain
is hard to understand and want related attributes grouped, e.g. `login`, `email` and `age` inside
a single `user` attribute. A radical change would break compatibility, so you want to reorganize
the schema while giving consumers time to migrate.

## Solution / Mechanics

Schema Compatibility Enforcer only controls which changes are allowed. Schema Migrator is the
procedure that makes schema evolution possible. It requires **non-transitive**
compatibility, because transitive modes guarantee consistency across all versions and would forbid
any removal or rename.

1. **Identify the evolution**:
   - **Rename**: the name is wrong or hard to understand (`from_page` -> `referral`).
   - **Type change**: reorganize (flat fields -> `user` struct) or optimize for processing
     (heterogeneous datetime text -> epoch timestamp).
   - **Removal**: easy only with a 100% guarantee that nobody reads the attribute. Otherwise find
     a substitute or cancel.
2. **Rename / type change**: create the new field, agree on a transition deadline,
   send **both** old and new attributes during the period, then publish a version with only the
   new attribute.
3. **Removal**: agree on a removal period; after the deadline publish a version
   without the field.

```
 v1 [from_page]  --expand-->  v2 [from_page (deprecated), referral]  --deadline-->  v3 [referral]
                              ^ dual write from ONE source value       ^ contract: drop old field
     consumers:   read from_page  ...  migrate to referral at their pace ...  must read referral
     lineage (Fine-Grained Tracker) confirms no reader of from_page is left before the contract
```

Anti-example: `ALTER TABLE dp.visits RENAME COLUMN from_page TO referral`
breaks the consumer's `INSERT INTO dp.connected_users_visits SELECT ... from_page ...` with
`ERROR: column "from_page" does not exist`. The correct first step is
`ALTER TABLE dp.visits ADD COLUMN referral ...`; drop `from_page` only after consumers adapt.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Size impact | Both versions are stored and sent: more storage, network transfer and I/O | Keep grace periods short; one migration at a time |
| Wide schemas | Protobuf best practices warn against hundreds of fields (each one takes space, builders hit Java limits). Delta collects stats on the first 32 columns only | Group fields; raise `delta.dataSkippingNumIndexedCols` knowing it slows writes |
| Impossible removal | A field used by a consumer can't be removed without an alternative attribute | Provide a substitute or cancel the removal |

## Key design decisions

- **Grace period length** and how it is announced (changelog, `doc`/`COMMENT` "DEPRECATED: use X").
- **Single source for dual writes**: derive old and new from one value so they cannot diverge.
- **Remaining readers**: lineage (Fine-Grained Tracker), query logs, a consumer registry.
- **Compatibility level**: non-transitive BACKWARD/FORWARD/FULL. Under FORWARD/FULL a field can
  only be removed if it is optional, so deprecated fields need defaults.
- **How the contract step passes guards**: if an event trigger blocks breaking DDL (Schema
  Compatibility Enforcer), the approved drop needs an explicit, audited allowlist.

## When to use / when not

| Situation | Use it? | Alternative |
| --- | --- | --- |
| Rename / retype / restructure a field others read | Yes | - |
| Field has provably no reader | Light version: announce, then drop | - |
| Compatibility mode is transitive | Not possible as is | Switch to non-transitive, or create a new topic/table version |
| Change is additive (new optional field) | Not needed | Schema Compatibility Enforcer allows it directly |
| Readers can't be coordinated at all (public API) | Long-lived dual fields | Versioned dataset (`visits_v2`) with its own lifecycle |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Old and new columns hold different values | Two code paths write them | Dual-write from one source value (Spark/Kafka examples) |
| Sync step fails every run after the drop | Sync references the dropped column | Guard it with an existence check (Airflow `DO $$ IF EXISTS`) |
| `DROP COLUMN` blocked: "Breaking DDL blocked ... use the Schema Migrator procedure" | DDL event trigger from the compatibility enforcer | Run the drop as `dp_schema_migrator` with `SET LOCAL dp.schema_migration = 'contract'` |
| Delta `DROP COLUMN` not supported | Column mapping disabled | `delta.columnMapping.mode = 'name'` (reader v2 / writer v5), as in `finalize()` |
| Final Avro schema rejected by the registry | Deprecated fields had no default, or mode is transitive | Give them `"default": null`; use non-transitive FULL/FORWARD |
| Protobuf type change corrupted readers | Wire type changed under the same tag | New field with a new tag; `reserved` the old tag and name after removal |

## Implementation notes per stack

**Spark** - `examples/schema_migrator/spark/spark_delta_schema_migrator.py`: three phases on
Delta. `add` adds and backfills `referral` and marks `from_page` deprecated, `write` dual-writes
from one source column, `finalize` refuses to run before the deadline and then drops the column
with column mapping.
```python
def dual_write(new_visits):
    (new_visits.withColumn("referral", F.col("source_referral"))
     .withColumn("from_page", F.col("source_referral")).drop("source_referral")
     .writeTo("default.visits").append())

def finalize():
    if dt.date.today() <= DEADLINE:
        raise SystemExit(f"Grace period not over (deadline {DEADLINE}) ...")
    spark.sql("ALTER TABLE default.visits SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', ...)")
    spark.sql("ALTER TABLE default.visits DROP COLUMN from_page")
```

**Kafka** - `examples/schema_migrator/kafka/kafka_schema_migrator_producer.py`: the grouping
change. The transition schema carries the deprecated flat `login/email/age` (nullable, default
null) and the new `user` record. The final schema drops the flat fields, which non-transitive
FULL accepts because they had defaults.
```python
SCHEMAS = {"transition": BASE_FIELDS + DEPRECATED + NEW_USER, "final": BASE_FIELDS + NEW_USER}

def to_record(visit: dict, phase: str) -> dict:
    user = {"login": visit["login"], "email": visit["email"], "age": visit["age"]}
    record = {"visit_id": visit["visit_id"], "page": visit["page"], "user": user}
    if phase == "transition":
        record.update(user)  # dual write from a single source of truth
    return record
```

**Airflow** - `examples/schema_migrator/airflow/schema_migrator_dag.py`: PostgreSQL rename as
expand (ADD COLUMN), sync (guarded backfill plus deprecation comment), then a deadline branch. The
contract step runs on a dedicated connection and approves itself to the DDL guard for one
transaction only:
```python
drop_old_column = SQLExecuteQueryOperator(task_id="drop_old_column", conn_id=MIGRATOR_CONN,
                                          autocommit=False, sql="""
    SET LOCAL dp.schema_migration = 'contract';
    ALTER TABLE dp.visits DROP COLUMN IF EXISTS from_page;
""")
add_new_column >> sync_new_column >> grace_period_over() >> [drop_old_column, grace_period_continues()]
```

## Related patterns

- **Schema Compatibility Enforcer**: checks that each step is compatible; its DDL guard allowlists the contract step.
- **Fine-Grained Tracker** (data observability patterns): finds consumers still reading the old field.
- **Dataset Materializer / views** (data storage patterns): can expose the old name over the new column during migration.
- **Vertical Partitioner** (data storage patterns): an alternative to one 60-attribute message when groups have different lifecycles.
