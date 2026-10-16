# Schema Compatibility Enforcer

> Category: schema consistency, producer side. Validates schema *changes*, not values.

## Problem

You run a sessionization job built with the Stateful Sessionizer pattern. It worked for months,
then the team producing your input made several changes and the job failed many times in one
month: the new team removed fields your application uses, thinking they were obsolete. You ask
them to build a solution that prevents schema-breaking changes.

## Solution / Mechanics

Every schema change is checked against a **compatibility mode** before data with the new schema
can be written. Three enforcement modes exist:

```
 (1) external service / library        (2) implicit with inserts          (3) event-driven DDL
 producer --register/validate--> SR    producer --INSERT--> table          user --ALTER TABLE--> DB
            409 if incompatible                 rejected if it doesn't                |
            v                                   match schema + constraints   event trigger fn
 producer --write--> topic                                                   RAISE -> rollback DDL
```

1. **External service or library**. Kafka Schema Registry exposes an API to producers and
   consumers, versions each schema and validates changes against the configured mode. A library
   works too: Avro's `SchemaValidator` checks a schema but doesn't let you set the rule centrally.
2. **Implicit with inserts**. Table formats and RDBMSs: the table's types, nullability and value
   constraints reject any record that doesn't match. There is no explicit compatibility mode.
3. **Event-driven for DDL**. PostgreSQL and SQL Server event triggers run SQL functions before a
   DDL commits (`DROP COLUMN`, `RENAME COLUMN`) and can roll back incompatible changes. The coarse
   option: don't grant `ALTER TABLE` at all.

**Compatibility modes**. Non-transitive modes compare consecutive versions;
transitive modes compare against all past (backward) or future (forward) versions.

| Mode | Allowed actions | Semantics | Upgrade first |
| --- | --- | --- | --- |
| Backward (non/transitive) | delete field, add optional field | new consumer reads old data | consumers |
| Forward (non/transitive) | add field, delete optional field | old consumer reads new data | producers |
| Full (non/transitive) | add optional field, delete optional field | both directions | any order |
| None | anything | no enforcement; excluded from the pattern | - |

**Transitive vs non-transitive**:
```
 v0: order_id LONG REQUIRED
 v1: order_id, amount DOUBLE DEFAULT 0.0      <- backward OK vs v0 (optional field added)
 v2: order_id, amount DOUBLE REQUIRED         <- vs v1: OK (v1 data always has amount)
                                                 vs v0: NOT OK (v0 data has no amount)
 BACKWARD accepts v2, BACKWARD_TRANSITIVE rejects it.
```
Once the mode is set, every schema change goes through the enforcement component and incompatible
evolutions are rejected.

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Interaction overhead | The producer validates records against the latest schema version, an extra round trip | Client-side schema caching; register through CI, not at produce time |
| Harder schema evolution | Every change must fit the mode; a rename becomes add-new + deprecate-old | Schema Migrator procedure with a grace period |

## Key design decisions

- **Mode per subject/table**, and transitive or not. Transitive modes forbid the removals and
  renames the Schema Migrator needs.
- **Where the check runs**: at produce time (registry serializer), in CI before deploying a
  producer (compatibility endpoint), at write time (table), or at DDL time (event trigger).
- **Escape hatch for approved breaking DDL**: an event trigger blocks your own migrations too.
  Decide up front how an approved Schema Migrator contract step passes (allowlist below).
- **Contract ownership**: the producer team owns the schema; consumers get notified of new versions.

## When to use / when not

| Situation | Use it? | Alternative |
| --- | --- | --- |
| Many producers/teams write to a shared topic or table | Yes | - |
| Consumers are stateful/long-running and break on missing fields | Yes (the problem above) | - |
| You need to rename or retype a field | Use together with | Schema Migrator |
| You need to validate values, not structure | No | Constraints Enforcer, AWAP |
| Single team owns producer and all consumers, deploys atomically | Optional | Integration tests in CI |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `409 ... READER_FIELD_MISSING_DEFAULT_VALUE` | Removed a field without default under FORWARD | Add a default first, or follow Schema Migrator |
| Bad schemas registered silently by producers | `auto.register.schemas=true` in prod | Disable it; register from CI after `POST /compatibility/...` |
| Delta `A schema mismatch detected when writing` | Implicit enforcement rejected an extra column | Intended. Opt in deliberately with `mergeSchema` |
| `mergeSchema` / `autoMerge` on everywhere | Enforcement disabled for additive changes | Keep `autoMerge` off; enable `mergeSchema` per reviewed write |
| v2 rejected although it is compatible with v1 | Subject uses a transitive mode | Expected: v2 is incompatible with an older version |
| Schema Migrator's `DROP COLUMN` blocked by the event trigger | Guard has no allowlist | `SET LOCAL dp.schema_migration = 'contract'` + `dp_schema_migrator` role (Airflow example) |
| Protobuf rename broke JSON consumers | Wire format uses tags, JSON uses names | Treat renames as breaking for JSON/text encodings |

## Implementation notes per stack

**Kafka** - `examples/schema_compatibility_enforcer/kafka/schema_registry_compatibility.py`: FORWARD on `visits_forward-value`, a dry-run `test_compatibility`
like a CI gate would call, then registration. v2 without `visit_id` is rejected, v3 adding a
field is accepted.
```python
client.set_compatibility(subject_name=SUBJECT, level="FORWARD")
for name, schema in (("v2 (drop required visit_id)", V2_BREAKING), ("v3 (add page)", V3_ADDITIVE)):
    compatible = client.test_compatibility(SUBJECT, schema)
    try:
        print(f"  registered id: {client.register_schema(SUBJECT, schema)}")
    except SchemaRegistryError as error:
        print(f"  rejected: {error.http_status_code} {error.error_message}")
```

**Spark** - `examples/schema_compatibility_enforcer/spark/spark_delta_schema_enforcement.py`: implicit mode. An extra `ad_id` column and a retyped `event_time` are
both rejected; `mergeSchema` is the explicit opt-in for the additive case.
```python
.config("spark.databricks.delta.schema.autoMerge.enabled", "false")   # enforcement by default
try:
    evolved.write.format("delta").mode("append").save(TABLE_DIR)
except AnalysisException as error:
    print("Rejected by implicit enforcement:", str(error).splitlines()[0])
evolved.write.format("delta").mode("append").option("mergeSchema", "true").save(TABLE_DIR)
```

**Airflow** - `examples/schema_compatibility_enforcer/airflow/schema_compatibility_gate_dag.py`:
a registry gate (compatibility endpoint, fail, then register) plus the event-driven DDL mode. The
PostgreSQL event trigger blocks DROP/RENAME/retype COLUMN on `dp`, except for an approved
Schema Migrator contract step:
```sql
IF COALESCE(current_setting('dp.schema_migration', true), '') = 'contract'
   AND pg_has_role(current_user, 'dp_schema_migrator', 'MEMBER') THEN
  RAISE NOTICE 'Approved schema migration on %', cmd.object_identity;
  CONTINUE;
END IF;
RAISE EXCEPTION 'Breaking DDL blocked on %: use the Schema Migrator procedure', cmd.object_identity;
```
Anyone can set a custom GUC, so the role membership is the real allowlist. `SET LOCAL` limits the
approval to one transaction. Event triggers need superuser to install.

## Related patterns

- **Schema Migrator**: the procedure for breaking changes inside these rules; uses the DDL allowlist.
- **Constraints Enforcer**: validates values. Its "implicit with inserts" mode is shared with this pattern.
- **Stateful Sessionizer** (data value patterns): the long-running consumer from the problem statement.
- **Dead-Letter** (error management patterns): where records with an unexpected shape go when you cannot block the producer.
- **Fine-Grained Tracker** (data observability patterns): shows which fields consumers actually read before you change them.
