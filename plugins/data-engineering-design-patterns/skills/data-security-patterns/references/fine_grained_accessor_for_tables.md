# Fine-Grained Accessor for Tables

Category: Access Control · Layer: database / warehouse / lakehouse

## Problem
HDFS/Hive workloads moved to a cloud data warehouse. Users and groups with table grants are
easy. Stakeholders add a requirement: a user authorized on a table may still not be allowed to
read **all its columns and rows**.

## Solution / Mechanics
Narrow table grants with column-level and row-level rules.

Column-level, three implementations:
1. **Column GRANT**: `GRANT SELECT(col_A, col_B) ON my_table TO some_user` (PostgreSQL, Redshift).
   Selecting an ungranted column fails with `permission denied`.
2. **Catalog policy tags**: BigQuery columns tagged in Data Catalog; readers need the Fine-Grained
   Reader role per tag.
3. **Masking functions**: the column is visible but its content is replaced unless the user
   qualifies (Databricks Unity Catalog `MASK`, Snowflake masking policies).

Row-level: a database object that injects a dynamic `WHERE` into every query on the table.
Databricks `ROW FILTER`, Redshift Row-Level Security, BigQuery/Snowflake row access policies,
PostgreSQL `CREATE POLICY`. Fallback for stores without it: a **guard view** filtering on
`current_user`. DynamoDB emulates row access with an IAM condition on
`dynamodb:LeadingKeys` = `${www.amazon.com:user_id}`.

```
 SELECT * FROM visits            (issued by analyst in group 'analysts')
        |
        v
 [1] table grant?  -- no --> permission denied
        | yes
 [2] column rules: GRANT SELECT(cols) -> unlisted column = error
                   MASK ip_mask(ip)  -> '***' unless is_member('engineers')
        |
 [3] row rule: WHERE country = <analyst's country>  (injected by the engine)
        |
        v
 rows the analyst may see, columns masked
```

## Consequences & trade-offs
- **Row-level security limits**: most implementations can only use attributes of
  the connection session (user name, group, IP). Anything else needs a mapping table.
- **Data types**: column permissions apply to whole columns. Nested fields must be
  unnested into another table, or exposed through a Dataset Materializer supporting fine-grained
  permissions.
- **Query overhead**: masks and filters run on every query. If latency suffers,
  materialize per-group tables/views, at the price of duplication and governance of many groups.
- Few drawbacks overall: the feature is native to the database.

## When to use / when not
| Situation | Use? | Alternative |
|---|---|---|
| SQL tables, readers must not see some columns | Yes (GRANT / tags / masks) | Separate table for sensitive columns |
| Multi-tenant table, each tenant sees own rows | Yes (row policy) | Per-tenant views or tables |
| Store has no column/row features | Guard views | Dataset Materializer per group |
| Object stores, topics, queues | No | Fine-Grained Accessor for Resources |
| Partner must never see PII even if rules break | Not enough alone | Anonymizer / Pseudo-Anonymizer before sharing |
| Kafka topic | No columns/rows | Split topics (see `examples/.../kafka/README.md`) |

## Key design decisions
- Deny-by-column (GRANT list) vs show-but-mask: masks keep `SELECT *` working for everyone.
- Source of truth for "who sees what": a role -> columns map in Git, applied idempotently.
- Row predicate input: session user, group membership, or a mapping table (user -> country).
- Owners/admins: PostgreSQL RLS skips table owners and superusers unless `FORCE ROW LEVEL SECURITY`.
- Verification: query `information_schema.column_privileges` after each apply.

## Gotchas & best practices
- A table-level grant is still required; fine-grained rules narrow it, they don't replace it.
- Re-applying column grants: `REVOKE ALL` first so removed columns disappear from the grant
  (revoking table privileges also revokes column privileges in PostgreSQL).
- `CREATE POLICY` without `ENABLE ROW LEVEL SECURITY` does nothing.
- RLS with no matching policy returns zero rows (default deny), not an error: test it.
- Guard views must be the only object granted; grant on the base table bypasses them.
- Masks must not be reversible (no `substr` of a hash that's also exposed elsewhere).

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `permission denied for table users` on `SELECT *` | Column GRANT working as designed | Select only granted columns, or use masks |
| Owner sees all rows | RLS ignores owners | `ALTER TABLE ... FORCE ROW LEVEL SECURITY` |
| User sees no rows at all | RLS enabled, no policy matches | Add policy or check `current_user` value |
| Mask not applied to nested field | Masks are per column | Unnest the field into its own column |
| Queries slowed after adding filters | Per-query function evaluation | Materialize per group; index filter columns |
| Removed column still readable | Grant re-applied without revoke | REVOKE then GRANT in one transaction |

## Implementation notes per stack
**Airflow + PostgreSQL** — `examples/fine_grained_accessor_for_tables/airflow/postgres_fine_grained_access_dag.py`:
```python
for role, columns in READERS.items():
    cols = ", ".join(_quote_ident(c) for c in columns)
    statements.append(f"REVOKE ALL ON dp.users FROM {role}")
    statements.append(f"GRANT SELECT({cols}) ON dp.users TO {role}")
...
"ALTER TABLE dp.users ENABLE ROW LEVEL SECURITY",
"DROP POLICY IF EXISTS user_row_access ON dp.users",
"CREATE POLICY user_row_access ON dp.users USING (login = current_user)",
```
A `verify` task compares `column_privileges` with the declared map.

**Spark / Unity Catalog** — `examples/fine_grained_accessor_for_tables/spark/unity_catalog_mask_and_filter.py`:
```python
f"""CREATE OR REPLACE FUNCTION {FQ}.ip_mask(ip STRING)
    RETURN CASE WHEN is_account_group_member('engineers') THEN ip ELSE '***' END""",
f"ALTER TABLE {FQ}.visits ALTER COLUMN ip SET MASK {FQ}.ip_mask",
f"ALTER TABLE {FQ}.visits SET ROW FILTER {FQ}.country_filter ON (country)",
```

**Kafka** — `examples/fine_grained_accessor_for_tables/kafka/README.md`: no column/row ACLs;
columns become topics (vertical split), rows become per-tenant topics, or sink to a store with RLS.

## Verification
- Connect as each role and run `SELECT *`: denied (GRANT) or masked (MASK) as declared.
- Connect as two users of different tenants: each sees only own rows.
- Connect as the owner: with `FORCE ROW LEVEL SECURITY` the policy applies too.

## Related patterns
- Fine-Grained Accessor for Resources: the same least-privilege idea for non-table resources.
- Dataset Materializer: per-group materialized views when policies cost too much.
- Vertical Partitioner: moving PII columns to a separate table makes column grants trivial.
- Pseudo-Anonymizer: masking as data transformation instead of query-time policy.
