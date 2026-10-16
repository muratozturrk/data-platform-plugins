# Fast Metadata Cleaner

Family: **Overwriting**. Make reloads idempotent by removing data with metadata operations (`TRUNCATE TABLE`, `DROP TABLE`) on small physical units, instead of a row-level `DELETE` on one big table.

## Problem
A daily batch job processes 500 GB to 1.5 TB of visit events. It stays idempotent in two steps: `DELETE` the rows added by the previous run, then `INSERT` the new ones. After three weeks the table has grown so much that the DELETE step degrades every day, and you need a design that scales with a continuously growing table.

Why DELETE hurts: it first has to find the matching rows, then rewrite every data file that holds them. The cost grows with the table, not with the day being reloaded.

## Solution / Mechanics
`TRUNCATE TABLE t` is semantically `DELETE FROM t` without a condition, but it does not scan the table. It is a metadata operation, like `DROP TABLE`. Both only work on whole tables, so the trick is to change how you see the dataset:
- Store it as **several physically separate tables** (for example one per week: `visits_2024_w01` ... `visits_2024_w52`).
- Expose them from **one place**, a view that is the `UNION ALL` of the units.
- The unit is the **idempotency granularity**: the scope that one metadata operation cleans.

The orchestration gets three extra steps:
1. **Analyze the execution date** with an Exclusive Choice (Airflow `BranchPythonOperator`). Monday or January 1 starts a new unit and takes the table-management branch. Any other day goes straight to the load.
2. **Create the idempotency environment**: `TRUNCATE` (preceded by "create table if none exists") or `DROP` (followed by `CREATE`).
3. **Update the single abstraction** (the view) so it includes the new unit. With DROP, remove the table from the view first, or readers get an error while the table is missing.

```
TRUNCATE-based
  analyze date --Mon/Jan 1--> create table if none exists -> include in view -> TRUNCATE --+
       |                                                                                 v
       +--------------------------other days-----------------------------> process day -> load to weekly table

DROP-based
  analyze date --Mon/Jan 1--> remove from view -> DROP -> CREATE -> include in view --+
       |                                                                              v
       +--------------------------other days--------------------------> process day -> load to weekly table

Fully loaded table: no router, every run does
  [create if none exists -> TRUNCATE] or [DROP -> CREATE]  -> process -> load
```

The router uses `ex_date.day_of_week == 1 or ex_date.day_of_year == 1`. The weekly task creates the weekly table from SQL and refreshes the view with a custom `PostgresViewManagerOperator`.

## Consequences & trade-offs
| Consequence | What it means | Mitigation |
|---|---|---|
| **Granularity = backfilling boundary** | With weekly units, fixing one day means replaying from the week's table-creation task, or the week ends up inconsistent. If only one day was wrong, the other days only rerun the load step. | Choose the unit from how often corrections happen. Document the replay range (Monday to Sunday). |
| **No fine-grained backfill** | You cannot backfill one data provider, user or customer: metadata operations act on whole tables. | Use Merger or Data Overwrite with a predicate for those cases. |
| **Metadata limits** | Object quotas: BigQuery 4,000 partitions per table, Redshift 200,000 tables. Several pipelines using the pattern reach them fast. | Add a **freezing** step: once no more changes are possible, turn weekly tables into immutable monthly or yearly tables. |
| **Store support** | Needs metadata operations: warehouses, lakehouses, RDBMS. Hard on plain object stores. | Use Data Overwrite there. |
| **Data exposition layer** | The dataset no longer lives in one place, and users do not want to know the physical split. | A view, or an equivalent logical structure that groups tables into one unit. |
| **Schema evolution** | A new *optional* field needs a separate pipeline that alters existing unit tables; doing it through the pattern would reprocess data. A new *required* field can ride on replays: re-creating the tables adds it. | Keep the schema in a template table and create units with `LIKE template`. |

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Unit size | day / week / month / year | Coarser: fewer objects, larger replays. Finer: cheaper replays, quota pressure. |
| TRUNCATE vs DROP | keep table / re-create table | TRUNCATE keeps grants, the view and the schema. DROP applies a new schema on rebuild, but needs the view detach step. |
| Unit naming | calendar week (`%W`) / ISO week | The "Monday or January 1" rule matches calendar-year weeks (`%W`): Jan 1 always opens a unit. ISO weeks would put Jan 1 into week 52/53 of the previous ISO year. |
| View maintenance | static list / rebuilt from catalog | Rebuild from `information_schema` or the catalog so no unit is ever missing. |
| Day replay inside a unit | replay the week / delete the day slice | A small `DELETE` of one day on a weekly table is cheap and keeps single-day retries idempotent. The unit cleanup itself stays metadata-only. |

## When to use / when not to use
| Situation | Use it? | Alternative |
|---|---|---|
| Large, growing, time-partitioned dataset; full unit reload per run | Yes | - |
| Warehouse, RDBMS or lakehouse with TRUNCATE/DROP and views | Yes | - |
| Object store without a metadata layer | No | Data Overwrite |
| Increments only (CDC, changed rows), no full unit available | No | Merger, Stateful Merger |
| Backfills scoped by customer or provider | No | Merger, Data Overwrite with `replaceWhere` |
| Thousands of units across many pipelines | Only with freezing | Partition-level overwrite |
| Every version must be kept | No (units are mutable) | Proxy |

## Gotchas & best practices
- Serialise runs (`max_active_runs=1`): seven daily runs write to one weekly table.
- The join after the branch needs `trigger_rule="none_failed_min_one_success"`, otherwise the skipped branch skips the load.
- Detach before DROP. If the view still references a dropped table, every reader query fails.
- Grant consumers access to the view, not to the units.
- Replaying the Monday run wipes Tuesday to Sunday: clear the whole week (`airflow tasks clear -s <monday> -e <sunday>`).
- Refuse empty loads: a successful run that loaded nothing after a TRUNCATE looks like data loss.

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `relation does not exist` on the view during a run | DROP flow without the "remove from view" step | Exclude the table, drop, create, then include it again. |
| Load task skipped on Mondays | Default `all_success` trigger rule after a branch | `none_failed_min_one_success` on the join task. |
| Week has only one day after a backfill | Monday was replayed alone, TRUNCATE removed the other days | Replay the full unit; later days rerun only the load. |
| `TRUNCATE TABLE` rejected on Delta | Some OSS Delta versions do not support it | `DELETE FROM t` without predicate: Delta commits it as a metadata-only removal of all files. |
| `create_topics` fails with TOPIC_ALREADY_EXISTS | Kafka topic deletion is asynchronous | Poll `list_topics` until the topic is gone, then retry the create. |
| Quota errors (tables, partitions) | Too many units | Freeze closed units into yearly tables, or use a coarser unit. |
| Jan 1 data lands in last year's table | ISO week naming | Use calendar-year week naming (`%Y` + `%W`). |

## Implementation notes per stack

### Airflow + PostgreSQL (main implementation)
[`examples/fast_metadata_cleaner/airflow/weekly_tables_truncate_dag.py`](../examples/fast_metadata_cleaner/airflow/weekly_tables_truncate_dag.py): router, both flows (DAG param `cleaning_mode`), view rebuilt from `information_schema`, daily load.
```python
@task.branch
def route(logical_date=None, params=None) -> str:
    if not should_start_new_unit(logical_date.date()):   # Monday or Jan 1
        return "skip_table_management"
    return f"{params['cleaning_mode']}_branch_start"      # truncate | drop

@task
def truncate_weekly_table(logical_date=None) -> None:
    hook().run(f"TRUNCATE TABLE {SCHEMA}.{table_for(logical_date)}", autocommit=True)

@task(trigger_rule="none_failed_min_one_success")
def load_day(logical_date=None, ds=None) -> int: ...
```

### Spark + Delta
[`examples/fast_metadata_cleaner/spark/spark_weekly_tables_metadata_cleaner.py`](../examples/fast_metadata_cleaner/spark/spark_weekly_tables_metadata_cleaner.py): weekly Delta tables, the view built from `spark.catalog.listTables`, the day written with `replaceWhere`, `--full-load` for the single fully loaded table variant.
```python
if mode == "drop":
    self.refresh_view(exclude=table)
    self.spark.sql(f"DROP TABLE IF EXISTS {self.db}.{table}")
    self.create_table(table, if_not_exists=False)
    self.refresh_view(include=table)
...
daily.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"visit_date = '{day.isoformat()}'") \
    .saveAsTable(f"{DATABASE}.{table}")
```

### Kafka (adaptation)
[`examples/fast_metadata_cleaner/kafka/kafka_weekly_topics_cleaner.py`](../examples/fast_metadata_cleaner/kafka/kafka_weekly_topics_cleaner.py): one topic per week. Topic delete and create are controller metadata operations. A regex subscription plays the role of the view.
```python
for _, fut in admin.delete_topics([topic], operation_timeout=30).items():
    fut.result()
while topic_exists(admin, topic):          # deletion is asynchronous
    time.sleep(2)
admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=3)])[topic].result()
consumer.subscribe([r"^visits_\d{4}_w\d{2}$"])   # the "view"
```
Cluster partition limits are the Kafka equivalent of the metadata quotas. Replaying a unit also invalidates consumer offsets on that topic.

## Related patterns
- **Data Overwrite**: the data-layer alternative when metadata operations are missing, and an option for fully loaded tables.
- **Exclusive Choice**: the routing step that decides whether a run starts a new unit.
- **Proxy**: also hides physical tables behind a view, but its tables are write-once instead of truncated.
- **Merger**: use it when only increments are available and a unit cannot be rebuilt.
- **Compactor**: the freezing step resembles it, merging closed small units into bigger ones.
