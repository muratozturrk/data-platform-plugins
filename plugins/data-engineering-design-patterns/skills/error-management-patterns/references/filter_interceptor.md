# Filter Interceptor

> Category: human/logic errors, observability of filtering. Batch, SQL and streaming.

## Problem

Not every error is technical. A wrong filter condition is a human mistake that exposes partial
or bad data to users. After a new release of a batch job on a distributed
framework, the filtered volume jumps **from 15% to 90%**. Is that the data, or a regression? The
execution plan cannot tell, because the optimizer **collapses all filter expressions into one**
step. The plan shows the rows filtered by all conditions together, not per condition
.

## Solution / Mechanics

Ideally physical-plan analysis would answer the question. It lacks precision once filters are
combined. The pattern **wraps each filter condition with a counter**.

**Programmatic API**:
1. wrap each condition with counter logic. The PySpark version counts the rows the filter
   *rejects*;
2. run the business logic. Counters are local to each task and merged by the framework
   (accumulators);
3. after the job's action, **gather all counters** explicitly on the driver.

**Declarative SQL**: a subquery or temporary table exposes the filter results as
columns, e.g. `a IS NOT NULL` becomes `a_is_not_null` and `b != "abc"` becomes `b_is_not_abc`.
The SQL example uses one `status_flag` column holding the first failing condition. The main
query filters on those columns, and a second query counts them.

```
 programmatic                                      SQL
 ------------                                      ---
 row ─► filter_1 ok? ─no─► acc_1 += 1, drop        SELECT CASE WHEN (c1) IS FALSE THEN 'f1'
        │yes                                                   WHEN (c2) IS FALSE THEN 'f2'
        ▼                                                      ELSE NULL END AS status_flag, ...
        filter_2 ok? ─no─► acc_2 += 1, drop          ├─► GROUP BY status_flag   -> per-filter stats
        │yes                                         └─► WHERE status_flag IS NULL -> output
        ▼
       keep  ──► write (action) ──► driver reads acc_1..acc_n -> stats
```

Stay pragmatic: use the programmatic API when it fits better, even in an
SQL-only codebase, and the reverse.

## Consequences & trade-offs

- **Runtime impact**: small for programmatic counters, which are simple local
  structures merged over the network. It is larger in SQL, where you may need a temporary table
  so the stats and the filtered output come from one evaluation.
- **Declarative languages**: SQL is less powerful here. The programmatic version is
  easier to read and maintain.
- **Streaming**: may turn a stateless job into a stateful one, because counters must
  persist across records. Counters also need **time boundaries**, such as processing-time
  windows. Otherwise you cannot tell which filters are active *now*, and you cannot see trends.
- **Attribution order**: with first-failure attribution, a row failing several filters counts
  only for the first. Reordering filters changes the numbers but not the output.
- **Accumulator semantics**: Spark guarantees exactly-once accumulator updates only inside
  actions. Updates inside transformations can repeat on task retries or speculative execution.
  Use approximate numbers, disable speculation, or use the SQL counts when exactness matters.

## Key design decisions

- **Counting rejects or passes**: count rejects per filter, plus the input and kept totals, so
  the ratios are self-explanatory.
- **First failure vs. all failures**: first failure gives disjoint counts that sum to the
  filtered total. All failures shows overlaps but no longer sums.
- **Where the stats go**: a per-run JSON/table keyed by partition date, so a regression check
  compares runs.
- **Streaming window**: tumbling processing-time windows (e.g. 1 minute) emitted to a metrics
  topic.
- **Alert rule**: compare each filter's share with its median over the last N runs. One filter
  growing suggests code; all filters growing suggests data.

## When to use / when not

| Situation | Use? | Alternative |
| --- | --- | --- |
| Filtered volume changed and you need to know which condition | Yes | none |
| Several conditions collapsed by the optimizer | Yes | Per-filter `count()` jobs (expensive, rescans) |
| Rows rejected because they are broken, not out of scope | No | Dead-Letter (keeps the rows) |
| Need the rejected rows themselves for audit | Partly | Add a side output / flag column (SQL variant gives it for free) |
| One trivial filter | Overkill | Input vs. output count |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Accumulators read as 0 | Read before the action ran, or the DataFrame was never materialized | Read after `write`/`count` |
| Counters higher than the rows removed | Task retries / speculative execution re-applied updates | `spark.speculation=false`; SQL mode for exact numbers |
| Counters doubled on a second action | Transformation re-executed by another action | Cache the input, or count with a single action |
| Per-filter counts don't sum to filtered rows | All-failures counting, or a NULL condition result (`NULL IS FALSE` is false, so the row is kept unflagged) | First-failure attribution; make every condition NULL-safe (`coalesce(cond, false)`) |
| SQL stats and output disagree | Input evaluated twice with non-deterministic data | Cache the flagged view or write a temporary table |
| Streaming stats meaningless | Counters since job start | Tumbling windows with timestamps |
| `TypeError` in a filter lambda on NULL | Later filter assumes earlier ones passed | Order filters (null checks first); first failure stops evaluation |

Note on NULL semantics: `WHEN (cond) IS FALSE` does not match when `cond` is NULL. A NULL `type`
is caught only because `type IS NOT NULL` comes first. Make NULL handling explicit in each
condition.

## Implementation notes per stack

**Spark, programmatic**: `examples/filter_interceptor/spark/spark_filter_interceptor.py`
:

```python
@dataclasses.dataclass
class FilterWithAccumulator:
    name: str
    filter: Callable[[Any], bool]
    accumulator: Any

filters = build_filters(lambda: spark.sparkContext.accumulator(0))
valid = input_df.mapInPandas(filter_devices, input_df.schema)   # keep_row() adds 1 on first failure
valid.write.mode("overwrite").format("delta").save(output_dir)  # action
stats = {f.name: f.accumulator.value for fs in filters.values() for f in fs}
```

**Spark, SQL** (`--mode sql`): `SQL_FLAGS` builds `status_flag` with `CASE WHEN (...)
IS FALSE`. The view is cached, then `GROUP BY status_flag` gives the stats and
`WHERE status_flag IS NULL` gives the output. Both modes write `_filter_stats/<date>.json`.

**Kafka (streaming)**: `examples/filter_interceptor/kafka/kafka_filter_interceptor.py`.
`WindowedFilterStats` holds per-filter counters in tumbling processing-time windows. Closed
windows go to `devices_filter_stats`. Windows also close on idle polls.

**Airflow**: `examples/filter_interceptor/airflow/filter_interceptor_dag.py`. Runs the Spark job,
then `check_filters` compares the filtered ratio and per-filter shares with the median of the
last 14 runs. It fails the run and names the suspicious filters, which separates "regression in
one filter" from "the data changed".

## Related patterns

- **Dead-Letter**: for rows that *cannot* be processed. The interceptor is for rows that *should
  not* be.
- **Windowed Deduplicator**: its duplicate ratio is the same kind of per-step volume signal.
- **Data quality / observability patterns** (later chapters): consume the per-filter stats as
  metrics and alerts.
- **Metadata Decorator**: attach the `status_flag` to kept/rejected rows when the rows
  themselves are needed.
