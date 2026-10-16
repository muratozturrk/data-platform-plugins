# Static Late Data Integrator

> Category: data error, time. Batch / orchestrated pipelines.

## Problem

A daily job computes statistics about the websites that refer blog posts. Results are considered
approximate for **15 days**, the maximum delay allowed for late data. Older records are skipped
. The job only processes the current day, so late data for the previous days is
ignored. The goal is to integrate that late data in the daily pipeline **without running 15
separate jobs every day**.

Ignoring late data is easy, but it loses value when late data is a significant share of the
dataset. An e-commerce store would not drop half of its orders.

Easy solution for you, but not for others: processing-time partitions make
late data vanish for *your* job. They push the problem to every consumer that partitions by
event time. Your 09:00 processing partition holds 10% of 08:00 events and 10% of 07:00 events,
and consumers must sort that out.

## Solution / Mechanics

1. **Define a static lookback window**: how far back each run reprocesses. It is
   static because it never changes. Data arriving later than the window is ignored. Example: a
   run for 2024-12-31 with a 14-day lookback reprocesses 2024-12-17..2024-12-30 plus the current
   day. Late data for 2024-12-15 is ignored.
2. **Place the integration in the pipeline**. There are three strategies:

```
 (a) sequential, late first      (b) parallel                  (c) sequential, current first
 [late D-N..D-1] -> [current D]  [current D]                   [current D] -> [late D-N..D-1]
                                 [late D-N..D-1]   (same time)
 stateful pipelines              stateless                     stateless, fresh data first
```

   - **Stateful** pipelines, where a run depends on previous results, must use late-first: you
     need a valid history before computing today;
   - **stateless** pipelines can use any strategy. Pick (b) or (c) to deliver current data first.
3. **Reprocess each lookback partition idempotently**: recompute it from everything present now
   and overwrite it. Overwriting makes repeated integration of the same date harmless.

```
 run 2024-12-31 (lookback 14)
   wait current partition ──► load 2024-12-31
                         └──► generate_backfilling_runs() = [12-17 ... 12-30]
                                   └──► integrate_late_data.expand(late_date=...)  (14 mapped tasks)
```

## Consequences & trade-offs

- **Snowball backfilling effect**: consumers that care about consistency must replay
  every partition you changed. Their consumers must do the same, and the whole operation becomes
  compute-intensive. All you can do is notify direct consumers and let them decide.
- **Overlapping executions and backfilling**: each run already covers N past days, so
  replaying several runs reprocesses the same dates many times. With a 4-day lookback
  and runs on 10-10, 10-11 and 10-12, restarting **only 10-12** is enough.

  | Execution date | Executed (late) dates |
  | --- | --- |
  | 2024-10-10 | 10-09, 10-08, 10-07, 10-06 |
  | 2024-10-11 | 10-10, 10-09, 10-08, 10-07 |
  | 2024-10-12 | 10-11, 10-10, 10-09, 10-08 |

- **Pipeline trigger**: the lookback integration must be part of the main pipeline.
  Separate backfill pipelines cause the same overlap.
- **Waste of resources**: lookback partitions are reprocessed even without new late
  data. Add a control task that runs integration only where late data exists, or move to the
  Dynamic Late Data Integrator.
- **Time requirement**: the dataset must be partitioned by time, or at least have a time
  attribute. Without time boundaries there is no "late".

## Key design decisions

- **Lookback size**: the business tolerance (e.g. 15 days). It also bounds the cost: every
  run costs (N + 1) partitions.
- **Placement**: late-first for stateful jobs; current-first or parallel for stateless ones.
- **Unit of work**: one task per partition. Dynamic Task Mapping gives per-date retries and
  visibility.
- **Idempotent write**: `replaceWhere` / partition overwrite / MERGE. Never append.
- **Change detection** (optional): partition modification time vs. last integration marker, to
  skip unchanged lookback dates.
- **Concurrency**: `max_active_runs=1`, because two concurrent runs integrate overlapping windows.

## When to use / when not

| Situation | Use? | Alternative |
| --- | --- | --- |
| Known, bounded maximum delay (SLA "final after N days") | Yes | none |
| Late data is rare and the lookback is mostly wasted | Possibly + change check | Dynamic Late Data Integrator |
| Late data can arrive arbitrarily late and must be included | No | Dynamic Late Data Integrator |
| Streaming job with a watermark | No (that is the Late Data Detector's allowed lateness) | Late Data Detector + state |
| Dataset without time partitions | No | Full reload (Full Loader) |

## Gotchas & troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Backfill takes N times longer than expected | Every run restarted; lookbacks overlap | Restart only the latest run of the range (`plan-backfill`) |
| Counts doubled after integration | Late partitions appended instead of overwritten | `replaceWhere` / overwrite partition / MERGE |
| Stateful output wrong after late data | Current day computed before the history was fixed | Late-first placement and `depends_on_past=True` |
| Late data older than N days lost | Expected: the window is static | Increase N, or switch to the dynamic pattern |
| Two runs fight over the same partition | `max_active_runs > 1` | Serialize runs |
| Consumers not refreshed | No change notification | Publish changed partitions or trigger consumers (snowball) |
| Mapped tasks fail with "too many map indexes" | Lookback larger than `max_map_length` | Raise `[core] max_map_length` or batch dates per task |

## Implementation notes per stack

**Airflow**: `examples/static_late_data_integrator/airflow/static_late_integrator_dag.py`
:

```python
@task
def generate_backfilling_runs() -> List[str]:
    logical_date = get_current_context()["logical_date"]
    start = logical_date - dt.timedelta(days=LOOKBACK_DAYS)
    return [(start + dt.timedelta(days=d)).date().isoformat() for d in range(LOOKBACK_DAYS)]

integrate_late_data = SparkSubmitOperator.partial(task_id="integrate_late_data", ...) \
    .expand(application_args=has_late_data(backfilling_dates))
file_to_load_sensor >> load_current_partition >> integrate_late_data >> marked
```
`LATE_DATA_STRATEGY` switches between the three placements. `has_late_data` keeps only the dates
whose raw partition changed after the last integration marker (the resource-waste control).

**Spark**: `examples/static_late_data_integrator/spark/spark_static_late_integrator.py`.
`process_partition` recomputes the stats of one `event_date` and overwrites it:

```python
(stats.write.format("delta").mode("overwrite")
   .option("replaceWhere", f"event_date = '{partition_date}'").save(output))
```
`--execution-date E --lookback-days N --order late_first` runs the whole window in one
application. `plan-backfill` implements the overlapping-executions reasoning (`runs_to_restart`).

**Kafka**: `examples/static_late_data_integrator/kafka/kafka_static_lookback_reprocessor.py`. It
seeks with `offsets_for_times` to `(E - N) 00:00`, reads up to the end offsets captured at start,
aggregates per event date and drops records older than the window. Each date is written as one
atomic file overwrite. No group commits: the window defines what is read.

## Related patterns

- **Late Data Detector**: decides what "late" is; the integrator decides what to do with it.
- **Dynamic Late Data Integrator**: removes the fixed window and the wasted reprocessing.
- **Dead-Letter**: the same snowball backfilling consequence when replaying records.
- **Fast Metadata Cleaner / Data Overwrite**: idempotent partition rewrites for each
  integration task.
- **Readiness Marker**: the sensor that starts each run on a complete current partition.
