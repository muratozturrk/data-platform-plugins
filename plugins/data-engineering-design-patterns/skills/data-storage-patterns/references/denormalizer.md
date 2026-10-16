# Denormalizer

> Category: data representation. Analytical read models (One Big Table, nested structs, star schema).

## Problem

A company built a relational model on its data warehouse. Nobody saw problems while volumes were
low, but the product took off and analysts now complain about query time. The most expensive
pattern joins the same **eight tables** in 80% of queries. Reads must get faster.

## Solution / Mechanics

Reduce or eliminate joins by **flattening values from the joined tables into a single row**, so
queries need no network exchange. There are two flattening styles, plus the star
schema as a partial step:

1. **Regular columns (One Big Table)**: each column of the joined tables is copied as a top-level
   column: `visit_id, user_id, user_name, device_id, device_full_name, visit_time, visited_page`.
2. **Nested structures**: rows of the joined tables go into one `STRUCT` column, and users read
   `device.full_name` instead of a top-level column.
3. **Star schema**: still fact plus dimensions, but no nested dimensions. Sub-dimensions are folded
   into the top-level dimension, so reads need only fact -> dimension joins.
4. **Combine with the Normalizer** when consistency still matters: build the normalized
   (snowflake) model first, derive the denormalized one from it, and keep them in sync with a
   sequence pattern (data flow patterns). The snowflake model can stay private.
5. **Pay once, read many**: the join cost is paid once per write, and every reader benefits.

```
 normalized (source of truth, may stay private)              denormalized (exposed)
 fact_visit -- dim_page -- dim_page_category      build     one_big_table: visit_id, page, category,
     |                                          ========>                  date, month_name, quarter_name
     +------ dim_date -- dim_date_month          (Local                    (no joins on read)
                     \-- dim_date_quarter        Sequencer / star: fact_visit -- dim_page(+category)
                                                 dataset)        \-- dim_date(+month, quarter)
```

## Consequences & trade-offs

| Consequence  | What happens | Mitigation |
| --- | --- | --- |
| Costly updates | Duplicated attributes mean one change rewrites many rows instead of one | Treat the table as a **snapshot** (point-in-time truth, no updates); otherwise accept slower updates for faster reads |
| Storage | The same information from joined tables repeats many times | Encodings, especially **dictionary** (`{1: "long name..."}`), which also speed up existence checks |
| One big antipattern | An OBT without domain logic (user details + past orders + current visit + favorite color) becomes a "trash bag" | Name test: if the name needs many "and"/"with", split it |
| Consistency | The denormalized copy drifts from its sources between rebuilds | Build from a normalized model with a sequence pattern; expose the refresh time |
| Read joins still present (star) | The star schema writes more tables and still needs joins on read | OBT for the hottest queries; star for flexible slicing |

## Key design decisions

- **Shape**: flat OBT (simplest SQL), nested STRUCTs (keeps domain grouping, needs engine
  support), or star (fewer joins, still dimensional).
- **Rebuild strategy**: full CTAS + atomic swap, incremental MERGE, or daily snapshot partitions.
- **Dimension version to embed**: as of event time (SCD2 join) or current state.
- **Scope**: one table per domain question, not one table per company.
- **Layout of the result**: partition and sort the OBT for its dominant predicates.

## When to use / when not

| Situation | Use Denormalizer? | Alternative |
| --- | --- | --- |
| Most queries join the same many tables; read latency matters | Yes | - |
| Attributes change often and must be current everywhere | Careful: costly updates | Normalizer + views; Dataset Materializer with frequent refresh |
| Need both consistency and fast reads | Yes, on top of a Normalizer model | - |
| Streaming events need reference attributes inline | Yes (stream-table enrichment) | Dynamic Joiner at read time |
| Storage-constrained, highly repetitive wide attributes | Maybe | Star schema; rely on dictionary encoding |
| Ad hoc exploration across unrelated domains | No (trash-bag OBT) | Domain-specific OBTs or star schema |

## Gotchas & troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| OBT has more rows than the fact table | Join fan-out (non-unique dimension key) | Check grain: `count(OBT) == count(fact)`, the DAG's row-count guard |
| Facts missing in OBT | INNER join on incomplete dimension | LEFT joins (as in the examples) |
| Readers see a half-built table | Rebuild in place | Build `*_next`, then atomic rename / `CREATE OR REPLACE` / Delta overwrite |
| Enriched stream has stale device data | Enrichment copies attributes at processing time | Accept as snapshot or re-enrich; join dimension on read for current values |
| Visits enriched with empty device fields | Dimension topic not fully loaded at startup | Bootstrap the table first (GlobalKTable-like) |
| Nobody understands the OBT | No domain boundary | Apply the naming test, split by domain |

## Implementation notes per stack

**Spark** - `examples/denormalizer/spark/spark_denormalizer.py`: from an inline snowflake model it
writes a flat One Big Table, a nested-STRUCT variant and a star schema on Delta.

```python
page_w_category = (dim_page.join(dim_page_category,
                                 dim_page.dim_page_category_id == dim_page_category.page_category_id,
                                 "left_outer").drop("dim_page_category_id", "page_category_id"))
obt = (fact_visit
       .join(page_w_category, fact_visit.dim_page_id == page_w_category.page_id, "left_outer")
       .join(date_w_month_quarter, fact_visit.dim_date_id == date_w_month_quarter.date_id, "left_outer")
       .drop("dim_page_id", "dim_date_id", "page_id", "date_id"))
obt.write.mode("overwrite").format("delta").save(f"{base}/one_big_table")
nested = obt.select("visit_id", "event_time",
                    F.struct("page", "category").alias("page"),
                    F.struct("date", "month_name", "quarter_name").alias("date"))
```

**Kafka** - `examples/denormalizer/kafka/kafka_stream_table_enricher.py`: the compacted `devices`
topic is cached in memory (tombstones delete), and each `visits` event is flattened with
`device_*` attributes into `visits_enriched`. Consumers read self-contained records, and each
record is a snapshot of the device at processing time.

```python
def enrich(visit: dict, devices: dict[str, dict]) -> dict:
    device = devices.get(visit.get("device_id"), {})
    return {**visit, **{f"device_{k}": v for k, v in device.items() if k != "device_id"}}
```

**Airflow** - `examples/denormalizer/airflow/denormalizer_dag.py`: a DAG triggered by the normalized
model's dataset rebuilds a PostgreSQL OBT with CTAS into `visits_obt_next`, checks the row count
against the fact table, then swaps it in atomically.

```python
@dag(schedule=[NORMALIZED_VISITS], start_date=datetime(2024, 1, 1), catchup=False)
def denormalizer():
    ...
    if obt != src:  # LEFT joins must not duplicate or drop facts
        raise ValueError(f"OBT has {obt} rows, visits has {src}: check join keys for fan-out")
    ...
    build_one_big_table() >> check_row_count() >> swap_tables()
SWAP_SQL = """ALTER TABLE IF EXISTS dp.visits_obt RENAME TO visits_obt_old;
ALTER TABLE dp.visits_obt_next RENAME TO visits_obt;
DROP TABLE IF EXISTS dp.visits_obt_old;"""  # one transaction -> atomic for readers
```

## Related patterns

- **Normalizer**: the consistent source model a denormalized table is built from.
- **Dataset Materializer**: an OBT is a materialized join; same refresh trade-offs.
- **Local Sequencer / Isolated Sequencer**: keep the denormalized table in sync after normalized loads.
- **Static Joiner / Dynamic Joiner**: the enrichment mechanics behind flattening, including SCD joins.
- **Horizontal Partitioner / Sorter**: organize the wide table for its main predicates.
- **Metadata Enhancer**: dictionary encoding and column stats offset the repetition cost.
