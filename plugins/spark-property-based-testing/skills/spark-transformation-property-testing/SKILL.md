---
name: spark-transformation-property-testing
description: "Property tests for PySpark DataFrame -> DataFrame transformations with the bundled spark_prop_testing library: TransformationPropertyTester (add_property, run, TransformationTestReport.assert_passed, shrunk failure_input, crashes as failures, from_prop with @prop), the standard property library (row_count_*, input_columns_preserved, no_new_nulls_introduced, column_types_preserved, output_is_non_empty_when_input_is_non_empty, make_sum_linearity_property, make_aggregation_monotonicity_property, make_idempotency_property, make_left_join_preserves_left_keys, make_window_rank_property), custom (input_df, output_df) properties, metamorphic relations, join fan-out, pipeline composition, determinism and money conservation. Use when testing that a Spark transformation keeps its invariants for any input."
---

# Spark Transformation Property Testing

A transformation is a function `DataFrame -> DataFrame`. A transformation property is a function
`(input_df, output_df) -> None` that raises `AssertionError` when an invariant breaks. The
bundled `TransformationPropertyTester` generates inputs from a schema, applies the transform,
runs every property and collects one report.

```python
from spark_prop_testing import (TransformationPropertyTester, prop,
    row_count_non_increasing, row_count_non_decreasing, row_count_exactly_preserved,
    input_columns_preserved, no_new_nulls_introduced, column_types_preserved,
    make_sum_linearity_property, make_idempotency_property,
    make_left_join_preserves_left_keys, make_window_rank_property)
from spark_prop_testing.transformations.standard_properties import (   # not re-exported
    output_is_non_empty_when_input_is_non_empty, make_aggregation_monotonicity_property)
```

Related skills: property families and Hypothesis settings in `spark-property-testing-foundations`;
input generation (`DataFrameConfig`, `ColumnConfig`, unique keys, related tables) in
`spark-dataframe-strategies`; schema contracts in `spark-schema-contract-testing`; GX output
rules in `spark-data-quality-property-testing`; `@spark_property`, the pytest plugin and the
CLI in `spark-prop-testing-toolkit`.

## When to Use

- A filter, join, aggregation, dedup or window step must keep row-count, column, null, type or
  sum invariants for every valid input.
- You cannot write the expected output, but you know how the output must react to an input
  change (metamorphic relation) or you have a simple Python model (differential property).
- Joins that may fan out, pipelines whose stages must compose, money that must add up.

## Decision table

| Testing need | Technique / API | Reference |
|---|---|---|
| Run several properties on one transform | `TransformationPropertyTester(...).add_property(p).run(n)` | `references/tester_api.md` |
| Declare properties next to the transform | `@prop(p1, p2, max_examples=n)` + `TransformationPropertyTester.from_prop` | `tester_api.md` |
| Filter / aggregate / enrich row counts | `row_count_non_increasing` / `_non_decreasing` / `_exactly_preserved` | `references/standard_properties.md` |
| Columns, nulls, types kept | `input_columns_preserved`, `no_new_nulls_introduced`, `column_types_preserved` | `standard_properties.md` |
| Measure neither created nor lost | `make_sum_linearity_property(col)` (conservation) | `standard_properties.md` |
| Aggregation without cross-row effects | `make_sum_linearity_property(col, transform=f)` | `standard_properties.md` |
| Unique keys / f(f(x)) == f(x) | `make_idempotency_property(keys[, transform])` | `standard_properties.md` |
| LEFT JOIN keeps left keys | `make_left_join_preserves_left_keys(key)` | `standard_properties.md` |
| Ranks start at 1, no gaps | `make_window_rank_property(part, rank, dense=)` | `standard_properties.md` |
| Rule specific to your logic | custom `(input_df, output_df)` property, `exceptAll` comparison | `references/custom_and_metamorphic_properties.md` |
| No oracle available | metamorphic relation: scale, add cancelled order, add duplicate | `custom_and_metamorphic_properties.md` |
| Join multiplies rows / money | `row_count_exactly_preserved` + sum conservation + dim-key uniqueness | `references/pipeline_properties.md` |
| Multi-stage pipeline, determinism, cents | stage properties, Python model, repartition check, Decimal sums | `pipeline_properties.md` |

## TransformationPropertyTester

`TransformationPropertyTester(spark, transform, input_schema, name="unnamed_transformation",
df_config=None)`; default `df_config` is `DataFrameConfig(min_rows=1, max_rows=100)` - pass a
small one. `add_property(p)` is fluent. `run(max_examples=None, phases=None)` (default 20, or `@prop`'s
value; `SPARK_PROP_MAX_EXAMPLES` overrides both) runs **each property as its own Hypothesis search** and
never raises. The `TransformationTestReport` has `results`, `passed`, `summary()` and
`assert_passed()`; each `PropertyTestResult` has `property_name` (the function `__name__`),
`passed`, `error_message`, `failure_input`.

```python
report = (TransformationPropertyTester(spark, clean_orders, RAW, name="clean", df_config=SMALL)
          .add_property(row_count_non_increasing)
          .add_property(make_idempotency_property(["order_id"], transform=clean_orders))
          .run(max_examples=5))
report.assert_passed()       # AssertionError with summary() if any property failed
```

- `failure_input`: up to 10 rows (`str(row.asDict())` per line) of the **shrunk** example -
  Hypothesis replays the minimal failure last and the tester keeps the last one.
- A transform or property that raises anything else is a failure, message prefixed with the
  exception type (`"AnalysisException: ..."`); assertion messages are kept as written.
- `from_prop(spark, transform, input_schema, df_config=None)` reads `@prop`'s properties and
  `max_examples`; a plain function raises `TypeError`.
- A failing property shrinks, each step another Spark run (tens of runs). For known-failing
  demos pass `run(max_examples=3, phases=(Phase.explicit, Phase.generate))` to skip shrinking.

## Standard properties (exact behaviour)

- Row counts: `row_count_non_increasing` (filters, aggregations), `row_count_non_decreasing`
  (enrichment), `row_count_exactly_preserved` (column maps), and
  `output_is_non_empty_when_input_is_non_empty`.
- `input_columns_preserved`: every input column name still exists. `no_new_nulls_introduced`:
  shared columns with 0 nulls in the input have 0 in the output (one aggregation per side).
  `column_types_preserved`: `simpleString()` equality, so `decimal(10,2)` -> `decimal(38,2)` and
  `array<int>` -> `array<bigint>` fail.
- `make_sum_linearity_property(col, transform=None, rel_tol=1e-9, abs_tol=1e-6)`: without
  `transform` sum conservation (`sum_conservation[col]`); with it the input is split by
  `abs(hash(all columns)) % 2` and `sum(f(A ∪ B)) == sum(f(A)) + sum(f(B))`
  (`sum_linearity[col]`). Compared with `math.isclose`; `group_columns` is ignored.
- `make_aggregation_monotonicity_property(col, transform, agg_fn="sum")` (`sum`, `count`,
  `max`, `min`): append a copy of one input row, `agg(f(X ∪ {r}))` must not decrease (`min`:
  not increase). `sum` is only monotone for non-negative values.
- `make_idempotency_property(keys, transform=None)`: output keys unique; with `transform` also
  same count and same multiset (row `Counter` on the driver) for `f(f(x))`; works for
  `dropDuplicates(subset)` dedups.
- `make_left_join_preserves_left_keys(key)`: every non-null input key is in the output (null keys
  never match). It does not check row counts.
- `make_window_rank_property(part, rank_col, dense=False)`: ranks >= 1, start at 1 per partition;
  `dense=True` adds max == distinct count (for `dense_rank`/`row_number`, not `rank` with ties);
  skips if `rank_col` is absent.

## Custom and metamorphic properties

Write `def p(input_df, output_df) -> None` with an `assert` (or `raise AssertionError`) whose
message names the rule, the numbers and sample rows. Never compare `collect()` lists: compare
multisets with `exceptAll` both ways or sorted tuples. Compare float sums with `math.isclose`,
decimals exactly. A metamorphic property closes over the transform and re-runs it on a modified
input: `amount × 3` ⇒ revenue × 3; one added cancelled order ⇒ revenue unchanged; one
re-delivered duplicate ⇒ dedup output unchanged. See `custom_and_metamorphic_properties.md`.

## Pipelines

Join fan-out: a duplicate key on the dimension side multiplies rows and money; catch it with
`row_count_exactly_preserved` + `make_sum_linearity_property(amount)` on the enrichment and
`make_idempotency_property([key])(dim, dim)` on the dimension. Composition: properties per stage
plus one end-to-end property against a plain-Python model. Determinism:
`f(df) == f(df.repartition(n))` with a total window order. Money: `DecimalType` and exact
equality per order (allocation remainders). See `pipeline_properties.md`.

## Gotchas

- Your own `exceptAll` comparisons can hit a Spark 3.5 analyzer bug after
  `dropDuplicates(subset)` (`IllegalStateException: Couldn't find <col>`); the built-in
  idempotency property compares on the driver and is not affected.
- `make_sum_linearity_property` with `transform` hashes all columns: map columns cannot be hashed.
- Each property re-runs the transform per example; budget `#properties × max_examples` Spark runs.
- `left_join_key_preservation` passes for an INNER JOIN whose only lost rows have null keys.

## Run it

```bash
PYTHONPATH=<plugin>/lib PYSPARK_PYTHON=$(which python) \
  python -m pytest -q skills/spark-transformation-property-testing/examples/test_tester_api.py
```

One file at a time on small machines; the examples use `local[1]`, 1 shuffle partition,
`max_examples` 2-5 and at most 8 rows (each file takes a few minutes on a slow laptop).

## Anti-patterns

- `row_count_exactly_preserved` on a filter, `dense=True` on `rank()`, `sum` monotonicity on
  signed values: the property is wrong, not the code.
- Default `df_config` (100 rows) with many properties: slow, and failures shrink longer.
- Asserting on `report.passed` without printing `report.summary()`.
- Comparing outputs with `collect() == collect()` or `subtract()` (drops duplicates).
- Exact `==` on float sums; tolerances on money.

## References

- `references/tester_api.md`, `references/standard_properties.md`,
  `references/custom_and_metamorphic_properties.md`, `references/pipeline_properties.md`.
- Examples: `examples/test_tester_api.py`, `test_standard_properties.py`,
  `test_custom_and_metamorphic_properties.py`, `test_pipeline_properties.py`.
