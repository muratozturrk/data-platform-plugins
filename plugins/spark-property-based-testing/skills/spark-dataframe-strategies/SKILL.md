---
name: spark-dataframe-strategies
description: "Generate PySpark DataFrames for Hypothesis property tests with the bundled spark_prop_testing library: strategy_for_type for every Spark type (nested, decimal, UTC timestamps), spark_dataframes with DataFrameConfig/ColumnConfig (custom_strategy, unique, null_config), NullConfig semantics, skewed/edge-case/id distribution helpers, profiles, bounded money, related columns and foreign keys via composite/flatmap, and performance. Use when writing or fixing the input strategies of a Spark property test."
---

# Spark DataFrame Strategies

A property test is only as good as its inputs. `spark_prop_testing.strategies` turns a Spark
`StructType` into a Hypothesis strategy of DataFrames, with per-column control of values,
uniqueness and nulls, plus helpers for realistic and adversarial distributions. Generated
DataFrames shrink like any Hypothesis value: fewer rows, simpler values, nulls.

```python
from spark_prop_testing import (spark_dataframes, DataFrameConfig, ColumnConfig, NullConfig,
    skewed_integers, edge_case_integers, edge_case_strings, realistic_id_strategy)
from spark_prop_testing.strategies.type_strategies import strategy_for_type
```

Concepts, settings and the test session are in `spark-property-testing-foundations`.

## When to Use

- Writing `@given(df=...)` inputs for a transformation, join or aggregation test.
- Columns need realistic domains (status enums, bounded amounts, key ranges, skew).
- Keys must be unique, foreign keys must exist, or `end_date >= start_date` must hold.
- Null handling must be exercised at a given rate or guaranteed at least once.
- Tests overflow, crash on generated values, or are too slow.

## Decision table

| Need | API | Reference |
|---|---|---|
| Values for one Spark type | `strategy_for_type(dtype)` | `references/type_strategies.md` |
| DataFrame for a schema | `spark_dataframes(spark, schema, DataFrameConfig(...))` | `references/dataframe_config.md` |
| Narrow a column's domain | `ColumnConfig(custom_strategy=st.integers(1, 99))` | `dataframe_config.md` |
| Primary key | `ColumnConfig(..., unique=True)` | `dataframe_config.md` |
| Nulls at a rate / always / at least one, per column or for all | `NullConfig(null_rate=…, always_include_null=…)`, `DataFrameConfig(default_null_config=…)` | `references/nulls_and_edge_cases.md` |
| Hot keys, boundaries, nasty strings, FK-like ids | `skewed_integers`, `edge_case_integers`, `edge_case_strings`, `realistic_id_strategy` | `nulls_and_edge_cases.md` |
| Ready-made row bounds | `strategies/profiles.py` | `dataframe_config.md` |
| Money | `st.decimals(min, max, places=2)` + `DecimalType(p, 2)` | `references/composing_related_data.md` |
| Related columns / related DataFrames | `st.composite`, `flatmap` | `composing_related_data.md` |

## strategy_for_type

Booleans; Byte/Short/Integer/Long over their full ranges; Float (`width=32`) and Double
finite without NaN/inf; `DecimalType(p, s)` → full range `±(10**p-1)·10**-s` with `s` places
(`DecimalType(5,2)` reaches 999.99); String = `st.text(max_size=100)` or `""`/`" "`;
Binary ≤ 256 bytes; Date 1900-2100; Timestamp = **UTC-aware** datetimes 1970-01-02 to
2100-12-31; Array (≤10 elements, `None` if
`containsNull`); Map (≤10 entries, `None` values if `valueContainsNull`); Struct (recursive,
nullable fields may be `None`). Anything else raises `NotImplementedError` (e.g.
`TimestampNTZType`, interval types).

Gotchas: full-range integers/doubles/decimals overflow in sums: bound them. Timestamps start
in 1970 because PySpark's `collect()` (`datetime.fromtimestamp`) raises `OSError` on Windows for
pre-1970 instants; a pre-1970 `custom_strategy` works on Linux/macOS or when you compare via
`cast("long")` without collecting.

## spark_dataframes, DataFrameConfig, ColumnConfig

```python
CONFIG = DataFrameConfig(min_rows=2, max_rows=15, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(["new", "paid", "shipped"])),
    "amount":   ColumnConfig(custom_strategy=st.integers(0, 1_000),
                             null_config=NullConfig(null_rate=0.1)),
})

@settings(settings.get_profile("spark"), max_examples=5)
@given(df=spark_dataframes(spark, ORDER_SCHEMA, CONFIG))
def test_x(df): ...
```

- Defaults: `DataFrameConfig(min_rows=1, max_rows=100, default_null_config=None)`;
  `ColumnConfig()` = type strategy, no nulls, not unique.
- Columns missing from `column_configs` get `ColumnConfig(null_config=default_null_config)` when
  `default_null_config` is set ("40% nulls everywhere"), else `ColumnConfig()`.
  `config.column(name)` returns the effective `ColumnConfig`. An explicit entry wins entirely
  (its own `null_config`, default no nulls).
- `custom_strategy` replaces the type strategy entirely; it must produce values of the column's
  type (int for IntegerType, `Decimal` for DecimalType...).
- `unique=True`: no repeated values (`st.lists(unique_by=...)`); a nullable unique column holds at
  most one null.
- Top-level nulls come **only** from `NullConfig` (per column or `default_null_config`), and
  only for `nullable=True` fields.
- Numeric ranges, value sets and formats are expressed through `custom_strategy`.

## NullConfig

`NullConfig(null_rate=0.0, always_include_null=False)`, `0 <= null_rate <= 1` else `ValueError`.
Rate `1.0` → column always null. Fractional rates are applied **per DataFrame** with a
Hypothesis-seeded `Random` (rate 0.5 over 100 rows ≈ 50 nulls, not 0% or 100%).
`always_include_null=True` → at least one null in every non-empty DataFrame. Both are ignored for
non-nullable fields. `with_nulls(strategy, cfg)` is the per-value wrapper (1/1000 resolution,
shrinks toward `None`).

## Distribution helpers

`skewed_integers(min_value=0, max_value=1_000_000, skew_value=None, skew_probability=0.7)`
(hot key = `skew_value` or `min_value`, shrinks toward it); `edge_case_integers(min, max)`
(0, ±1, bounds, bounds∓1 + uniform; pass bounds inside the column type and the domain);
`edge_case_strings()` (`""`, `" "`, `"\t\n"`, `"NULL"`, `"None"`, `"NaN"`, SQL injection, `café`,
emoji, 1000×`a`, random text); `realistic_id_strategy(max_id=100)` = `st.integers(1, max_id)`.

## Profiles (`strategies/profiles.py`)

Four functions returning a `DataFrameConfig`:
`edge_case_profile()` 1-10 rows; `stress_test_profile()` 1 000-10 000 rows;
`high_null_profile(null_rate=0.4)` 50-500 rows, `default_null_config=NullConfig(null_rate)` (every
nullable column without its own config); `production_like_profile(id_cardinality=1000,
id_columns=(), null_rate=0.02)` 100-1000 rows, `realistic_id_strategy(max_id=id_cardinality)` on
each of `id_columns` and a background `null_rate` on the other nullable columns. Override columns
via `config.column_configs[name] = ColumnConfig(...)`.

## Composing related data

Per-column strategies are independent. For cross-column rules use `st.composite` (draw start,
then end) or `flatmap`; build rows yourself and `spark.createDataFrame(rows, schema)`. For two
DataFrames, draw parent keys first and `flatmap` into a child `spark_dataframes` whose FK column
uses `st.sampled_from(keys)`.

## Performance

Cost ≈ `max_examples × (Python row generation + createDataFrame + every Spark action)`. Keep
`max_rows` ≤ 20-50 for logic tests (bugs show up with 1-3 rows after shrinking), 3-20 examples
locally, 100 in CI. Prefer one aggregation over many `count()`s. Test row-level Python logic
without Spark. Large `min_rows` (profiles with hundreds/thousands of rows) cost seconds per
example and need the suppressed `large_base_example`/`data_too_large` checks; for volume use
`spark.range(n)` rather than generated rows.

## Run it

```bash
PYSPARK_PYTHON=$(which python) PYTHONPATH=<plugin>/lib \
  python -m pytest -q skills/spark-dataframe-strategies/examples/test_dataframe_config.py
```

One Spark test file at a time on small machines (`local[1]`, `shuffle.partitions=1`).

## Anti-patterns

- `st.integers()` / default IntegerType strategy for amounts that get summed.
- A float `custom_strategy` for an IntegerType column (createDataFrame type error).
- Expecting uniqueness/min-value expectations to pass without `unique=True`/bounds.
- `.filter()`/`assume()` to enforce `end >= start` (health checks, `Unsatisfiable`): construct it.
- Expecting profile nulls on `nullable=False` fields or on columns you configured explicitly.
- `@given(df=...)` with a literal `...` placeholder.
- Hundreds of rows "to be realistic" in every logic test.

## References

- `references/type_strategies.md`, `references/dataframe_config.md`,
  `references/nulls_and_edge_cases.md`, `references/composing_related_data.md`.
- Examples: `examples/test_type_strategies.py`, `test_dataframe_config.py`,
  `test_nulls_and_edge_cases.py`, `test_composing_related_data.py`.
