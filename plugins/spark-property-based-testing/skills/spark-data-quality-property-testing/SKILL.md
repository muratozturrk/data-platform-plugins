---
name: spark-data-quality-property-testing
description: "Data-quality property tests for PySpark with Great Expectations 1.x as bundled in spark_prop_testing: build_ge_context/df_to_ge_batch, ExpectationSuiteBuilder + ColumnExpectationSpec, add_custom, GEPropertyValidator and SuiteValidationResult, auto_generate_suite from a schema, DataQualityPropertySuite with QualitySuiteConfig and UnifiedQualityReport (run vs run_property_loop), strategies constrained to match the suite, detecting broken transformations with GX inside Hypothesis, YAML ge_expectations for the CLI, GX 1.x pitfalls and performance. Use when expressing data-quality rules as GX expectations and testing that pipelines keep them for any input."
---

# Spark Data-Quality Property Testing

Great Expectations (GX) states data-quality rules declaratively: not null, unique, in a set,
between bounds, matches a regex. Hypothesis finds inputs that break rules. The bundled bridge
puts the two together: a GX suite becomes the property, generated DataFrames are the inputs,
and a transformation is correct if its output passes the suite for every valid input.

```python
from spark_prop_testing import (ExpectationSuiteBuilder, ColumnExpectationSpec, GEPropertyValidator,
    auto_generate_suite, DataQualityPropertySuite, QualitySuiteConfig, UnifiedQualityReport)
from spark_prop_testing.ge_integration.context_factory import build_ge_context, df_to_ge_batch
```

GX is an optional extra (`pip install -e "<plugin>/lib[ge]"`, `great-expectations>=1.3`) and is
imported lazily: these names load on first access; strategies, schema and transformation layers
work without it. Set `GX_ANALYTICS_ENABLED=false` in tests. Hypothesis settings and the session
are in `spark-property-testing-foundations`, strategies in `spark-dataframe-strategies`, schema
contracts in `spark-schema-contract-testing`.

## When to Use

- Quality rules (keys unique, status in a set, amounts in range, null budgets) must hold for
  every output of a transformation, not only for today's data.
- A GX suite already exists and you want to prove a new transformation keeps it.
- Bootstrapping a baseline suite from a schema.
- One report over schema, GX and custom checks, in tests or as a pipeline gate.
- Declaring suites in YAML for `spark-prop-test run`.

## Decision table

| Testing need | Technique / API | Reference |
|---|---|---|
| Build a suite in code | `ExpectationSuiteBuilder(name).add_column(ColumnExpectationSpec(...)).build()` | `references/gx_suite_builder.md` |
| Any other GX expectation | `.add_custom(expectation_type, kwargs)` | `gx_suite_builder.md` |
| Baseline suite from a schema | `auto_generate_suite(schema, suite_name, min_rows)` | `gx_suite_builder.md` |
| Validate one DataFrame | `GEPropertyValidator().validate(df, suite)` → `SuiteValidationResult` | `references/ge_property_bridge.md` |
| Suite as a Hypothesis property | validator inside `@given`, strategy constrained to the suite | `ge_property_bridge.md` |
| Detect a broken transformation | validate `f(df)` for generated `df` | `ge_property_bridge.md` |
| Schema + GX + custom in one report | `DataQualityPropertySuite(spark, QualitySuiteConfig(...))` | `references/quality_suite.md` |
| Transform under test over generated raw input | `QualitySuiteConfig(transform=f, input_schema=RAW)` + `run_property_loop()` | `quality_suite.md` |
| Real batch in a pipeline vs generated data | `run(df)` vs `run_property_loop()` | `quality_suite.md` |
| Suites in YAML / CLI | `ge_expectations` in `spark_prop_test.yaml`, `ConfigLoader` | `quality_suite.md` |
| Odd GX results | one expectation per type+column, `mostly`, percent, nulls | `references/gx_pitfalls.md` |

## GX 1.x plumbing

`build_ge_context()` = `gx.get_context(mode="ephemeral")` (in memory, nothing on disk).
`df_to_ge_batch(context, df, datasource_name="spark_datasource", asset_name="test_asset")`
gets-or-creates a Spark data source → DataFrame asset → whole-DataFrame batch definition and
returns `get_batch(batch_parameters={"dataframe": df})`; `batch.validate(suite)` runs it.
`GEPropertyValidator(context=None)` wraps that and parses the result. Create the validator
**once** (session fixture), not per example.

## ExpectationSuiteBuilder + ColumnExpectationSpec

```python
SUITE = (ExpectationSuiteBuilder("orders_contract")
    .add_row_count(min_rows=1, max_rows=20)
    .add_column(ColumnExpectationSpec("order_id", not_null=True, unique=True, min_value=1))
    .add_column(ColumnExpectationSpec("status", not_null=True, value_set=STATUSES))
    .add_column(ColumnExpectationSpec("amount", min_value=0, max_value=10_000))
    .add_custom("expect_column_values_to_be_of_type", {"column": "amount", "type_": "IntegerType"})
    .build())
```

Spec fields: `not_null`, `unique`, `min_value`/`max_value`, `value_set`, `regex_pattern`,
`min_length`/`max_length`, `max_null_fraction` (→ not-null with `mostly = 1 − fraction`, only
when `not_null` is off). `auto_generate_suite` adds row count ≥ `min_rows`, NOT NULL for
`nullable=False` fields and type bounds for byte/short/int/long/float columns.

## GEPropertyValidator and SuiteValidationResult

`validate(df, suite)` → `SuiteValidationResult(suite_name, success, results)`, with `failures`,
`summary()`, `assert_success()`. Each `ExpectationResult` has `expectation_type`, `column`,
`success`, `unexpected_count`, `unexpected_pct` (a **0–1 fraction**; GX reports 0–100) and
`details` (an expectation that raised is a failure with `"error: ..."`). It never raises for
data problems.

## The key pattern: constrain the strategy to the suite

Generated data must be *valid input* for the rules you test. A unique or min/max expectation on
a column generated with the default type strategy fails on the data, not on your code.

```python
MATCHING = DataFrameConfig(min_rows=1, max_rows=20, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),   # unique + min 1
    "status":   ColumnConfig(custom_strategy=st.sampled_from(STATUSES)),           # value_set
    "amount":   ColumnConfig(custom_strategy=st.integers(0, 5_000)),               # range
})

@settings(settings.get_profile("spark"), max_examples=2)
@given(df=spark_dataframes(spark, ORDERS, MATCHING))
def check(df):
    result = validator.validate(apply_discount(df), SUITE)
    assert result.success, result.summary()
```

Anti-pattern: `order_id` from `st.integers(1, 3)` (or the default IntegerType strategy) without
`unique=True` → duplicates; `min_value=1` with the default strategy → negatives. The suite
"fails" every run and hides real bugs. Constrain *inputs* to the input contract; the property
then checks the *output* suite. A broken transformation (`amount - 10_000`) is then caught on
the first example (`examples/test_ge_property_bridge.py`, `xfail(strict=True)`).

## DataQualityPropertySuite

`QualitySuiteConfig(suite_name, expected_schema=None, ge_suite=None, df_config=DataFrameConfig(),
max_examples=20, custom_properties=[], transform=None, input_schema=None)`. `run(df)` validates
one DataFrame with all layers: `SchemaValidator(expected_schema)`, the GX suite, and each custom
`fn(df)` (an `AssertionError` is recorded). `run_property_loop()` generates DataFrames from
`input_schema` (default `expected_schema`) + `df_config`, applies `transform` if set, and runs
`run()` on the **output**, Hypothesis shrinking failures. `UnifiedQualityReport`:
`schema_results`, `schema_valid`, `ge_results`, `property_failures`, `is_valid` (structured
flags), `summary()`, `assert_valid()`. After the loop the report carries the per-layer results
of the last example (on failure: the minimal failing one) plus the failure text.

### Transform under test

```python
quality = DataQualityPropertySuite(spark, QualitySuiteConfig(
    suite_name="orders_clean", input_schema=RAW_ORDERS, df_config=RAW_CONFIG,   # raw inputs
    transform=clean_orders, expected_schema=ORDERS, ge_suite=GE_SUITE,          # output contract
    max_examples=2))
quality.run_property_loop().assert_valid()
```

`df_config` describes valid *raw* input; the schema and GX suite describe the *output*. A
transform that forgets to normalise `status` fails `expect_column_values_to_be_in_set` on the
first example (`examples/test_quality_suite.py`, `xfail(strict=True)`).

## YAML suites (CLI)

`spark_prop_test.yaml` → `suites.<name>`: `schema_module`, `schema_name`, optional
`input_schema_name` (+ `input_schema_module`) and `transform: module:function`, `max_examples`,
`min_rows`, `max_rows`, `null_rates`, and `ge_expectations` (`row_count_min`/`row_count_max`,
`columns.<col>` with the spec fields above). `spark-prop-test run -c spark_prop_test.yaml -n 5`
runs `run_property_loop()` per suite. Without a `transform`, the generator is aligned with
`ge_expectations.columns` (unique, min/max, value_set, not_null), so the suite can hold on
generated data; with a `transform`, inputs are generated unaligned and the transform must make
any input valid. `SPARK_PROP_MASTER` sets the CLI's master (default `local[2]`).

## GX 1.x pitfalls (short)

One expectation per type+column (a second `between` on a column replaces the first); `mostly`
is the fraction that must pass; percent vs fraction; column-map expectations ignore nulls and
count percent over non-null values; ephemeral context = nothing persisted. See
`references/gx_pitfalls.md`.

## Performance

A GX validation per example is several Spark jobs (seconds each on a laptop). Keep
`max_examples` at 2–5 and rows ≤ 20 for GX properties, reuse one validator, skip shrinking in
known-failing demos (`phases=[Phase.generate]`), and put the many-example search in cheap
Spark-only properties. In pipelines call `run(df)` on the real batch; reserve
`run_property_loop()` for tests.

## Run it

```bash
GX_ANALYTICS_ENABLED=false PYSPARK_PYTHON=$(which python) PYTHONPATH=<plugin>/lib \
  python -m pytest -q skills/spark-data-quality-property-testing/examples/test_ge_property_bridge.py
spark-prop-test init && spark-prop-test run -c spark_prop_test.yaml -n 3 --report
```

One file at a time on small machines (`local[1]`); each file takes about a minute or more.

## Anti-patterns

- Unique/min/max/value-set expectations over unconstrained generated columns.
- A new `GEPropertyValidator()`/context per example.
- Two expectations of the same type on one column (only the last survives).
- Reading `unexpected_pct` as 0–100, or expecting nulls to count as range violations.
- `run_property_loop()` with 20+ examples in a pipeline job.
- Asserting `result.success` without printing `result.summary()`.

## References

- `references/gx_suite_builder.md`, `references/ge_property_bridge.md`,
  `references/quality_suite.md`, `references/gx_pitfalls.md`.
- Examples: `examples/test_gx_suite_builder.py`, `test_ge_property_bridge.py`,
  `test_quality_suite.py`.
