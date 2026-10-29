# GX 1.x pitfalls with Spark property tests

## Concept

The bundled layer targets **Great Expectations 1.x** (tested with 1.24; needs >= 1.3). The
0.x API (`SparkDFDataset`, `ge.from_pandas`, `get_validator`, `ExpectationConfiguration(
expectation_type=...)`, file-based data contexts, checkpoints in YAML) is gone. Most surprises
come from GX semantics that differ from a naive reading of the expectation names, and from
running a full validation per Hypothesis example.

## The GX 1.x flow used by the library

```python
context = gx.get_context(mode="ephemeral")                       # build_ge_context()
ds      = context.data_sources.add_spark(name="spark_datasource")
asset   = ds.add_dataframe_asset(name=suite.name)
bd      = asset.add_batch_definition_whole_dataframe("whole_dataframe")
batch   = bd.get_batch(batch_parameters={"dataframe": df})       # df_to_ge_batch()
result  = batch.validate(suite, result_format="COMPLETE")        # GEPropertyValidator.validate()
```

Expectations are added as `ExpectationConfiguration(type=..., kwargs=...)` through
`suite.add_expectation_configuration(...)`; validation results expose
`exp_result.expectation_config.type` / `.kwargs`, `exp_result.result["unexpected_count"]`,
`["unexpected_percent"]` and `exp_result.exception_info`.

## Pitfalls

### One expectation per type + column

A suite holds at most one expectation per (type, column). Adding a second
`expect_column_values_to_be_between` on `qty` **replaces** the first (last wins, no error):

```python
suite = (ExpectationSuiteBuilder("dup")
    .add_column(ColumnExpectationSpec("qty", min_value=0, max_value=100))
    .add_custom("expect_column_values_to_be_between", {"column": "qty", "min_value": 1, "max_value": 5})
    .build())
assert len(suite.expectations) == 1      # only 1..5 is checked
```

That is why the builder emits the null check once: `not_null=True` wins over
`max_null_fraction`. For two ranges on one column, combine them or use a different expectation
type (`expect_column_min_to_be_between` + `expect_column_max_to_be_between`).

### `mostly` is the fraction that must pass

`mostly=0.95` means "at least 95 % of the (non-null) values satisfy it". The builder converts
`max_null_fraction=f` into not-null with `mostly = 1 − f`. `max_null_fraction=0.5` with 2 of 4
values null passes; 3 of 4 fails.

### Percent vs fraction

GX's `unexpected_percent` is **0–100**. `ExpectationResult.unexpected_pct` divides by 100, so
it is a **0–1 fraction**; `str(result)` formats it as a percentage (`1 violations (25.0%)`).
Do not divide again, and do not compare `unexpected_pct` with `5` meaning 5 %.

### Nulls are not violations of column-map expectations

`to_be_between`, `to_be_in_set`, `to_match_regex`, `value_lengths_to_be_between` and
`to_be_unique` skip nulls, and `unexpected_percent` is computed over **non-null** values:
`qty = [3, None, 50, 2]` with `max_value=10` gives `unexpected_count=1`, `unexpected_pct=1/3`.
A column of only nulls passes a range check. Pair range/set rules with `not_null` or a
`max_null_fraction` when nulls are not allowed.

### The ephemeral context

`build_ge_context()` keeps everything in memory: nothing is written to `gx/`, there are no
Data Docs, no stored suites or validation history, and parallel test processes cannot clash.
Each `GEPropertyValidator()` (and each `DataQualityPropertySuite`) creates a new context;
building one costs noticeable time, so share one per test session. If you need Data Docs or
history in a pipeline, create a file context yourself and pass it: `GEPropertyValidator(context=...)`.

### Analytics and logging

GX sends usage statistics unless `GX_ANALYTICS_ENABLED=false`. The library sets it with
`os.environ.setdefault` when `context_factory` is imported; it does not override an explicit value and may come after GX was imported
elsewhere. Set it in the environment (CI) or at the top of the test module before importing GX.
The library also lowers the `great_expectations` logger to WARNING.

### Errors look like failures

An expectation that raises (column missing, wrong column type for a regex, unknown kwarg) is
reported with `success=False` and `details="error: <message>"`. A suite on a drifted schema
therefore fails with errors, not with violations: validate the schema first
(`DataQualityPropertySuite` does both).

### Unknown expectation names fail at `build()`

`add_custom("expect_nonsense", {...})` is accepted; `build()` raises
`InvalidExpectationConfigurationError: ... expect_nonsense not found`.

### Cost per example

One validation = one Spark job per metric (row count, null count, min, max, distinct count,
regex, ...), on a Python-RDD DataFrame that needs Python workers. On a laptop that is seconds
per example, more on Windows. Hence:

- `max_examples` 2–5 and `max_rows` ≤ 20 for GX properties; many examples go to cheap
  Spark-only or pure-Python properties;
- one shared validator; one suite per test (an asset per suite name is reused);
- shrinking reruns the suite per candidate: accept it, or `phases=[Phase.generate]` for demos;
- in pipelines `run(df)` on the real batch, once.

## Pitfalls / troubleshooting table

| Symptom | Cause | Fix |
|---|---|---|
| Second range rule silently ignored | one expectation per type+column | combine rules / other expectation type |
| `unexpected_pct` 100× too small or large | fraction vs percent | library value is 0–1 |
| All-null column passes range check | nulls skipped | add `not_null` / `max_null_fraction` |
| `AttributeError: ... SparkDFDataset` / `get_validator` | 0.x code | use the bundled 1.x helpers |
| Network calls / slow import in CI | analytics on | `GX_ANALYTICS_ENABLED=false` before import |
| Suite "fails" with `error:` details | expectation raised | check schema first, read `details` |
| Tests minutes long | GX per example, new context per test | small `max_examples`, session-scoped validator |
| Nothing in `gx/` after the run | ephemeral context | expected; pass a file context if needed |

## Related topics

- `gx_suite_builder.md` - how specs map to expectation types.
- `ge_property_bridge.md` - result objects and the Hypothesis loop.
- `quality_suite.md` - `run` vs `run_property_loop` cost.
