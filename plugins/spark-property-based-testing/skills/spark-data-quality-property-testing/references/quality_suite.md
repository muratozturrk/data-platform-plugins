# DataQualityPropertySuite, QualitySuiteConfig, UnifiedQualityReport and YAML suites

## Concept

`DataQualityPropertySuite` runs three layers against a DataFrame and collects them in one
`UnifiedQualityReport`: **schema** (`SchemaValidator(expected_schema)`, skill
`spark-schema-contract-testing`), **content** (a GX suite through `GEPropertyValidator`) and
**custom properties** (functions `fn(df) -> None` that raise `AssertionError`).

| | `run(df)` | `run_property_loop()` |
|---|---|---|
| Input | one DataFrame you pass (a real batch, a fixture, `f(df)`) | DataFrames generated from `input_schema` (default `expected_schema`) + `df_config`, passed through `transform` if set |
| Question | "does this batch meet the contract?" | "does the contract hold for the output of every generated input?" |
| Where | pipelines (gate before write), integration tests | unit/property tests, `spark-prop-test run` |
| Cost | one GX validation | `max_examples` validations (+ shrinking on failure) |
| Report | all layers for that DataFrame | all layers of the last example (on failure the minimal failing one) + failure text |

## API reference

```python
from spark_prop_testing import DataQualityPropertySuite, QualitySuiteConfig, UnifiedQualityReport

@dataclass
class QualitySuiteConfig:
    suite_name: str
    expected_schema: StructType | None = None      # OUTPUT contract
    ge_suite: gx.ExpectationSuite | None = None    # OUTPUT content rules
    df_config: DataFrameConfig = DataFrameConfig() # INPUT generator (valid raw input)
    max_examples: int = 20                          # SPARK_PROP_MAX_EXAMPLES overrides
    custom_properties: list[Callable[[DataFrame], None]] = []
    transform: Callable[[DataFrame], DataFrame] | None = None   # code under test
    input_schema: StructType | None = None          # generation schema (default expected_schema)

DataQualityPropertySuite(spark, config)
    .run(df) -> UnifiedQualityReport                # never raises for data problems
    .run_property_loop() -> UnifiedQualityReport    # ValueError without expected_schema/input_schema

@dataclass
class UnifiedQualityReport:
    suite_name: str
    schema_results: list[str]          # SchemaValidationResult.summary() texts
    schema_valid: list[bool]           # structured flags behind is_valid
    ge_results: list[SuiteValidationResult]
    property_failures: list[str]       # custom property messages / the loop's failure text
    is_valid -> bool                   # all(schema_valid) and all GX success and no property failure
    summary() -> str
    assert_valid() -> None             # AssertionError(summary())
```

Validity comes from flags, not text (a column named `ERROR_CODE` does not fail a report); schema
WARNINGs do not invalidate. `run_property_loop()` uses
`spark_settings(max_examples, phases=config.phases)` (`deadline=None`, Spark health checks
suppressed; `phases=None` keeps the loaded profile's phases) and catches any exception, so a crashing transform or custom
property is reported, not raised.

## Worked example

`examples/test_quality_suite.py`:

```python
CONFIG = QualitySuiteConfig(suite_name="orders", expected_schema=ORDERS, ge_suite=GE_SUITE,
                            df_config=DF_CONFIG, max_examples=2, custom_properties=[no_duplicate_rows])
quality = DataQualityPropertySuite(spark, CONFIG)       # session fixture: owns one GX context
quality.run(batch).assert_valid()                        # real batch, all layers
report = quality.run(bad_batch)                          # duplicate key + unknown status + extra column
assert report.schema_valid == [True]                     # extra column = INFO
assert report.property_failures == ["duplicate rows"]    # raise explicitly: pytest rewrites `assert`
```

### Transform under test

```python
def clean_orders(df):                                    # raw → contract
    return df.select("order_id", F.lower(F.trim("status")).alias("status"),
                     (F.col("amount_cents") / 100).cast("int").alias("amount"))

quality = DataQualityPropertySuite(spark, QualitySuiteConfig(
    suite_name="orders_clean", input_schema=RAW_ORDERS, df_config=RAW_CONFIG,   # " NEW", "Paid ", ...
    transform=clean_orders, expected_schema=ORDERS, ge_suite=GE_SUITE, max_examples=2))
report = quality.run_property_loop()
report.assert_valid()
assert report.schema_valid == [True] and report.ge_results[0].success
```

`df_config` constrains the *input* contract (unique raw keys, raw status spellings, cent
ranges); the schema and suite judge the *output*. `clean_orders_buggy` (status not normalised)
fails `expect_column_values_to_be_in_set` on the first example; the example marks it
`xfail(strict=True)` and passes `QualitySuiteConfig(phases=(Phase.explicit, Phase.generate))`
to skip shrinking, since every shrink step is a full GX validation.

In a pipeline, call `run(out)` on the real batch before the write and quarantine/alert with
`report.summary()` when `not report.is_valid`; generated-data loops test code, not data.

## YAML suites and the CLI

`ConfigLoader(path).load() -> dict[str, SuiteConfig]` (`name`, `schema`, `input_schema`,
`transform`, `df_config`, `ge_suite`, `max_examples`) reads `spark_prop_test.yaml` (UTF-8):

```yaml
suites:
  orders_generated:                       # no transform: data generated from ORDERS is validated
    schema_module: myproject.schemas      # importable; the CLI puts the working dir on sys.path
    schema_name: ORDERS
    max_examples: 20
    min_rows: 1
    max_rows: 100
    null_rates: {note: 0.05}              # → ColumnConfig(null_config=NullConfig(rate))
    ge_expectations:
      row_count_min: 1
      columns:
        order_id: {not_null: true, unique: true, min_value: 1}
        status:   {value_set: [new, paid]}
  orders_clean:                           # transform under test
    schema_module: myproject.schemas
    schema_name: ORDERS                   # output contract
    input_schema_name: RAW_ORDERS         # + optional input_schema_module (default schema_module)
    transform: myproject.pipeline:clean_orders
    ge_expectations:
      columns:
        order_id: {not_null: true, unique: true}
```

Column keys: `not_null`, `unique`, `min_value`, `max_value`, `value_set`, `regex_pattern`,
`min_length`, `max_length`, `max_null_fraction`. GX is imported only when a suite has
`ge_expectations`; `transform` must be `module:function` (else `ValueError`).

**Generator alignment.** Without a `transform` the generated data *is* the validated data, so
the loader shapes the generator from `ge_expectations.columns`: `unique` → unique column,
`min_value`/`max_value` → bounded integers (byte/short/int/long) or floats, `value_set` →
`sampled_from`, `not_null` → no nulls on a nullable column. The suite can then hold by
construction, including the `init` template's `id`/`status` rules. `regex_pattern` and length rules are not aligned. With
a `transform`, nothing is aligned: inputs come from `input_schema` with only row bounds and
`null_rates`, and the transform must turn *any* such input into valid output (dedupe, clamp,
normalise); that is the property.

```bash
spark-prop-test init                         # template spark_prop_test.yaml
SPARK_PROP_MASTER=local[1] spark-prop-test run -c spark_prop_test.yaml -n 5 -s orders_clean --report
```

`run` calls `run_property_loop()` per suite; `-n` sets `SPARK_PROP_MAX_EXAMPLES`; the table and
`--report` HTML (autoescaped) show the schema/GX results of the last (or minimal failing)
example; exit code 1 if any suite fails.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Transform suite fails on `unique`/ranges of the output | unaligned raw input reaches the output unchanged | make the transform enforce it (dedupe, clamp), or constrain `df_config` in Python |
| YAML regex/length rule fails on generated data | only unique/min/max/value_set/not_null are aligned | add a `transform`, or test that rule in Python with `custom_strategy` |
| `value_set` + `unique` with many rows fails to generate | fewer distinct values than `min_rows` | lower `min_rows` or drop `unique` |
| `ValueError: transform must be 'module:function'` | dotted path only | `pkg.module:function` |
| Slow loop / slow failing loop | GX per example, shrinking reruns GX | `max_examples=2–5`, `-n`; `QualitySuiteConfig(phases=(Phase.explicit, Phase.generate))` or CLI `--no-shrink` for known failures |
| `is_valid` though schema warnings | warnings are not failures | inspect `schema_results` or validate with `strict_types=True` separately |

## Related topics

- `ge_property_bridge.md` - the bare validator loop with full control.
- `gx_suite_builder.md` - the suite fields used in YAML.
- Skill `spark-schema-contract-testing` - the schema layer in depth.
