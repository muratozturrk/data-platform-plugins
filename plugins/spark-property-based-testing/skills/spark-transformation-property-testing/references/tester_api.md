# TransformationPropertyTester, reports and @prop

## Concept

`TransformationPropertyTester` turns a transformation and a list of properties into a battery of
Hypothesis searches. For every property it generates `input_df` from `input_schema` +
`df_config`, computes `output_df = transform(input_df)` and calls `prop(input_df, output_df)`.
Each property runs independently (its own examples, its own shrinking), so one report says which
invariants hold and, for each broken one, the minimal input that breaks it.

```
input_schema + df_config ──Hypothesis──▶ input_df ──transform──▶ output_df
                                            └──────────── prop(input_df, output_df) ──▶ raise / pass
```

## API reference

```python
from spark_prop_testing import TransformationPropertyTester, prop
from spark_prop_testing.transformations.base import (
    PropertyTestResult, TransformationTestReport, Transformation, TransformationProperty)

Transformation         = Callable[[DataFrame], DataFrame]
TransformationProperty = Callable[[DataFrame, DataFrame], None]   # raises on failure

TransformationPropertyTester(
    spark: SparkSession,
    transform: Transformation,
    input_schema: StructType,
    name: str = "unnamed_transformation",
    df_config: DataFrameConfig = None,        # default DataFrameConfig(min_rows=1, max_rows=100)
)
.add_property(prop) -> TransformationPropertyTester          # fluent
.run(max_examples: int | None = None, phases: tuple | None = None) -> TransformationTestReport
TransformationPropertyTester.from_prop(spark, transform, input_schema, df_config=None)

@dataclass
class PropertyTestResult:
    property_name: str                 # getattr(prop, "__name__", repr(prop))
    passed: bool
    failure_input: str | None = None   # up to 10 rows, one str(row.asDict()) per line
    error_message: str | None = None

@dataclass
class TransformationTestReport:
    transformation_name: str
    results: list[PropertyTestResult]
    passed -> bool                     # all(r.passed)
    summary() -> str                   # "Transformation: 'name' — PASSED ✓" + one line per property
    assert_passed()                    # AssertionError("Transformation properties failed:\n" + summary())

prop(*standard_properties, max_examples: int = 20)   # sets _prop_properties, _prop_max_examples,
                                                      # _is_prop_transform on the function
```

`max_examples` resolution in `run()`: argument → `@prop` value (via `from_prop`) → 20;
`SPARK_PROP_MAX_EXAMPLES` (or pytest `--spark-prop-max-examples`) overrides all of them. The
settings are `deadline=None` with the Spark health checks suppressed; `phases` comes from the
argument when given, other keys (derandomize, database) from the loaded Hypothesis profile.

## What ends up in a result

| Situation | `passed` | `error_message` | `failure_input` |
|---|---|---|---|
| Property holds on all examples | True | None | None |
| `AssertionError` in the property | False | the assertion text | rows of the shrunk example |
| Transform or property raises `X` | False | `"X: <message>"` (e.g. `AnalysisException: ...`) | rows, if the input can be collected |
| Hypothesis error outside the test (health check, unsatisfiable) | False | `"<Type>: <message>"` | None |

`run()` never raises for these; call `report.assert_passed()` in a test. A property that is
`Flaky` (passes on replay) is reported with the last recorded failure.

## Worked example

```python
SMALL = DataFrameConfig(min_rows=1, max_rows=5, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(["cancelled", "paid"])),
    "amount":   ColumnConfig(custom_strategy=st.integers(-50, 500)),
})

def paid_orders(df):
    return df.filter(F.col("status") == "paid")

report = (TransformationPropertyTester(spark, paid_orders, ORDERS, df_config=SMALL)
          .add_property(row_count_non_increasing)
          .add_property(row_count_exactly_preserved)     # wrong for a filter
          .run(max_examples=3))
print(report.summary())
# Transformation: 'unnamed_transformation' — FAILED ✗
#   ✓ row_count_non_increasing
#   ✗ row_count_exactly_preserved — row_count_exactly_preserved violated: input=1, output=0
#       minimal failing input:
#       {'order_id': 1, 'status': 'cancelled', 'amount': 0}
```

`@prop` keeps the contract next to the code; the function itself is unchanged:

```python
@prop(row_count_non_increasing, input_columns_preserved, max_examples=3)
def positive_amounts(df):
    return df.filter(F.col("amount") >= 0)

TransformationPropertyTester.from_prop(spark, positive_amounts, ORDERS, df_config=SMALL) \
    .run().assert_passed()            # 3 examples per property, name = "positive_amounts"
```

## Skipping shrinking for known failures

Shrinking a DataFrame failure costs tens of extra Spark runs (minutes on a slow laptop). A test
that *expects* a failure can skip the shrink phase per run:

```python
from hypothesis import Phase

report = tester.run(max_examples=3, phases=(Phase.explicit, Phase.generate))
assert not report.passed
```

Without shrinking, `failure_input` is the first failing example; Hypothesis tries the simplest
example first, so it is often already minimal. `DataQualityPropertySuite` has the same knob as
`QualitySuiteConfig(phases=...)`, the CLI as `spark-prop-test run --no-shrink`.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| A failing run takes minutes | shrinking: every step reruns transform + property | small `df_config` (≤ 5-8 rows), `run(phases=(Phase.explicit, Phase.generate))` for demos |
| `TypeError: ... is not decorated with @prop` | `from_prop` on a plain function | decorate it or use the constructor |
| Property name `<lambda>` in the report | name = `__name__` | use `def` or set `p.__name__` |
| Every property fails with `AnalysisException` | transform references a missing column | the message is the finding; fix the transform or schema |
| Runs slower than expected | each property is a separate search: `#properties × max_examples` transforms | fewer, stronger properties; cheap structural checks on one fixed DataFrame |
| `max_examples` ignored | `SPARK_PROP_MAX_EXAMPLES` set (by `--spark-prop-max-examples` or CLI `-n`) | unset it |
| `FailedHealthCheck` in `error_message` | strategy too restrictive (`unique=True` on a tiny range) | widen the strategy |
| Nothing found on 100-row inputs | invariants rarely break on random large data | small inputs + targeted strategies (`spark-dataframe-strategies`) |

## Related topics

- `standard_properties.md`: every built-in property with its exact semantics.
- `custom_and_metamorphic_properties.md`: writing your own properties.
- Skill `spark-prop-testing-toolkit`: `@spark_property`, plugin options, CLI.
- Skill `spark-property-testing-foundations`: profiles, shrinking, replay, flakiness.
