# GX suites as Hypothesis properties

## Concept

A GX suite is a set of predicates over a DataFrame. Inside `@given` it becomes a property:
*for every DataFrame the strategy can produce, `f(df)` passes the suite*. Two contracts are
involved and must not be confused:

- the **input contract** (what valid inputs look like) lives in the strategy
  (`DataFrameConfig`/`ColumnConfig`);
- the **output contract** (what the transformation promises) is the GX suite.

If the strategy produces inputs the suite rejects before any transformation ran, the test
reports data that was never valid, not a bug. So the first property to write is
"generated input passes the input suite"; it validates the strategy itself.

## API reference

```python
from spark_prop_testing import GEPropertyValidator
from spark_prop_testing.ge_integration.context_factory import build_ge_context, df_to_ge_batch
from spark_prop_testing.ge_integration.property_bridge import ExpectationResult, SuiteValidationResult

build_ge_context()                                   # gx.get_context(mode="ephemeral")
df_to_ge_batch(context, df, datasource_name="spark_datasource", asset_name="test_asset")  # -> gx Batch
GEPropertyValidator(context=None).validate(df, suite) -> SuiteValidationResult   # never raises for data

@dataclass
class ExpectationResult:
    expectation_type: str
    column: str | None            # None for table-level expectations
    success: bool
    unexpected_count: int = 0
    unexpected_pct: float = 0.0   # fraction 0–1 (GX's unexpected_percent / 100)
    details: str = ""             # "error: ..." when the expectation raised

@dataclass
class SuiteValidationResult:
    suite_name: str
    success: bool
    results: list[ExpectationResult]
    failures -> list[ExpectationResult]
    summary() -> str              # one ✓/✗ line per expectation
    assert_success() -> None      # AssertionError("GE suite validation failed:\n" + summary())
```

`validate()` uses `result_format="COMPLETE"` and the suite name as the asset name: the data
source, asset and batch definition are created on first use and reused for later DataFrames.

## Worked example

`examples/test_ge_property_bridge.py`:

```python
SUITE = (ExpectationSuiteBuilder("orders_contract")
    .add_row_count(min_rows=1, max_rows=20)
    .add_column(ColumnExpectationSpec("order_id", not_null=True, unique=True, min_value=1))
    .add_column(ColumnExpectationSpec("status", not_null=True, value_set=STATUSES))
    .add_column(ColumnExpectationSpec("amount", min_value=0, max_value=10_000))
    .build())

MATCHING = DataFrameConfig(min_rows=1, max_rows=20, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(STATUSES)),
    "amount":   ColumnConfig(custom_strategy=st.integers(0, 5_000)),
})

@pytest.fixture(scope="session")
def validator():
    return GEPropertyValidator()             # one ephemeral context for all examples

GX_FAST = settings(settings.get_profile("spark"), max_examples=2)

def test_generated_data_satisfies_suite(spark, validator):      # strategy honours the input contract
    @GX_FAST
    @given(df=spark_dataframes(spark, ORDERS, MATCHING))
    def check(df):
        validator.validate(df, SUITE).assert_success()
    check()

def test_transformation_preserves_contract(spark, validator):   # the real property
    @GX_FAST
    @given(df=spark_dataframes(spark, ORDERS, MATCHING))
    def check(df):
        result = validator.validate(apply_discount(df), SUITE)
        assert result.success, result.summary()
    check()
```

### Detecting a broken transformation

`apply_discount_buggy` subtracts an absolute 10 000 instead of 10 %. With the matching strategy
every input has `amount <= 5_000`, so every output violates `amount >= 0`; the first example
fails with

```
✗ [amount] expect_column_values_to_be_between — 1 violations (100.0%)
```

The example is `xfail(strict=True)` and uses `phases=[Phase.generate]`: shrinking would rerun
the whole GX suite for every candidate. In a real test keep shrinking on (that is how you get
the 1-row counterexample) but expect it to take several validations.

### The anti-pattern

```python
UNCONSTRAINED_KEYS = DataFrameConfig(min_rows=5, max_rows=10, column_configs={
    **MATCHING.column_configs,
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 3)),   # no unique=True
})
```

5+ rows over 3 key values always contain a duplicate, so `expect_column_values_to_be_unique`
fails on the input before any code runs. The same happens, less predictably, with the default
IntegerType strategy and `min_value=1` (negatives, zero), or `st.text()` and a `value_set`.

### Showing the failing rows

Hypothesis prints a DataFrame argument as `DataFrame[order_id: int, ...]`, without rows.
Include data in the message, cheaply:

```python
assert result.success, result.summary() + "\n" + "\n".join(map(str, df.limit(20).collect()))
```

or call `hypothesis.note(df.collect())` before validating (printed for the final failing example).

### Auto suite as a smoke property

`auto_generate_suite(ORDERS)` (NOT NULL + integer type bounds) holds for any type-correct
generated data; validating `f(df)` against it catches nulls in NOT NULL outputs and overflow
wrap-around, without writing a suite.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Suite fails on generated input | strategy wider than the input contract | constrain: `unique=True`, bounds, `sampled_from` |
| Each example takes seconds | full GX validation = several Spark jobs | `max_examples` 2–5, rows ≤ 20, one validator |
| Very slow failing run | shrinking reruns GX per candidate | accept, or `phases=[Phase.generate]` for known failures |
| `Flaky` / `DeadlineExceeded` | settings without `deadline=None` | inherit `settings.get_profile("spark")` |
| Failure shows no rows | DataFrame repr has no data | add `df.collect()` to the message / `note()` |
| Expectation "failed" with `error: ...` | GX raised (wrong column type, missing column) | read `details`; fix suite or schema |
| Different contexts per test are slow | `GEPropertyValidator()` per test/example | session-scoped fixture |

## Related topics

- `gx_suite_builder.md` - building `SUITE`.
- `quality_suite.md` - the same loop with schema and custom checks, `run_property_loop()`.
- `gx_pitfalls.md` - unexpected percent, `mostly`, nulls.
- Skill `spark-dataframe-strategies` - `unique=True`, bounded strategies.
