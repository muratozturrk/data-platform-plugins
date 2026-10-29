# @spark_property and @prop

## Concept

Raw Hypothesis + Spark needs the same three lines in every test: a settings decorator without a
deadline and with the slow-data health checks suppressed, a `@given` with a DataFrame strategy
bound to a session, and a session. `@spark_property` folds them into one decorator while keeping
the test a normal pytest function. `@prop` is the transformation-side counterpart: it attaches
the properties a transform must satisfy to the transform itself.

```python
# raw
@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large,
                                                HealthCheck.large_base_example], max_examples=20)
@given(df=spark_dataframes(spark, ORDERS, SMALL))      # needs a module-level spark
def test_x(df): ...

# with the decorator
@spark_property(schema=ORDERS, config=SMALL, max_examples=20)
def test_x(spark, df): ...
```

## API reference

```python
from spark_prop_testing import spark_property, prop

spark_property(
    schema: StructType,
    config: DataFrameConfig = None,     # default DataFrameConfig(min_rows=1, max_rows=100)
    max_examples: int | None = 20,      # SPARK_PROP_MAX_EXAMPLES overrides, read at call time
    spark: SparkSession = None,         # fixed session; otherwise see below
)

prop(*standard_properties, max_examples: int = 20)
# sets on the function: _prop_properties (list), _prop_max_examples, _is_prop_transform = True
```

What the wrapper does on each call:

1. Session: the `spark=` argument of the decorator, else the `spark` keyword pytest passes
   (the fixture), else `SparkSession.getActiveSession()`, else a new local session
   (`SPARK_PROP_MASTER`, default `local[2]`, AQE off, 4 shuffle partitions, UI off).
2. Settings: `deadline=None`, health checks `too_slow`, `data_too_large`, `large_base_example`
   suppressed, `max_examples = SPARK_PROP_MAX_EXAMPLES or max_examples or 20`; other keys
   (derandomize, phases, database) come from the loaded profile.
3. `@given(df=spark_dataframes(session, schema, config))` around your function; your function
   is called with `df=` plus every argument pytest passed (`spark` included if you declared it).

Signature handling: the wrapper's `__signature__` is your signature minus `df`, and
`__wrapped__` is removed, so pytest never looks for a `df` fixture. A function without a `df`
parameter raises `TypeError("<name> must take a 'df' parameter")` at decoration time.

## Worked example

```python
ORDERS = StructType([StructField("order_id", IntegerType(), False),
                     StructField("status", StringType(), False),
                     StructField("amount", IntegerType(), False)])
SMALL = DataFrameConfig(min_rows=1, max_rows=5, column_configs={
    "status": ColumnConfig(custom_strategy=st.sampled_from(["paid", "cancelled"])),
    "amount": ColumnConfig(custom_strategy=st.integers(0, 1_000)),
})

@pytest.fixture
def threshold():
    return 100

@spark_property(schema=ORDERS, config=SMALL, max_examples=3)
def test_filter_never_adds_rows(spark, df):              # spark = plugin fixture
    assert df.filter(F.col("status") == "paid").count() <= df.count()

@spark_property(schema=ORDERS, config=SMALL, max_examples=2)
def test_other_fixtures_stay_visible(df, threshold):     # spark = active session
    assert df.filter(F.col("amount") >= threshold).filter(F.col("amount") < threshold).count() == 0

class TestOrderInvariants:
    @spark_property(schema=ORDERS, config=SMALL, max_examples=2)
    def test_method_style(self, spark, df):
        assert df.sparkSession is spark
```

`inspect.signature(test_filter_never_adds_rows)` is `(spark)`. Calling a decorated function
directly (`check(spark=spark)`) runs the whole Hypothesis search, which is handy in helpers.

`@prop` on a transformation:

```python
@prop(row_count_non_increasing, input_columns_preserved, max_examples=5)
def paid_orders(df):
    return df.filter(F.col("status") == "paid")

paid_orders(df)                                          # unchanged behaviour
TransformationPropertyTester.from_prop(spark, paid_orders, ORDERS, df_config=SMALL).run().assert_passed()
```

The properties and `max_examples` travel with the function; tests (or a registry that imports
all transforms) pick them up with `from_prop`. The CLI does not read `@prop`;
`spark-prop-test run` only runs YAML suites.

## When not to use the decorator

- Several generated inputs (orders + customers): write `@given(orders=..., customers=...)` with
  `settings(settings.get_profile("spark"), max_examples=...)` (skill `spark-dataframe-strategies`).
- Strategies that need `data.draw` mid-test (`st.data()`).
- Parametrised tests: `@pytest.mark.parametrize` on top works, but each case gets its own search;
  keep `max_examples` small.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `fixture 'df' not found` | a `df` parameter on a test without `@spark_property`/`@given`, or `@given(df=...)` with an Ellipsis placeholder | decorate it; give `@given` a real strategy |
| `TypeError: ... must take a 'df' parameter` | parameter named differently | name it `df` |
| New SparkSession per test, slow | test does not take `spark` and no session is active | take the `spark` fixture |
| `max_examples` has no effect | `SPARK_PROP_MAX_EXAMPLES` set (by `--spark-prop-max-examples` or CLI `-n`) | unset it |
| `DeadlineExceeded` / `Flaky` with raw `@given` | Hypothesis' 200 ms default deadline | the decorator, or the `spark` profile |
| Same examples in every test under `spark-ci` | derandomize seeds from the internal test function, which is the same for every decorated test | expected; vary schemas/configs, or run a randomized nightly job |
| `@prop` transform not run by `spark-prop-test run` | CLI reads YAML only | `from_prop` in a test |

## Related topics

- `pytest_plugin.md`: the `spark` fixture, profiles, `--spark-prop-max-examples`.
- Skill `spark-transformation-property-testing`: `TransformationPropertyTester` and the
  standard properties used with `@prop`.
- Skill `spark-property-testing-foundations` (`hypothesis_for_spark.md`): settings in detail.
