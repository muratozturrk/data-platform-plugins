# strategy_for_type: Spark type → Hypothesis strategy

## Concept

`strategy_for_type(spark_type)` returns a strategy producing Python values that
`spark.createDataFrame(rows, schema)` accepts for that type. `spark_dataframes` calls it for
every column without a `custom_strategy`; nested types recurse. The defaults cover the whole
legal domain of each type - good for crash-hunting, usually too wide for business logic (sums
overflow, strings are random unicode). Narrow columns with `ColumnConfig(custom_strategy=...)`.

## API

```python
from spark_prop_testing.strategies.type_strategies import strategy_for_type
strategy_for_type(spark_type: DataType) -> st.SearchStrategy   # NotImplementedError if unmapped
```

| Spark type | Strategy (bundled behaviour) | Python value |
|---|---|---|
| `BooleanType` | `st.booleans()` | `bool` |
| `ByteType` / `ShortType` / `IntegerType` / `LongType` | `st.integers` over -2^7..2^7-1 / 2^15 / 2^31 / 2^63 | `int` |
| `FloatType` | `st.floats(width=32, allow_nan=False, allow_infinity=False)` (float32-representable) | `float` |
| `DoubleType` | `st.floats(width=64, allow_nan=False, allow_infinity=False)` (full range) | `float` |
| `DecimalType(p, s)` | `st.decimals(±(10**p-1)·10**-s, places=s)` (full range) | `Decimal` |
| `StringType` | `one_of(st.text(0..100), just(""), just(" "))` | `str` |
| `BinaryType` | `st.binary(0..256)` | `bytes` |
| `DateType` | `st.dates(1900-01-01, 2100-12-31)` | `date` |
| `TimestampType` | `st.datetimes(1970-01-02, 2100-12-31, timezones=just(UTC))` | aware `datetime` |
| `ArrayType(e, containsNull)` | `st.lists(elem, 0..10)`, elem `one_of(none, e)` if `containsNull` | `list` |
| `MapType(k, v, valueContainsNull)` | `st.dictionaries(k, v, max_size=10)`, `None` values if allowed | `dict` |
| `StructType` (nested) | `st.fixed_dictionaries`, nullable fields `one_of(none, ...)` | `dict` |
| others (`TimestampNTZType`, `DayTimeIntervalType`, `NullType`, `CharType`, UDTs) | `NotImplementedError` | - |

Details that matter:

- **Decimal bounds** cover the whole type: `DecimalType(5,2)` → `[-999.99, 999.99]`,
  `DecimalType(4,4)` → `[-0.9999, 0.9999]`. Sums of extreme values widen to `DecimalType(p+10, s)`
  in Spark, but your Python model may still need bounds.
- **Timestamps are UTC-aware** on purpose: PySpark converts naive datetimes with
  `time.mktime` (local time), which fails on Windows before 1970 and is ambiguous around DST;
  aware values go through `calendar.timegm` and convert exactly.
- **Range starts at 1970-01-02**: collecting a `TimestampType` uses `datetime.fromtimestamp`,
  which returns a *naive* datetime in the Python process' local zone and raises
  `OSError [Errno 22]` on Windows for negative epochs. For pre-1970 data use a `custom_strategy`
  (not on Windows, or compare via `cast("long")` without collecting).
- **Top-level vs nested nulls**: nested struct fields, array elements and map values get `None`
  from the type strategy; top-level columns only via `NullConfig`.
- Map keys come from the key type's strategy; Spark map keys can't be null, and the strategy
  never generates `None` keys.

## Worked example

`examples/test_type_strategies.py` (pure Hypothesis unless noted):

```python
@given(strategy_for_type(DecimalType(5, 2)))
def test_decimal_fits_precision_and_scale(d):
    assert abs(d) <= Decimal("999.99") and d.as_tuple().exponent >= -2

@given(strategy_for_type(TimestampType()))
def test_timestamps_are_utc_aware(ts):
    assert ts.tzinfo is timezone.utc

def test_nested_types_include_nulls_where_allowed():
    arr = strategy_for_type(ArrayType(IntegerType(), containsNull=True))
    assert find(arr, lambda xs: None in xs) == [None]

@given(strategy_for_type(FloatType()))
def test_float_type_values_are_finite_float32(v):
    assert v == v and struct.unpack("f", struct.pack("f", v))[0] == v
```

A wide nested schema (Spark):

```python
WIDE = StructType([
    StructField("dec", DecimalType(12, 2)), StructField("ts", TimestampType()),
    StructField("tags", ArrayType(StringType(), containsNull=True)),
    StructField("attrs", MapType(StringType(), IntegerType(), valueContainsNull=True)),
    StructField("addr", StructType([
        StructField("city", StringType(), True),
        StructField("geo", StructType([StructField("lat", DoubleType(), False)]), True)])),
    ...])

@given(df=spark_dataframes(spark, WIDE, DataFrameConfig(min_rows=1, max_rows=5)))
def check(df):
    assert df.schema == WIDE
```

Narrowing per column - typical business domains:

```python
ColumnConfig(custom_strategy=st.integers(0, 10_000))                    # quantity / cents
ColumnConfig(custom_strategy=st.sampled_from(["EUR", "USD", "TRY"]))   # enum
ColumnConfig(custom_strategy=st.from_regex(r"[A-Z]{2}\d{6}", fullmatch=True))  # codes
ColumnConfig(custom_strategy=st.datetimes(datetime(2020, 1, 1), datetime(2030, 1, 1),
                                          timezones=st.just(timezone.utc)))
ColumnConfig(custom_strategy=st.floats(-1e6, 1e6, allow_nan=False))      # summable doubles
```

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ArithmeticException` / overflow, `inf` sums | full-range Integer/Long/Double/Decimal | bound via `custom_strategy` |
| `TypeError: field x: IntegerType() can not accept object 1.5` | float strategy for an int column | match the Python type to the Spark type |
| `NotImplementedError: ... TimestampNTZType` | type not mapped | `custom_strategy=st.datetimes(...)` (naive) |
| `OSError: [Errno 22]` when collecting (Windows) | pre-1970 timestamp from a custom strategy → `fromtimestamp` | compare via `cast("long")`, or keep ≥ 1970 |
| Wall-clock assertions off by hours | collected timestamps are local naive | compare instants / UTC session zone |
| Strings with control chars break a CSV/regex step | random `st.text` | that's a finding; or narrow with `alphabet=` |

## Related topics

- `dataframe_config.md` - where type strategies are plugged in and overridden.
- `nulls_and_edge_cases.md` - adversarial value strategies.
- Foundations skill `flakiness_and_determinism.md` - floats and time zones in assertions.
