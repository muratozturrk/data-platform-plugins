# Building GX suites: ExpectationSuiteBuilder, ColumnExpectationSpec, auto_generate_suite

## Concept

A GX `ExpectationSuite` is a named list of expectations. In GX 1.x every expectation is a
typed object (`ExpectColumnValuesToBeUnique`, ...); the bundled builder creates them from
`ExpectationConfiguration(type="<snake_case name>", kwargs={...})` and adds them with
`suite.add_expectation_configuration(...)`. You describe *what each column must satisfy* in a
`ColumnExpectationSpec`; the builder turns each set field into one expectation. Unset fields
are not checked.

## API reference

```python
from spark_prop_testing import ColumnExpectationSpec, ExpectationSuiteBuilder, auto_generate_suite

@dataclass
class ColumnExpectationSpec:
    column: str
    not_null: bool = False
    unique: bool = False
    min_value: Any = None
    max_value: Any = None
    value_set: list | None = None
    regex_pattern: str | None = None
    min_length: int | None = None
    max_length: int | None = None
    max_null_fraction: float | None = None      # 0.05 → at most 5 % nulls

ExpectationSuiteBuilder(suite_name: str)
    .add_column(spec) -> builder
    .add_row_count(min_rows=None, max_rows=None) -> builder    # last call wins
    .add_custom(expectation_type: str, kwargs: dict) -> builder
    .build() -> gx.ExpectationSuite

auto_generate_suite(schema: StructType, suite_name="auto_generated_suite", min_rows=1) -> gx.ExpectationSuite
```

### Spec field → GX expectation

| Spec field(s) | GX expectation type | kwargs |
|---|---|---|
| `add_row_count(min, max)` | `expect_table_row_count_to_be_between` | `min_value`, `max_value` |
| `not_null=True` | `expect_column_values_to_not_be_null` | `column` |
| `max_null_fraction=f` (only if `not_null` is False) | `expect_column_values_to_not_be_null` | `column`, `mostly=1 - f` |
| `unique=True` | `expect_column_values_to_be_unique` | `column` |
| `min_value` / `max_value` | `expect_column_values_to_be_between` | `column`, `min_value`, `max_value` |
| `value_set` | `expect_column_values_to_be_in_set` | `column`, `value_set` |
| `regex_pattern` | `expect_column_values_to_match_regex` | `column`, `regex` |
| `min_length` / `max_length` | `expect_column_value_lengths_to_be_between` | `column`, `min_value`, `max_value` |
| `add_custom(t, kw)` | `t` | `kw` (added after the column specs) |

A `None` bound means "open on that side". GX normalises numeric kwargs (an int bound reads back
as a float from `expectation.configuration.kwargs`).

### `add_custom`

The escape hatch for every other GX expectation, by snake_case type name, e.g.
`expect_column_values_to_be_of_type` (`type_="IntegerType"`),
`expect_column_pair_values_a_to_be_greater_than_b`, `expect_column_mean_to_be_between`,
`expect_compound_columns_to_be_unique` (`column_list=[...]`),
`expect_column_values_to_not_match_regex`. Unknown names fail at `build()` time in GX.

### `auto_generate_suite`

| From the schema | Expectation |
|---|---|
| always | row count `>= min_rows` |
| `nullable=False` | not null |
| `ByteType`/`ShortType`/`IntegerType`/`LongType` | between the type's min and max |
| `FloatType` | between `-3.4028235e38` and `3.4028235e38` (float32 max) |
| Double, Decimal, String, Date, Timestamp, nested | nothing beyond not-null |

It is a baseline: type bounds only catch overflow-like corruption. Add business rules (value
sets, ranges, regexes, uniqueness) on top. The float bound is the exact float32 maximum, so any
finite FloatType value (the full-range `width=32` strategy included) satisfies it. GX
compares through doubles, so very large LongType values near ±2^63 are compared approximately.

## Worked example

`examples/test_gx_suite_builder.py`:

```python
suite = (
    ExpectationSuiteBuilder("orders")
    .add_row_count(min_rows=1, max_rows=1_000)
    .add_column(ColumnExpectationSpec("order_id", not_null=True, unique=True, min_value=1))
    .add_column(ColumnExpectationSpec("status", value_set=["new", "paid"], max_null_fraction=0.1))
    .add_column(ColumnExpectationSpec("sku", regex_pattern=r"^[A-Z]{3}-\d+$", min_length=5, max_length=12))
    .add_custom("expect_column_values_to_be_of_type", {"column": "qty", "type_": "IntegerType"})
    .build()
)
assert len(suite.expectations) == 9

def kinds(suite):        # {(type, column): kwargs} for assertions on suite structure
    return {(e.expectation_type, e.configuration.kwargs.get("column")): dict(e.configuration.kwargs)
            for e in suite.expectations}
```

Testing the *suite* itself (structure, kwargs) needs no Spark and is fast; validating data
against it does (see `ge_property_bridge.md`).

### Suites from domain metadata

Because specs are plain dataclasses, build them from whatever describes the domain:

```python
SPECS = [ColumnExpectationSpec(c, not_null=True) for c in KEY_COLUMNS] + [
    ColumnExpectationSpec("status", value_set=list(Status)),
    ColumnExpectationSpec("amount", min_value=0, max_value=MAX_ORDER),
]
builder = ExpectationSuiteBuilder("orders")
for spec in SPECS:
    builder.add_column(spec)
```

Use the same constants to build the input strategy (`st.sampled_from(list(Status))`,
`st.integers(0, MAX_ORDER)`), so suite and strategy cannot drift apart.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Suite has fewer expectations than specs | GX keeps one per type+column; later replaces earlier | one spec per column, or `add_custom` a different type |
| `mostly` missing for a column | `not_null=True` wins over `max_null_fraction` | use only one of them |
| Row-count limits not as set | `add_row_count` called twice; only the last pair is kept | call it once |
| `build()` raises for a custom type | misspelled GX type or kwarg (`type_` not `type`) | check the GX gallery name |
| Auto suite passes data that is obviously wrong | only type bounds and NOT NULL are generated | add domain ranges/sets with `add_column` |
| `ModuleNotFoundError: great_expectations` on import of the builder | `ge` extra missing | `pip install -e "<plugin>/lib[ge]"` |
| `ImportError: spark_prop_testing.ExpectationSuiteBuilder needs Great Expectations` | lazy import failed | same; the message includes the original error |

## Related topics

- `ge_property_bridge.md` - running a suite against DataFrames.
- `gx_pitfalls.md` - GX 1.x semantics behind the table above.
- `quality_suite.md` - suites from YAML (`ge_expectations`).
