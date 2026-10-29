# Nulls, skew and edge cases

## Concept

Most pipeline bugs live at the edges: nulls, empty strings, the string `"NULL"`, boundary
integers, one key holding most rows. Field nullability in the schema says nulls are *allowed*;
`NullConfig` decides how often they *appear* in generated data. The distribution helpers bias
values toward the cases that break joins, aggregations and parsers.

## API

```python
class NullConfig:
    def __init__(self, null_rate: float = 0.0, always_include_null: bool = False)
    # ValueError unless 0.0 <= null_rate <= 1.0

with_nulls(base_strategy, null_config) -> st.SearchStrategy      # strategies/null_strategy.py

skewed_integers(min_value=0, max_value=1_000_000, skew_value=None, skew_probability=0.7)
edge_case_integers(min_value=-2_147_483_648, max_value=2_147_483_647)
edge_case_strings()
realistic_id_strategy(max_id=100)
```

### NullConfig semantics in `spark_dataframes`

| Setting | Behaviour (nullable fields only) |
|---|---|
| `null_rate=0.0` (default) | never null |
| `null_rate=1.0` | always null (`st.none()` for that column) |
| `0 < null_rate < 1` | applied **per DataFrame**: each row is nulled with probability `null_rate` using a Hypothesis-seeded `Random` (`st.randoms(use_true_random=True)`) |
| `always_include_null=True` | if a non-empty DataFrame has no null in the column, the first row's value is set to null → ≥1 null |
| `unique=True` + rate | at most one null in the column |
| `nullable=False` | `NullConfig` ignored |
| `DataFrameConfig(default_null_config=NullConfig(...))` | same semantics for every nullable column without a `column_configs` entry (used by `high_null_profile`, `production_like_profile`) |

Why per DataFrame: per-value injection can't honour a rate, because Hypothesis duplicates list
elements and favours boundary choices - a 0.5 rate could produce 100% nulls. Rates have no
lower limit (0.001 still produces nulls in large frames).

`with_nulls` (the per-value wrapper, used by `spark_dataframes` for rate 1.0 and available for
your own strategies): rate 0 → base strategy, 1 → `st.none()`, otherwise
`st.integers(0, 999).flatmap(lambda r: none if r < rate*1000 else base)`: 1/1000 resolution and
**shrinks toward `None`** (minimal failing examples prefer nulls).

### Distribution helpers

| Helper | Generates | Shrinks to | Use for |
|---|---|---|---|
| `skewed_integers` | `skew_value` (default `min_value`) with probability ≈ `skew_probability` (1/1000 resolution), else uniform in range | the hot key | join/aggregation skew, hot partitions, top-N |
| `edge_case_integers` | `0, -1, 1, min, max, min+1, max-1` or uniform | `0` | off-by-one, boundary filters; pass the column/business bounds |
| `edge_case_strings` | `""`, `" "`, `"\t\n"`, `"NULL"`, `"None"`, `"NaN"`, `"'DROP TABLE users;--"`, `"café"`, `"🔥"`, `"a"*1000`, `st.text(1..100)` | `""` | parsing, trimming, null-like sentinels, escaping (HTML reports, SQL), length limits |
| `realistic_id_strategy` | `st.integers(1, max_id)` | `1` | FK-like columns with repeats (join fan-out) |

`skew_probability` outside `[0, 1]` raises `ValueError`. `edge_case_integers` defaults to the
IntegerType range: summing such values overflows - pass narrower bounds for summed columns.

## Worked example

`examples/test_nulls_and_edge_cases.py`:

```python
cfg = DataFrameConfig(min_rows=1, max_rows=20, column_configs={
    "id":     ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True,
                           null_config=NullConfig(null_rate=0.5)),
    "always": ColumnConfig(null_config=NullConfig(null_rate=1.0)),
    "some":   ColumnConfig(custom_strategy=st.integers(0, 9),
                           null_config=NullConfig(always_include_null=True)),
})

@FAST
@given(df=spark_dataframes(spark, SCHEMA, cfg))
def check(df):
    n = df.count()
    nulls = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first()
    assert nulls["always"] == n
    assert nulls["some"] >= 1
    assert nulls["id"] <= 1
    assert nulls["half"] == 0          # no ColumnConfig → no nulls
```

A null-contract property for a transformation:

```python
cfg = DataFrameConfig(max_rows=20, column_configs={
    "amount": ColumnConfig(custom_strategy=st.integers(0, 1_000),
                           null_config=NullConfig(null_rate=0.3, always_include_null=True)),
    "status": ColumnConfig(custom_strategy=edge_case_strings()),
    "user_id": ColumnConfig(custom_strategy=skewed_integers(1, 50, skew_probability=0.8)),
})
# properties: no new nulls in key columns; null amounts excluded from totals, not counted as 0;
# status "NULL" / "" are not treated as SQL NULL; hot user_id doesn't change per-user totals
```

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| No nulls although `null_rate=0.4` | field `nullable=False`, or the column has an explicit `ColumnConfig` (overrides `default_null_config`) | nullable field; set `null_config` on that entry |
| "Always null" column in every example | `null_rate=1.0` is literally always | use a fractional rate or `always_include_null` |
| Null count varies a lot between examples | rate is a probability; small frames vary | assert bounds/≥1, use `always_include_null` for guarantees |
| Key uniqueness expectation fails on nulls | unique columns may hold one null | non-nullable key field |
| Sum overflow with `edge_case_integers` | defaults are IntegerType bounds | pass narrow bounds |
| Expected `NULL` handling, got string `"NULL"` | `edge_case_strings` sentinel strings | that is the test: the pipeline must not confuse them |
| Generated `<script>` in a report | `edge_case_strings`/`st.text` | escape output (the library's HTML report autoescapes) |

## Related topics

- `dataframe_config.md` - where `NullConfig` plugs in.
- `composing_related_data.md` - skewed foreign keys between two DataFrames.
- Foundations skill `property_taxonomy.md` - null-related invariants (`no_new_nulls_introduced`).
