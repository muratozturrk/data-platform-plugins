# Composing related data: cross-column rules, foreign keys, money

## Concept

`ColumnConfig` strategies are drawn independently per column, so they cannot express
`end_date >= start_date`, `total == qty * price`, or "every order's customer exists". Express
those **constructively** - draw the first value, derive the rest - instead of filtering:
filters (`.filter`, `assume`) waste examples, trigger `filter_too_much`, and shrink badly.

| Relation | Tool |
|---|---|
| Columns of one row depend on each other | `@st.composite` row strategy, or `flatmap` |
| A DataFrame depends on another (FKs) | draw parent keys → `flatmap` into the child strategy |
| Fixed set of valid values | `st.sampled_from(keys)` |
| Derived column | compute it in the composite (or `map`) instead of generating it |
| Money | `st.decimals(..., places=2)` with a `DecimalType(p, 2)` column; or integer cents |

Rows built this way go through `spark.createDataFrame(rows, schema)` directly (that is what
`spark_dataframes` does internally), so `unique_by` / null handling are yours to add with
`st.lists(..., unique_by=...)`.

## API (Hypothesis)

```python
@st.composite
def rows(draw): ...                       # draw(strategy) inside; returns one value
strategy.flatmap(lambda v: other(v))      # the second strategy depends on the first value
strategy.map(f)                           # derive a value
st.lists(row, min_size, max_size, unique_by=lambda r: r["id"])
st.tuples(st.just(customers_df), orders_strategy)
st.sampled_from(list_of_keys)             # FK values; shrinks to the first key
st.decimals(min_value=Decimal("0.00"), max_value=Decimal("100000.00"), places=2,
            allow_nan=False, allow_infinity=False)
```

## Worked example

`examples/test_composing_related_data.py`.

Related columns:

```python
MONEY = DecimalType(12, 2)
money = st.decimals(min_value=Decimal("0.00"), max_value=Decimal("100000.00"),
                    places=2, allow_nan=False, allow_infinity=False)

@st.composite
def subscription_rows(draw):
    start = draw(st.dates(date(2000, 1, 1), date(2030, 12, 31)))
    length = draw(st.integers(0, 365))
    return {"sub_id": draw(st.integers(1, 10_000)), "start_date": start,
            "end_date": start + timedelta(days=length), "fee": draw(money)}

rows_strategy = st.lists(subscription_rows(), min_size=1, max_size=15,
                         unique_by=lambda r: r["sub_id"])

@given(rows=rows_strategy)
def check(rows):
    df = spark.createDataFrame(rows, SUBS)
    assert df.filter(F.col("end_date") < F.col("start_date")).count() == 0
    assert df.agg(F.sum("fee")).first()[0] == sum(r["fee"] for r in rows)   # exact
```

Generating a *length* (`0..365` days) rather than a second date makes the relation hold by
construction and shrinks to `end == start`, the boundary case. The `flatmap` form:

```python
date_ranges = st.dates(date(2000, 1, 1), date(2030, 12, 31)).flatmap(
    lambda s: st.tuples(st.just(s), st.dates(min_value=s, max_value=s + timedelta(days=365))))
```

Foreign keys between two DataFrames:

```python
def customers_and_orders(spark):
    customer_ids = st.lists(st.integers(1, 500), min_size=1, max_size=8, unique=True)

    def build(ids):
        customers = spark.createDataFrame([(i,) for i in ids], CUSTOMERS)
        orders = spark_dataframes(spark, ORDERS, DataFrameConfig(min_rows=0, max_rows=15,
            column_configs={
                "order_id":    ColumnConfig(custom_strategy=st.integers(1, 10_000), unique=True),
                "customer_id": ColumnConfig(custom_strategy=st.sampled_from(ids)),
                "amount":      ColumnConfig(custom_strategy=money),
            }))
        return st.tuples(st.just(customers), orders)

    return customer_ids.flatmap(build)

@given(pair=customers_and_orders(spark))
def check(pair):
    customers, orders = pair
    assert orders.join(customers, "customer_id", "left_anti").count() == 0
```

Variants: skewed FKs - `st.sampled_from(ids)` → `skewed_integers` over indices or
`st.one_of(st.just(ids[0]), st.sampled_from(ids))`; deliberate orphans - add
`st.integers(10_001, 20_000)` to the FK strategy and assert the pipeline quarantines them;
customers without orders - allow `min_rows=0` (as above).

### Bounded money

- `DecimalType(12, 2)` + `st.decimals(places=2)` within `±10**10`: sums are exact
  (Spark widens the sum to `DecimalType(22, 2)`), no float tolerance needed.
- The default `DecimalType(p, s)` strategy already respects precision but spans the full
  range; set business bounds (non-negative, max order value).
- Integer cents (`LongType` + `st.integers(0, 10_000_000)`) are an alternative.
- Avoid `DoubleType` money: the default strategy spans ±1.8e308 (sums overflow to `inf`)
  and float sums depend on partitioning.
- Currency conversion / rounding: generate the rate as a `Decimal` with fixed places and
  assert `ROUND_HALF_EVEN`/`HALF_UP` behaviour explicitly.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `filter_too_much` / `Unsatisfiable` | constraint enforced by filtering | construct it (length/offset, `sampled_from`) |
| FK test passes trivially | child generated with 0 rows every time | check with `event()`/statistics, set `min_rows` |
| `flatmap` strategy shrinks poorly | the dependent part restarts when the parent shrinks | keep the parent small (few keys) |
| Composite key duplicates | `unique=True` is per column | `st.lists(..., unique_by=lambda r: (r["a"], r["b"]))` |
| Decimal sum mismatch | float strategy or `places` > scale | `places == scale`, Decimal values |
| `TypeError` creating DataFrame from composite rows | dict keys/types don't match schema | keys = field names, Python types per `type_strategies.md` |
| Spark session captured in a module-level strategy | strategy built before the fixture exists | build strategies inside the test from the `spark` fixture |

## Related topics

- `dataframe_config.md` - `unique`, `custom_strategy`, row bounds.
- `nulls_and_edge_cases.md` - skew and edge cases for FK columns.
- Foundations skill `property_taxonomy.md` - join properties (`make_left_join_preserves_left_keys`).
