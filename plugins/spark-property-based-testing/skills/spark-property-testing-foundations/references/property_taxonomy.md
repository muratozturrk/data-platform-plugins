# Property taxonomy for Spark pipelines

## Concept

A property is a statement about *all* valid inputs. Most useful data-pipeline properties fall
into five families. Pick from several families for one transformation: each family catches a
different class of bug, and two weak properties together are often stronger than one clever one.

| Family | Shape | Typical Spark example | Catches |
|---|---|---|---|
| Invariant | `P(f(x))` or `P(x, f(x))` | filter never adds rows; output keys unique; no negative amount; schema unchanged | wrong predicate, fan-out joins, null handling |
| Idempotency | `f(f(x)) == f(x)` | dedupe, upsert/merge, trimming, normalising | stateful or order-dependent cleanup |
| Round-trip | `g(f(x)) == x` | write+read Parquet, `to_json`/`from_json`, encode/decode | lossy casts, precision, time-zone shifts |
| Metamorphic | `f(t(x))` related to `f(x)` | `sum(df ∪ df) = 2·sum(df)`; stricter filter → fewer rows; extra unrelated key leaves other groups unchanged | aggregation bugs without needing the expected value |
| Oracle / differential | `f(x) == model(x)` | Spark vs plain Python on `collect()`; new vs old job; SQL vs DataFrame API | logic errors anywhere the model is obviously right |
| Determinism | `f(x) == f(x')` where `x'` is `x` re-partitioned/re-ordered | `f(df.repartition(n)) == f(df)` | `first()`, non-total window orders, partition-dependent ids |

## API (bundled library)

The library ships ready-made properties in `spark_prop_testing.transformations.standard_properties`.
Each property has the signature `(input_df, output_df) -> None` and raises `AssertionError`:

```python
row_count_non_increasing, row_count_non_decreasing, row_count_exactly_preserved
output_is_non_empty_when_input_is_non_empty
input_columns_preserved, no_new_nulls_introduced, column_types_preserved   # full types

# factories returning a property
make_idempotency_property(key_columns, transform=None)   # keys unique; with transform: f(f(x)) == f(x)
make_sum_linearity_property(sum_column, group_columns=None, transform=None,
                            rel_tol=1e-9, abs_tol=1e-6)  # conservation / sum(f(A∪B)) = sum(f(A))+sum(f(B))
make_aggregation_monotonicity_property(agg_column, transform, agg_fn="sum")  # sum|count|max|min
make_window_rank_property(partition_col, rank_col, dense=False)  # dense=True also checks contiguity
make_left_join_preserves_left_keys(join_key)             # null keys ignored
```

They are run by `TransformationPropertyTester` or attached with `@prop(...)`. Hand-written
properties use plain `@given` + `spark_dataframes` (see the worked example).

## Worked example

Transformation: drop cancelled/negative orders, then total per customer
(`examples/test_property_taxonomy.py`).

```python
def clean(df):
    return df.filter((F.col("status") != "cancelled") & (F.col("amount") >= 0))

def totals(df):
    return clean(df).groupBy("customer_id").agg(F.sum("amount").alias("total"))

# invariant
out = clean(df)
assert out.count() <= df.count()
assert out.filter(F.col("amount") < 0).count() == 0

# idempotency: multiset equality in Spark
once, twice = clean(df), clean(clean(df))
assert once.exceptAll(twice).count() == 0 and twice.exceptAll(once).count() == 0

# metamorphic
assert as_dict(totals(df.unionAll(df))) == {k: 2 * v for k, v in as_dict(totals(df)).items()}

# oracle
assert as_dict(totals(df)) == python_totals(df.collect())

# determinism
assert as_dict(totals(df.repartition(parts))) == as_dict(totals(df))
```

The strategy bounds `amount` to `-50..1000` so that sums cannot overflow `IntegerType`
→ `LongType` arithmetic in the model; `order_id` is `unique=True` because it is a key.

### Choosing properties - a checklist

1. What can never happen in the output? (invariants: ranges, uniqueness, not-null, schema)
2. Is running it twice safe? (idempotency)
3. Is there an inverse? (round-trip)
4. What happens if I duplicate, split, filter, re-order or add unrelated rows? (metamorphic)
5. Is there a simpler implementation I trust? (oracle)
6. Should partitioning or input order matter? (determinism)

### Metamorphic relations that work well on DataFrames

- **Union split**: `agg(A ∪ B)` combines `agg(A)` and `agg(B)` (sum, count, min, max; not avg).
- **Duplication**: `count`/`sum` double, `min`/`max`/`countDistinct` unchanged.
- **Filter tightening**: a stricter predicate gives a subset (`exceptAll` is empty).
- **Irrelevant rows**: adding rows for a new key leaves other keys' results unchanged.
- **Permutation**: shuffling rows (`repartition`, `orderBy(rand)`) changes nothing.
- **Scaling**: multiplying an amount column by `k` multiplies sums by `k` (use integers/decimals).

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Property always passes, finds nothing | Restates the implementation, or strategy too narrow | Use a different family (metamorphic/oracle); widen strategy; check `hypothesis.event` stats |
| Oracle disagrees only on nulls | Python model treats `None` differently than SQL (`NULL != 'x'` is NULL, filtered out) | Model SQL three-valued logic explicitly |
| Metamorphic `avg` fails | `avg` is not additive over unions | Use sum and count separately |
| Idempotency fails with `==` on lists | Row order | Compare multisets (`exceptAll` both ways, or sorted rows) |
| Overflow / `inf` failures | Unbounded generated numbers | Bound strategies (`custom_strategy=st.integers(...)`) |
| Fails only with duplicates of a key | Key not unique in generated data | `ColumnConfig(unique=True)` for key columns |
| `exceptAll` fails with maps | Map columns are not comparable | Compare sorted `collect()` or convert maps |

## Related topics

- `hypothesis_for_spark.md` - running these properties with the right settings.
- `flakiness_and_determinism.md` - writing the equality checks robustly.
- Skill `spark-dataframe-strategies` - generating inputs that make the properties meaningful.
