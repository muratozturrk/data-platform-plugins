# The standard property library

## Concept

`spark_prop_testing.transformations.standard_properties` holds ready-made
`(input_df, output_df) -> None` properties for the invariants most Spark steps share. Plain
properties are functions; parameterised ones are factories (`make_*`) that return a property
whose `__name__` carries the parameters (`sum_conservation[amount]`,
`idempotency[order_id]`, ...), which is what the tester report shows. They are ordinary
functions: call them on two fixed DataFrames for an exact, fast unit test, or register them on a
`TransformationPropertyTester` for generated inputs.

## API reference

| Property | Checks | Message on failure |
|---|---|---|
| `row_count_non_increasing` | `out <= in` | `input=5, output=7 (output has 2 extra rows)` |
| `row_count_non_decreasing` | `out >= in` | `... (output lost 2 rows)` |
| `row_count_exactly_preserved` | `out == in` | `input=5, output=3` |
| `output_is_non_empty_when_input_is_non_empty` | `in > 0 ⇒ out > 0` | `Transformation produced empty output from non-empty input` |
| `input_columns_preserved` | input column names ⊆ output | `columns dropped by transformation: ['customer']` |
| `no_new_nulls_introduced` | shared columns with 0 input nulls have 0 output nulls | `on column 'amount': input had 0 nulls, output has 1` |
| `column_types_preserved` | `simpleString()` equal for shared columns | `on 'price': type changed decimal(10,2) → decimal(38,2)` |

```python
make_sum_linearity_property(sum_column, group_columns=None, transform=None,
                            rel_tol=1e-9, abs_tol=1e-6)
make_aggregation_monotonicity_property(agg_column, transform, agg_fn="sum")  # sum|count|max|min
make_idempotency_property(key_columns, transform=None)
make_left_join_preserves_left_keys(join_key)
make_window_rank_property(partition_col, rank_col, dense=False)
```

`output_is_non_empty_when_input_is_non_empty` and `make_aggregation_monotonicity_property` are
not re-exported by `spark_prop_testing`; import them from the module.

### Semantics of the factories

- **Sum conservation** (no `transform`): `isclose(sum(in[col]), sum(out[col]))`; nulls count as
  0. For re-grouping, pivots, joins that must not fan out, splits that must add up.
- **Sum linearity** (`transform=f`): if the input has ≥ 2 rows, split it by
  `abs(hash(*all columns)) % 2` (data-based, so identical rows land in the same part) and require
  `sum(output) ≈ sum(f(A)) + sum(f(B))`. Fails for anything that looks across rows: shares of a
  total, global top-N, normalisation, dedup across parts. One part may be empty, then it passes.
- **Monotonicity**: `after = agg(f(input ∪ input.limit(1)))`, `before = agg(output)`; requires
  `after >= before` (`min`: `<=`); skips empty input or null aggregates. `sum` is monotone only
  over non-negative values, `avg` is never monotone (rejected: `ValueError`).
- **Idempotency**: `count(output) == count(distinct keys)`; with `transform` also
  `count(f(output)) == count(output)` and the row multisets on the common columns are equal
  (`collections.Counter` of collected rows on the driver, so `dropDuplicates(subset)` works;
  fine for test-sized data).
- **Left-join keys**: `distinct(non-null input keys) - distinct(output keys)` is empty; the
  message lists up to five missing keys. Row counts, values and fan-out are not checked.
- **Window rank**: no rank `< 1`; `min(rank) == 1` per partition; with `dense=True`
  `max(rank) == countDistinct(rank)` per partition. Returns silently if `rank_col` is absent.

## Worked example

```python
orders = spark.createDataFrame(
    [(1, "ann", 10.0), (2, "bob", 0.5), (3, "ann", 7.25), (4, None, 3.0), (5, "cy", 1.0)], ORDERS)

make_sum_linearity_property("amount")(orders, orders.groupBy("customer").agg(F.sum("amount").alias("amount")))

def share_of_total(df):                     # not linear: depends on every row
    total = df.agg(F.sum("amount")).first()[0]
    return df.withColumn("amount", F.col("amount") / F.lit(total))
make_sum_linearity_property("amount", transform=share_of_total)(orders, share_of_total(orders))
# AssertionError: sum_linearity violated on 'amount': sum(f(A∪B))=1.0, sum(f(A))+sum(f(B))=2.0
#   (≈ values: each part is normalised to its own total)

names = spark.createDataFrame([("ann", "Ann"), ("bob", "Bob")], "customer string, name string")
make_left_join_preserves_left_keys("customer")(orders, orders.join(names, "customer", "inner"))
# AssertionError: ... 1 keys from left table missing in output. Sample missing: ['cy']
#   (the null-key row is lost too, but null keys are ignored)

w = Window.partitionBy("customer").orderBy(F.col("amount").desc())
ranked = orders.unionByName(orders.limit(1)).withColumn("rnk", F.rank().over(w))   # tie → 1, 1, 3
make_window_rank_property("customer", "rnk")(orders, ranked)              # passes
make_window_rank_property("customer", "rnk", dense=True)(orders, ranked)  # gaps → fails
```

A battery on generated data for an enrichment join:

```python
(TransformationPropertyTester(spark, enrich, ORDERS, name="enrich", df_config=GEN)
 .add_property(row_count_exactly_preserved)
 .add_property(make_left_join_preserves_left_keys("customer"))
 .add_property(make_sum_linearity_property("amount"))
 .run(max_examples=2).assert_passed())
```

## Choosing properties per step

| Step | Properties |
|---|---|
| `filter` | `row_count_non_increasing`, `input_columns_preserved`, `column_types_preserved` |
| `withColumn` / cast / rename-free map | `row_count_exactly_preserved`, `no_new_nulls_introduced`, `column_types_preserved` (for untouched columns) |
| LEFT JOIN to a unique dimension | `row_count_exactly_preserved`, `make_left_join_preserves_left_keys`, sum conservation of measures |
| `groupBy().agg(sum)` | `row_count_non_increasing`, sum conservation, linearity with `transform`, monotonicity |
| dedup | `make_idempotency_property(keys, transform)`, `row_count_non_increasing` |
| window rank | `make_window_rank_property(..., dense=True)` for `row_number`/`dense_rank` |

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `IllegalStateException: Couldn't find <col>` in your own comparison | Spark 3.5 analyzer bug: `exceptAll` after `dropDuplicates(subset)` | compare `Counter(map(tuple, df.collect()))` or `groupBy(*cols).count()`; the built-in idempotency property already does |
| `rank()` fails `dense=True` | ties leave gaps by definition | `dense=False` for `rank()` |
| Sum conservation fails by 1e-12 on huge values | `abs_tol=1e-6` plus `rel_tol=1e-9` too tight for your scale | pass `rel_tol` / `abs_tol` |
| Monotonicity `[sum]` fails | negative values in the column | constrain the strategy or use `count`/`max` |
| Linearity "passes" for a non-linear transform | split put all rows in one part, or a single row | more examples, ≥ 2 rows (`min_rows=2`) |
| `cannot hash map type` | linearity hashes every column | drop map columns before the property |
| `column_types_preserved` misses nullability drift | `simpleString()` ignores nullability | `SchemaValidator` (skill `spark-schema-contract-testing`) |
| Left join with fan-out passes | key preservation ignores counts | add `row_count_exactly_preserved` and sum conservation |
| `group_columns` has no effect | accepted for compatibility, unused | group inside the transform |

## Related topics

- `tester_api.md`: how properties are run and reported.
- `custom_and_metamorphic_properties.md`: when no standard property fits.
- `pipeline_properties.md`: fan-out and money conservation built from these properties.
- Skill `spark-property-testing-foundations` (`property_taxonomy.md`): the property families.
