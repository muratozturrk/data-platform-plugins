# Custom and metamorphic transformation properties

## Concept

When no standard property states your rule, write one. A custom property has the same signature
as the built-ins, `(input_df, output_df) -> None`, raises `AssertionError` on violation, and runs
on the same tester. Two kinds are worth having:

- **Invariants of your logic**: "revenue equals the sum of paid amounts", "one row per
  customer", "avg lies between min and max".
- **Metamorphic relations**: you do not know the expected output, but you know how it must
  change when the input changes in a controlled way. The property closes over the transform,
  builds a modified input, runs the transform again and compares the two outputs.

| Relation | Input change | Expected output change |
|---|---|---|
| Scaling | `amount × k` | every `revenue × k` |
| Irrelevant row | add a cancelled order | revenue unchanged |
| Re-delivery | add a duplicate of an input row | dedup output unchanged |
| Permutation | `repartition(n)` / `orderBy(rand)` | output unchanged (determinism) |
| Superset | add any row | count/max not lower (`make_aggregation_monotonicity_property`) |
| Split | A ∪ B | `sum(f(A∪B)) = sum(f(A)) + sum(f(B))` (`make_sum_linearity_property`) |

## API: the shape of a property

```python
def revenue_only_from_paid_orders(input_df, output_df):
    paid = input_df.filter("status = 'paid'").agg(F.sum("amount")).first()[0] or 0
    total = output_df.agg(F.sum("revenue")).first()[0] or 0
    assert total == paid, f"revenue {total} != sum of paid amounts {paid}"

def make_cancelled_order_property(transform):            # factory: closes over the transform
    def cancelled_order_changes_nothing(input_df, output_df):
        new_id = input_df.agg(F.max("order_id")).first()[0] + 1
        customer = input_df.first()["customer"]
        extra = input_df.sparkSession.createDataFrame(
            [(new_id, customer, "cancelled", 999)], input_df.schema)
        assert_same_rows(output_df, transform(input_df.unionByName(extra)), "+1 cancelled order")
    return cancelled_order_changes_nothing
```

Rules that keep properties useful:

1. **Message = rule + numbers + sample rows.** `f"{n} unexpected rows (e.g. {df.limit(3).collect()})"`.
   The tester shows the message next to the minimal input, so the failure explains itself.
2. **No row-order assumptions.** Spark gives no order without `orderBy`, and partitioning changes
   it. Compare multisets:

   ```python
   def assert_same_rows(expected, actual, label):
       cols = expected.columns
       extra = actual.select(*cols).exceptAll(expected.select(*cols))
       missing = expected.select(*cols).exceptAll(actual.select(*cols))
       n_extra, n_missing = extra.count(), missing.count()
       if n_extra or n_missing:
           raise AssertionError(f"{label}: {n_extra} unexpected rows (e.g. {extra.limit(3).collect()}), "
                                f"{n_missing} missing rows (e.g. {missing.limit(3).collect()})")
   ```

   `exceptAll` keeps duplicates (`{a, a}` vs `{a}` differ); `subtract` is a set difference and
   would call them equal. For small outputs `sorted(map(tuple, df.collect()))` also works.
3. **Floats need tolerances, money does not.** `0.1 + 0.2 != 0.3`; Spark's summation order
   depends on partitioning. Use `math.isclose(a, b, rel_tol=1e-9, abs_tol=...)` or a
   scale-relative tolerance; use `DecimalType` and exact equality for currency.
4. **Use `raise AssertionError` in helpers** inside test modules if you need the exact text:
   pytest's assertion rewriting appends its own diff to `assert` messages.
5. **Name it.** The report uses `__name__`; give closures a descriptive inner name.
6. **Guard degenerate inputs**: empty input, single row, all-null column (`or 0` on sums).

## Worked example: revenue per customer

```python
def revenue_by_customer(df):
    return (df.filter(F.col("status") != "cancelled")
              .groupBy("customer").agg(F.sum("amount").alias("revenue")))

def make_scaling_property(transform, factor=3):
    def revenue_scales_with_amount(input_df, output_df):
        scaled = transform(input_df.withColumn("amount", F.col("amount") * factor))
        expected = output_df.withColumn("revenue", F.col("revenue") * factor)
        assert_same_rows(expected, scaled, f"amount x{factor}")
    return revenue_scales_with_amount

(TransformationPropertyTester(spark, revenue_by_customer, ORDERS, df_config=GEN)
 .add_property(revenue_only_from_paid_orders)
 .add_property(make_scaling_property(revenue_by_customer))
 .add_property(make_cancelled_order_property(revenue_by_customer))
 .run(max_examples=2).assert_passed())
```

The buggy `revenue_including_cancelled` (no status filter) fails the cancelled-order relation
on the first (simplest) example, one paid order of 0: `+1 cancelled order: 1 unexpected rows
(e.g. [Row(customer='ann', revenue=999)]), 1 missing rows ...`. No expected output was written
anywhere.

Dedup under re-delivery:

```python
def dedupe_orders(df):
    w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def make_duplicate_row_property(transform):
    def duplicate_row_changes_nothing(input_df, output_df):
        assert_same_rows(output_df, transform(input_df.unionByName(input_df.limit(1))), "+1 duplicate row")
    return duplicate_row_changes_nothing
```

Float invariant with a tolerance:

```python
def average_within_min_max(input_df, output_df):
    stats = input_df.groupBy("customer").agg(F.min("amount").alias("lo"), F.max("amount").alias("hi"))
    for row in output_df.join(stats, "customer").collect():
        tol = 1e-9 * max(1.0, abs(row.lo), abs(row.hi))
        assert row.lo - tol <= row.avg_amount <= row.hi + tol, f"avg out of range: {row.asDict()}"
```

## Designing the modified input

- Keep it **valid**: the extra cancelled order gets a fresh `order_id` (max + 1) and an existing
  customer; otherwise you test a different input domain.
- Make the change **observable**: an amount of 999 makes a missing filter visible; amount 0
  would hide it.
- Change **one thing** per relation, so a failure names the cause.
- `input_df.limit(1)` is a cheap "some existing row"; it is fine for relations that hold for
  every row.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Property fails only sometimes on equal data | compared `collect()` lists | `exceptAll` both ways / sorted tuples |
| Duplicates not detected | `subtract` / `distinct` comparison | `exceptAll` |
| `0.30000000000000004 != 0.3` | float sum | `math.isclose`; Decimal for money |
| `TypeError: unsupported operand ... NoneType` | sum over empty input is `None` | `or 0`, early return on empty input |
| Metamorphic property never fails on a buggy transform | modification not observable (amount 0, unknown key) | pick values the bug must react to |
| Messages end with pytest's `assert ... where ...` | assertion rewriting | `raise AssertionError(msg)` in helpers |
| `exceptAll` fails with `IllegalStateException: Couldn't find <col>` | Spark 3.5 analyzer bug after `dropDuplicates(subset)` | compare `Counter(tuple(r) for r in df.collect())` on both sides |
| Slow property | several actions per call (`count`, `collect`, transform again) | one aggregation for several numbers |

## Related topics

- `standard_properties.md`: built-in relations (linearity, monotonicity, idempotency).
- `pipeline_properties.md`: determinism, model comparison, money conservation.
- Skill `spark-property-testing-foundations` (`property_taxonomy.md`): metamorphic and
  differential properties in general.
- Skill `spark-dataframe-strategies`: generating inputs that make relations observable.
