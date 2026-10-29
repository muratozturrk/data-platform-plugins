# Pipeline properties: fan-out, composition, determinism, money

## Concept

Single-step properties do not see bugs that live between steps: a dimension with a duplicate
key, a dedup whose winner depends on partitioning, rounding that loses a cent per order. Four
patterns cover most of them:

| Pattern | Question | Properties |
|---|---|---|
| Join fan-out | does enrichment multiply rows or money? | `row_count_exactly_preserved`, sum conservation, unique dim keys |
| Composition | do the stages keep their contracts and does the whole equal a model? | per-stage battery + differential property |
| Determinism | same input, different partitioning/order → same output? | `f(df) == f(df.repartition(n))`, `f(df.orderBy(rand))` |
| Money conservation | do allocations add up to the cent? | exact `DecimalType` sums per entity |

## API used

Everything is built from the tester and the standard library (`tester_api.md`,
`standard_properties.md`) plus a multiset comparison:

```python
def assert_same_rows(expected, actual, label):
    cols = expected.columns
    extra = actual.select(*cols).exceptAll(expected.select(*cols)).count()
    missing = expected.select(*cols).exceptAll(actual.select(*cols)).count()
    assert extra == 0 and missing == 0, f"{label}: {extra} unexpected, {missing} missing rows"
```

## Join fan-out

```python
dim = spark.createDataFrame([("ann", "EU"), ("ann", "US"), ("bob", "EU")], "customer string, region string")

def enrich(df):
    return df.join(dim, "customer", "left")

report = (TransformationPropertyTester(spark, enrich, ORDERS, df_config=NONZERO_AMOUNTS)
          .add_property(row_count_exactly_preserved)
          .add_property(make_sum_linearity_property("amount"))     # sum_conservation[amount]
          .run(max_examples=2))
# ✗ row_count_exactly_preserved — ... input=1, output=2
# ✗ sum_conservation[amount] — sum conservation violated on 'amount': input=1.0, output=2.0

make_idempotency_property(["customer"])(dim, dim)     # the guard on the dimension itself
# AssertionError: idempotency violated: output has duplicate keys in ['customer'] (2 unique vs 3 rows)
```

- Generate amounts ≥ 0.01 for the money check: with amount 0 the duplicated row adds nothing.
- `make_left_join_preserves_left_keys` alone passes here: every key is still present.
- Put the dimension-key uniqueness check in the pipeline too (a cheap gate before the join).
- With generated dimensions, compose two strategies (`spark-dataframe-strategies`,
  `composing_related_data.md`) and keep the key column `unique=True` for the valid case.

## Composition

Test each stage against its own contract, then the whole pipeline against an obviously-correct
Python model on the collected rows (differential property). Stage tests localise a failure;
the model test catches wrong wiring between stages.

```python
def dedupe(df):        # total order → deterministic winner
    w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "status", "customer")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def pipeline(df):
    return revenue(paid_only(dedupe(df)))

def model(rows):
    best = {}
    for r in sorted(rows, key=lambda r: (-r.amount, r.status, r.customer)):
        best.setdefault(r.order_id, r)
    out = {}
    for r in best.values():
        if r.status == "paid":
            out[r.customer] = out.get(r.customer, Decimal("0")) + r.amount
    return out

def matches_model(input_df, output_df):
    got = {r.customer: r.amount for r in output_df.collect()}
    assert got == model(input_df.collect()), f"pipeline {got} != model {model(input_df.collect())}"
```

Stage battery: `make_idempotency_property(["order_id"], transform=dedupe)` on `dedupe`,
`row_count_non_increasing` on `paid_only`, sum conservation on `revenue`. Generate `order_id`
from a small range **without** `unique=True` here: re-deliveries are the case dedup exists for.

## Determinism

```python
def deterministic(input_df, output_df):
    assert_same_rows(output_df, pipeline(input_df.repartition(3)), "repartition(3)")
    assert_same_rows(output_df, pipeline(input_df.orderBy(F.rand(7))), "reordered")
```

Sources of non-determinism: `first()`/`last()` without ordering, `row_number()` over a
non-total order (ties), `dropDuplicates(subset)` choosing an arbitrary row,
`monotonically_increasing_id()`, `limit()` without `orderBy`, float sums in different orders
(compare with a tolerance). On `local[1]` with one partition some of these stay hidden: the
`repartition(3)` variant forces another layout. See skill `spark-property-testing-foundations`
(`flakiness_and_determinism.md`).

## Money conservation

Use `DecimalType` and exact equality; a tolerance would hide exactly the bug you look for.

```python
MONEY = DecimalType(12, 2)

def split_exact(df, n=3):                     # floor to the cent, remainder on the last one
    base = F.floor(F.col("amount") * 100 / n) / 100
    return (df.withColumn("i", F.explode(F.sequence(F.lit(1), F.lit(n))))
              .withColumn("installment", F.when(F.col("i") < n, base)
                          .otherwise(F.col("amount") - base * (n - 1)).cast(MONEY)))

def installments_sum_to_amount(input_df, output_df):
    per_order = output_df.groupBy("order_id", "amount").agg(F.sum("installment").alias("paid"))
    bad = per_order.filter(F.col("paid") != F.col("amount"))
    assert bad.count() == 0, f"installments do not add up: {bad.limit(3).collect()}"
```

`round(amount / 3, 2)` for every installment pays 3 × 33.33 = 99.99 for 100.00; the property
names the order (`paid=Decimal('99.99')`). Strategy: `st.decimals(0, 10_000, places=2)`; keep
`order_id` unique so per-order sums are well defined. Pipeline-wide: total revenue equals the
sum of paid, deduplicated input amounts (`make_sum_linearity_property` without transform when
no row is filtered, a custom property otherwise).

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Fan-out not found | amount 0 or dimension key never generated | non-zero amounts; sample keys that exist in the dim (first value = duplicated key) |
| Model and pipeline disagree on ties | non-total window order | add tie-breaker columns, mirror them in the model |
| Determinism check passes locally, fails on the cluster | one partition hides ordering bugs | `repartition(n)` / `orderBy(rand)` variants |
| Money off by 1e-15 | double instead of decimal | `DecimalType(p, s)` end-to-end |
| `decimal` precision grows (`decimal(22,2)`) | Spark widens on `sum`/arithmetic | cast back; `column_types_preserved` will flag it |
| Your `exceptAll` helper fails with `Couldn't find <col>` | Spark 3.5 analyzer bug after `dropDuplicates(subset)` | compare collected rows with `Counter` (as `make_idempotency_property` does) |
| Slow pipeline properties | whole pipeline per example, several actions per property | 2-3 examples, ≤ 6 rows; stage tests carry the volume |

## Related topics

- `standard_properties.md`, `custom_and_metamorphic_properties.md`.
- Skill `spark-dataframe-strategies`: related tables, unique keys, decimal strategies.
- Skill `spark-data-quality-property-testing`: GX rules on the pipeline output.
- Skill `spark-prop-testing-toolkit`: running these suites in CI.
