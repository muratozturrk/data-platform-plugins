# Null contracts

## Concept

A NOT NULL contract has two halves:

1. **Schema half**: the output `StructField` is `nullable=False`. `SchemaValidator` reports a
   contract column that is nullable in the actual schema as a WARNING ("nullable contract
   weakened"); a column stricter than the contract is INFO.
2. **Data half**: the column really holds no nulls, for every input. Spark does not enforce
   `nullable=False` on derived DataFrames; it is an optimiser hint. Only
   `spark.createDataFrame(rows, schema)` checks it (PySpark 3.5 raises `[CANNOT_BE_NONE]`).
   Parquet/JSON/CSV readers mark every column nullable, and an expression can produce nulls
   while the schema says otherwise after a `cast`/`select` through a nullable source.

The data half is a property: *for any input, including inputs full of nulls, the output has
zero nulls in its NOT NULL columns*. The schema half is a cheap extra check on the same output.

How Spark infers nullability of common expressions:

| Expression | Output nullable? |
|---|---|
| `F.coalesce(c, F.lit(x))` | no (one non-nullable child is enough) |
| `F.when(cond, v)` without `otherwise` | yes |
| `F.when(cond, v).otherwise(F.lit(x))` | only if `v` is nullable |
| `F.upper(c)`, `F.trim(c)`, arithmetic on `c` | same as `c` |
| `F.col(c).cast(t)` | same as `c`, plus string → number casts can yield null (non-ANSI mode) |
| left/outer join, right-side columns | yes |
| `F.lit(None)` / `F.lit(None).cast(t)` | yes |

## API reference

Nothing special is needed beyond the strategies and the validator:

```python
from spark_prop_testing import DataFrameConfig, ColumnConfig, NullConfig, SchemaValidator, spark_dataframes

NullConfig(null_rate=0.0, always_include_null=False)   # per column (ColumnConfig.null_config)
DataFrameConfig(default_null_config=NullConfig(...))   # all nullable columns without a ColumnConfig
SchemaValidator(contract).validate(out).warnings       # schema half
```

Null-count helper, one aggregation for all contract columns (one Spark job, not one per column):

```python
def null_counts(df, contract):
    cols = [f.name for f in contract.fields if not f.nullable]
    row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    return {c: (row[c] or 0) for c in cols}          # sum over 0 rows is NULL → 0
```

## Worked example

`examples/test_null_contracts.py`: raw customers with nullable `country`/`orders`, an output
contract that makes all three columns NOT NULL.

```python
CONFIG = DataFrameConfig(
    min_rows=1, max_rows=15,
    default_null_config=NullConfig(null_rate=0.5, always_include_null=True),
    column_configs={"customer_id": ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True)},
)

def standardise(df):
    return df.select(
        "customer_id",
        F.coalesce(F.upper(F.trim("country")), F.lit("UNKNOWN")).alias("country"),
        F.coalesce("orders", F.lit(0)).alias("orders"),
    )

def test_output_honours_null_contract(spark):
    @FAST
    @given(df=spark_dataframes(spark, RAW, CONFIG))
    def check(df):
        counts = null_counts(standardise(df), OUTPUT_CONTRACT)
        assert all(n == 0 for n in counts.values()), counts
    check()
```

`always_include_null=True` guarantees every generated DataFrame has at least one null in each
nullable input column, so even 3 examples exercise the null path. The buggy variant
(`F.when(F.col("orders").isNotNull(), F.col("orders"))`, no `otherwise`) fails on the first
example; the example marks it `xfail(strict=True)` and skips shrinking
(`phases=[Phase.generate]`) because every shrink step is another Spark job.

The schema half on the same transformations:

```python
result = SchemaValidator(OUTPUT_CONTRACT).validate(standardise(df))
assert result.issues == []                                  # coalesce kept NOT NULL
weakened = SchemaValidator(OUTPUT_CONTRACT).validate(standardise_buggy(df))
assert weakened.is_valid and [i.column for i in weakened.warnings] == ["orders"]
```

`is_valid` stays `True` for the buggy output: a weakened NOT NULL is only a WARNING, which is
why the runtime property is needed.

### Nested and conditional contracts

- Struct fields: `F.col("addr.zip").isNull()` in the same aggregation; arrays:
  `F.exists("tags", lambda x: x.isNull())` (Spark 3.1+).
- "NOT NULL unless status = 'cancelled'": count `F.col("shipped_at").isNull() & (F.col("status") != "cancelled")`.
- Null *rate* budgets ("at most 5 % null") belong to Great Expectations' `mostly`
  (skill `spark-data-quality-property-testing`) or `no_new_nulls_introduced` for "not more
  nulls than the input".

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Null property never fails, even for a buggy transform | inputs had no nulls (no `NullConfig`, or NOT NULL input fields) | `always_include_null=True` / `default_null_config` on nullable input columns |
| `[CANNOT_BE_NONE]` in a test fixture | `createDataFrame` with `None` in a `nullable=False` field | expected: the only place Spark enforces it; use a nullable input schema |
| Validator passes, nulls in output | weakened NOT NULL is a WARNING only | add the runtime null-count property |
| Validator warns although output has no nulls | expression inferred nullable (`when` without `otherwise`, outer join) | `coalesce`/`otherwise` with a literal, or accept the WARNING |
| `TypeError: '>' not supported ... NoneType` | `sum` over zero rows returns NULL | `row[c] or 0` |
| Slow: one job per column | `filter(...).count()` per column | one `agg` with all columns |
| Nulls only after a cast | non-ANSI string → int cast returns NULL for garbage | validate before casting; property on edge-case strings |

## Related topics

- `schema_validator.md` - nullable severities.
- `schema_evolution.md` - nullable → NOT NULL is a breaking change.
- Skill `spark-dataframe-strategies` - `NullConfig`, `default_null_config`, `always_include_null`.
- Skill `spark-property-testing-foundations` - `no_new_nulls_introduced` and other standard properties.
