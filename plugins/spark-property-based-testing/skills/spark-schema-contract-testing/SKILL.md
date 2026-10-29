---
name: spark-schema-contract-testing
description: "Schema contracts for PySpark with the bundled spark_prop_testing library: SchemaValidator (missing/extra columns, strict_types, allow_extra_cols, check_column_order, nullable contracts), the four check_type_compatibility levels and their full rule table (integers, floats, decimals, nested array/map/struct, any to string, date/timestamp), SchemaEvolutionChecker breaking vs safe changes, SchemaValidationResult with returns Success/Failure, runtime NOT NULL property tests, generated schema variants and CI gates between pipeline versions. Use when a pipeline must keep an output schema, a source schema drifts, or a schema change needs a go/no-go check."
---

# Spark Schema Contract Testing

A schema contract is the `StructType` a pipeline promises its consumers: column names, types
and NOT NULL flags. `spark_prop_testing.schema` checks a DataFrame against such a contract
(`SchemaValidator`), classifies a type change on a four-level scale
(`check_type_compatibility`) and decides whether a new schema version breaks the old one
(`SchemaEvolutionChecker`). Property tests make the contract executable: for any generated
input the output keeps the schema *and* really has no nulls where it promises none.

```python
from spark_prop_testing import SchemaValidator, SchemaEvolutionChecker, SchemaValidationResult
from spark_prop_testing.schema.type_compatibility import Compatibility, check_type_compatibility
from spark_prop_testing.schema.validation_result import Severity, ValidationIssue
```

Settings, the test session and property families are in `spark-property-testing-foundations`;
generating inputs (`spark_dataframes`, `NullConfig`, `unique=True`) is in
`spark-dataframe-strategies`. None of this layer needs Great Expectations.

## When to Use

- A job's output schema is consumed downstream (tables, APIs, files) and must not drift.
- A source system changed a type (int → string, decimal precision) and you need to know if it breaks.
- A new pipeline version changes its output schema: gate the release in CI.
- A NOT NULL column must stay null-free for *any* input, including inputs full of nulls.
- Choosing how strict a check is (exact types, extra columns, column order).

## Decision table

| Testing need | Technique / API | Reference |
|---|---|---|
| Does this DataFrame match the contract? | `SchemaValidator(contract).validate(df)` | `references/schema_validator.md` |
| Exact types / no extra columns / fixed order | `strict_types=True`, `allow_extra_cols=False`, `check_column_order=True` | `schema_validator.md` |
| Fail a test or a job on errors | `result.raise_if_invalid()`, `result.to_result()` (`Success`/`Failure`) | `schema_validator.md` |
| Is type A readable as type B? | `check_type_compatibility(source, target)` | `references/type_compatibility_rules.md` |
| Is schema v2 a breaking change for v1? | `SchemaEvolutionChecker().check(old, new)` | `references/schema_evolution.md` |
| Block breaking schema changes in CI | evolution check across published versions + validator on the output | `schema_evolution.md` |
| Output really has no nulls in NOT NULL columns | one-aggregation null count inside `@given` | `references/null_contracts.md` |
| Validator classifies drift correctly | generated drop / add / retype variants | `schema_validator.md`, `schema_evolution.md` |

## SchemaValidator

`SchemaValidator(expected_schema, strict_types=False, allow_extra_cols=True,
check_column_order=False, schema_name="schema").validate(df)` reads only `df.schema` (no Spark
job) and never raises. Checks, by column name:

| Finding | Severity |
|---|---|
| Contract column missing | ERROR |
| Extra column | INFO (`allow_extra_cols=False`: WARNING) |
| Type EXACT / COMPATIBLE / COERCIBLE / INCOMPATIBLE | none / WARNING / WARNING / ERROR (`strict_types`: every non-exact type is ERROR) |
| Expected NOT NULL, actual nullable | WARNING |
| Expected nullable, actual NOT NULL | INFO |
| Order differs (`check_column_order=True`, extra columns ignored) | WARNING, `column=None` |

```python
result = SchemaValidator(CONTRACT, strict_types=True).validate(out)
result.raise_if_invalid()            # AssertionError with result.summary()
```

Gotchas: `is_valid` means "no ERROR": without `strict_types`, extra columns, lossy casts and a
weakened NOT NULL only warn; assert on `result.warnings` when they matter. Every column-level
message starts with `"Column '<name>': ..."` (missing, extra, type, nullable); still match on
`issue.column` and `issue.severity`, not on text.

## SchemaValidationResult

`issues: list[ValidationIssue(severity, column, message, suggestion)]`, `errors`, `warnings`,
`is_valid`, `summary()`, `raise_if_invalid()`, and `to_result()` → `returns` `Success(result)` /
`Failure(list of ERROR issues)`:

```python
outcome = SchemaValidator(CONTRACT).validate(df).to_result()
outcome.map(lambda r: write(df)).alt(lambda errors: alert(errors))   # railway style
if not is_successful(outcome): ...                                  # returns.pipeline
```

## check_type_compatibility

`check_type_compatibility(source, target) -> (Compatibility, message)`: can data of `source` be
read as `target`? EXACT (identical, nested types included), COMPATIBLE (lossless), COERCIBLE
(possible but lossy), INCOMPATIBLE (no safe path). Short version of the rules:
integer widening by digits (3/5/10/19) is COMPATIBLE, narrowing COERCIBLE; int → float is
COMPATIBLE only up to 7 (float) / 15 (double) digits, so int → float and long → double are
COERCIBLE; decimal → decimal is COMPATIBLE iff integer digits (p−s) and scale both do not
shrink; float/double → decimal and decimal → float/double are COERCIBLE; array/map/struct
recurse (worst part wins, struct fields matched by name), and nullable → NOT NULL elements,
map values or struct fields make it at least COERCIBLE; anything → string COMPATIBLE; date →
timestamp COMPATIBLE, timestamp → date COERCIBLE; everything else (string → int, boolean →
int, timestamp → long) INCOMPATIBLE. Full table:
`references/type_compatibility_rules.md`.

Direction matters: the validator calls it as `(actual, expected)`, the evolution checker as
`(old, new)`.

## SchemaEvolutionChecker

`SchemaEvolutionChecker().check(old, new, old_name="v_old", new_name="v_new")` →
`EvolutionReport` (`issues`, `has_breaking_changes`, `breaking_changes`, `safe_changes`,
`summary()`). Breaking: removed column (a rename = removed + added), added NOT NULL column,
nullable → NOT NULL, a type change that is COERCIBLE or INCOMPATIBLE (decimal narrowing,
decimal → double, `array<string>` → `array<int>`). Safe: added nullable column, NOT NULL →
nullable, COMPATIBLE type changes (int → long, decimal(10,2) → decimal(12,2), date → timestamp).
Types are compared as full `DataType`s, so a change only in nested nullability (struct field,
`containsNull`, `valueContainsNull`) is seen too: nullable → NOT NULL inside is breaking, the
reverse safe.

## Null contracts at runtime

`nullable=False` is metadata. `createDataFrame` rejects `None` in it, but derived DataFrames,
reads (Parquet makes everything nullable) and `when` without `otherwise` are never checked.
Assert it as a property, on inputs generated with nulls:

```python
CONFIG = DataFrameConfig(default_null_config=NullConfig(null_rate=0.5, always_include_null=True))

@FAST
@given(df=spark_dataframes(spark, RAW, CONFIG))
def check(df):
    out = standardise(df)
    cols = [f.name for f in OUTPUT_CONTRACT.fields if not f.nullable]
    row = out.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    assert all((row[c] or 0) == 0 for c in cols), row
```

Add `SchemaValidator(OUTPUT_CONTRACT).validate(out).warnings == []` to also catch a schema whose
NOT NULL flag got lost (`coalesce(col, lit(x))` keeps NOT NULL; `when` without `otherwise`
does not). See `references/null_contracts.md`.

## Generated schema variants

Drive the validator with Hypothesis: drop a sampled column (→ exactly one ERROR on it), add a
generated column of a sampled type (→ INFO, still valid), retype a column (→ the severity its
compatibility level maps to). Same for the evolution checker: dropping is always breaking,
adding is breaking iff NOT NULL, retyping is breaking iff COERCIBLE/INCOMPATIBLE. Schema-only
checks are fast: use `spark.createDataFrame([], schema)` (no job) or plain `StructType`s.

## CI gates between pipeline versions

Keep published contracts in code (`ORDERS_V1`, `ORDERS_V2`, ...). A test checks each
consecutive pair and the candidate against the latest with `SchemaEvolutionChecker`, and the
job output against the candidate with `SchemaValidator(..., strict_types=True)`. A breaking
change then fails the build with `report.summary()` as the message; an intended break means a
new major version, not a weaker test. `spark-prop-test validate <file> --schema-module m
--schema-name S` exits non-zero on schema ERRORs for files already written.

## Run it

```bash
PYSPARK_PYTHON=$(which python) PYTHONPATH=<plugin>/lib \
  python -m pytest -q skills/spark-schema-contract-testing/examples/test_schema_validator.py
```

`test_type_compatibility_rules.py` and `test_schema_evolution.py` need no SparkSession. Run one
Spark file at a time on small machines (`local[1]`, `shuffle.partitions=1`).

## Anti-patterns

- Treating `is_valid` as "schema identical": warnings (lossy casts, lost NOT NULL) pass.
- Comparing `df.schema == contract`: fails on harmless nullability/metadata, says nothing about why.
- Trusting `nullable=False` on a derived DataFrame without a runtime null check.
- Asserting on issue message text instead of `column` / `severity`.
- Testing the null contract only on inputs without nulls (use `always_include_null=True`).
- Calling `check_type_compatibility(expected, actual)`: the direction is source → target.
- Gating releases with the default validator when lossy casts must fail: use `strict_types=True`.

## References

- `references/schema_validator.md`, `references/type_compatibility_rules.md`,
  `references/schema_evolution.md`, `references/null_contracts.md`.
- Examples: `examples/test_schema_validator.py`, `test_type_compatibility_rules.py`,
  `test_schema_evolution.py`, `test_null_contracts.py`.
