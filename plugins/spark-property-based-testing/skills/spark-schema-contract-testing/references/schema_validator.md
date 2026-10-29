# SchemaValidator and SchemaValidationResult

## Concept

`SchemaValidator` compares the schema of a DataFrame (the *actual* schema) with a contract
(the *expected* `StructType`) and returns a list of findings with a severity each. It never
raises and never runs a Spark job: only `df.schema` is read, so it is cheap enough to call
after every transformation, inside property tests and as a gate before a write.

Columns are matched **by name**. The five checks run in this order:

1. missing contract columns,
2. extra columns,
3. type compatibility of every contract column that exists (`check_type_compatibility(actual, expected)`),
4. nullable contract of every contract column that exists,
5. column order (only with `check_column_order=True`).

## API reference

```python
SchemaValidator(
    expected_schema: StructType,
    strict_types: bool = False,        # every non-exact type (COMPATIBLE, COERCIBLE) becomes ERROR
    allow_extra_cols: bool = True,     # False: extra columns are WARNING instead of INFO
    check_column_order: bool = False,  # True: order of the contract columns is checked
    schema_name: str = "schema",       # label in summary()
).validate(df: DataFrame) -> SchemaValidationResult

class Severity(Enum): ERROR, WARNING, INFO

@dataclass(frozen=True)
class ValidationIssue:
    severity: Severity
    column: Optional[str]              # None for the column-order finding
    message: str
    suggestion: Optional[str] = None

@dataclass
class SchemaValidationResult:
    issues: list[ValidationIssue]
    schema_name: str
    errors -> list[ValidationIssue]     # property
    warnings -> list[ValidationIssue]   # property
    is_valid -> bool                    # no ERROR-level issue
    summary() -> str
    raise_if_invalid() -> None          # AssertionError("Schema validation failed:\n" + summary())
    to_result() -> returns.result.Result # Success(self) | Failure(self.errors)
```

### Severity matrix

| Situation | default | `strict_types=True` | `allow_extra_cols=False` |
|---|---|---|---|
| contract column missing | ERROR | ERROR | ERROR |
| extra column | INFO | INFO | WARNING |
| type EXACT | - | - | - |
| type COMPATIBLE (int → long, date → timestamp, x → string) | WARNING | ERROR | WARNING |
| type COERCIBLE (long → int, double → float, double → decimal, decimal narrowing) | WARNING | ERROR | WARNING |
| type INCOMPATIBLE (string → int, string → timestamp) | ERROR | ERROR | ERROR |
| expected NOT NULL, actual nullable | WARNING | WARNING | WARNING |
| expected nullable, actual NOT NULL | INFO | INFO | INFO |
| order differs (`check_column_order=True`) | WARNING | WARNING | WARNING |

`strict_types=True` turns every non-exact type into an ERROR, lossy (COERCIBLE) and lossless
(COMPATIBLE) alike: use it for release gates where any type drift must stop the build.

Messages: every column-level finding starts with `"Column '<name>': "` (missing, extra, type,
nullable weakened); the "stricter than expected" INFO reads `"Column '<name>' is stricter ..."`.
Filter by `issue.column` and `issue.severity`.

### `returns` integration

```python
from returns.pipeline import is_successful
from returns.result import Failure, Success

outcome = SchemaValidator(CONTRACT).validate(df).to_result()
outcome.map(lambda result: write(df))          # runs only on Success
outcome.alt(lambda errors: log(errors))        # runs only on Failure; errors = list[ValidationIssue]
outcome.unwrap()                               # result, or raises UnwrapFailedError
outcome.failure()                              # the error list (raises on Success)
is_successful(outcome)                         # bool
```

`Failure` carries only the ERROR issues; warnings are lost on that branch, use the result
object itself if you need them.

## Worked example

`examples/test_schema_validator.py`:

```python
CONTRACT = StructType([
    StructField("order_id", LongType(),         nullable=False),
    StructField("amount",   DecimalType(12, 2), nullable=False),
    StructField("status",   StringType(),       nullable=True),
    StructField("placed",   TimestampType(),    nullable=True),
])

def frame(spark, *fields):     # empty DataFrame: schema only, no Spark job
    return spark.createDataFrame([], StructType([StructField(n, t, nl) for n, t, nl in fields]))

df = frame(spark, ("order_id", IntegerType(), False),     # COMPATIBLE  → WARNING
           ("amount", DoubleType(), False),               # COERCIBLE   → WARNING
           ("status", StringType(), True),
           ("placed", StringType(), True))                # INCOMPATIBLE → ERROR
result = SchemaValidator(CONTRACT).validate(df)
assert {i.column for i in result.errors} == {"placed"}
strict = SchemaValidator(CONTRACT, strict_types=True).validate(df)
assert {i.column for i in strict.errors} == {"order_id", "amount", "placed"}
```

Generated variants assert the classification for *every* column and type in a pool:

```python
@FAST
@given(name=st.sampled_from(NAMES), dtype=TYPES)
def check(name, dtype):
    df = frame(spark, *[(n, dtype if n == name else t, nl) for n, t, nl in fields_of(CONTRACT)])
    result = SchemaValidator(CONTRACT).validate(df)
    level = check_type_compatibility(dtype, CONTRACT[name].dataType)[0]
    assert severities(result, name) == EXPECTED_SEVERITY[level]
```

The same file drops a sampled column (exactly one ERROR, on that column) and adds a generated
one (INFO by default, WARNING with `allow_extra_cols=False`, valid either way).

### Where to call it

- After each transformation in a property test: `SchemaValidator(OUT).validate(f(df)).raise_if_invalid()`.
- Before a write in the job: `to_result()` and route `Failure` to alerting instead of writing.
- On files already written: `spark-prop-test validate <path> --schema-module m --schema-name S`
  (`--format parquet|csv|json`; csv is read with header + inferSchema; exit code 1 on ERROR).
- In `DataQualityPropertySuite` (`expected_schema=`), see skill `spark-data-quality-property-testing`.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `is_valid` but downstream breaks on a cast | by default COERCIBLE types and lost NOT NULL are warnings | `strict_types=True` for types; assert `result.warnings == []` for nullability |
| Every column "drifts" on a CSV | read without `inferSchema`: all strings | the CLI `validate` uses header+inferSchema; do the same |
| Many nullable WARNINGs after reading Parquet | Parquet/JSON readers mark columns nullable | runtime null check (`null_contracts.md`), or accept and filter by column |
| Order warning although order looks right | extra columns are ignored, but contract columns must appear in contract order | `df.select(*[f.name for f in CONTRACT])` |
| `strict_types=True` fails on a harmless int → long | strict mode rejects every non-exact type | cast in the job, or use the default mode and inspect warnings |
| Test asserts on message text and breaks | message wording differs per check | assert on `column`/`severity` |
| Nested struct field missing reported as type ERROR | a struct missing a target field is INCOMPATIBLE | expected; the message lists `field '<x>' missing` |

## Related topics

- `type_compatibility_rules.md` - what decides WARNING vs ERROR for a type.
- `schema_evolution.md` - comparing two schema versions instead of a DataFrame and a contract.
- `null_contracts.md` - the runtime half of a NOT NULL contract.
- Skill `spark-dataframe-strategies` - inputs for property tests around the validator.
