# Schema evolution and CI gates

## Concept

`SchemaValidator` compares data with a contract. `SchemaEvolutionChecker` compares two
**contracts**: the schema consumers rely on today (`old`) and the one a new pipeline version
will produce (`new`). Every difference becomes an `EvolutionIssue` marked breaking or safe.
"Breaking" is judged from the consumer's side: existing readers, queries and writers of the old
schema must keep working with data of the new one.

| Change | Classification | Why |
|---|---|---|
| column removed | BREAKING | readers selecting it fail |
| column renamed | BREAKING | reported as removed + added |
| nullable column added | safe | old readers ignore it, old writers leave it null |
| NOT NULL column added | BREAKING | existing writers do not provide it |
| nullable → NOT NULL | BREAKING | existing nulls fail writes |
| NOT NULL → nullable | safe | relaxed constraint (consumers that assumed NOT NULL should still null-check) |
| type change COMPATIBLE (int → long, decimal(10,2) → decimal(12,2), date → timestamp, x → string) | safe | lossless |
| type change COERCIBLE (long → int, decimal narrowing, decimal → double, timestamp → date) | BREAKING | data loss |
| type change INCOMPATIBLE (string → int, array<string> → array<int>) | BREAKING | no conversion |
| nested nullable → NOT NULL (struct field, array element, map value) | BREAKING | existing nested nulls no longer fit (COERCIBLE) |

A column counts as changed when its full `DataType` differs (`old.dataType != new.dataType`),
which includes nested nullability: struct field `nullable`, array `containsNull`, map
`valueContainsNull`. The change is then classified with `check_type_compatibility(old, new)`, so
decimal precision/scale, nested element, key, value and field types, and nested nullable → NOT
NULL (COERCIBLE → breaking) are all covered (see `type_compatibility_rules.md`).

## API reference

```python
from spark_prop_testing import SchemaEvolutionChecker
from spark_prop_testing.schema.evolution_checker import EvolutionIssue, EvolutionReport

SchemaEvolutionChecker().check(old_schema, new_schema, old_name="v_old", new_name="v_new") -> EvolutionReport

@dataclass
class EvolutionIssue:
    is_breaking: bool
    description: str          # str(issue) == "[BREAKING] ..." or "[SAFE] ..."

@dataclass
class EvolutionReport:
    old_version: str
    new_version: str
    issues: list[EvolutionIssue]
    has_breaking_changes -> bool
    breaking_changes -> list[EvolutionIssue]
    safe_changes -> list[EvolutionIssue]
    summary() -> str          # status line, counts, one line per issue
```

Descriptions name the column: `"Column 'amount' was REMOVED ..."`,
`"Column 'channel' (StringType()) was ADDED — nullable: safe addition"`,
`"Column 'amount': type changed decimal(10,2) → decimal(10,4). Decimal narrowing ..."`,
`"Column 'order_id': nullable → NOT NULL ..."`.

A column whose type *and* nullability change yields two issues (one each).

## Worked example

`examples/test_schema_evolution.py`:

```python
ORDERS_V1 = StructType([
    StructField("order_id", IntegerType(),           nullable=False),
    StructField("amount",   DecimalType(10, 2),      nullable=True),
    StructField("tags",     ArrayType(StringType()), nullable=True),
    StructField("placed",   DateType(),              nullable=True),
])
ORDERS_V2 = StructType([
    StructField("order_id", LongType(),              nullable=True),   # widen + relax: 2 safe issues
    StructField("amount",   DecimalType(12, 2),      nullable=True),   # widen
    StructField("tags",     ArrayType(StringType()), nullable=True),
    StructField("placed",   TimestampType(),         nullable=True),   # date → timestamp
    StructField("channel",  StringType(),            nullable=True),   # nullable addition
])
report = SchemaEvolutionChecker().check(ORDERS_V1, ORDERS_V2, "orders_v1", "orders_v2")
assert not report.has_breaking_changes and len(report.safe_changes) == 5
```

Generated variants turn the classification rules into properties over every column:

```python
@given(st.sampled_from(NAMES))
def test_dropping_any_column_is_breaking(name): ...

@given(new_name, NEW_TYPES, st.booleans())
def test_added_column_is_breaking_iff_not_null(name, dtype, nullable): ...

@given(st.sampled_from(NAMES), NEW_TYPES)
def test_retype_is_breaking_iff_lossy(name, dtype):
    level = check_type_compatibility(ORDERS_V1[name].dataType, dtype)[0]
    assert CHECKER.check(ORDERS_V1, retyped).has_breaking_changes == (
        level in (Compatibility.COERCIBLE, Compatibility.INCOMPATIBLE))
```

### CI gate between pipeline versions

```python
PUBLISHED = {"v1": ORDERS_V1, "v2": ORDERS_V2}      # contracts consumers have seen
CANDIDATE = build_output_schema()                    # what this commit produces

def test_published_versions_evolve_safely():
    versions = list(PUBLISHED)
    for old, new in zip(versions, versions[1:]):
        report = CHECKER.check(PUBLISHED[old], PUBLISHED[new], old, new)
        assert not report.has_breaking_changes, report.summary()

def test_candidate_does_not_break_latest():
    report = CHECKER.check(PUBLISHED["v2"], CANDIDATE, "v2", "candidate")
    assert not report.has_breaking_changes, report.summary()
```

Pair it with the runtime side: run the job's transformation on generated input and
`SchemaValidator(CANDIDATE, strict_types=True).validate(out).raise_if_invalid()`, so the
declared candidate schema is what the code actually produces. Obtain `CANDIDATE` from the
transformation on an empty DataFrame (`f(spark.createDataFrame([], INPUT)).schema`, no job) to
keep the gate fast. When a break is intentional, publish a new major contract (`v3` as a new
table/topic) rather than editing an old one; the example marks a breaking candidate as
`xfail(strict=True)` to show the gate failing.

Run the schema-only gates as a separate fast CI step (no SparkSession needed for
`StructType` comparisons) before the slower Spark property suites.

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Breaking change although `simpleString()` is identical | a nested field, array element or map value became NOT NULL (full `DataType` is compared) | keep nested nullability, or publish a new major version |
| Nested NOT NULL → nullable reported as a SAFE type change | relaxing nested nullability is lossless | expected; consumers that relied on it still need a runtime null check |
| A rename is "two issues" | no rename detection | expected; a rename is breaking for consumers |
| int → string reported safe | COMPATIBLE (always representable) | add a `strict_types` validator gate if stringification is unwanted |
| NOT NULL → nullable reported safe but a consumer breaks | consumer assumed no nulls | treat as breaking in your gate: inspect `safe_changes` descriptions |
| Gate passes but production schema differs | `CANDIDATE` hand-written | derive it from the transformation, validate output with `SchemaValidator` |
| Column order changed, no issue | evolution ignores order | `SchemaValidator(check_column_order=True)` if positional readers exist |

## Related topics

- `type_compatibility_rules.md` - the rule table behind every type change.
- `schema_validator.md` - validating data against the candidate contract.
- `null_contracts.md` - runtime enforcement of NOT NULL.
- Skill `spark-property-testing-foundations` - `spark-ci` profile for the CI run.
