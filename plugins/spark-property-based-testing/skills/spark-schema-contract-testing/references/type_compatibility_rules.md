# Type compatibility rules

## Concept

`check_type_compatibility(source, target)` answers: can values of type `source` be read as
type `target`, and what can be lost? It returns one of four levels and an explanation:

| Level | Meaning | Validator severity | Evolution checker |
|---|---|---|---|
| `EXACT` | identical type, nested types included | none | no change |
| `COMPATIBLE` | lossless conversion exists | WARNING (ERROR with `strict_types`) | safe |
| `COERCIBLE` | conversion possible but lossy (overflow, rounding, precision, time part, nulls into NOT NULL) | WARNING (ERROR with `strict_types`) | breaking |
| `INCOMPATIBLE` | no safe conversion | ERROR | breaking |

`Compatibility` is an `Enum` ordered by `.value` (EXACT < COMPATIBLE < COERCIBLE <
INCOMPATIBLE); nested types combine their parts with the *worst* level.

## API reference

```python
from spark_prop_testing.schema.type_compatibility import Compatibility, check_type_compatibility

check_type_compatibility(source: DataType, target: DataType) -> tuple[Compatibility, str]
```

- `SchemaValidator` calls it as `(actual column type, contract type)`.
- `SchemaEvolutionChecker` calls it as `(old type, new type)`.

## Rule table (in evaluation order)

Integral digits: Byte 3, Short 5, Integer 10, Long 19. Exact float digits: Float 7, Double 15.

| # | Source → target | Level | Why |
|---|---|---|---|
| 1 | `array<a>` → `array<b>` | level of `a → b`; at least COERCIBLE if `containsNull` true → false | element recursion |
| 2 | `map<k1,v1>` → `map<k2,v2>` | worst of `k1 → k2`, `v1 → v2`; at least COERCIBLE if `valueContainsNull` true → false | key/value recursion |
| 3 | struct → struct | worst over target fields | fields matched by **name**, order ignored |
| 3a | target field missing in source | INCOMPATIBLE | |
| 3b | source field nullable, target field NOT NULL | at least COERCIBLE | nested null contract |
| 3c | extra source fields | COMPATIBLE | |
| 4 | `decimal(p,s)` → same `(p,s)` | EXACT | |
| 4a | decimal → decimal, `p2−s2 >= p1−s1` and `s2 >= s1` | COMPATIBLE | integer digits and scale both kept |
| 4b | otherwise | COERCIBLE | overflow (fewer integer digits) or rounding (smaller scale) |
| 5 | same non-parameterised class | EXACT | `IntegerType → IntegerType` |
| 6 | integer → wider/equal integer | COMPATIBLE | int → long, byte → int |
| 6a | integer → narrower integer | COERCIBLE | long → int overflows |
| 7 | integer → float, digits ≤ 7 / 15 | COMPATIBLE | byte/short → float, int → double |
| 7a | integer → float, more digits | COERCIBLE | int → float, long → double (above 2^53) |
| 8 | float → double | COMPATIBLE | |
| 8a | double → float | COERCIBLE | |
| 9 | float/double → integer | COERCIBLE | truncation |
| 10 | integer → `decimal(p,s)`, `p−s >= digits` | COMPATIBLE | int → decimal(10,0), long → decimal(19,0) |
| 10a | integer → decimal with fewer integer digits | COERCIBLE | int → decimal(10,2) has 8 |
| 11 | float/double → decimal | COERCIBLE | rounding / overflow (and NaN/inf not representable) |
| 11a | decimal → integer or float/double | COERCIBLE | rounding / overflow |
| 12 | anything → string | COMPATIBLE | always representable, usually unintended |
| 13 | date → timestamp | COMPATIBLE | |
| 13a | timestamp → date | COERCIBLE | time of day lost |
| 14 | everything else | INCOMPATIBLE | string → int/date, boolean ↔ int, timestamp → long, timestamp ↔ timestamp_ntz, array ↔ map |

Notable consequences:

- `array<int> → array<string>` is COMPATIBLE (rule 1 then 12), `array<string> → array<int>` INCOMPATIBLE.
- `decimal(10,2) → decimal(10,4)` is COERCIBLE although precision is equal: integer digits 8 → 6.
- `decimal(10,2) → decimal(12,4)` is COMPATIBLE: 8 → 8 integer digits, scale 2 → 4.
- `double → decimal(38,10)` and the reverse are both COERCIBLE (rounding either way).
- `array<int>` (elements nullable) → `array<int NOT NULL>` is COERCIBLE; the reverse (NOT NULL elements read as nullable) is EXACT.
- `struct<a:int,b:string>` read as `struct<b:string,a:int>` is EXACT: order is not compared.
- Anything (arrays and structs too) → string is COMPATIBLE, so `strict_types=True` is the way to
  reject "someone stringified the column".

## Worked example

`examples/test_type_compatibility_rules.py` encodes the table as a parametrised test and adds
properties of the rules themselves over a recursive strategy of Spark types:

```python
ATOMIC = st.sampled_from([ByteType(), ShortType(), IntegerType(), LongType(), FloatType(),
                          DoubleType(), StringType(), BooleanType(), BinaryType(), DateType(),
                          TimestampType()]) \
    | st.builds(lambda p, s: DecimalType(p, min(s, p)), st.integers(1, 38), st.integers(0, 38))
TYPES = st.recursive(ATOMIC, lambda inner: st.one_of(
    st.builds(ArrayType, inner, st.booleans()),
    st.builds(MapType, ATOMIC, inner, st.booleans()),
    st.lists(inner, min_size=1, max_size=3).map(
        lambda ts: StructType([StructField(f"f{i}", t) for i, t in enumerate(ts)]))), max_leaves=6)

@given(TYPES)
def test_every_type_is_exact_with_itself(t):
    assert check_type_compatibility(t, t)[0] == Compatibility.EXACT

@given(ATOMIC, ATOMIC)
def test_array_and_map_recurse(src, tgt):
    level = check_type_compatibility(src, tgt)[0]
    assert check_type_compatibility(ArrayType(src), ArrayType(tgt))[0] == level
```

Other properties in the file: anything → string is COMPATIBLE (EXACT for string), and a
lossless widening is never lossless in reverse. These run without Spark (hundreds of examples
per second).

## Pitfalls / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| A widening shows up as WARNING | COMPATIBLE is a warning by default | expected; `strict_types=True` makes it an ERROR |
| int → float flagged lossy | float has 7 exact digits, int needs 10 | use double (15) or keep int |
| long → double flagged lossy | > 2^53 not representable | use `decimal(19,0)` or keep long |
| decimal precision raised but still COERCIBLE | scale grew more than precision, integer digits shrank | raise precision by at least the scale increase |
| double → decimal is a WARNING, not EXACT | rounding / overflow possible (COERCIBLE) | cast explicitly with validated ranges, or contract the source as decimal |
| `array<x>` → `array<x NOT NULL>` flagged lossy | nullable elements may not fit a NOT NULL element type | filter nulls (`F.filter(c, lambda x: x.isNotNull())`) and keep the runtime null check |
| struct field order change ignored | fields matched by name | `check_column_order` works only on top-level columns |
| `TimestampNTZType` vs `TimestampType` INCOMPATIBLE | different classes, no rule | convert explicitly with a time zone |

## Related topics

- `schema_validator.md` - how the levels become severities.
- `schema_evolution.md` - how the levels become breaking/safe.
- Skill `spark-dataframe-strategies` - `strategy_for_type` for the same types.
