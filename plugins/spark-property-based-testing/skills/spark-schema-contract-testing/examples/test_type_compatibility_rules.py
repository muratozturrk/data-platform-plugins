"""
check_type_compatibility(source, target): the four levels and the full rule table,
plus properties of the rules themselves (reflexivity, nested recursion, any -> string).

No SparkSession needed: the rules work on DataType objects only.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_type_compatibility_rules.py
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql.types import (
    ArrayType, BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType,
    FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField,
    StructType, TimestampNTZType, TimestampType,
)

from spark_prop_testing.schema.type_compatibility import Compatibility, check_type_compatibility

EXACT, COMPATIBLE, COERCIBLE, INCOMPATIBLE = (
    Compatibility.EXACT, Compatibility.COMPATIBLE, Compatibility.COERCIBLE, Compatibility.INCOMPATIBLE,
)

RULES = [
    # integers: widening by decimal digits (3, 5, 10, 19)
    (ByteType(),    IntegerType(), COMPATIBLE),
    (IntegerType(), LongType(),    COMPATIBLE),
    (LongType(),    IntegerType(), COERCIBLE),
    (ShortType(),   ByteType(),    COERCIBLE),
    # integer -> float: exact only up to 7 (float) / 15 (double) digits
    (ShortType(),   FloatType(),   COMPATIBLE),
    (IntegerType(), FloatType(),   COERCIBLE),
    (IntegerType(), DoubleType(),  COMPATIBLE),
    (LongType(),    DoubleType(),  COERCIBLE),
    # floats
    (FloatType(),   DoubleType(),  COMPATIBLE),
    (DoubleType(),  FloatType(),   COERCIBLE),
    (DoubleType(),  IntegerType(), COERCIBLE),
    (DoubleType(),  DecimalType(38, 10), COERCIBLE),      # rounding / overflow
    (FloatType(),   DecimalType(10, 2),  COERCIBLE),
    # decimals: integer digits (p - s) and scale must both not shrink
    (DecimalType(10, 2), DecimalType(10, 2), EXACT),
    (DecimalType(10, 2), DecimalType(12, 2), COMPATIBLE),
    (DecimalType(10, 2), DecimalType(12, 4), COMPATIBLE),
    (DecimalType(10, 2), DecimalType(10, 4), COERCIBLE),   # 8 -> 6 integer digits
    (DecimalType(10, 4), DecimalType(10, 2), COERCIBLE),   # scale 4 -> 2 rounds
    (IntegerType(), DecimalType(10, 0), COMPATIBLE),
    (IntegerType(), DecimalType(10, 2), COERCIBLE),        # only 8 integer digits
    (LongType(),    DecimalType(38, 0), COMPATIBLE),
    (DecimalType(5, 2), DoubleType(), COERCIBLE),
    (DecimalType(5, 0), IntegerType(), COERCIBLE),
    # any -> string, date <-> timestamp, everything else
    (IntegerType(), StringType(), COMPATIBLE),
    (DateType(),    StringType(), COMPATIBLE),
    (BinaryType(),  StringType(), COMPATIBLE),
    (StringType(),  IntegerType(), INCOMPATIBLE),
    (StringType(),  DateType(),    INCOMPATIBLE),
    (DateType(),    TimestampType(), COMPATIBLE),
    (TimestampType(), DateType(),  COERCIBLE),
    (TimestampType(), TimestampNTZType(), INCOMPATIBLE),
    (BooleanType(), IntegerType(), INCOMPATIBLE),
    (TimestampType(), LongType(),  INCOMPATIBLE),
    # nested: recurse into elements, keys/values and fields
    (ArrayType(IntegerType()), ArrayType(IntegerType()), EXACT),
    (ArrayType(IntegerType()), ArrayType(LongType()),    COMPATIBLE),
    (ArrayType(IntegerType()), ArrayType(StringType()),  COMPATIBLE),
    (ArrayType(StringType()),  ArrayType(IntegerType()), INCOMPATIBLE),
    (ArrayType(IntegerType(), False), ArrayType(IntegerType(), True), EXACT),     # NOT NULL -> nullable elements
    (ArrayType(IntegerType(), True), ArrayType(IntegerType(), False), COERCIBLE), # nullable -> NOT NULL elements
    (MapType(StringType(), IntegerType(), True), MapType(StringType(), IntegerType(), False), COERCIBLE),
    (MapType(StringType(), IntegerType()), MapType(StringType(), LongType()), COMPATIBLE),
    (MapType(StringType(), LongType()),    MapType(StringType(), IntegerType()), COERCIBLE),
    (ArrayType(IntegerType()), MapType(IntegerType(), IntegerType()), INCOMPATIBLE),
]


@pytest.mark.parametrize("source,target,expected", RULES,
                         ids=[f"{s.simpleString()}->{t.simpleString()}" for s, t, _ in RULES])
def test_rule_table(source, target, expected):
    """Each documented source -> target pair has exactly the documented compatibility level."""
    level, message = check_type_compatibility(source, target)
    assert level == expected, message


def _struct(*fields):
    return StructType([StructField(n, t, nullable) for n, t, nullable in fields])


def test_struct_rules():
    """Structs: worst field wins; missing field INCOMPATIBLE; extra field COMPATIBLE; nullable->NOT NULL COERCIBLE."""
    base = _struct(("a", IntegerType(), False), ("b", StringType(), True))
    assert check_type_compatibility(base, base)[0] == EXACT
    reordered = _struct(("b", StringType(), True), ("a", IntegerType(), False))
    assert check_type_compatibility(reordered, base)[0] == EXACT          # by name, not position
    widened = _struct(("a", LongType(), False), ("b", StringType(), True))
    assert check_type_compatibility(base, widened)[0] == COMPATIBLE
    extra = _struct(("a", IntegerType(), False), ("b", StringType(), True), ("c", DateType(), True))
    assert check_type_compatibility(extra, base)[0] == COMPATIBLE
    assert check_type_compatibility(base, extra)[0] == INCOMPATIBLE         # target field c missing
    loose = _struct(("a", IntegerType(), True), ("b", StringType(), True))
    level, msg = check_type_compatibility(loose, base)
    assert level == COERCIBLE and "nullable" in msg


# ── properties of the rules ─────────────────────────────────────────────────

ATOMIC = st.sampled_from([
    ByteType(), ShortType(), IntegerType(), LongType(), FloatType(), DoubleType(),
    StringType(), BooleanType(), BinaryType(), DateType(), TimestampType(),
]) | st.builds(lambda p, s: DecimalType(p, min(s, p)), st.integers(1, 38), st.integers(0, 38))

TYPES = st.recursive(
    ATOMIC,
    lambda inner: st.one_of(
        st.builds(ArrayType, inner, st.booleans()),
        st.builds(MapType, ATOMIC, inner, st.booleans()),
        st.lists(inner, min_size=1, max_size=3).map(
            lambda ts: StructType([StructField(f"f{i}", t) for i, t in enumerate(ts)])),
    ),
    max_leaves=6,
)

QUICK = settings(max_examples=200, deadline=None, database=None)


@QUICK
@given(TYPES)
def test_every_type_is_exact_with_itself(t):
    """Reflexivity: any (nested) type is EXACT with itself."""
    assert check_type_compatibility(t, t)[0] == EXACT


@QUICK
@given(ATOMIC, ATOMIC)
def test_array_and_map_recurse(src, tgt):
    """array<a> -> array<b> and map<k,a> -> map<k,b> have the level of a -> b."""
    level = check_type_compatibility(src, tgt)[0]
    assert check_type_compatibility(ArrayType(src), ArrayType(tgt))[0] == level
    assert check_type_compatibility(MapType(StringType(), src), MapType(StringType(), tgt))[0] == level


@QUICK
@given(TYPES)
def test_anything_reads_as_string(t):
    """Any type -> StringType is at least COMPATIBLE (EXACT only for string itself)."""
    level = check_type_compatibility(t, StringType())[0]
    assert level == (EXACT if isinstance(t, StringType) else COMPATIBLE)


@QUICK
@given(ATOMIC, ATOMIC)
def test_widening_is_not_symmetric(a, b):
    """If a -> b is a lossless widening between different types, b -> a is never lossless."""
    if check_type_compatibility(a, b)[0] == COMPATIBLE and not isinstance(b, StringType):
        assert check_type_compatibility(b, a)[0] in (COERCIBLE, INCOMPATIBLE)
