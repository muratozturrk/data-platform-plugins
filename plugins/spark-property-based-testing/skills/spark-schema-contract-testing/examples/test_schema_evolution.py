"""
SchemaEvolutionChecker: breaking vs safe changes between two schema versions,
generated schema variants, and the checker used as a CI gate between pipeline versions.

No SparkSession needed: the checker compares StructType objects.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_schema_evolution.py
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql.types import (
    ArrayType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from spark_prop_testing import SchemaEvolutionChecker
from spark_prop_testing.schema.type_compatibility import Compatibility, check_type_compatibility

CHECKER = SchemaEvolutionChecker()

ORDERS_V1 = StructType([
    StructField("order_id", IntegerType(),       nullable=False),
    StructField("amount",   DecimalType(10, 2),  nullable=True),
    StructField("tags",     ArrayType(StringType()), nullable=True),
    StructField("placed",   DateType(),          nullable=True),
])
# v2: widen id and amount, date -> timestamp, new nullable column, NOT NULL relaxed: all safe
ORDERS_V2 = StructType([
    StructField("order_id", LongType(),          nullable=True),
    StructField("amount",   DecimalType(12, 2),  nullable=True),
    StructField("tags",     ArrayType(StringType()), nullable=True),
    StructField("placed",   TimestampType(),     nullable=True),
    StructField("channel",  StringType(),        nullable=True),
])


def test_safe_evolution():
    """Widening types, date->timestamp, a new nullable column and relaxed NOT NULL are all safe."""
    report = CHECKER.check(ORDERS_V1, ORDERS_V2, "orders_v1", "orders_v2")
    assert not report.has_breaking_changes, report.summary()
    assert len(report.safe_changes) == 5      # 4 type/nullability changes + 1 added column


@pytest.mark.parametrize("new_field,fragment", [
    (None,                                                   "REMOVED"),
    (StructField("amount", DecimalType(10, 4), True),        "type changed"),   # 8 -> 6 integer digits
    (StructField("amount", DecimalType(10, 0), True),        "type changed"),   # scale 2 -> 0 rounds
    (StructField("amount", DoubleType(), True),              "type changed"),   # decimal -> double rounds
    (StructField("tags", ArrayType(IntegerType()), True),    "type changed"),   # nested element retyped
], ids=["removed", "decimal-int-digits", "decimal-scale", "decimal-to-double", "array-element"])
def test_breaking_changes(new_field, fragment):
    """Removing a column, narrowing a decimal, or retyping a nested element is breaking."""
    target = "amount" if new_field is None or new_field.name == "amount" else new_field.name
    fields = [f for f in ORDERS_V1.fields if f.name != target]
    if new_field is not None:
        fields.append(new_field)
    report = CHECKER.check(ORDERS_V1, StructType(fields))
    assert report.has_breaking_changes
    assert any(fragment in str(i) for i in report.breaking_changes), report.summary()


def test_not_null_changes():
    """nullable -> NOT NULL and a new NOT NULL column are breaking; a rename is remove + add."""
    stricter = StructType([StructField(f.name, f.dataType, False) for f in ORDERS_V1.fields])
    assert CHECKER.check(ORDERS_V1, stricter).has_breaking_changes

    with_required = StructType(ORDERS_V1.fields + [StructField("region", StringType(), False)])
    assert CHECKER.check(ORDERS_V1, with_required).has_breaking_changes

    renamed = StructType([StructField("total" if f.name == "amount" else f.name, f.dataType, f.nullable)
                          for f in ORDERS_V1.fields])
    descriptions = [str(i) for i in CHECKER.check(ORDERS_V1, renamed).issues]
    assert any("'amount' was REMOVED" in d for d in descriptions)
    assert any("'total'" in d and "ADDED" in d for d in descriptions)


def test_nested_nullability_is_compared():
    """Nested nullable -> NOT NULL (struct field, array elements) is breaking; the reverse is safe."""
    old = StructType([StructField("addr", StructType([StructField("zip", StringType(), True)]), True),
                      StructField("tags", ArrayType(StringType(), containsNull=True), True)])
    new = StructType([StructField("addr", StructType([StructField("zip", StringType(), False)]), True),
                      StructField("tags", ArrayType(StringType(), containsNull=False), True)])
    assert old["addr"].dataType.simpleString() == new["addr"].dataType.simpleString()   # same text ...
    report = CHECKER.check(old, new)
    assert len(report.breaking_changes) == 2, report.summary()                          # ... still compared
    assert not CHECKER.check(new, old).has_breaking_changes


# ── generated schema variants ────────────────────────────────────────────────

NAMES = [f.name for f in ORDERS_V1.fields]
NEW_TYPES = st.sampled_from([IntegerType(), LongType(), StringType(), DateType(), DoubleType()])
QUICK = settings(max_examples=100, deadline=None, database=None)


@QUICK
@given(st.sampled_from(NAMES))
def test_dropping_any_column_is_breaking(name):
    """Dropping any existing column is always a breaking change."""
    dropped = StructType([f for f in ORDERS_V1.fields if f.name != name])
    assert CHECKER.check(ORDERS_V1, dropped).has_breaking_changes


@QUICK
@given(st.text("abcdefgh_", min_size=1, max_size=8).filter(lambda n: n not in NAMES), NEW_TYPES, st.booleans())
def test_added_column_is_breaking_iff_not_null(name, dtype, nullable):
    """Adding a column is breaking exactly when it is NOT NULL."""
    report = CHECKER.check(ORDERS_V1, StructType(ORDERS_V1.fields + [StructField(name, dtype, nullable)]))
    assert report.has_breaking_changes == (not nullable)


@QUICK
@given(st.sampled_from(NAMES), NEW_TYPES)
def test_retype_is_breaking_iff_lossy(name, dtype):
    """Retyping a column is breaking exactly when the old -> new conversion is COERCIBLE or INCOMPATIBLE."""
    old = ORDERS_V1[name]
    new = StructType([StructField(f.name, dtype, f.nullable) if f.name == name else f
                      for f in ORDERS_V1.fields])
    level = check_type_compatibility(old.dataType, dtype)[0]
    lossy = level in (Compatibility.COERCIBLE, Compatibility.INCOMPATIBLE)
    assert CHECKER.check(ORDERS_V1, new).has_breaking_changes == lossy


# ── CI gate between pipeline versions ───────────────────────────────────────

PUBLISHED = {"v1": ORDERS_V1, "v2": ORDERS_V2}
CANDIDATE = StructType(ORDERS_V2.fields[:-1])        # v3 drops `channel`


def test_ci_gate_v2_is_backward_compatible():
    """CI gate: every published version may only evolve safely to the next one."""
    versions = list(PUBLISHED)
    for old, new in zip(versions, versions[1:]):
        report = CHECKER.check(PUBLISHED[old], PUBLISHED[new], old, new)
        assert not report.has_breaking_changes, report.summary()


@pytest.mark.xfail(strict=True, reason="demonstrates the gate blocking v3, which drops a published column")
def test_ci_gate_blocks_breaking_candidate():
    """CI gate: the candidate schema must not break the latest published version."""
    report = CHECKER.check(PUBLISHED["v2"], CANDIDATE, "v2", "v3-candidate")
    assert not report.has_breaking_changes, report.summary()
