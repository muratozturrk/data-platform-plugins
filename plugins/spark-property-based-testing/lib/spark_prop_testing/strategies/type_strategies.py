# spark_prop_testing/strategies/type_strategies.py

"""
Maps every Spark DataType → a Hypothesis strategy that generates
valid Python values for that type.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

from hypothesis import strategies as st
from pyspark.sql.types import (
    BooleanType, ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, StringType, BinaryType,
    DateType, TimestampType, DecimalType,
    ArrayType, MapType, StructType, StructField, DataType,
)


# ── Numeric bounds per Spark type ────────────────────────────────────────────
_INT_BOUNDS = {
    ByteType:    (-128,          127),
    ShortType:   (-32_768,       32_767),
    IntegerType: (-2_147_483_648, 2_147_483_647),
    LongType:    (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
}


def strategy_for_type(spark_type: DataType) -> st.SearchStrategy:
    """
    Returns a Hypothesis strategy for any Spark DataType.
    Supports primitives, decimals, dates, arrays, maps, and structs.
    """

    # ── Boolean ──────────────────────────────────────────────────────────────
    if isinstance(spark_type, BooleanType):
        return st.booleans()

    # ── Integer family ───────────────────────────────────────────────────────
    for int_type, (lo, hi) in _INT_BOUNDS.items():
        if isinstance(spark_type, int_type):
            return st.integers(min_value=lo, max_value=hi)

    # ── Floating point ───────────────────────────────────────────────────────
    if isinstance(spark_type, FloatType):
        # width=32 without explicit bounds: ±3.4e38 are not exact float32
        # values, and Hypothesis rejects them as bounds.
        return st.floats(allow_nan=False, allow_infinity=False, width=32)

    if isinstance(spark_type, DoubleType):
        return st.floats(
            allow_nan=False, allow_infinity=False,
            width=64,
        )

    # ── Decimal (precision, scale) ───────────────────────────────────────────
    if isinstance(spark_type, DecimalType):
        precision = spark_type.precision  # total digits
        scale     = spark_type.scale      # digits after decimal point
        # Largest value the type holds, e.g. decimal(5,2) → 999.99
        max_value = Decimal(10 ** precision - 1).scaleb(-scale)

        return st.decimals(
            min_value=-max_value,
            max_value=max_value,
            places=scale,
            allow_nan=False,
            allow_infinity=False,
        )

    # ── String ───────────────────────────────────────────────────────────────
    if isinstance(spark_type, StringType):
        return st.one_of(
            st.text(min_size=0, max_size=100),
            st.just(""),                      # always test empty string
            st.just(" "),                     # whitespace-only
        )

    # ── Binary ───────────────────────────────────────────────────────────────
    if isinstance(spark_type, BinaryType):
        return st.binary(min_size=0, max_size=256)

    # ── Date & Timestamp ─────────────────────────────────────────────────────
    if isinstance(spark_type, DateType):
        return st.dates(
            min_value=date(1900, 1, 1),
            max_value=date(2100, 12, 31),
        )

    if isinstance(spark_type, TimestampType):
        # Timezone-aware (UTC) values: PySpark converts naive datetimes with
        # time.mktime(), which fails on Windows before 1970 ("mktime argument
        # out of range") and is ambiguous/non-existent around DST changes.
        # Aware values go through calendar.timegm and round-trip exactly.
        # 1970-01-02 lower bound: PySpark converts collected timestamps with
        # datetime.fromtimestamp, which raises OSError on Windows for negative
        # epochs. Use a custom_strategy for pre-1970 data on Linux/macOS.
        return st.datetimes(
            min_value=datetime(1970, 1, 2),
            max_value=datetime(2100, 12, 31),
            timezones=st.just(timezone.utc),
        )

    # ── Array<elementType> ───────────────────────────────────────────────────
    if isinstance(spark_type, ArrayType):
        element_strategy = strategy_for_type(spark_type.elementType)

        if spark_type.containsNull:
            element_strategy = st.one_of(st.none(), element_strategy)

        return st.lists(element_strategy, min_size=0, max_size=10)

    # ── Map<keyType, valueType> ───────────────────────────────────────────────
    if isinstance(spark_type, MapType):
        key_strategy   = strategy_for_type(spark_type.keyType)
        value_strategy = strategy_for_type(spark_type.valueType)

        if spark_type.valueContainsNull:
            value_strategy = st.one_of(st.none(), value_strategy)

        return st.dictionaries(
            keys=key_strategy,
            values=value_strategy,
            max_size=10,
        )

    # ── Nested Struct ────────────────────────────────────────────────────────
    if isinstance(spark_type, StructType):
        return _strategy_for_struct(spark_type)

    raise NotImplementedError(
        f"No strategy implemented for Spark type: {type(spark_type).__name__}"
    )


def _strategy_for_struct(schema: StructType) -> st.SearchStrategy:
    """Recursively builds a strategy for a StructType (nested struct)."""
    field_strategies = {}

    for field in schema.fields:
        field_strat = strategy_for_type(field.dataType)

        if field.nullable:
            # nullable field → sometimes produce None
            field_strat = st.one_of(st.none(), field_strat)

        field_strategies[field.name] = field_strat

    return st.fixed_dictionaries(field_strategies)