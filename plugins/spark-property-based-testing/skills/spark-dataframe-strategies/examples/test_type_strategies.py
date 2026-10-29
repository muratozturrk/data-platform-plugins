"""
strategy_for_type: what each Spark type generates. Most tests here are pure
Hypothesis (no Spark); the last two build DataFrames.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_type_strategies.py
"""

import struct
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType,
    FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField,
    StructType, TimestampNTZType, TimestampType,
)

from spark_prop_testing import ColumnConfig, DataFrameConfig, spark_dataframes
from spark_prop_testing._hypothesis import register_spark_profiles
from spark_prop_testing.strategies.type_strategies import strategy_for_type

register_spark_profiles()
# too_slow: the first st.text draw builds Hypothesis' unicode table (seconds, cached in .hypothesis/)
PURE = settings(database=None, max_examples=200, suppress_health_check=[HealthCheck.too_slow])
FAST = settings(settings.get_profile("spark"), max_examples=3)

INT_BOUNDS = {
    ByteType(): (-2**7, 2**7 - 1), ShortType(): (-2**15, 2**15 - 1),
    IntegerType(): (-2**31, 2**31 - 1), LongType(): (-2**63, 2**63 - 1),
}


@PURE
@given(st.data())
def test_integer_types_stay_in_their_range(data):
    """Byte/Short/Integer/Long values are ints inside the Spark type's range."""
    spark_type, (lo, hi) = data.draw(st.sampled_from(list(INT_BOUNDS.items())))
    v = data.draw(strategy_for_type(spark_type))
    assert isinstance(v, int) and lo <= v <= hi


@PURE
@given(st.data())
def test_scalar_types_python_values(data):
    """Each scalar type yields the Python type PySpark expects; floats are finite."""
    cases = [
        (BooleanType(), bool), (StringType(), str), (BinaryType(), bytes),
        (DateType(), date), (DoubleType(), float),
    ]
    spark_type, py_type = data.draw(st.sampled_from(cases))
    v = data.draw(strategy_for_type(spark_type))
    assert isinstance(v, py_type)
    if py_type is float:
        assert v == v and abs(v) != float("inf")          # no NaN, no inf
    if py_type is date:
        assert date(1900, 1, 1) <= v <= date(2100, 12, 31)


@PURE
@given(strategy_for_type(FloatType()))
def test_float_type_values_are_finite_float32(v):
    """FloatType values are finite and representable as float32."""
    assert v == v and abs(v) <= 3.4028234663852886e38
    assert struct.unpack("f", struct.pack("f", v))[0] == v


@PURE
@given(strategy_for_type(TimestampType()))
def test_timestamps_are_utc_aware(ts):
    """TimestampType values are UTC-aware datetimes from 1970-01-02 to 2100-12-31."""
    assert ts.tzinfo is timezone.utc
    assert datetime(1970, 1, 2, tzinfo=timezone.utc) <= ts <= datetime(2100, 12, 31, tzinfo=timezone.utc)


@PURE
@given(strategy_for_type(DecimalType(5, 2)))
def test_decimal_fits_precision_and_scale(d):
    """DecimalType(5,2): |value| <= 999.99 with at most 2 decimal places."""
    assert isinstance(d, Decimal) and d.is_finite()
    assert abs(d) <= Decimal("999.99")
    assert d.as_tuple().exponent >= -2


def test_decimal_reaches_the_full_range():
    """The decimal strategy reaches the type's extremes (999.99, and 0.9999 for DecimalType(4,4))."""
    assert find(strategy_for_type(DecimalType(5, 2)), lambda d: d >= Decimal("999.99"),
                settings=PURE) == Decimal("999.99")
    assert find(strategy_for_type(DecimalType(4, 4)), lambda d: d > 0, settings=PURE) > 0


def test_nested_types_include_nulls_where_allowed():
    """Arrays/maps/structs generate None for nullable elements, values and fields."""
    arr = strategy_for_type(ArrayType(IntegerType(), containsNull=True))
    assert find(arr, lambda xs: None in xs, settings=PURE) == [None]

    mp = strategy_for_type(MapType(StringType(), IntegerType(), valueContainsNull=True))
    assert find(mp, lambda m: None in m.values(), settings=PURE) == {"": None}

    struct = strategy_for_type(StructType([StructField("inner", IntegerType(), True)]))
    assert find(struct, lambda s: s["inner"] is None, settings=PURE) == {"inner": None}

    no_null_arr = strategy_for_type(ArrayType(IntegerType(), containsNull=False))
    assert find(no_null_arr, lambda xs: len(xs) >= 2, settings=PURE) == [0, 0]


def test_unsupported_types_raise():
    """Types without a mapping (e.g. TimestampNTZType) raise NotImplementedError."""
    with pytest.raises(NotImplementedError):
        strategy_for_type(TimestampNTZType())


# ── Spark: every supported type builds a DataFrame ───────────────────────────
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-type-strategies")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


WIDE = StructType([
    StructField("b",   BooleanType()),
    StructField("i",   IntegerType()),
    StructField("l",   LongType()),
    StructField("d",   DoubleType()),
    StructField("dec", DecimalType(12, 2)),
    StructField("s",   StringType()),
    StructField("bin", BinaryType()),
    StructField("dt",  DateType()),
    StructField("ts",  TimestampType()),
    StructField("tags",  ArrayType(StringType(), containsNull=True)),
    StructField("attrs", MapType(StringType(), IntegerType(), valueContainsNull=True)),
    StructField("addr",  StructType([
        StructField("city", StringType(), nullable=True),
        StructField("geo",  StructType([StructField("lat", DoubleType(), False)]), nullable=True),
    ])),
])


def test_all_types_build_a_dataframe(spark):
    """A wide schema with nested types becomes a DataFrame of the same schema and size."""
    @FAST
    @given(df=spark_dataframes(spark, WIDE, DataFrameConfig(min_rows=1, max_rows=5)))
    def check(df):
        assert df.schema == WIDE
        # compare in Spark: casting avoids Python datetime conversion of timestamps
        assert df.select(F.col("ts").cast("long")).count() == df.count() >= 1
        assert df.filter(F.col("dec") > F.lit(Decimal("9999999999.99"))).count() == 0
    check()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only datetime.fromtimestamp limit")
# Why the TimestampType strategy starts at 1970-01-02: a pre-1970 value cannot be collected on Windows.
def test_collecting_pre_1970_timestamps_fails_on_windows(spark):
    """On Windows, collect() of a pre-1970 TimestampType value raises OSError in PySpark 3.5."""
    schema = StructType([StructField("ts", TimestampType(), False)])
    df = spark.createDataFrame([(datetime(1950, 6, 1, tzinfo=timezone.utc),)], schema)
    assert df.select(F.col("ts").cast("long")).first()[0] == -618105600   # fine in Spark
    with pytest.raises(OSError):
        df.collect()
