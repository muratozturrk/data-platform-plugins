"""
SchemaValidator: missing/extra columns, strict_types / allow_extra_cols / check_column_order,
nullable contracts, SchemaValidationResult (severities, raise_if_invalid, to_result) and
property tests that generate schema variants and assert how the validator classifies them.

Only schemas are inspected, so empty DataFrames are enough (no Spark job runs).

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_schema_validator.py
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)
from returns.pipeline import is_successful
from returns.result import Failure, Success

from spark_prop_testing import SchemaValidator
from spark_prop_testing._hypothesis import register_spark_profiles
from spark_prop_testing.schema.type_compatibility import Compatibility, check_type_compatibility
from spark_prop_testing.schema.validation_result import Severity

register_spark_profiles()
FAST = settings(settings.get_profile("spark"), max_examples=5, database=None)

CONTRACT = StructType([
    StructField("order_id", LongType(),          nullable=False),
    StructField("amount",   DecimalType(12, 2),  nullable=False),
    StructField("status",   StringType(),        nullable=True),
    StructField("placed",   TimestampType(),     nullable=True),
])


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-schema-validator")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def frame(spark, *fields):
    """An empty DataFrame with the given (name, type, nullable) fields."""
    return spark.createDataFrame([], StructType([StructField(n, t, nl) for n, t, nl in fields]))


def same_as_contract(spark):
    return frame(spark, *[(f.name, f.dataType, f.nullable) for f in CONTRACT.fields])


def severities(result, column):
    return {i.severity for i in result.issues if i.column == column}


def test_exact_schema_is_clean(spark):
    """A DataFrame with exactly the contract schema yields no issues at all."""
    result = SchemaValidator(CONTRACT).validate(same_as_contract(spark))
    assert result.is_valid and result.issues == []
    assert isinstance(result.to_result(), Success)


def test_missing_extra_and_order(spark):
    """Missing column = ERROR; extra column = INFO (WARNING when allow_extra_cols=False); order = WARNING."""
    df = frame(spark, ("status", StringType(), True), ("order_id", LongType(), False),
               ("amount", DecimalType(12, 2), False), ("debug", StringType(), True))
    lenient = SchemaValidator(CONTRACT, check_column_order=True).validate(df)
    assert not lenient.is_valid
    assert severities(lenient, "placed") == {Severity.ERROR}
    assert severities(lenient, "debug") == {Severity.INFO}
    assert any(i.column is None and "order" in i.message for i in lenient.warnings)

    strict = SchemaValidator(CONTRACT, allow_extra_cols=False).validate(df)
    assert severities(strict, "debug") == {Severity.WARNING}     # still not an ERROR


def test_type_levels_and_strict_types(spark):
    """COMPATIBLE and COERCIBLE = WARNING (both ERROR with strict_types); INCOMPATIBLE = ERROR."""
    df = frame(spark, ("order_id", IntegerType(), False),        # int -> long: COMPATIBLE
               ("amount", DoubleType(), False),                  # double -> decimal: COERCIBLE
               ("status", StringType(), True),
               ("placed", StringType(), True))                   # string -> timestamp: INCOMPATIBLE
    default = SchemaValidator(CONTRACT).validate(df)
    assert severities(default, "order_id") == {Severity.WARNING}
    assert severities(default, "amount") == {Severity.WARNING}
    assert severities(default, "placed") == {Severity.ERROR}

    strict = SchemaValidator(CONTRACT, strict_types=True).validate(df)
    assert {e.column for e in strict.errors} == {"order_id", "amount", "placed"}


def test_every_message_names_its_column(spark):
    """Every column-level message (missing, extra, type, nullable) starts with "Column '<name>'"."""
    df = frame(spark, ("order_id", IntegerType(), True), ("status", StringType(), False),
               ("debug", StringType(), True))
    result = SchemaValidator(CONTRACT, allow_extra_cols=False).validate(df)
    column_issues = [i for i in result.issues if i.column is not None]
    assert {i.column for i in column_issues} == {"order_id", "amount", "placed", "status", "debug"}
    # the "stricter than expected" INFO reads "Column 'x' is stricter ...", without the colon
    assert all(i.message.startswith(f"Column '{i.column}'") for i in column_issues), result.summary()


def test_nullable_contract(spark):
    """Expected NOT NULL but nullable = WARNING (still valid); stricter than expected = INFO."""
    df = frame(spark, ("order_id", LongType(), True), ("amount", DecimalType(12, 2), False),
               ("status", StringType(), False), ("placed", TimestampType(), True))
    result = SchemaValidator(CONTRACT).validate(df)
    assert result.is_valid                                      # warnings do not invalidate
    assert severities(result, "order_id") == {Severity.WARNING}
    assert severities(result, "status") == {Severity.INFO}


def test_result_api(spark):
    """raise_if_invalid raises with the summary; to_result() is Failure(list of ERROR issues)."""
    result = SchemaValidator(CONTRACT, schema_name="orders").validate(
        frame(spark, ("order_id", StringType(), False)))
    with pytest.raises(AssertionError, match="Schema validation failed"):
        result.raise_if_invalid()

    outcome = result.to_result()
    assert isinstance(outcome, Failure) and not is_successful(outcome)
    errors = outcome.failure()
    assert {e.column for e in errors} == {"order_id", "amount", "status", "placed"}
    assert all(e.severity is Severity.ERROR for e in errors)
    # railway style: map only runs on Success, alt only on Failure
    assert outcome.map(lambda r: "load").alt(lambda errs: len(errs)).failure() == 4


# ── generated schema variants ────────────────────────────────────────────────

NAMES = [f.name for f in CONTRACT.fields]
TYPES = st.sampled_from([IntegerType(), LongType(), DoubleType(), StringType(), DateType(),
                         TimestampType(), DecimalType(10, 2), DecimalType(14, 4), ArrayType(StringType())])
EXPECTED_SEVERITY = {
    Compatibility.EXACT: set(),
    Compatibility.COMPATIBLE: {Severity.WARNING},
    Compatibility.COERCIBLE: {Severity.WARNING},
    Compatibility.INCOMPATIBLE: {Severity.ERROR},
}


def fields_of(schema):
    return [(f.name, f.dataType, f.nullable) for f in schema.fields]


def test_dropped_column_is_an_error(spark):
    """Dropping any contract column makes the result invalid with an ERROR on that column."""
    @FAST
    @given(name=st.sampled_from(NAMES))
    def check(name):
        df = frame(spark, *[f for f in fields_of(CONTRACT) if f[0] != name])
        result = SchemaValidator(CONTRACT).validate(df)
        assert not result.is_valid and [e.column for e in result.errors] == [name]
    check()


def test_added_column_never_invalidates(spark):
    """Adding any column keeps the result valid: INFO by default, WARNING with allow_extra_cols=False."""
    @FAST
    @given(name=st.from_regex(r"x_[a-z]{1,6}", fullmatch=True), dtype=TYPES)
    def check(name, dtype):
        df = frame(spark, *fields_of(CONTRACT), (name, dtype, True))
        assert severities(SchemaValidator(CONTRACT).validate(df), name) == {Severity.INFO}
        strict = SchemaValidator(CONTRACT, allow_extra_cols=False).validate(df)
        assert strict.is_valid and severities(strict, name) == {Severity.WARNING}
    check()


def test_retyped_column_is_classified_by_compatibility(spark):
    """Retyping one column gives exactly the severity its compatibility level maps to."""
    @FAST
    @given(name=st.sampled_from(NAMES), dtype=TYPES)
    def check(name, dtype):
        df = frame(spark, *[(n, dtype if n == name else t, nl) for n, t, nl in fields_of(CONTRACT)])
        result = SchemaValidator(CONTRACT).validate(df)
        level = check_type_compatibility(dtype, CONTRACT[name].dataType)[0]
        assert severities(result, name) == EXPECTED_SEVERITY[level]
        assert result.is_valid == (level is not Compatibility.INCOMPATIBLE)
        # strict_types: anything non-exact is an ERROR
        strict = SchemaValidator(CONTRACT, strict_types=True).validate(df)
        assert strict.is_valid == (level is Compatibility.EXACT)
    check()
