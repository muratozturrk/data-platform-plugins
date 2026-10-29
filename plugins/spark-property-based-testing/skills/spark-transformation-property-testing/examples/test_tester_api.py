"""
TransformationPropertyTester: register properties, run them over generated inputs, read the
report (failure message + minimal failing rows, crashes as failures), and build a tester from an
@prop-decorated transform.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_tester_api.py
"""

import pytest
from hypothesis import Phase
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_prop_testing import (
    ColumnConfig, DataFrameConfig, TransformationPropertyTester, input_columns_preserved, prop,
    row_count_exactly_preserved, row_count_non_increasing,
)
from spark_prop_testing.transformations.standard_properties import (
    output_is_non_empty_when_input_is_non_empty,
)

ORDERS = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("status",   StringType(),  nullable=False),
    StructField("amount",   IntegerType(), nullable=False),
])
SMALL = DataFrameConfig(min_rows=1, max_rows=5, column_configs={
    "order_id": ColumnConfig(custom_strategy=st.integers(1, 1_000), unique=True),
    "status":   ColumnConfig(custom_strategy=st.sampled_from(["cancelled", "paid"])),
    "amount":   ColumnConfig(custom_strategy=st.integers(-50, 500)),
})


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tester-api")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# Known-failing demos skip shrinking (each shrink step is another Spark run).
NO_SHRINK = (Phase.explicit, Phase.generate)


def paid_orders(df):
    return df.filter(F.col("status") == "paid")


def test_fluent_run_and_report(spark):
    """A filter keeps its columns and never adds rows; every property gets its own result."""
    report = (
        TransformationPropertyTester(spark, paid_orders, ORDERS, name="paid_orders", df_config=SMALL)
        .add_property(row_count_non_increasing)
        .add_property(input_columns_preserved)
        .run(max_examples=3)
    )
    report.assert_passed()
    assert [r.property_name for r in report.results] == [
        "row_count_non_increasing", "input_columns_preserved"]
    assert report.passed and "PASSED" in report.summary()


def test_failure_is_reported_with_failing_rows(spark):
    """run() never raises for a violated property; the result carries the message and the failing rows."""
    report = (
        TransformationPropertyTester(spark, paid_orders, ORDERS, df_config=SMALL)
        .add_property(row_count_exactly_preserved)      # wrong property for a filter
        .run(max_examples=3, phases=NO_SHRINK)
    )
    (result,) = report.results
    assert not result.passed and not report.passed
    assert result.error_message.startswith("row_count_exactly_preserved violated: input=1, output=0")
    # Hypothesis tries the simplest example first (one row, first sampled_from value, 0);
    # with shrinking on (phases=None), failure_input is always the shrunk (minimal) example.
    assert result.failure_input == "{'order_id': 1, 'status': 'cancelled', 'amount': 0}"
    assert "minimal failing input" in str(result)
    with pytest.raises(AssertionError, match="Transformation properties failed"):
        report.assert_passed()


def test_crash_is_a_failure_not_an_error(spark):
    """A transformation that raises is reported as a failed property with the exception type."""
    def broken(df):
        return df.select("order_id", F.col("amount_eur"))      # column does not exist

    report = (
        TransformationPropertyTester(spark, broken, ORDERS, df_config=SMALL)
        .add_property(row_count_non_increasing)
        .run(max_examples=2, phases=NO_SHRINK)
    )
    (result,) = report.results
    assert not result.passed
    assert result.error_message.startswith("AnalysisException")
    assert "amount_eur" in result.error_message


def test_non_empty_output_property(spark):
    """A filter that can remove every row violates output_is_non_empty_when_input_is_non_empty."""
    report = (
        TransformationPropertyTester(spark, paid_orders, ORDERS, df_config=SMALL)
        .add_property(output_is_non_empty_when_input_is_non_empty)
        .run(max_examples=3, phases=NO_SHRINK)
    )
    assert report.results[0].error_message == "Transformation produced empty output from non-empty input"


@prop(row_count_non_increasing, input_columns_preserved, max_examples=3)
def positive_amounts(df):
    return df.filter(F.col("amount") >= 0)


def test_prop_decorated_transform(spark):
    """@prop keeps the plain function; from_prop() registers its properties and its max_examples."""
    assert positive_amounts._is_prop_transform and positive_amounts._prop_max_examples == 3
    tester = TransformationPropertyTester.from_prop(spark, positive_amounts, ORDERS, df_config=SMALL)
    report = tester.run()            # max_examples from @prop
    report.assert_passed()
    assert report.transformation_name == "positive_amounts"
    assert [r.property_name for r in report.results] == [
        "row_count_non_increasing", "input_columns_preserved"]
    # the decorator did not change the function itself
    assert positive_amounts(spark.createDataFrame([(1, "paid", -1)], ORDERS)).count() == 0


def test_from_prop_rejects_plain_functions(spark):
    """from_prop() needs an @prop-decorated transform."""
    with pytest.raises(TypeError, match="not decorated with @prop"):
        TransformationPropertyTester.from_prop(spark, paid_orders, ORDERS)
