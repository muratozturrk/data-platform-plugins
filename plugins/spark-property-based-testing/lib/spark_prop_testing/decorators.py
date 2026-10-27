# spark_prop_testing/decorators.py

"""
High-level decorators that make writing property tests
feel natural and require minimal boilerplate.

Before (raw):
    @settings(deadline=None, suppress_health_check=[HealthCheck.too_slow], max_examples=20)
    @given(df=spark_dataframes(spark, schema, config))
    def test_something(df):
        ...

After (with decorators):
    @spark_property(schema=MY_SCHEMA, max_examples=20)
    def test_something(spark, df):
        ...
"""

import functools
import inspect
from typing import Callable, Optional

from hypothesis import given
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ._hypothesis import spark_settings
from .strategies.dataframe_strategy import spark_dataframes, DataFrameConfig


def spark_property(
    schema:       StructType,
    config:       DataFrameConfig  = None,
    max_examples: Optional[int]    = 20,
    spark:        SparkSession     = None,
):
    """
    Decorator: turns a ``(spark, df)``, ``(df)`` or ``(self, spark, df)`` test
    into a Hypothesis-driven property test over generated DataFrames.

    Usage:
        @spark_property(schema=ORDER_SCHEMA, max_examples=25)
        def test_my_invariant(spark, df):
            result = my_transform(df)
            assert result.count() <= df.count()

    ``df`` is generated; every other parameter (``spark``, other fixtures)
    stays visible to pytest. ``spark`` comes from the pytest fixture when the
    test asks for it, otherwise from the active/a local test session.
    """
    _config = config or DataFrameConfig(min_rows=1, max_rows=100)

    def decorator(test_fn: Callable) -> Callable:
        sig = inspect.signature(test_fn)
        if "df" not in sig.parameters:
            raise TypeError(f"{test_fn.__name__} must take a 'df' parameter")

        @functools.wraps(test_fn)
        def wrapper(*args, **kwargs):
            _spark = spark or kwargs.get("spark") or _get_or_create_spark()

            @spark_settings(max_examples)
            @given(df=spark_dataframes(_spark, schema, _config))
            def _inner(df):
                bound = dict(kwargs)
                if "spark" in sig.parameters:
                    bound.setdefault("spark", _spark)
                test_fn(*args, df=df, **bound)

            _inner()

        # Hide the generated 'df' from pytest's fixture resolution; without
        # this pytest looks for a fixture named 'df' and errors out.
        wrapper.__signature__ = sig.replace(
            parameters=[p for name, p in sig.parameters.items() if name != "df"]
        )
        del wrapper.__wrapped__
        return wrapper

    return decorator


def prop(
    *standard_properties,
    max_examples: int = 20,
):
    """
    Marks a transformation with the standard properties it must satisfy.

    Usage:
        from spark_prop_testing import prop, row_count_non_increasing

        @prop(row_count_non_increasing, input_columns_preserved)
        def my_transform(df):
            return df.filter(F.col("amount") > 0)

    The decorated function is still the plain transformation. The CLI
    (``spark-prop-test run``) and ``TransformationPropertyTester.from_prop``
    read the registered properties and run them.
    """
    def decorator(transform_fn: Callable) -> Callable:
        transform_fn._prop_properties    = list(standard_properties)
        transform_fn._prop_max_examples  = max_examples
        transform_fn._is_prop_transform  = True
        return transform_fn

    return decorator


def _get_or_create_spark() -> SparkSession:
    """Gets active session or creates a test-safe one."""
    active = SparkSession.getActiveSession()
    if active:
        return active
    return (
        SparkSession.builder
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
