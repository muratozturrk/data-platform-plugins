"""
How the library's Hypothesis profiles behave, how shrinking and replay work,
and the suppress_health_check pitfall. Only the last test starts Spark.

Run:
    PYTHONPATH=<plugin>/lib python -m pytest -q test_hypothesis_settings.py
"""

import pytest
from hypothesis import HealthCheck, find, given, seed, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from spark_prop_testing import DataFrameConfig, spark_property
from spark_prop_testing._hypothesis import (
    MAX_EXAMPLES_ENV,
    SPARK_HEALTH_CHECKS,
    effective_max_examples,
    register_spark_profiles,
)

register_spark_profiles()


def test_spark_profile_disables_deadline_and_slow_checks():
    """The 'spark' profile has no deadline and suppresses the Spark-hostile health checks."""
    p = settings.get_profile("spark")
    assert p.deadline is None
    assert p.max_examples == 20
    assert set(p.suppress_health_check) == {
        HealthCheck.too_slow, HealthCheck.data_too_large, HealthCheck.large_base_example,
    }


def test_spark_ci_profile_is_reproducible():
    """'spark-ci' = 100 derandomized examples, print_blob on, no example database."""
    p = settings.get_profile("spark-ci")
    assert (p.max_examples, p.derandomize, p.print_blob) == (100, True, True)
    assert p.deadline is None
    assert p.database is None          # derandomize=True implies database=None


def test_explicit_settings_inherit_unnamed_keys():
    """@settings(max_examples=...) on top of the profile keeps deadline=None."""
    s = settings(settings.get_profile("spark"), max_examples=5)
    assert s.deadline is None and s.max_examples == 5


def test_explicit_suppress_health_check_replaces_profile_list():
    """Passing suppress_health_check REPLACES the profile's list; extend it instead."""
    wrong = settings(settings.get_profile("spark"), suppress_health_check=[HealthCheck.filter_too_much])
    assert HealthCheck.large_base_example not in wrong.suppress_health_check

    right = settings(
        settings.get_profile("spark"),
        suppress_health_check=[*SPARK_HEALTH_CHECKS, HealthCheck.filter_too_much],
    )
    assert set(SPARK_HEALTH_CHECKS) <= set(right.suppress_health_check)


def test_env_var_overrides_library_runner_examples(monkeypatch):
    """SPARK_PROP_MAX_EXAMPLES wins over the max_examples a library runner asks for."""
    assert effective_max_examples(None) == 20
    monkeypatch.setenv(MAX_EXAMPLES_ENV, "3")
    assert effective_max_examples(50) == 3


def test_shrinking_finds_the_minimal_counterexample():
    """Shrinking: the first failing list is reduced to the smallest one, [101]."""
    smallest = find(
        st.lists(st.integers()),
        lambda xs: sum(xs) > 100,
        settings=settings(database=None, max_examples=500),
    )
    assert smallest == [101]


def test_seed_replays_the_same_examples():
    """@seed (with no database) makes generation repeatable run after run."""
    def run():
        seen = []

        @seed(20240601)
        @settings(database=None, max_examples=10)
        @given(st.integers())
        def collect(x):
            seen.append(x)

        collect()
        return seen

    assert run() == run()


def test_failure_report_contains_reproduce_blob():
    """print_blob=True attaches an @reproduce_failure(...) line to the failure."""
    @settings(database=None, print_blob=True, max_examples=200)
    @given(st.lists(st.integers()))
    def bad(xs):
        assert sum(xs) <= 100

    with pytest.raises(AssertionError) as info:
        bad()
    notes = "\n".join(getattr(info.value, "__notes__", []))
    assert "xs=[101]" in notes
    assert "@reproduce_failure(" in notes


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="demonstrates a false property and its shrunk counterexample")
@settings(database=None, max_examples=200)
@given(st.lists(st.integers(0, 1_000)))
def test_false_property_is_reported_shrunk(xs):
    """(False on purpose) 'a list of non-negative ints never sums above 100'."""
    assert sum(xs) <= 100


# ── the decorator path: spark_property applies the Spark settings itself ─────
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pbt-settings")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


SCHEMA = StructType([StructField("x", IntegerType(), nullable=False)])


@spark_property(schema=SCHEMA, config=DataFrameConfig(min_rows=0, max_rows=10), max_examples=3)
def test_spark_property_decorator(spark, df):
    """count() equals the number of collected rows (decorator supplies deadline=None)."""
    assert df.count() == len(df.collect())
