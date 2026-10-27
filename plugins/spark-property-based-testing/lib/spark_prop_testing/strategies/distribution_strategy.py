# spark_prop_testing/strategies/distribution_strategy.py

"""
Real production data is NOT uniformly distributed.
This module provides strategies that mimic realistic distributions:
  - Skewed (a few values dominate → like Pareto/power-law)
  - Edge-case-heavy (boundary values, zero, negatives)
  - Gaussian (bell curve around a mean)
"""

from hypothesis import strategies as st
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType


def skewed_integers(
    min_value: int = 0,
    max_value: int = 1_000_000,
    skew_value: int = None,
    skew_probability: float = 0.7,
) -> st.SearchStrategy:
    """
    Generates integers where one value dominates (e.g., user_id skew
    in clickstream data where top 1% of users generate 70% of events).

    Args:
        skew_value: The dominant value. Defaults to min_value.
        skew_probability: How often skew_value appears (0.0–1.0).
    """
    if not 0.0 <= skew_probability <= 1.0:
        raise ValueError(f"skew_probability must be in [0, 1], got {skew_probability}")
    skew_value = skew_value if skew_value is not None else min_value
    uniform = st.integers(min_value=min_value, max_value=max_value)
    threshold = skew_probability * 1000

    # Approximate probability with 1/1000 resolution; shrinks towards the
    # skewed value (the hot key), which is usually the interesting case.
    return st.integers(min_value=0, max_value=999).flatmap(
        lambda r: st.just(skew_value) if r < threshold else uniform
    )


def edge_case_integers(
    min_value: int = -2_147_483_648,
    max_value: int = 2_147_483_647,
) -> st.SearchStrategy:
    """
    Heavily weights boundary values, zero, -1, +1.
    Catches off-by-one errors and boundary condition bugs.
    """
    edge_cases = [
        st.just(0),
        st.just(-1),
        st.just(1),
        st.just(min_value),
        st.just(max_value),
        st.just(min_value + 1),
        st.just(max_value - 1),
    ]
    return st.one_of(
        *edge_cases,
        st.integers(min_value=min_value, max_value=max_value),
    )


def edge_case_strings() -> st.SearchStrategy:
    """
    Generates strings that commonly break pipelines:
    empty, whitespace-only, unicode, SQL injection patterns,
    very long strings, newlines embedded in values.
    """
    return st.one_of(
        st.just(""),
        st.just(" "),
        st.just("\t\n"),
        st.just("NULL"),             # string "NULL" ≠ SQL NULL
        st.just("None"),
        st.just("NaN"),
        st.just("'DROP TABLE users;--"),   # SQL injection test
        st.just("café"),                   # unicode
        st.just("🔥"),                     # emoji
        st.just("a" * 1000),              # very long
        st.text(min_size=1, max_size=100), # random normal
    )


def realistic_id_strategy(max_id: int = 100) -> st.SearchStrategy:
    """
    Simulates a foreign key column with limited cardinality.
    Real data has ids that repeat — this creates JOIN-testable data.
    """
    return st.integers(min_value=1, max_value=max_id)