# spark_prop_testing/strategies/profiles.py

"""
Pre-built DataFrameConfig profiles for common data patterns.
Import these instead of building configs from scratch each time.
"""

from .dataframe_strategy import DataFrameConfig, ColumnConfig
from .null_strategy       import NullConfig
from .distribution_strategy import edge_case_strings, realistic_id_strategy


def high_null_profile(null_rate: float = 0.4) -> DataFrameConfig:
    """Simulates late-arriving or sparse data with many nulls."""
    return DataFrameConfig(
        min_rows=50,
        max_rows=500,
        column_configs={
            # Applied to any column named 'value', 'amount', 'score'
            # You override specific columns in your test
        }
    )


def stress_test_profile() -> DataFrameConfig:
    """Large row counts for performance property tests."""
    return DataFrameConfig(min_rows=1_000, max_rows=10_000)


def edge_case_profile() -> DataFrameConfig:
    """Maximises likelihood of hitting boundary conditions."""
    return DataFrameConfig(min_rows=1, max_rows=10)


def production_like_profile(id_cardinality: int = 1000) -> DataFrameConfig:
    """Simulates realistic production data patterns."""
    return DataFrameConfig(
        min_rows=100,
        max_rows=1000,
    )