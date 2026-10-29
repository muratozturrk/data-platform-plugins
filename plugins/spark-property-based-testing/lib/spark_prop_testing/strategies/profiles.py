# spark_prop_testing/strategies/profiles.py

"""
Pre-built DataFrameConfig profiles for common data patterns.
Import these instead of building configs from scratch each time, then
override specific columns via ``config.column_configs[...]``.
"""

from typing import Iterable

from .dataframe_strategy import DataFrameConfig, ColumnConfig
from .null_strategy       import NullConfig
from .distribution_strategy import realistic_id_strategy


def high_null_profile(null_rate: float = 0.4) -> DataFrameConfig:
    """Sparse / late-arriving data: every nullable column gets ``null_rate``."""
    return DataFrameConfig(
        min_rows=50,
        max_rows=500,
        default_null_config=NullConfig(null_rate=null_rate),
    )


def stress_test_profile() -> DataFrameConfig:
    """Large row counts for performance property tests (slow: use few examples)."""
    return DataFrameConfig(min_rows=1_000, max_rows=10_000)


def edge_case_profile() -> DataFrameConfig:
    """Tiny DataFrames (incl. a single row), where boundary bugs show up first."""
    return DataFrameConfig(min_rows=1, max_rows=10)


def production_like_profile(
    id_cardinality: int = 1000,
    id_columns: Iterable[str] = (),
    null_rate: float = 0.02,
) -> DataFrameConfig:
    """
    Realistic shape: 100–1000 rows, low-cardinality id columns (so joins and
    group-bys hit repeated keys) and a small background null rate on every
    other nullable column.
    """
    return DataFrameConfig(
        min_rows=100,
        max_rows=1000,
        column_configs={
            col: ColumnConfig(custom_strategy=realistic_id_strategy(max_id=id_cardinality))
            for col in id_columns
        },
        default_null_config=NullConfig(null_rate=null_rate),
    )
