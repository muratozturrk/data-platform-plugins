# spark_prop_testing/strategies/dataframe_strategy.py

"""
The top-level strategy that composes everything above into
a single, ergonomic API for generating PySpark DataFrames.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any

from hypothesis import strategies as st
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField

from .type_strategies  import strategy_for_type
from .null_strategy    import NullConfig, with_nulls


@dataclass
class ColumnConfig:
    """
    Per-column configuration for DataFrame generation.

    Args:
        null_config:       Controls null injection (default: no nulls)
        custom_strategy:   Override the auto-detected type strategy entirely
                           (use it for narrower numeric ranges, value sets, ...)
        unique:            No duplicate values in this column (keys)
    """
    null_config:     NullConfig            = field(default_factory=NullConfig)
    custom_strategy: Optional[st.SearchStrategy] = None
    unique:          bool                  = False   # no duplicate values in this column (keys)


@dataclass
class DataFrameConfig:
    """
    Global configuration for DataFrame generation.

    Args:
        min_rows:      Minimum rows to generate (default 1)
        max_rows:      Maximum rows to generate (default 100)
        column_configs: Per-column overrides keyed by column name
        default_null_config: NullConfig for nullable columns that have no
                       entry in column_configs (e.g. "40% nulls everywhere")
    """
    min_rows:       int                      = 1
    max_rows:       int                      = 100
    column_configs: Dict[str, ColumnConfig]  = field(default_factory=dict)
    default_null_config: Optional[NullConfig] = None

    def column(self, name: str) -> ColumnConfig:
        """Effective config for a column (explicit entry, else the default nulls)."""
        if name in self.column_configs:
            return self.column_configs[name]
        if self.default_null_config is not None:
            return ColumnConfig(null_config=self.default_null_config)
        return ColumnConfig()


def spark_dataframes(
    spark:   SparkSession,
    schema:  StructType,
    config:  DataFrameConfig = None,
) -> st.SearchStrategy:
    """
    Returns a Hypothesis strategy that generates PySpark DataFrames
    conforming to the given schema.

    Usage:
        @given(df=spark_dataframes(spark, my_schema))
        def test_something(df):
            ...

    Args:
        spark:  Active SparkSession
        schema: Target StructType schema
        config: Optional DataFrameConfig for fine-grained control
    """
    config = config or DataFrameConfig()

    # Build per-field strategies
    row_strategy = _build_row_strategy(schema, config)

    # Build list-of-rows strategy with configured row bounds
    unique_cols = [
        f.name for f in schema.fields
        if config.column(f.name).unique
    ]
    rows_strategy = st.lists(
        row_strategy,
        min_size=config.min_rows,
        max_size=config.max_rows,
        # Each unique column independently has no repeated values
        # (a nullable unique column allows at most one null).
        unique_by=tuple(_column_getter(c) for c in unique_cols) or None,
    )

    # Fractional null rates are applied per DataFrame with a Hypothesis-seeded
    # Random (reproducible). Per-value injection cannot honour a rate:
    # Hypothesis duplicates list elements and favours boundary values, so a
    # 0.5 rate could yield 100% nulls in a 200-row DataFrame.
    rate_cols = {
        f.name: config.column(f.name).null_config.null_rate
        for f in schema.fields
        if f.nullable and 0.0 < config.column(f.name).null_config.null_rate < 1.0
    }
    if rate_cols:
        rows_strategy = st.tuples(rows_strategy, st.randoms(use_true_random=True)).map(
            lambda pair: _apply_null_rates(pair[0], rate_cols, set(unique_cols), pair[1])
        )

    # NullConfig(always_include_null=True): guarantee ≥1 null per DataFrame
    forced_null_cols = [
        f.name for f in schema.fields
        if f.nullable and config.column(f.name).null_config.always_include_null
    ]
    if forced_null_cols:
        rows_strategy = rows_strategy.map(
            lambda rows: _force_nulls(rows, forced_null_cols)
        )

    # Map rows → actual DataFrame using st.builds pattern
    return rows_strategy.map(
        lambda rows: _rows_to_dataframe(spark, rows, schema)
    )


def _apply_null_rates(rows, rate_cols, unique_cols, rnd) -> List[Dict[str, Any]]:
    rows = [dict(r) for r in rows]
    for col, rate in rate_cols.items():
        nulled = False
        for r in rows:
            if rnd.random() < rate:
                if col in unique_cols and nulled:
                    continue        # a unique column may hold a single null
                r[col] = None
                nulled = True
    return rows


def _column_getter(column: str):
    return lambda row: row[column]


def _force_nulls(rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
    if not rows:
        return rows
    rows = [dict(r) for r in rows]
    for col in columns:
        if all(r[col] is not None for r in rows):
            rows[0][col] = None
    return rows


def _build_row_strategy(
    schema: StructType,
    config: DataFrameConfig,
) -> st.SearchStrategy:
    """
    Builds a strategy for a single row as a dict.
    Applies column-level overrides and null injection.
    """
    field_strategies: Dict[str, st.SearchStrategy] = {}

    for struct_field in schema.fields:
        col_name   = struct_field.name
        col_config = config.column(col_name)

        # ── Use custom strategy if provided ──────────────────────────────────
        if col_config.custom_strategy is not None:
            strat = col_config.custom_strategy
        else:
            strat = strategy_for_type(struct_field.dataType)

        # ── Null injection: rate 1.0 → always null; fractional rates are
        #    applied per DataFrame in spark_dataframes() ─────────────────────────
        if struct_field.nullable and col_config.null_config.null_rate >= 1.0:
            strat = with_nulls(strat, col_config.null_config)

        field_strategies[col_name] = strat

    return st.fixed_dictionaries(field_strategies)


def _rows_to_dataframe(
    spark:  SparkSession,
    rows:   List[Dict[str, Any]],
    schema: StructType,
) -> DataFrame:
    """Converts list of dicts → Spark DataFrame, handling empty case."""
    if not rows:
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(rows, schema=schema)