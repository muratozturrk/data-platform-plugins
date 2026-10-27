# spark_prop_testing/strategies/null_strategy.py

"""
Controls HOW nulls are injected into generated data.
This is separate from field-level nullability — it lets you
simulate realistic production null rates.
"""

from typing import Optional
from hypothesis import strategies as st


class NullConfig:
    """
    Configures null injection behavior for a column.

    Args:
        null_rate: Probability [0.0, 1.0] of a value being None.
                   0.0 = never null, 1.0 = always null.
        always_include_null: If True, guarantees at least one null
                             appears in the generated dataset.
    """

    def __init__(
        self,
        null_rate: float = 0.0,
        always_include_null: bool = False,
    ):
        if not 0.0 <= null_rate <= 1.0:
            raise ValueError(f"null_rate must be in [0, 1], got {null_rate}")

        self.null_rate           = null_rate
        self.always_include_null = always_include_null

    def __repr__(self):
        return f"NullConfig(rate={self.null_rate}, force={self.always_include_null})"


def with_nulls(
    base_strategy: st.SearchStrategy,
    null_config: NullConfig,
) -> st.SearchStrategy:
    """
    Wraps a base strategy to inject nulls at the configured rate.

    null_rate is an approximate probability per value (Hypothesis does not
    guarantee distributions). Shrinking moves towards None, so a minimal
    failing example prefers nulls — usually the interesting case.
    ``always_include_null`` is applied per DataFrame by spark_dataframes().
    """
    if null_config.null_rate == 0.0:
        return base_strategy

    if null_config.null_rate == 1.0:
        return st.none()

    # 1/1000 resolution, so rates such as 0.001 still produce nulls
    # (a 0–100 weight table truncated anything below 0.01 to "never").
    threshold = null_config.null_rate * 1000
    return st.integers(min_value=0, max_value=999).flatmap(
        lambda r: st.none() if r < threshold else base_strategy
    )