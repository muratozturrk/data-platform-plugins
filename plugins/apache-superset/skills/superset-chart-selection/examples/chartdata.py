"""Small helpers around POST /api/v1/chart/data results used by the chapter 2 labs."""
from __future__ import annotations

import pandas as pd


def time_axis(column: str, time_grain: str = "P1M") -> dict:
    """x-axis column spec exactly as the ECharts Timeseries buildQuery sends it."""
    return {"columnType": "BASE_AXIS", "expressionType": "SQL", "label": column,
            "sqlExpression": column, "timeGrain": time_grain}


def temporal_filter(column: str, time_range: str) -> dict:
    return {"col": column, "op": "TEMPORAL_RANGE", "val": time_range}


def adhoc_temporal_filter(column: str, time_range: str) -> dict:
    """The same filter as stored in a chart's form_data (adhoc_filters)."""
    return {"clause": "WHERE", "expressionType": "SIMPLE", "subject": column,
            "operator": "TEMPORAL_RANGE", "comparator": time_range, "isExtra": False}


def as_timestamp(value) -> pd.Timestamp:
    """Chart data serializes timestamps as epoch milliseconds (or ISO strings)."""
    if isinstance(value, (int, float)):
        return pd.Timestamp(value, unit="ms")
    return pd.Timestamp(value)


def column(rows: list[dict], key: str) -> list:
    return [r[key] for r in rows]
