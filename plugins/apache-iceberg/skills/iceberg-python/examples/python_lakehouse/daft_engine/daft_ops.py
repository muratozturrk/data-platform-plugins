"""Daft on Iceberg: PyIceberg supplies metadata, Daft plans scan tasks and executes on Arrow."""
import contextlib
import io
import re

import daft
from daft import col


def read(table) -> daft.DataFrame:
    """Daft converts the table's FileIO properties (s3.endpoint etc.) into its own IOConfig."""
    return daft.read_iceberg(table)


def mean_distance_of_expensive_trips(df: daft.DataFrame, threshold: float = 100.0) -> dict:
    """Mean distance of expensive trips per passenger count. agg() takes expressions in Daft 0.7, not a {col: [fn]} dict."""
    return (df.filter(col("total_amount") > threshold)
              .groupby("passenger_count")
              .agg(col("trip_distance").mean())
              .sort("passenger_count")
              .to_pydict())


def scan_tasks(df: daft.DataFrame) -> int:
    """Scan tasks (one per data file here) left in Daft's optimized plan."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(show_all=True)
    return int(re.search(r"Num Scan Tasks = (\d+)", buf.getvalue()).group(1))


def write(df: daft.DataFrame, table, mode: str = "append", overwrite_filter=None) -> dict:
    """Daft commits Iceberg snapshots itself (older docs say it cannot); returns the file log."""
    kwargs = {"overwrite_filter": overwrite_filter} if overwrite_filter is not None else {}
    return df.write_iceberg(table, mode=mode, **kwargs).to_pydict()
