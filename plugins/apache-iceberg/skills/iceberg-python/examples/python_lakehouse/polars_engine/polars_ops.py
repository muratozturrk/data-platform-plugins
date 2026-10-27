"""Polars on Iceberg: lazy scans with pushdown, and writes delegated to PyIceberg."""
import re

import polars as pl

from common.lakehouse import S3_PROPS


def scan(table_or_metadata, snapshot_id: int | None = None) -> pl.LazyFrame:
    """Lazy Iceberg scan. Polars 2.0 reads data files with its own Rust object store, so the
    S3 endpoint must be passed explicitly; it is NOT taken from the PyIceberg table's FileIO."""
    return pl.scan_iceberg(table_or_metadata, snapshot_id=snapshot_id, storage_options=S3_PROPS)


def avg_distance_by_passengers(lf: pl.LazyFrame) -> pl.DataFrame:
    """Average distance per passenger count for multi-passenger trips."""
    return (
        lf.filter(pl.col("passenger_count") > 1)
        .group_by("passenger_count")
        .agg(pl.col("trip_distance").mean().alias("avg_distance"))
        .sort("passenger_count")
        .collect()
    )


def files_in_plan(lf: pl.LazyFrame) -> int:
    """Data files left in the optimized plan after Polars applied Iceberg pruning."""
    plan = lf.explain()
    m = re.search(r"SCAN \[(.*?)\]", plan)
    if not m or not m.group(1):
        return 0
    more = re.search(r"\.\.\. (\d+) other sources", m.group(1))
    return 1 + (int(more.group(1)) if more else 0)


def append(table, df: pl.DataFrame) -> None:
    """Append via write_iceberg after casting to the table's Arrow schema: Polars infers Int64
    for Python ints and PyIceberg 0.9.1 refuses to narrow long to int."""
    target = table.schema().as_arrow()
    df.cast({name: pl.from_arrow(target.empty_table()).schema[name] for name in df.columns}) \
      .write_iceberg(table, mode="append")
