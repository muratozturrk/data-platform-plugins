"""PostgreSQL 'warehouse' helpers: load business data for a lab, run checks."""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterable, Iterator

import pandas as pd
import psycopg


def dsn() -> str:
    return os.environ.get("WAREHOUSE_DSN", "postgresql://analyst:analyst@warehouse:5432/warehouse")


@contextmanager
def connect() -> Iterator[psycopg.Connection]:
    with psycopg.connect(dsn()) as conn:
        yield conn


_PG_TYPES = {"int64": "BIGINT", "int32": "INTEGER", "float64": "DOUBLE PRECISION",
             "bool": "BOOLEAN", "datetime64[ns]": "TIMESTAMP", "object": "TEXT", "category": "TEXT"}


def _pg_type(dtype) -> str:
    """Map a pandas dtype to a Postgres type (any datetime64 unit -> TIMESTAMP)."""
    name = str(dtype)
    if name.startswith("datetime64"):
        return "TIMESTAMP"
    return _PG_TYPES.get(name, "TEXT")


def load_dataframe(df: pd.DataFrame, table: str, schema: str = "public") -> int:
    """Replace `schema.table` with the contents of `df` (types derived from dtypes). Returns row count."""
    cols = ", ".join(f'"{c}" {_pg_type(df[c].dtype)}' for c in df.columns)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        cur.execute(f'CREATE TABLE "{schema}"."{table}" ({cols})')
        with cur.copy(f'COPY "{schema}"."{table}" FROM STDIN') as copy:
            for row in df.itertuples(index=False, name=None):
                copy.write_row(tuple(None if pd.isna(v) else v for v in row))
        conn.commit()
    return len(df)


def query(sql: str, params: Iterable | None = None) -> list[tuple]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def scalar(sql: str, params: Iterable | None = None):
    return query(sql, params)[0][0]
