"""Notebook / ML consumption: snapshot-pinned feature extraction into Arrow.

The training job binds to one Iceberg snapshot, so features and labels cannot change mid-run
or between reruns while ingestion keeps committing. Features are computed with DuckDB over a
PyIceberg Arrow scan, the lightweight notebook stack (JupyterLab + DuckDB + PyIceberg).
"""
import hashlib

import duckdb
import numpy as np
import pyarrow as pa

FEATURE_SQL = """
SELECT customer_id,
       count(*)                                              AS orders,
       sum(quantity * unit_price)                            AS spend,
       avg(quantity)                                         AS avg_quantity,
       avg(discount)                                         AS avg_discount,
       CAST(max(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS DOUBLE) AS label_refunded
FROM sales GROUP BY customer_id ORDER BY customer_id
"""
FEATURES = ["orders", "spend", "avg_quantity", "avg_discount"]


def extract_training_set(catalog, table: str, snapshot_id: int | None = None) -> tuple[pa.Table, int]:
    t = catalog.load_table(table)
    snapshot_id = snapshot_id or t.current_snapshot().snapshot_id
    scan = t.scan(selected_fields=("customer_id", "quantity", "unit_price", "discount", "status"),
                  snapshot_id=snapshot_id)
    con = duckdb.connect()
    # One thread: parallel float aggregation sums in a different order on every run, so the same
    # snapshot would give bit-different features. Determinism matters more than speed here.
    con.execute("SET threads TO 1")
    con.register("sales", scan.to_arrow())
    return con.execute(FEATURE_SQL).arrow(), snapshot_id


def fingerprint(features: pa.Table) -> str:
    """Content hash of the training set (what an MLflow run would log next to the snapshot id)."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, features.schema) as writer:
        writer.write_table(features)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def train_linear(features: pa.Table) -> np.ndarray:
    """Least-squares baseline on the Arrow columns (zero-copy to NumPy for numeric columns)."""
    x = np.column_stack([features.column(c).to_numpy() for c in FEATURES] + [np.ones(features.num_rows)])
    std = x[:, :-1].std(axis=0)
    x[:, :-1] = (x[:, :-1] - x[:, :-1].mean(axis=0)) / np.where(std == 0, 1.0, std)
    y = features.column("label_refunded").to_numpy()
    coef, *_ = np.linalg.lstsq(x, y, rcond=None)
    return coef
