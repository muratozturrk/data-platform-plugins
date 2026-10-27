"""Two BI-style clients rendering one dashboard from the semantic layer.

- "superset": SQLAlchemy engine with the `trino://` dialect, which is how Apache Superset
  (and many Python BI tools) connect to Trino: https://superset.apache.org/docs/configuration/databases#trino
- "dbapi": a plain DB-API cursor, the same request/response shape a JDBC/ODBC client such as
  Tableau, Power BI or Excel uses.
Neither client contains metric logic: tiles name metrics, the semantic layer compiles them.
"""
from dataclasses import dataclass

from sqlalchemy import create_engine, text

from common.lakehouse import TRINO_CATALOG, TRINO_HOST, TRINO_PORT, trino_connect
from common.semantic import GOVERNED_VIEW, compile_query


@dataclass(frozen=True)
class Tile:
    title: str
    metrics: tuple[str, ...]
    dimensions: tuple[str, ...] = ()


SALES_DASHBOARD = [
    Tile("Revenue by region", ("net_revenue", "completed_orders"), ("region",)),
    Tile("Category mix", ("gross_revenue", "refund_rate"), ("category",)),
    Tile("KPI header", ("net_revenue", "avg_order_value", "refund_rate")),
]


def tile_sql(tile: Tile, semantic_schema: str) -> str:
    return compile_query(list(tile.metrics), list(tile.dimensions), f"{semantic_schema}.{GOVERNED_VIEW}")


def render_superset_style(user: str, semantic_schema: str, tiles=SALES_DASHBOARD) -> dict[str, list[tuple]]:
    engine = create_engine(f"trino://{user}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}")
    with engine.connect() as conn:
        out = {t.title: [tuple(r) for r in conn.execute(text(tile_sql(t, semantic_schema)))] for t in tiles}
    engine.dispose()
    return out


def render_dbapi_style(user: str, semantic_schema: str, tiles=SALES_DASHBOARD) -> dict[str, list[tuple]]:
    out = {}
    with trino_connect(user) as conn:
        cur = conn.cursor()
        for t in tiles:
            cur.execute(tile_sql(t, semantic_schema))
            out[t.title] = [tuple(r) for r in cur.fetchall()]
    return out


def same_dashboard(a: dict, b: dict, tol: float = 1e-9) -> bool:
    def close(x, y):
        return abs(x - y) <= tol * max(1.0, abs(x)) if isinstance(x, float) else x == y
    return a.keys() == b.keys() and all(
        len(a[k]) == len(b[k]) and all(close(x, y) for ra, rb in zip(a[k], b[k]) for x, y in zip(ra, rb))
        for k in a)
