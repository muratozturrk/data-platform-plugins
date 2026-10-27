"""A minimal semantic layer: metrics and dimensions defined once, compiled to SQL for any engine.

Modeled on the metric/dimension split of dbt Semantic Layer (MetricFlow) and Cube:
https://docs.getdbt.com/docs/build/metrics-overview , https://cube.dev/docs/product/data-modeling/concepts
Only ANSI SQL is emitted, so the same definition runs on Trino, DuckDB and Spark SQL.
"""
from dataclasses import dataclass

from common.lakehouse import trino_query


@dataclass(frozen=True)
class Metric:
    name: str
    description: str
    sql: str                  # aggregate expression over the source table, or a ratio of other metrics
    derived: bool = False     # True: `sql` references other metrics by {name}


DIMENSIONS: dict[str, str] = {
    "order_date": "CAST(order_ts AS DATE)",
    "region": "region",
    "category": "category",
    "merchant_id": "merchant_id",
}

METRICS: dict[str, Metric] = {m.name: m for m in [
    Metric("net_revenue", "Completed orders after discount",
           "SUM(CASE WHEN status = 'completed' THEN quantity * unit_price * (1 - discount) END)"),
    Metric("gross_revenue", "All non-cancelled orders before discount",
           "SUM(CASE WHEN status <> 'cancelled' THEN quantity * unit_price END)"),
    Metric("completed_orders", "Number of completed order lines",
           "COUNT(CASE WHEN status = 'completed' THEN 1 END)"),
    Metric("refunded_orders", "Number of refunded order lines",
           "COUNT(CASE WHEN status = 'refunded' THEN 1 END)"),
    Metric("avg_order_value", "net_revenue / completed_orders",
           "{net_revenue} / NULLIF({completed_orders}, 0)", derived=True),
    Metric("refund_rate", "refunded / (completed + refunded)",
           "CAST({refunded_orders} AS DOUBLE) / NULLIF({completed_orders} + {refunded_orders}, 0)", derived=True),
]}


def metric_sql(name: str) -> str:
    m = METRICS[name]
    if not m.derived:
        return m.sql
    return m.sql.format(**{k: f"({metric_sql(k)})" for k in METRICS if "{" + k + "}" in m.sql})


def compile_query(metrics: list[str], dimensions: list[str], source: str,
                  where: str | None = None, order: bool = True) -> str:
    """SELECT <dims>, <metrics> FROM source [WHERE] GROUP BY <dims>. Unknown names raise KeyError."""
    unknown = [n for n in metrics if n not in METRICS] + [d for d in dimensions if d not in DIMENSIONS]
    if unknown:
        raise KeyError(f"not in the semantic model: {unknown}")
    cols = [f"{DIMENSIONS[d]} AS {d}" for d in dimensions] + [f"{metric_sql(m)} AS {m}" for m in metrics]
    sql = f"SELECT {', '.join(cols)} FROM {source}"
    if where:
        sql += f" WHERE {where}"
    if dimensions:
        idx = ", ".join(str(i + 1) for i in range(len(dimensions)))
        sql += f" GROUP BY {idx}" + (f" ORDER BY {idx}" if order else "")
    return sql


# --- publishing the model on the SQL endpoint ------------------------------------------------

GOVERNED_VIEW = "orders_governed"


def publish_governed_view(raw_table: str, semantic_schema: str) -> str:
    """Expose the gold table to consumers only through a view in a `*_semantic` schema.

    SECURITY DEFINER: consumers need no access to the raw schema. Row filters and column masks
    for consumers are attached to this view in config/trino/rules.json.
    Statements run as the platform owner (admin).
    """
    view = f"{semantic_schema}.{GOVERNED_VIEW}"
    trino_query(f"CREATE SCHEMA IF NOT EXISTS {semantic_schema}")
    trino_query(f"CREATE OR REPLACE VIEW {view} SECURITY DEFINER AS SELECT order_id, merchant_id, customer_id, "
            f"customer_email, region, category, quantity, unit_price, discount, status, order_ts FROM {raw_table}")
    return view


def drop_semantic_schema(semantic_schema: str) -> None:
    for (name,) in trino_query(f"SHOW TABLES IN {semantic_schema}"):
        trino_query(f"DROP VIEW IF EXISTS {semantic_schema}.{name}")
    trino_query(f"DROP SCHEMA IF EXISTS {semantic_schema}")
