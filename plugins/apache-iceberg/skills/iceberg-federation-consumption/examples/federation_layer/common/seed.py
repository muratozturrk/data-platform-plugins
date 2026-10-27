"""Seed the lakehouse side: Iceberg order facts written by Trino itself (no Spark needed).

PostgreSQL is seeded by postgres/init.sql (customers, promotions, fx_rates, order_returns).
"""
from common.federation import run, scalar

ORDERS = "lake.sales.orders"

ORDERS_CTAS = f"""
CREATE TABLE {ORDERS}
WITH (partitioning = ARRAY['month(order_date)'], format = 'PARQUET')
AS SELECT
    orderkey                                   AS order_id,
    CAST(custkey AS integer)                   AS customer_id,
    orderstatus                                AS status,
    CAST(totalprice AS decimal(12, 2))         AS amount,
    element_at(ARRAY['USD', 'EUR', 'GBP'], CAST(orderkey % 3 AS integer) + 1) AS currency,
    orderdate                                  AS order_date,
    orderpriority                              AS priority
FROM tpch.tiny.orders
"""


def seed_lakehouse() -> int:
    """Idempotent: creates schemas and the orders table once per fresh environment."""
    run("CREATE SCHEMA IF NOT EXISTS lake.sales")
    run("CREATE SCHEMA IF NOT EXISTS lake.semantic")
    run("CREATE SCHEMA IF NOT EXISTS lake.accel")
    exists = scalar("SELECT count(*) FROM lake.information_schema.tables "
                    "WHERE table_schema = 'sales' AND table_name = 'orders'")
    if not exists:
        run(ORDERS_CTAS)
    return scalar(f"SELECT count(*) FROM {ORDERS}")
