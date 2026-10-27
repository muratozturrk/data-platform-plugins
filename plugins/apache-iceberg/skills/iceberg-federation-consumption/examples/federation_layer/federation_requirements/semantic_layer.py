"""A semantic layer as code: business definitions are SQL views stored in the Iceberg REST catalog.

Each definition is written once, versioned in its comment, deployed with CREATE OR REPLACE VIEW,
and then reused by every tool (BI, notebooks, apps, agents) through the federation engine.
Views are SECURITY DEFINER (Trino's default): consumers need access to the view, not to the
operational tables behind it.
"""
from __future__ import annotations

from dataclasses import dataclass

from common.federation import run

SCHEMA = "lake.semantic"


@dataclass(frozen=True)
class Definition:
    name: str
    version: int
    description: str
    sql: str

    @property
    def comment(self) -> str:
        return f"{self.description} [definition v{self.version}]"


NET_REVENUE_V1 = Definition(
    name="net_revenue_daily", version=1,
    description="Net revenue in USD per order day: currency converted, returns excluded",
    sql="""
SELECT o.order_date,
       count(*)                       AS orders,
       sum(o.amount * f.rate_to_usd)  AS net_revenue_usd
FROM lake.sales.orders o
JOIN pg.public.fx_rates f        ON f.currency = o.currency
LEFT JOIN pg.public.order_returns r ON r.order_id = o.order_id
WHERE r.order_id IS NULL
GROUP BY o.order_date""")

# Finance changes the rule centrally: pending orders ('P') no longer count as revenue.
NET_REVENUE_V2 = Definition(
    name="net_revenue_daily", version=2,
    description="Net revenue in USD per order day: currency converted, returns and pending orders excluded",
    sql=NET_REVENUE_V1.sql.replace("WHERE r.order_id IS NULL",
                                   "WHERE r.order_id IS NULL AND o.status <> 'P'"))

CUSTOMER_360 = Definition(
    name="customer_360", version=1,
    description="One row per customer: live CRM attributes plus lifetime order metrics",
    sql="""
SELECT c.customer_id, c.name, c.email, c.region, c.segment, c.status,
       count(o.order_id)            AS orders,
       coalesce(sum(o.amount), 0)   AS lifetime_value
FROM pg.public.customers c
LEFT JOIN lake.sales.orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.email, c.region, c.segment, c.status""")

DEFINITIONS = [NET_REVENUE_V1, CUSTOMER_360]


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def deploy(defn: Definition, user: str = "admin") -> None:
    run(f"CREATE OR REPLACE VIEW {SCHEMA}.{defn.name} COMMENT {_quote(defn.comment)} "
        f"SECURITY DEFINER AS {defn.sql}", user=user)


def deploy_all(definitions=DEFINITIONS) -> None:
    for d in definitions:
        deploy(d)


def catalog_of_definitions(user: str = "admin") -> dict[str, str]:
    """Discoverability: what a BI tool or an AI agent sees when it browses the semantic layer."""
    rows = run("SELECT table_name, comment FROM system.metadata.table_comments "
               "WHERE catalog_name = 'lake' AND schema_name = 'semantic'", user=user).rows
    return {name: comment for name, comment in rows}
