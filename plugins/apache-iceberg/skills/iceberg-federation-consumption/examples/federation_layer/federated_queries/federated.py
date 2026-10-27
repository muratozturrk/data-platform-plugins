"""Federated queries: Iceberg facts (lakehouse) joined with live operational data (PostgreSQL).

The same answer is also computed WITHOUT the federation engine, by reading each source with its
own native client (PyIceberg for the lakehouse, psycopg for PostgreSQL) and joining in Python.
That is the "integration code in every tool" the federation layer replaces.
"""
from __future__ import annotations

import os
from collections import defaultdict
from decimal import Decimal

from pyiceberg.catalog import load_catalog

from common.federation import pg_query, run

# Revenue of ACTIVE customers by region: history from Iceberg, status from the OLTP system.
REVENUE_BY_REGION_SQL = """
SELECT c.region,
       count(*)      AS orders,
       sum(o.amount) AS revenue
FROM lake.sales.orders o
JOIN pg.public.customers c ON o.customer_id = c.customer_id
WHERE c.status = 'active'
GROUP BY c.region
ORDER BY c.region
"""

# Customer 360 lookup a fraud analyst runs: live account state + order history.
CUSTOMER_LOOKUP_SQL = """
SELECT c.customer_id, c.status, c.credit_limit,
       count(o.order_id) AS orders, coalesce(sum(o.amount), 0) AS lifetime_value
FROM pg.public.customers c
LEFT JOIN lake.sales.orders o ON o.customer_id = c.customer_id
WHERE c.customer_id = {customer_id}
GROUP BY c.customer_id, c.status, c.credit_limit
"""


def revenue_by_region() -> list[tuple[str, int, Decimal]]:
    return [tuple(r) for r in run(REVENUE_BY_REGION_SQL).rows]


def customer_lookup(customer_id: int) -> dict:
    res = run(CUSTOMER_LOOKUP_SQL.format(customer_id=int(customer_id)))
    return dict(zip(res.columns, res.rows[0])) if res.rows else {}


def iceberg_catalog():
    return load_catalog("rest", **{
        "uri": os.getenv("ICEBERG_REST_URI", "http://rest:8181"),
        "s3.endpoint": os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"),
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),  # local dev default
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),  # local dev default
        "s3.region": os.getenv("AWS_REGION", "us-east-1"),
    })


def revenue_by_region_without_federation() -> list[tuple[str, int, Decimal]]:
    """Point-to-point integration: two clients, two type systems, a hand-written join."""
    orders = iceberg_catalog().load_table("sales.orders").scan(
        selected_fields=("customer_id", "amount")).to_arrow().to_pylist()
    region_of = dict(pg_query("SELECT customer_id, region FROM customers WHERE status = 'active'"))
    agg: dict[str, list] = defaultdict(lambda: [0, Decimal("0")])
    for o in orders:
        region = region_of.get(o["customer_id"])
        if region is not None:
            agg[region][0] += 1
            agg[region][1] += o["amount"]
    return [(r, n, s) for r, (n, s) in sorted(agg.items())]
