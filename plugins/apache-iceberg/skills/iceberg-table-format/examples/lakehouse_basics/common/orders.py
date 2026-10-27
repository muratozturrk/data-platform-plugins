"""Deterministic e-commerce orders data: the shape of a typical OLTP table exported for analytics.

Wide text columns (address, user agent, notes) are what make row layouts expensive for analytics:
an aggregate over `country` and `amount` does not need them, but a row store must read them anyway.
"""
import random
from datetime import datetime, timedelta, timezone

import pyarrow as pa

COUNTRIES = ["US", "DE", "TR", "GB", "FR", "NL", "BR", "IN", "JP", "CA"]
STATUSES = ["created", "paid", "shipped", "delivered", "returned"]
AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]
START = datetime(2025, 1, 1, tzinfo=timezone.utc)

SCHEMA = pa.schema([
    ("order_id", pa.int64()),
    ("customer_id", pa.int64()),
    ("country", pa.string()),
    ("status", pa.string()),
    ("amount", pa.float64()),
    ("order_ts", pa.timestamp("us", tz="UTC")),
    ("shipping_address", pa.string()),
    ("user_agent", pa.string()),
    ("notes", pa.string()),
])


def orders(n: int, seed: int = 7, first_id: int = 1, days: int = 30) -> pa.Table:
    rnd = random.Random(seed)
    rows = {name: [] for name in SCHEMA.names}
    for i in range(first_id, first_id + n):
        rows["order_id"].append(i)
        rows["customer_id"].append(rnd.randint(1, 50_000))
        rows["country"].append(rnd.choice(COUNTRIES))
        rows["status"].append(rnd.choice(STATUSES))
        rows["amount"].append(round(rnd.uniform(5, 500), 2))
        rows["order_ts"].append(START + timedelta(seconds=rnd.randrange(days * 86_400)))
        rows["shipping_address"].append(
            f"{rnd.randint(1, 9999)} {rnd.choice(['Main', 'Oak', 'Elm', 'Park'])} Street, "
            f"Apt {rnd.randint(1, 300)}, {rnd.randint(10000, 99999)}")
        rows["user_agent"].append(rnd.choice(AGENTS))
        rows["notes"].append("gift wrap, leave at door" if rnd.random() < 0.3 else
                             f"customer note {rnd.getrandbits(64):x}")
    return pa.table(rows, schema=SCHEMA)
