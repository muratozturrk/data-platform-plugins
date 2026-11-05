"""Deterministic synthetic fixtures with an independent oracle (Faker seeded). Produces SQL + expected JSON."""
import json, random, pathlib, sys
from decimal import Decimal
from faker import Faker

def generate(day: str, n: int, seed: int, out: pathlib.Path):
    fake = Faker("tr_TR"); Faker.seed(seed); rnd = random.Random(seed)
    rows, revenue, cancelled = [], Decimal("0"), 0
    for i in range(n):
        amount = Decimal(rnd.randint(100, 100000)) / 100
        is_cancelled = rnd.random() < 0.01
        if is_cancelled: cancelled += 1
        else: revenue += amount
        rows.append((f"o{i:06d}", fake.uuid4(), str(amount), is_cancelled, f"{day} {rnd.randint(0, 23):02d}:{rnd.randint(0, 59):02d}:00"))
    dups = rnd.sample(rows, 3)                                         # 3 duplicate rows with later _updated_at (same amount)
    rows += [(r[0], r[1], r[2], r[3], r[4].replace(":00", ":30")) for r in dups]
    sql = "CREATE TABLE IF NOT EXISTS public.orders(order_id text, customer_id text, order_total_amount numeric, is_cancelled boolean, _updated_at timestamp);\n"
    sql += "\n".join(f"INSERT INTO public.orders VALUES ('{a}','{b}',{c},{str(d).lower()},'{e}');" for a, b, c, d, e in rows)
    out.with_suffix(".sql").write_text(sql)
    expected = {"row_count": n, "cancelled": cancelled, "revenue_amount": str(revenue), "duplicates_in_source": 3}
    (out.parent / "expected" / (out.stem + ".json")).write_text(json.dumps(expected, indent=2))
    print(expected)

if __name__ == "__main__":
    generate(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), pathlib.Path(sys.argv[4]))   # e.g. 2026-11-01 1284 42 fixtures/erp_orders_day_2026-11-01
