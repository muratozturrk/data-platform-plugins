"""Lab assumption: the existing (pre-Iceberg) platform that the audit inspects.

* legacy lake on S3: Hive-style Parquet, vendor CSV, JSON-lines ticks, compliance CSV
  (a typical mix: heavy reliance on CSV and JSON, some newer datasets in Parquet).
* query workload against the PostgreSQL warehouse, so pg_stat_statements has something real to report.
"""
import io
import json
from datetime import date, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from common.lakehouse import LEGACY_BUCKET, s3_client

TRADE_DAYS = [date(2024, 1, 1) + timedelta(days=d) for d in range(5)]
FILES_PER_DAY, ROWS_PER_FILE = 4, 500


def _put(key: str, body: bytes) -> None:
    s3_client().put_object(Bucket=LEGACY_BUCKET, Key=key, Body=body)


def seed_legacy_lake() -> dict[str, int]:
    """Write the legacy lake. Returns rows per dataset (ground truth for the tests)."""
    trade_id = 0
    for day in TRADE_DAYS:
        for part in range(FILES_PER_DAY):
            ids = list(range(trade_id, trade_id + ROWS_PER_FILE))
            trade_id += ROWS_PER_FILE
            tbl = pa.table({
                "trade_id": pa.array(ids, pa.int64()),
                "symbol": [("AAPL", "MSFT", "SAP", "ASML")[i % 4] for i in ids],
                "qty": pa.array([1 + i % 100 for i in ids], pa.int32()),
                "price": [100.0 + (i % 997) / 10 for i in ids],
                "desk_region": ["EU" if i % 3 == 0 else "US" for i in ids],
            })
            buf = io.BytesIO()
            pq.write_table(tbl, buf)
            _put(f"trades_history/trade_date={day}/part-{part:05d}.parquet", buf.getvalue())

    for d, day in enumerate(TRADE_DAYS):
        rows = [f"{day},v{i % 4},{('AAPL', 'SAP')[i % 2]},{100 + i % 50}" for i in range(200)]
        _put(f"vendor_feeds/{day}.csv", ("received_on,vendor,symbol,px\n" + "\n".join(rows)).encode())

    for f in range(3):
        lines = [json.dumps({"ts": f"2024-01-0{f + 1}T09:30:{i % 60:02d}", "symbol": "SAP", "bid": 100 + i % 7,
                             "ask": 101 + i % 7}) for i in range(300)]
        _put(f"market_ticks/ticks-{f}.json", "\n".join(lines).encode())

    for f in range(2):
        rows = [f"{f * 100 + i},client{i}@example.com,2023-12-{1 + i % 28:02d},KYC" for i in range(100)]
        _put(f"compliance_records/filing-{f}.csv",
             ("record_id,client_email,filed_on,doc_type\n" + "\n".join(rows)).encode())

    return {"trades_history": len(TRADE_DAYS) * FILES_PER_DAY * ROWS_PER_FILE,
            "vendor_feeds": len(TRADE_DAYS) * 200, "market_ticks": 900, "compliance_records": 200}


# (statement, parameters, repetitions): dashboards, analysts, ETL, compliance.
WORKLOAD = [
    ("SELECT symbol, sum(net_qty) FROM reporting.daily_positions WHERE as_of = %s GROUP BY symbol",
     ("2024-01-05",), 30),
    ("SELECT * FROM reporting.v_desk_exposure WHERE desk_region = %s", ("EU",), 20),
    ("SELECT count(*), avg(price) FROM trading.trades WHERE symbol = %s", ("SAP",), 10),
    ("INSERT INTO staging.vendor_feed_raw (received_at, payload) VALUES (now(), %s::jsonb)",
     ('{"vendor": "v9"}',), 5),
    ("SELECT client_id, email FROM crm.clients WHERE country = %s", ("DE",), 3),
]


def run_workload(conn) -> int:
    """Reset pg_stat_statements, then replay the workload. Returns the number of statements executed."""
    n = 0
    with conn.cursor() as cur:
        cur.execute("SELECT pg_stat_statements_reset()")
        for sql, params, times in WORKLOAD:
            for _ in range(times):
                cur.execute(sql, params)
                n += 1
        cur.execute("DELETE FROM staging.vendor_feed_raw WHERE payload->>'vendor' = 'v9'")  # keep re-runs stable
    return n
