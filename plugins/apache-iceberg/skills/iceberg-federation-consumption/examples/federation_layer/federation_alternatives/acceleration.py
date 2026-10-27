"""Materialized acceleration: the trade-off behind Dremio reflections and Spice.ai-style views.

A federated result is materialized as an Iceberg table (CTAS) and refreshed on a schedule.
Queries then read the lakehouse copy instead of the operational source: no source load and
lakehouse read performance, paid for with staleness between refreshes.

Trino cannot create materialized views on an Iceberg REST catalog (verified in this lab), so the
pattern is built from CREATE OR REPLACE TABLE ... AS, which keeps each refresh as a snapshot.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from common.federation import run, scalar

SCHEMA = "lake.accel"


@dataclass(frozen=True)
class AccelerationTable:
    name: str
    source_sql: str          # the federated query or semantic view to accelerate

    @property
    def table(self) -> str:
        return f"{SCHEMA}.{self.name}"

    def refresh(self) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run(f"CREATE OR REPLACE TABLE {self.table} COMMENT 'refreshed_at={stamp}' AS {self.source_sql}")
        return stamp

    def refreshed_at(self) -> str:
        comment = scalar("SELECT comment FROM system.metadata.table_comments "
                         f"WHERE catalog_name = 'lake' AND schema_name = 'accel' AND table_name = '{self.name}'")
        return comment.removeprefix("refreshed_at=")

    def stale_rows(self) -> int:
        """Rows that differ between the copy and the live federated result (both directions)."""
        return scalar(f"""SELECT count(*) FROM (
            (SELECT * FROM ({self.source_sql}) EXCEPT SELECT * FROM {self.table})
            UNION ALL
            (SELECT * FROM {self.table} EXCEPT SELECT * FROM ({self.source_sql})))""")

    def snapshots(self) -> int:
        return scalar(f'SELECT count(*) FROM {SCHEMA}."{self.name}$snapshots"')

    def drop(self) -> None:
        run(f"DROP TABLE IF EXISTS {self.table}")


CUSTOMER_360_ACCEL = AccelerationTable(
    name="customer_360_accel",
    source_sql="SELECT customer_id, region, segment, status, orders, lifetime_value "
               "FROM lake.semantic.customer_360")
