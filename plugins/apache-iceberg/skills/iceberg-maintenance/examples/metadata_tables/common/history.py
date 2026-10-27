"""Build an Iceberg table with a rich, deterministic history.

Every metadata table has something to show: appends, a MOR delete (position
deletes), a partition overwrite, a tag, partition evolution, a branch with its
own commit, and two expired snapshots (so the all_* tables differ from the
current-state tables).

Timeline on `main` (S1, S2 are expired at the end):

    S1 append     ids 1-30   EU, US, APAC   3 files  (spec 0: region)
    S2 append     ids 31-50  EU, US         2 files
    S3 delete     ids 1, 2, 31 (MOR)        2 position delete files
    S4 overwrite  APAC replaced             1 file in, 1 file out
       tag audit_s4 -> S4
       ALTER TABLE ADD PARTITION FIELD months(order_date)    (spec 1)
    S5 append     ids 51-60  EU, Feb 2025   1 file (spec 1)
       branch dev -> S5
    S6 append on dev: ids 61-65 US          1 file (spec 1), main unchanged
       expire_snapshots(S1, S2)
"""
from dataclasses import dataclass

ROWS_SQL = """
SELECT id,
       CASE WHEN id <= 10 OR id BETWEEN 31 AND 40 OR id BETWEEN 51 AND 60 THEN 'EU'
            WHEN id <= 20 OR id BETWEEN 41 AND 50 OR id >= 61 THEN 'US'
            ELSE 'APAC' END AS region,
       CAST(id * {factor} AS DOUBLE) AS amount,
       date_add(DATE '{start}', CAST(id % 28 AS INT)) AS order_date
FROM range({lo}, {hi})
"""

MOR = ("'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
       "'write.merge.mode'='merge-on-read'")


@dataclass(frozen=True)
class History:
    table: str
    s1: int
    s2: int
    s3: int
    s4: int
    s5: int
    s6: int

    @property
    def expired(self) -> list[int]:
        return [self.s1, self.s2]

    @property
    def main_lineage(self) -> list[int]:
        """Retained snapshots on main, oldest first."""
        return [self.s3, self.s4, self.s5]


def rows(lo: int, hi: int, factor: float = 10.0, start: str = "2025-01-01") -> str:
    return ROWS_SQL.format(lo=lo, hi=hi + 1, factor=factor, start=start)


def current_snapshot(spark, table: str, ref: str = "main") -> int:
    return spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{ref}'").first().snapshot_id


def build_history(spark, table: str, format_version: int = 2) -> History:
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"""
        CREATE TABLE {table} (id BIGINT, region STRING, amount DOUBLE, order_date DATE)
        USING iceberg PARTITIONED BY (region)
        TBLPROPERTIES ('format-version'='{format_version}', {MOR})
    """)
    ids = []
    spark.sql(f"INSERT INTO {table} {rows(1, 30)}")
    ids.append(current_snapshot(spark, table))
    spark.sql(f"INSERT INTO {table} {rows(31, 50)}")
    ids.append(current_snapshot(spark, table))
    spark.sql(f"DELETE FROM {table} WHERE id IN (1, 2, 31)")
    ids.append(current_snapshot(spark, table))
    # Replace the APAC partition only (dynamic partition overwrite).
    spark.sql(rows(21, 30, factor=20.0)).writeTo(table).overwritePartitions()
    ids.append(current_snapshot(spark, table))
    spark.sql(f"ALTER TABLE {table} CREATE TAG audit_s4")
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD months(order_date)")
    spark.sql(f"INSERT INTO {table} {rows(51, 60, start='2025-02-01')}")
    # order_date = Feb 1 + (id % 28) days: always February 2025, one spec-1 partition.
    ids.append(current_snapshot(spark, table))
    spark.sql(f"ALTER TABLE {table} CREATE BRANCH dev")
    spark.sql(f"INSERT INTO {table}.branch_dev {rows(61, 65, start='2025-02-01')}")
    ids.append(current_snapshot(spark, table, "dev"))
    s1, s2 = ids[0], ids[1]
    spark.sql(f"CALL lake.system.expire_snapshots(table => '{table}', snapshot_ids => array({s1}, {s2}))")
    return History(table, *ids)
