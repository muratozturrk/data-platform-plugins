# spark_readiness_marker_db.py
#
# Database-backed Readiness Marker: writes and reads readiness signals
# via a PostgreSQL table. More universal than marker files — any tool
# that can query a database can check readiness without Hadoop/S3 access.
#
# Use when: multiple teams or tools need to coordinate on data readiness,
#           or when the pipeline runs in a cloud environment where
#           filesystem access is restricted.
#
# Requirements: pyspark, psycopg2-binary

import os
import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

log = logging.getLogger(__name__)

DB_CONN = dict(
    host="state-db", dbname="pipeline_state",
    user="pipeline",  password=os.environ["DB_PASSWORD"]
)

# DDL — run once:
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dataset_readiness (
    id              SERIAL PRIMARY KEY,
    dataset         TEXT        NOT NULL,
    partition_date  DATE        NOT NULL,
    batch_id        TEXT        NOT NULL,
    row_count       BIGINT      NOT NULL,
    schema_hash     TEXT        NOT NULL,
    delta_version   INT,
    extra           JSONB,
    written_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset, partition_date)   -- one marker per dataset per date
);
"""


@dataclass
class DBReadinessMarker:
    dataset:        str
    partition_date: str
    batch_id:       str
    row_count:      int
    schema_hash:    str
    delta_version:  Optional[int] = None
    extra:          Optional[dict] = None
    written_at:     Optional[str]  = None


class DBReadinessMarkerWriter:

    def __init__(self, spark: SparkSession, db_conn_kwargs: dict):
        self.spark  = spark
        self._db    = db_conn_kwargs
        self._ensure_table()

    def _ensure_table(self):
        with psycopg2.connect(**self._db) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
            conn.commit()

    def _schema_hash(self, df: DataFrame) -> str:
        import hashlib
        return hashlib.md5(",".join(sorted(df.columns)).encode()).hexdigest()[:8]

    def write_marker(
        self,
        df:             DataFrame,
        dataset:        str,
        partition_date: str,
        batch_id:       str,
        delta_version:  Optional[int] = None,
        extra:          Optional[dict] = None,
    ) -> DBReadinessMarker:
        """
        Upserts a readiness marker into the database.
        ON CONFLICT DO UPDATE makes this safe to call multiple times —
        re-running the upstream job simply updates the marker.
        """
        marker = DBReadinessMarker(
            dataset        = dataset,
            partition_date = partition_date,
            batch_id       = batch_id,
            row_count      = df.count(),
            schema_hash    = self._schema_hash(df),
            delta_version  = delta_version,
            extra          = extra or {},
            written_at     = datetime.now(tz=timezone.utc).isoformat(),
        )

        with psycopg2.connect(**self._db) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dataset_readiness
                        (dataset, partition_date, batch_id,
                         row_count, schema_hash, delta_version,
                         extra, written_at)
                    VALUES
                        (%(dataset)s, %(partition_date)s, %(batch_id)s,
                         %(row_count)s, %(schema_hash)s, %(delta_version)s,
                         %(extra)s, NOW())
                    ON CONFLICT (dataset, partition_date)
                    DO UPDATE SET
                        batch_id      = EXCLUDED.batch_id,
                        row_count     = EXCLUDED.row_count,
                        schema_hash   = EXCLUDED.schema_hash,
                        delta_version = EXCLUDED.delta_version,
                        extra         = EXCLUDED.extra,
                        written_at    = NOW()
                """, {
                    **asdict(marker),
                    "extra": json.dumps(marker.extra),
                })
            conn.commit()

        log.info(
            "DB marker written | dataset=%s | date=%s | rows=%d",
            dataset, partition_date, marker.row_count
        )
        return marker

    def read_marker(
        self,
        dataset:        str,
        partition_date: str,
    ) -> Optional[DBReadinessMarker]:
        """
        Returns the readiness marker for a dataset+date, or None if
        no marker exists yet (= upstream not finished).
        """
        with psycopg2.connect(**self._db) as conn:
            with conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.execute("""
                    SELECT dataset, partition_date::text, batch_id,
                           row_count, schema_hash, delta_version,
                           extra, written_at::text
                    FROM dataset_readiness
                    WHERE dataset = %s
                      AND partition_date = %s
                """, (dataset, partition_date))
                row = cur.fetchone()

        if row is None:
            return None

        return DBReadinessMarker(**dict(row))

    def delete_marker(self, dataset: str, partition_date: str):
        """Removes the marker — forces downstream to wait for re-ingestion."""
        with psycopg2.connect(**self._db) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM dataset_readiness
                    WHERE dataset = %s AND partition_date = %s
                """, (dataset, partition_date))
            conn.commit()
        log.info("DB marker deleted: dataset=%s date=%s", dataset, partition_date)

    def list_ready_datasets(self, partition_date: str) -> list[str]:
        """Returns all dataset names that are ready for a given date."""
        with psycopg2.connect(**self._db) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dataset FROM dataset_readiness
                    WHERE partition_date = %s
                    ORDER BY written_at
                """, (partition_date,))
                return [row[0] for row in cur.fetchall()]