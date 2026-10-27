"""Table health from Iceberg metadata tables.

Reads `partitions`, `files`, `manifests`, `snapshots` and `refs` and condenses them
into a TableHealth snapshot that a planner (planner.py) can turn into actions, the
way self-optimizing services (Apache Amoro, AWS Glue optimizers) decide what to do.
"""
from dataclasses import dataclass, field
from datetime import date, datetime

from pyspark.sql import SparkSession

MiB = 1024 * 1024


@dataclass(frozen=True)
class PartitionHealth:
    day: date
    data_files: int
    small_files: int
    delete_files: int
    data_bytes: int


@dataclass(frozen=True)
class TableHealth:
    table: str
    ts_column: str
    data_files: int
    small_files: int
    delete_files: int
    manifests: int
    avg_files_per_manifest: float
    snapshots: int
    oldest_snapshot_at: datetime | None
    tag_snapshot_at: dict[str, datetime] = field(default_factory=dict)
    partitions: list[PartitionHealth] = field(default_factory=list)

    @property
    def small_file_ratio(self) -> float:
        return self.small_files / self.data_files if self.data_files else 0.0

    def as_metrics(self) -> dict[str, float]:
        """Flat gauges for a metrics backend (Prometheus, CloudWatch, Datadog)."""
        return {
            "iceberg_data_files": self.data_files,
            "iceberg_small_file_ratio": round(self.small_file_ratio, 4),
            "iceberg_delete_files": self.delete_files,
            "iceberg_manifests": self.manifests,
            "iceberg_avg_files_per_manifest": round(self.avg_files_per_manifest, 2),
            "iceberg_snapshots": self.snapshots,
        }


def collect_health(spark: SparkSession, table: str, ts_column: str = "event_ts",
                   small_file_bytes: int = 32 * MiB) -> TableHealth:
    """Assumes a daily partition transform on ts_column (`days(ts_column)`)."""
    part_field = f"{ts_column}_day"
    partitions = [
        PartitionHealth(r.day, r.data_files, r.small_files, r.delete_files, r.data_bytes)
        for r in spark.sql(f"""
            SELECT partition.{part_field} AS day,
                   count_if(content = 0) AS data_files,
                   count_if(content = 0 AND file_size_in_bytes < {small_file_bytes}) AS small_files,
                   count_if(content > 0) AS delete_files,
                   coalesce(sum(IF(content = 0, file_size_in_bytes, 0)), 0) AS data_bytes
            FROM {table}.files
            GROUP BY partition.{part_field}
            ORDER BY day
        """).collect()
    ]
    manifests = spark.sql(f"""
        SELECT count(*) AS n,
               coalesce(sum(added_data_files_count + existing_data_files_count), 0) AS tracked
        FROM {table}.manifests WHERE content = 0
    """).first()
    snaps = spark.sql(f"SELECT count(*) AS n, min(committed_at) AS oldest FROM {table}.snapshots").first()
    tag_times = {
        r.name: r.committed_at for r in spark.sql(f"""
            SELECT r.name, s.committed_at
            FROM {table}.refs r JOIN {table}.snapshots s ON r.snapshot_id = s.snapshot_id
            WHERE r.type = 'TAG'
        """).collect()
    }
    return TableHealth(
        table=table,
        ts_column=ts_column,
        data_files=sum(p.data_files for p in partitions),
        small_files=sum(p.small_files for p in partitions),
        delete_files=sum(p.delete_files for p in partitions),
        manifests=manifests.n,
        avg_files_per_manifest=manifests.tracked / manifests.n if manifests.n else 0.0,
        snapshots=snaps.n,
        oldest_snapshot_at=snaps.oldest,
        tag_snapshot_at=tag_times,
        partitions=partitions,
    )
