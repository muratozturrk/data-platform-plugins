"""PyIceberg as the control plane: every table operation against a REST catalog."""
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import And, EqualTo, GreaterThan, GreaterThanOrEqual, LessThan
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import StringType


def overwrite_day(table, day: date, rows: pa.Table) -> None:
    """Replace one partition atomically (the legacy `overwrite(where=...)` form, in the 0.9.1 API)."""
    table.overwrite(rows, overwrite_filter=EqualTo("pickup_date", day.isoformat()))


def delete_refunds(table, max_amount: float) -> None:
    """Row-level delete by predicate. PyIceberg 0.9.1 rewrites the affected files (copy-on-write)."""
    table.delete(delete_filter=LessThan("total_amount", max_amount))


def add_borough_column(table) -> None:
    with table.update_schema() as update:
        update.add_column("pickup_borough", StringType(), doc="Added by schema evolution")


def bucket_by_vendor(table, buckets: int = 4) -> None:
    """Partition evolution: new files are also bucketed by vendor_id; old files keep their spec."""
    with table.update_spec() as update:
        update.add_field("vendor_id", BucketTransform(buckets), "vendor_bucket")


def snapshot_log(table) -> list[tuple[int, str, int]]:
    """(snapshot_id, operation, total-records) for every snapshot, oldest first."""
    return [(s.snapshot_id, s.summary.operation.value, int(s.summary["total-records"]))
            for s in table.metadata.snapshots]


def planned_files(table, row_filter=None, snapshot_id=None) -> int:
    """How many data files the scan would open: PyIceberg's two-level pruning result."""
    scan = table.scan(row_filter=row_filter, snapshot_id=snapshot_id) if row_filter is not None \
        else table.scan(snapshot_id=snapshot_id)
    return len(list(scan.plan_files()))


def expensive_trips(table, threshold: float = 100.0) -> pa.Table:
    """Analytic scan: filter pushed into planning, projection of three columns."""
    return table.scan(row_filter=GreaterThan("total_amount", threshold),
                      selected_fields=("trip_id", "pickup_date", "total_amount")).to_arrow()


def day_range(start: date, end: date):
    return And(GreaterThanOrEqual("pickup_date", start.isoformat()), LessThan("pickup_date", end.isoformat()))


def total(arrow: pa.Table, column: str = "total_amount") -> float:
    return round(pc.sum(arrow[column]).as_py() or 0.0, 2)
