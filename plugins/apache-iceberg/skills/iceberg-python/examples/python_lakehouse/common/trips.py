"""Synthetic NYC-taxi-style trips, used by every example in this environment."""
import random
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (DateType, DoubleType, IntegerType, LongType, NestedField,
                             TimestamptzType)

START = date(2023, 1, 1)

TRIPS_SCHEMA = Schema(
    NestedField(1, "trip_id", LongType(), required=False),
    NestedField(2, "vendor_id", IntegerType(), required=False),
    NestedField(3, "driver_id", IntegerType(), required=False),
    NestedField(4, "pickup_ts", TimestamptzType(), required=False),
    NestedField(5, "pickup_date", DateType(), required=False),
    NestedField(6, "passenger_count", IntegerType(), required=False),
    NestedField(7, "trip_distance", DoubleType(), required=False),
    NestedField(8, "total_amount", DoubleType(), required=False),
)

# Identity partition on pickup_date: the classic partition-overwrite example filters on it.
TRIPS_SPEC = PartitionSpec(PartitionField(source_id=5, field_id=1000, transform=IdentityTransform(),
                                          name="pickup_date"))

ARROW_SCHEMA = pa.schema([
    ("trip_id", pa.int64()), ("vendor_id", pa.int32()), ("driver_id", pa.int32()),
    # timestamptz in Iceberg: Arrow data must be tz-aware UTC.
    ("pickup_ts", pa.timestamp("us", tz="UTC")), ("pickup_date", pa.date32()),
    ("passenger_count", pa.int32()), ("trip_distance", pa.float64()), ("total_amount", pa.float64()),
])


def make_trips(n: int, days: int = 5, first_id: int = 1, seed: int = 7, start: date = START) -> pa.Table:
    rnd = random.Random(seed)
    rows = {f.name: [] for f in ARROW_SCHEMA}
    for i in range(n):
        d = start + timedelta(days=i % days)
        ts = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(seconds=rnd.randrange(86400))
        dist = round(rnd.uniform(0.3, 25.0), 2)
        rows["trip_id"].append(first_id + i)
        rows["vendor_id"].append(rnd.choice([1, 2]))
        rows["driver_id"].append(rnd.randint(1, 5))
        rows["pickup_ts"].append(ts)
        rows["pickup_date"].append(d)
        rows["passenger_count"].append(rnd.choice([1, 1, 1, 2, 2, 3, 4]))
        rows["trip_distance"].append(dist)
        rows["total_amount"].append(round(3.0 + dist * 4.5 + rnd.uniform(0, 15), 2))
    return pa.table(rows, schema=ARROW_SCHEMA)


def create_trips_table(catalog, identifier: str, rows: int = 5000, days: int = 5):
    """Partitioned trips table with one append per day (one data file per partition)."""
    if catalog.table_exists(identifier):
        catalog.purge_table(identifier)
    table = catalog.create_table(identifier, schema=TRIPS_SCHEMA, partition_spec=TRIPS_SPEC)
    if rows:
        table.append(make_trips(rows, days=days))
    return table
