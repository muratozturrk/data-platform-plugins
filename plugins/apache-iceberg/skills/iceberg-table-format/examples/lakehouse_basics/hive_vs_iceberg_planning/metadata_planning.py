"""Query planning in a Hive-style layout vs an Iceberg table, measured in object store requests.

Hive style: partitions are directories (`order_date=2025-01-10/`). To plan a query the engine LISTs
the partition directories, then LISTs each selected partition to find its files. Pruning works only
when the query filters on the partition column itself.

Iceberg: the catalog points to metadata.json; the engine reads the manifest list and the manifests
(GETs of a few Avro files) and gets every data file with its partition values and column stats.
No LIST at all, and filters on the source column (`order_ts`) prune through hidden partitioning.
"""
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import PyArrowFileIO

from common.lakehouse import s3_client, split_uri


class CountingFileIO(PyArrowFileIO):
    """PyIceberg FileIO that records every file it opens (configured via `py-io-impl`)."""
    opened: list[str] = []

    def new_input(self, location: str):
        CountingFileIO.opened.append(location)
        return super().new_input(location)


class ListCounter:
    """Counts S3 LIST requests issued by boto3 inside a `with` block."""

    def __init__(self, client):
        self.client, self.calls = client, 0

    def _count(self, **_):
        self.calls += 1

    def __enter__(self):
        self.client.meta.events.register("before-call.s3.ListObjectsV2", self._count)
        return self

    def __exit__(self, *exc):
        self.client.meta.events.unregister("before-call.s3.ListObjectsV2", self._count)


def write_hive_layout(table: pa.Table, prefix_uri: str) -> int:
    """Write one Parquet file per `order_date=YYYY-MM-DD/` directory. Returns the number of partitions."""
    bucket, prefix = split_uri(prefix_uri)
    days = pc.strftime(table["order_ts"], format="%Y-%m-%d")
    table = table.append_column("order_date", days)
    for day in sorted(set(days.to_pylist())):
        part = table.filter(pc.equal(table["order_date"], day)).drop(["order_date"])
        sink = pa.BufferOutputStream()
        pq.write_table(part, sink)
        s3_client().put_object(Bucket=bucket, Key=f"{prefix}/order_date={day}/part-00000.parquet",
                               Body=sink.getvalue().to_pybytes())
    return len(set(days.to_pylist()))


def hive_plan(prefix_uri: str, partition_days: set[date] | None) -> list[str]:
    """Find the files to scan the way a Hive-style engine does: list directories, then files.

    partition_days=None means the filter is not on the partition column, so nothing can be pruned.
    """
    bucket, prefix = split_uri(prefix_uri)
    client = s3_client()
    dirs = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/", Delimiter="/"):
        dirs += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
    if partition_days is not None:
        wanted = {f"order_date={d:%Y-%m-%d}/" for d in partition_days}
        dirs = [d for d in dirs if d.rsplit("/", 2)[-2] + "/" in wanted]
    files = []
    for d in dirs:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=d):
            files += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", [])]
    return files


def iceberg_plan(table, row_filter) -> list[str]:
    """Data files PyIceberg selects for a filter, using metadata only."""
    return [task.file.file_path for task in table.scan(row_filter=row_filter).plan_files()]
