"""A plain Parquet "dataset" on object storage: a prefix full of files, no table format.

This is how many pipelines still land data (Kafka Connect S3 sink, Python batch jobs,
Hive-style external tables): the dataset is "whatever files are under the prefix".
"""
import io
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from common.lakehouse import list_objects, put_object, read_object


class WriterCrashed(RuntimeError):
    pass


def write_batch(prefix: str, table: pa.Table, n_files: int, crash_after: int | None = None) -> list[str]:
    """Write `table` as n_files Parquet objects, one PUT per file (like a sink task flushing).

    crash_after=k simulates a job that dies after k files: nothing removes the k files.
    """
    written = []
    chunk = -(-table.num_rows // n_files)
    for i in range(n_files):
        if crash_after is not None and i == crash_after:
            raise WriterCrashed(f"writer died after {i} of {n_files} files")
        buf = io.BytesIO()
        pq.write_table(table.slice(i * chunk, chunk), buf)
        uri = f"{prefix.rstrip('/')}/part-{uuid.uuid4().hex}.parquet"
        put_object(uri, buf.getvalue())
        written.append(uri)
    return written


def read_dataset(prefix: str) -> pa.Table:
    """What a directory-based reader does: LIST the prefix, read every Parquet file it finds."""
    uris = [u for u, _ in list_objects(prefix) if u.endswith(".parquet")]
    tables = [pq.read_table(io.BytesIO(read_object(u))) for u in uris]
    return pa.concat_tables(tables, promote_options="permissive") if tables else pa.table({})


def overwrite_dataset(prefix: str, table: pa.Table) -> None:
    """'Overwrite' without a table format = replace a fixed object name (last writer wins)."""
    buf = io.BytesIO()
    pq.write_table(table, buf)
    put_object(f"{prefix.rstrip('/')}/current.parquet", buf.getvalue())


def read_current(prefix: str) -> pa.Table:
    return pq.read_table(io.BytesIO(read_object(f"{prefix.rstrip('/')}/current.parquet")))
