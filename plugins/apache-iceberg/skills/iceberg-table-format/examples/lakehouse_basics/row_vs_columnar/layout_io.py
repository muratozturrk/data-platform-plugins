"""Row layout vs column layout: measure the bytes each one must read for OLTP and OLAP access.

Row layout   = CSV with a primary-key offset index (how a row store finds a record: index -> page -> row).
Column layout = Parquet (what warehouses and lakehouses store: values of one column kept together).
All numbers are measured by wrapping the file in a byte-counting reader.
"""
import csv
import io
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


class CountingReader(io.RawIOBase):
    """Read-only file wrapper that counts the bytes actually returned to the reader."""

    def __init__(self, path: str):
        self._f = open(path, "rb")
        self.bytes_read = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        return self._f.seek(offset, whence)

    def tell(self):
        return self._f.tell()

    def readinto(self, buffer):
        n = self._f.readinto(buffer)
        self.bytes_read += n or 0
        return n

    def read(self, size=-1):
        data = self._f.read(size)
        self.bytes_read += len(data)
        return data

    def close(self):
        self._f.close()
        super().close()


@dataclass
class Layouts:
    csv_path: str
    parquet_path: str
    csv_offsets: dict[int, int]       # order_id -> byte offset of its row (the "primary key index")


def write_layouts(table: pa.Table, directory: str, row_group_size: int = 64_000) -> Layouts:
    csv_path, parquet_path = f"{directory}/orders.csv", f"{directory}/orders.parquet"
    pacsv.write_csv(table, csv_path)
    pq.write_table(table, parquet_path, row_group_size=row_group_size, compression="snappy")

    offsets, pos = {}, 0
    with open(csv_path, "rb") as f:
        pos += len(f.readline())                       # header
        for line in f:
            offsets[int(line.split(b",", 1)[0])] = pos
            pos += len(line)
    return Layouts(csv_path, parquet_path, offsets)


def analytic_query(layouts: Layouts, fmt: str) -> tuple[dict[str, float], int]:
    """Revenue per country (touches 2 of 9 columns). Returns (result, bytes read)."""
    reader = CountingReader(layouts.csv_path if fmt == "csv" else layouts.parquet_path)
    with reader:
        if fmt == "csv":
            opts = pacsv.ConvertOptions(include_columns=["country", "amount"])
            t = pacsv.read_csv(reader, convert_options=opts)
        else:
            t = pq.read_table(reader, columns=["country", "amount"])
        agg = t.group_by("country").aggregate([("amount", "sum")])
        result = {c: round(v, 2) for c, v in zip(agg["country"].to_pylist(), agg["amount_sum"].to_pylist())}
        return result, reader.bytes_read


def point_lookup(layouts: Layouts, fmt: str, order_id: int) -> tuple[dict, int]:
    """Fetch one full record by primary key. Returns (record, bytes read)."""
    if fmt == "csv":
        reader = CountingReader(layouts.csv_path)
        with reader:
            reader.seek(layouts.csv_offsets[order_id])   # index lookup -> jump straight to the row
            data = b""
            while b"\n" not in data:
                data += reader.read(256)                 # read in small blocks, like DB pages
            fields = next(csv.reader([data.split(b"\n", 1)[0].decode()]))
            return {"order_id": int(fields[0]), "_columns": len(fields)}, reader.bytes_read
    reader = CountingReader(layouts.parquet_path)
    with reader:
        t = pq.read_table(reader, filters=[("order_id", "=", order_id)])
        rec = t.filter(pc.equal(t["order_id"], order_id)).to_pylist()[0]
        return {"order_id": rec["order_id"], "_columns": len(rec)}, reader.bytes_read
