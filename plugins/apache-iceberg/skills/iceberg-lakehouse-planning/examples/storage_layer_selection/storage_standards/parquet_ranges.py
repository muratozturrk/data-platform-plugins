"""How a Parquet reader uses S3 range reads (footer, projection, row groups).

`S3RangeFile` is a seekable file object whose reads become HTTP range GETs. Handing it to
pyarrow shows the access pattern every engine follows on object storage: read the footer
from the end of the file, then fetch only the column chunks and row groups the query needs.
Iceberg's S3FileIO does the same through `S3InputStream` with `Range` headers.
"""
import io

import pyarrow as pa
import pyarrow.parquet as pq


class S3RangeFile(io.RawIOBase):
    def __init__(self, s3, bucket: str, key: str):
        self.s3, self.bucket, self.key = s3, bucket, key
        self.size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
        self.pos = 0
        self.requests: list[tuple[int, int]] = []          # (offset, length) of each range GET

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.pos, io.SEEK_END: self.size}[whence]
        self.pos = base + offset
        return self.pos

    def read(self, n: int = -1) -> bytes:
        end = self.size if n is None or n < 0 else min(self.size, self.pos + n)
        if end <= self.pos:
            return b""
        body = self.s3.get_object(Bucket=self.bucket, Key=self.key,
                                  Range=f"bytes={self.pos}-{end - 1}")["Body"].read()
        self.requests.append((self.pos, len(body)))
        self.pos += len(body)
        return body

    def readinto(self, b) -> int:
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)

    @property
    def bytes_read(self) -> int:
        return sum(n for _, n in self.requests)


def write_wide_table(s3, bucket: str, key: str, rows: int = 200_000, columns: int = 10,
                     row_group_rows: int = 20_000) -> int:
    """A sorted, wide Parquet file: `id` plus `columns - 1` double columns, 10 row groups."""
    import numpy as np
    rng = np.random.default_rng(42)
    data = {"id": pa.array(np.arange(rows, dtype="int64"))}
    for c in range(1, columns):
        data[f"m{c}"] = pa.array(rng.random(rows))
    buf = io.BytesIO()
    pq.write_table(pa.table(data), buf, row_group_size=row_group_rows, compression="snappy")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return buf.tell()
