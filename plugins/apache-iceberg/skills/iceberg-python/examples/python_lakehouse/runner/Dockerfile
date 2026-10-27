# Pure-Python lakehouse client: no JVM, no Spark. Every engine runs in-process.
# pyiceberg[s3fs] backtracks for a long time; [pyarrow] uses PyArrow's native S3 filesystem.
# pyarrow 19.0.1 is the newest version PyIceberg 0.9.1 accepts (pyarrow<20).
# pyiceberg-core (Rust) is required to WRITE bucket/truncate-partitioned tables in 0.9.1.
# DuckDB extensions come from PyPI wheels (duckdb-extension-*), so the image never needs the
# DuckDB extension repository. Wheels are version-locked: duckdb must be 1.5.5 to match them.
# spicepy 1.0.5 is installed on purpose: the PyPI name belongs to a circuit simulator, not the
# Spice.ai SDK (platform_sdks/test_spice.py asserts this). The labs talk to Spice with pyarrow.flight instead.
# bauplan 0.3.4 is installed only to check the documented method names; it needs a cloud API key.
FROM python:3.11-slim
RUN pip install --no-cache-dir \
        "pyiceberg[pyarrow,sql-sqlite,pyiceberg-core]==0.9.1" pyiceberg-core==0.4.0 pyarrow==19.0.1 pandas==2.3.3 \
        polars==2.0.0 daft==0.7.26 \
        duckdb==1.5.5 duckdb-extensions==1.5.5 duckdb-extension-iceberg==1.5.5 \
        duckdb-extension-httpfs==1.5.5 duckdb-extension-avro==1.5.5 \
        spicepy==1.0.5 bauplan==0.3.4 \
        pytest==8.3.4 requests==2.32.5
# Install the extension binaries into ~/.duckdb once; tests then only need LOAD.
RUN python -c "from duckdb_extensions import import_extension as i; [i(e) for e in ('avro', 'httpfs', 'iceberg')]"
WORKDIR /labs
