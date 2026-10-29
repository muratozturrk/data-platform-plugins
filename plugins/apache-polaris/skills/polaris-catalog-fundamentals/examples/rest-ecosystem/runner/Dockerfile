# Test runner: Polaris REST, PyIceberg 0.9.1 (+ Polars and DuckDB on its Arrow output), Trino over HTTP.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.3 boto3==1.35.99 "pyiceberg[pyarrow]==0.9.1" \
    polars==1.31.0 duckdb==1.3.2
WORKDIR /labs
CMD ["sleep", "infinity"]
