# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus Python readers and test tooling.
# The base image already ships pyarrow 17.0.0 and duckdb 1.2.1.
# No WORKDIR here: it breaks the base image entrypoint. Use `docker compose exec -w /home/iceberg/labs`.
FROM tabulario/spark-iceberg:latest
RUN pip install --no-cache-dir pytest==8.3.4 boto3==1.35.99 fastavro==1.10.0 "pyiceberg[pyarrow]==0.9.1"
