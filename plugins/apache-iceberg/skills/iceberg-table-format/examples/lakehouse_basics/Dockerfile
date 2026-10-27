# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus test tooling.
# The image already ships pyiceberg 0.7.1, pyarrow 17.0.0 and duckdb 1.2.1.
# No WORKDIR here: it breaks the image's entrypoint. Use `docker compose exec -w`.
FROM tabulario/spark-iceberg:latest
RUN pip install --no-cache-dir pytest==8.3.4 boto3==1.35.99
