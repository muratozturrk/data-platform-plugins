# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus raw-file readers and test tooling.
# fastavro: manifest lists / manifests; pyroaring: deletion-vector bitmaps; zstandard: Puffin blob codec.
# No WORKDIR here: it breaks the base image entrypoint. Use `docker compose exec -w /home/iceberg/labs`.
FROM tabulario/spark-iceberg:latest
RUN pip install --no-cache-dir pytest==8.3.4 boto3==1.35.99 fastavro==1.10.0 pyroaring==1.0.0 \
      zstandard==0.23.0 "pyiceberg[pyarrow]==0.9.1"
