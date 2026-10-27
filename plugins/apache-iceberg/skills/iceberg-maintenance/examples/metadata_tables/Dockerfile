# Spark 3.5.5 + Iceberg 1.8.1 runtime (tabulario/spark-iceberg) plus test tooling.
# PyIceberg is the second client used to cross-check the metadata tables (querying/test_querying.py).
FROM tabulario/spark-iceberg:latest
RUN pip install --no-cache-dir pytest==8.3.4 "pyiceberg[pyarrow]==0.9.1"
