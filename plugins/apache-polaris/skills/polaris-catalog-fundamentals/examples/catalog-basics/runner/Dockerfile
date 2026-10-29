# Lightweight test runner: PyIceberg + PyArrow against Polaris, fastavro to read Iceberg's Avro
# metadata files directly. No Spark needed here.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.3 boto3==1.35.99 \
    "pyiceberg[pyarrow,sql-sqlite]==0.9.1" fastavro==1.10.0 pyyaml==6.0.2 numpy==2.2.6 SQLAlchemy==2.0.36
WORKDIR /labs
CMD ["sleep", "infinity"]
