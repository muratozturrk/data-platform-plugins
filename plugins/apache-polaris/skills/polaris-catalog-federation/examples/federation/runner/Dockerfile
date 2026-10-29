# Test runner: PyIceberg + PyArrow against Polaris and three external Iceberg REST catalogs.
# s3fs is needed because Lakekeeper answers with remote signing (S3V4RestSigner), which PyIceberg
# only supports through fsspec. No Spark needed.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.3 boto3==1.41.5 s3fs==2025.3.0 \
    "pyiceberg[pyarrow,sql-sqlite]==0.9.1" fastavro==1.10.0 pyyaml==6.0.2 numpy==2.2.6 SQLAlchemy==2.0.36
WORKDIR /labs
CMD ["sleep", "infinity"]
