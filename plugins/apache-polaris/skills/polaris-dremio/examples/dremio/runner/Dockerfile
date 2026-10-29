# Test runner: Dremio REST API (setup + SQL), Polaris REST, PyIceberg reads, boto3 for landing files.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.3 boto3==1.35.99 "pyiceberg[pyarrow]==0.9.1"
WORKDIR /labs
CMD ["sleep", "infinity"]
