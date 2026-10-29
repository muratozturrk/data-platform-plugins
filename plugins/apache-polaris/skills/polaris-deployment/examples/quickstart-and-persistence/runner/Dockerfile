# Test runner: plain HTTP (requests), PyIceberg for data, and the Docker SDK so tests can
# restart Polaris containers (in-memory vs durable persistence).
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.4 boto3==1.35.99 \
    "pyiceberg[pyarrow]==0.9.1" docker==7.1.0
WORKDIR /labs
CMD ["sleep", "infinity"]
