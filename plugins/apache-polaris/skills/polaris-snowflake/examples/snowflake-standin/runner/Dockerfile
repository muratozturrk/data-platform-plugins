# Test runner: REST calls, PyIceberg reads with vended credentials, the Polaris CLI
# (PyPI package apache-polaris), Docker SDK to read server logs.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.3 boto3==1.43.1 \
    "pyiceberg[pyarrow]==0.9.1" apache-polaris==1.8.0 docker==7.1.0
WORKDIR /labs
CMD ["sleep", "infinity"]
