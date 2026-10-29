# Test runner: Polaris CLI (PyPI apache-polaris), REST calls, Docker SDK (logs, exit codes),
# cryptography (RSA keys, token signature check).
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.3 apache-polaris==1.8.0 docker==7.1.0 \
    cryptography==44.0.0 "pyiceberg[pyarrow]==0.9.1"
WORKDIR /labs
CMD ["sleep", "infinity"]
