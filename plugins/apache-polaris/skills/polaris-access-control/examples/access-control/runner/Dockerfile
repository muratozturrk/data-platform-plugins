# Lightweight test runner: these examples exercise the Polaris control plane (management + catalog
# REST APIs), so no Spark is needed. PyIceberg reads/writes data with vended credentials only.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 requests==2.32.3 boto3==1.35.99 \
    "pyiceberg[pyarrow]==0.9.1" pyyaml==6.0.2
WORKDIR /labs
CMD ["sleep", "infinity"]
