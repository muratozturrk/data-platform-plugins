# Lightweight test runner: the federation engine (Trino) does all the query work.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest==8.3.4 trino==0.336.0 "psycopg[binary]==3.2.9" \
        requests==2.34.2 "pyiceberg[pyarrow]==0.9.1"
WORKDIR /labs
