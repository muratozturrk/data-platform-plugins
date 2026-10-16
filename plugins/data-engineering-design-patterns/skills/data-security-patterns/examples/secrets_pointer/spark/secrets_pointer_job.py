"""Secrets Pointer - PySpark exporting PostgreSQL tables to JSON, credentials resolved by NAME
from AWS Secrets Manager at runtime.

The code, the config and Git only know the secret NAME (the pointer). The value is
fetched by the driver, kept in a small TTL cache, passed to the JDBC reader through
`properties` (never in the URL, which shows up in the Spark UI/plan) and never logged:
a logging filter redacts every resolved value.

Rotation handling ("cache invalidation" consequence): if the database
rejects the cached credentials (SQLSTATE 28P01/28000), the secret is invalidated,
fetched again and the read retried ONCE. A second failure fails the job; a restart
reloads the secret and the idempotent partition overwrite makes the replay safe.

Secret format: JSON {"username": ..., "password": ...} (RDS-managed format) or a plain
string (then --db-user / DB_USER gives the login). Access to the secret is itself
protected by IAM (Fine-Grained Accessor for Resources): grant secretsmanager:GetSecretValue
on this secret ARN only. The AWS credentials come from the workload identity (Secretless
Connector), so there's no bootstrap key either.

Run:
  pip install boto3 pyspark
  export DB_SECRET_ID=dp/postgres/devices-reader JDBC_URL=jdbc:postgresql://db:5432/dp AWS_REGION=eu-west-1
  spark-submit --packages org.postgresql:postgresql:42.7.3 secrets_pointer_job.py \
      --tables dp.devices dp.users --output-dir s3a://dp-export/postgres --export-date 2024-06-01
  python secrets_pointer_job.py --self-test     # cache/redaction/parsing checks, no Spark or AWS needed
"""
import argparse
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

LOGGER = logging.getLogger("secrets_pointer")
AUTH_FAILURE_MARKERS = ("28P01", "28000", "password authentication failed")


# --------------------------------------------------------------------------- redaction
class RedactingFilter(logging.Filter):
    """Replaces every registered secret value in log messages with '***'."""

    def __init__(self) -> None:
        super().__init__()
        self._values: set = set()

    def register(self, value: str) -> None:
        if value:
            self._values.add(value)

    def redact(self, text: str) -> str:
        for value in sorted(self._values, key=len, reverse=True):  # longest first: no partial leftovers
            text = text.replace(value, "***")
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redact(record.getMessage())
        record.args = ()
        return True


REDACTOR = RedactingFilter()


# --------------------------------------------------------------------------- secrets
@dataclass(frozen=True)
class DbCredentials:
    user: str
    password: str

    def __repr__(self) -> str:  # a stray print(credentials) must not leak the password
        return f"DbCredentials(user={self.user!r}, password='***')"


def parse_secret(secret_string: str, default_user: Optional[str]) -> DbCredentials:
    try:
        payload = json.loads(secret_string)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        user = payload.get("username") or default_user
        password = payload.get("password")
    else:
        user, password = default_user, secret_string
    if not user or not password:
        raise ValueError("secret must contain a password and a username (or pass --db-user)")
    REDACTOR.register(password)  # the raw JSON is registered by the cache; the bare password must be too
    return DbCredentials(user=user, password=password)


class TtlSecretCache:
    """Caches secret strings for `ttl_seconds`; `invalidate` forces the next get() to re-fetch."""

    def __init__(self, fetch: Callable[[str], str], ttl_seconds: float,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._fetch, self._ttl, self._clock = fetch, ttl_seconds, clock
        self._entries: Dict[str, tuple] = {}
        self._lock = threading.Lock()

    def get(self, secret_id: str) -> str:
        with self._lock:
            entry = self._entries.get(secret_id)
            if entry and self._clock() - entry[1] < self._ttl:
                return entry[0]
            value = self._fetch(secret_id)
            REDACTOR.register(value)
            self._entries[secret_id] = (value, self._clock())
            return value

    def invalidate(self, secret_id: str) -> None:
        with self._lock:
            self._entries.pop(secret_id, None)


def secrets_manager_fetcher(region: Optional[str]) -> Callable[[str], str]:
    import boto3
    from botocore.config import Config
    client = boto3.client("secretsmanager", region_name=region,
                          config=Config(retries={"max_attempts": 5, "mode": "standard"}))

    def fetch(secret_id: str) -> str:
        LOGGER.info("resolving secret %s", secret_id)  # the name only
        response = client.get_secret_value(SecretId=secret_id)
        if "SecretString" not in response:
            raise ValueError(f"secret {secret_id} is binary; store DB credentials as SecretString")
        return response["SecretString"]

    return fetch


def is_auth_failure(error: Exception) -> bool:
    message = str(error)
    return any(marker in message for marker in AUTH_FAILURE_MARKERS)


def with_credentials_refresh(action: Callable[[DbCredentials], object], cache: TtlSecretCache,
                             secret_id: str, default_user: Optional[str]):
    """Run `action` with cached credentials; on an auth failure refresh the secret and retry once."""
    credentials = parse_secret(cache.get(secret_id), default_user)
    try:
        return action(credentials)
    except Exception as error:  # Py4JJavaError wraps the PSQLException
        if not is_auth_failure(error):
            raise
        LOGGER.warning("authentication failed for %s, secret probably rotated: refreshing once", secret_id)
        cache.invalidate(secret_id)
        return action(parse_secret(cache.get(secret_id), default_user))


# --------------------------------------------------------------------------- job
def parse_args(argv) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--secret-id", default=os.environ.get("DB_SECRET_ID", "dp/postgres/devices-reader"),
                        help="secret NAME/ARN in Secrets Manager - never the value")
    parser.add_argument("--db-user", default=os.environ.get("DB_USER"), help="login if the secret is a plain string")
    parser.add_argument("--jdbc-url", default=os.environ.get("JDBC_URL", "jdbc:postgresql://localhost:5432/dp"))
    parser.add_argument("--tables", nargs="+", default=os.environ.get("TABLES", "dp.devices").split(","))
    parser.add_argument("--output-dir", default=os.environ.get("OUTPUT_DIR", "/tmp/dp/postgres_export"))
    parser.add_argument("--export-date", default=os.environ.get("EXPORT_DATE", time.strftime("%Y-%m-%d")))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--ttl-seconds", type=float, default=float(os.environ.get("SECRET_TTL_SECONDS", "900")))
    parser.add_argument("--fetch-size", default=os.environ.get("JDBC_FETCH_SIZE", "10000"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if "?" in args.jdbc_url and "password" in args.jdbc_url.lower():
        parser.error("credentials must not be part of the JDBC URL - use the secret pointer")
    return args


def export_table(spark, args: argparse.Namespace, table: str, credentials: DbCredentials) -> int:
    devices = spark.read.jdbc(
        url=args.jdbc_url,
        table=table,
        properties={"driver": "org.postgresql.Driver", "user": credentials.user,
                    "password": credentials.password, "fetchsize": str(args.fetch_size)},
    )
    target = f"{args.output_dir.rstrip('/')}/{table.replace('.', '_')}/export_date={args.export_date}"
    # Overwriting exactly this partition directory makes a retried/replayed run idempotent.
    devices.write.mode("overwrite").json(target)
    rows = spark.read.json(target).count()
    LOGGER.info("exported %d rows of %s to %s", rows, table, target)
    return rows


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for handler in logging.getLogger().handlers:
        handler.addFilter(REDACTOR)
    args = parse_args(argv)
    if args.self_test:
        _self_check()
        return

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("secrets-pointer-export").getOrCreate()
    # The default spark.redaction.regex ((?i)secret|password|token|access[.]key) hides the `password`
    # JDBC option in the UI/event log; a password inside the URL would NOT match it.
    cache = TtlSecretCache(secrets_manager_fetcher(args.region), args.ttl_seconds)
    failures = []
    for table in args.tables:
        try:
            with_credentials_refresh(lambda creds: export_table(spark, args, table, creds),
                                     cache, args.secret_id, args.db_user)
        except Exception as error:
            LOGGER.error("export of %s failed: %s", table, REDACTOR.redact(str(error))[:500])
            failures.append(table)
    if failures:
        raise SystemExit(f"failed tables: {failures}")


# --------------------------------------------------------------------------- self-check
def _self_check() -> None:
    now = [0.0]
    calls = []

    def fake_fetch(secret_id: str) -> str:
        calls.append(secret_id)
        return json.dumps({"username": "reader", "password": f"pw-{len(calls)}"})

    cache = TtlSecretCache(fake_fetch, ttl_seconds=60, clock=lambda: now[0])
    first = cache.get("db")
    assert cache.get("db") == first and len(calls) == 1, "cached within TTL"
    now[0] = 61
    assert cache.get("db") != first and len(calls) == 2, "re-fetched after TTL"

    assert parse_secret("plain-pw", "u") == DbCredentials("u", "plain-pw")
    assert "pw-" not in repr(parse_secret(first, None))
    parse_secret(cache.get("db"), None)  # pw-2, cached
    assert REDACTOR.redact("connect with pw-1 then pw-2") == "connect with *** then ***"

    attempts = []

    def flaky(creds: DbCredentials) -> str:
        attempts.append(creds.password)
        if len(attempts) == 1:
            raise RuntimeError("FATAL: password authentication failed for user (SQLSTATE 28P01)")
        return "ok"

    assert with_credentials_refresh(flaky, cache, "db", None) == "ok"
    assert attempts[0] != attempts[1] and len(calls) == 3, "auth failure must refresh the secret once"
    try:
        with_credentials_refresh(lambda c: (_ for _ in ()).throw(ValueError("boom")), cache, "db", None)
        raise AssertionError("non-auth errors must propagate")
    except ValueError:
        pass
    print("self-check ok")


if __name__ == "__main__":
    main(sys.argv[1:])
