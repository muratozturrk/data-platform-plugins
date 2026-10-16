"""Pseudo-Anonymizer - PySpark: generalization, masking, range bucketing and keyed hashing.

Keeps business meaning while hiding raw PII:
  * country -> geographical area (generalization)            via mapInPandas
  * ssn     -> 0***-0***-1*** (masking: collisions are possible) via mapInPandas
  * salary  -> "50000-60000" range (type change int -> string) via a column expression
  * email   -> HMAC-SHA256(secret, email) in a pandas UDF: stable join key, irreversible
              without the secret. Plain sha2() of a low-entropy value can be brute-forced, and
              sha2(concat(lit(secret), email)) would print the secret in the Spark plan/UI.
WARNING: pseudo-anonymized data can be re-identified when joined with other
datasets (country + role + ...). Treat it as personal data.

Run:
  export PSEUDO_HMAC_KEY=<random secret from your secrets manager>
  spark-submit pseudo_anonymizer_job.py
  python pseudo_anonymizer_job.py --self-test   # pure-function checks, no Spark needed
"""
import hashlib
import hmac
import os
import sys
from typing import Iterator

import pandas

COUNTRY_AREAS = {"Poland": "eu", "France": "eu", "Spain": "eu", "the USA": "na"}


def country_area(country: str) -> str:
    return COUNTRY_AREAS.get(country, "other")  # unknown -> generic bucket, never the raw value


def mask_ssn(ssn: str) -> str:
    return f"{ssn[0]}***-{ssn[5]}***-{ssn[10]}***" if ssn else ssn


def keyed_hash(value: str, key: bytes) -> str:
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest() if value is not None else None


def pseudo_anonymize_users(batches: Iterator[pandas.DataFrame]) -> Iterator[pandas.DataFrame]:
    for rows in batches:
        rows["country"] = rows["country"].apply(country_area)
        rows["ssn"] = rows["ssn"].apply(mask_ssn)
        yield rows


def hmac_udf(key: bytes):
    """The key travels inside the pickled UDF closure, not as a literal in the query plan."""
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("string")
    def hmac_column(values: pandas.Series) -> pandas.Series:
        return values.apply(lambda v: keyed_hash(v, key))

    return hmac_column


def main() -> None:
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.appName("pseudo-anonymizer").getOrCreate()
    hmac_key = os.environ["PSEUDO_HMAC_KEY"].encode("utf-8")
    users = spark.createDataFrame([
        (1, "Poland", "0940-0000-1000", 50000, "a@example.com"),
        (2, "France", "0469-0930-1000", 60000, "b@example.com"),
        (3, "the USA", "1230-0000-3940", 80000, "c@example.com"),
        (4, "Spain", "8502-1095-9303", 52000, "d@example.com"),
    ], "user_id INT, country STRING, ssn STRING, salary INT, email STRING")

    pseudo = (users.mapInPandas(pseudo_anonymize_users, users.schema)
              .withColumn("salary", F.expr("""
                  CASE WHEN salary < 50000 THEN '0-50000'
                       WHEN salary < 60000 THEN '50000-60000'
                       ELSE '60000+' END"""))
              .withColumn("email", hmac_udf(hmac_key)("email")))
    pseudo.show(truncate=False)


def _self_check() -> None:
    assert country_area("Poland") == "eu" and country_area("Narnia") == "other"
    # e.g. 0940-0000-1000 and 0469-0930-1000 collide -> information loss
    assert mask_ssn("0940-0000-1000") == mask_ssn("0469-0930-1000") == "0***-0***-1***"
    a, b = keyed_hash("a@example.com", b"k1"), keyed_hash("a@example.com", b"k2")
    assert a == keyed_hash("a@example.com", b"k1") and a != b and len(a) == 64
    print("self-check ok")


if __name__ == "__main__":
    _self_check() if "--self-test" in sys.argv else main()
