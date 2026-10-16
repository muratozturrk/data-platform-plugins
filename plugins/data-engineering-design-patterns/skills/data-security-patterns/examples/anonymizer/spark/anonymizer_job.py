"""Anonymizer - PySpark: removal, perturbation and synthetic replacement of PII.

Three techniques, all irreversible (the consumer can't re-identify the user):
  1. data removal         : drop `birthday` and `personal_id`
  2. data perturbation    : add noise - IP keeps the shape but loses meaning,
                            age is shifted by random +/- 3 years
  3. synthetic replacement: Faker generates realistic-looking but fake emails/names
Information loss is the price: anonymized columns are useless for joins,
models or exact insights - agree on the column list with consumers.

Run:
  pip install pyspark pandas pyarrow faker
  spark-submit anonymizer_job.py  (env INPUT_PATH/OUTPUT_PATH optional; defaults use inline sample)
"""
import os
import random

import pandas
from faker import Faker
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

COLUMNS_TO_DROP = ["birthday", "personal_id"]


@pandas_udf(StringType())
def replace_email(emails: pandas.Series) -> pandas.Series:
    faker = Faker()
    return emails.apply(lambda _: faker.email())


@pandas_udf(StringType())
def replace_name(names: pandas.Series) -> pandas.Series:
    faker = Faker()
    return names.apply(lambda _: faker.name())


def perturb_ip(ip: str) -> str:
    """Insert a random digit in each octet: 123.45.67.89 -> 1823.475.6.789 (not a valid IP anymore)."""
    if ip is None:
        return None
    rng = random.SystemRandom()
    octets = []
    for octet in ip.split("."):
        pos = rng.randint(0, len(octet))
        octets.append(octet[:pos] + str(rng.randint(0, 9)) + octet[pos:])
    return ".".join(octets)


perturb_ip_udf = F.udf(perturb_ip, StringType())


def anonymize(users: DataFrame) -> DataFrame:
    return (users.drop(*COLUMNS_TO_DROP)
            .withColumn("email", replace_email("email"))
            .withColumn("full_name", replace_name("full_name"))
            .withColumn("ip", perturb_ip_udf("ip"))
            .withColumn("age", F.col("age") + (F.floor(F.rand() * 7) - 3).cast("int")))


def main() -> None:
    spark = SparkSession.builder.appName("anonymizer").getOrCreate()
    input_path = os.environ.get("INPUT_PATH")
    if input_path:
        users = spark.read.json(input_path)
    else:
        users = spark.createDataFrame([
            ("1", "Jane Doe", "jane@example.com", "1990-01-01", "123-45-678", "123.45.67.89", 34),
            ("2", "John Roe", "john@example.com", "1985-05-12", "987-65-432", "10.20.30.40", 39),
        ], "user_id STRING, full_name STRING, email STRING, birthday STRING, personal_id STRING, ip STRING, age INT")
    anonymized = anonymize(users)
    assert not set(COLUMNS_TO_DROP) & set(anonymized.columns), "PII columns leaked"
    output_path = os.environ.get("OUTPUT_PATH")
    if output_path:
        anonymized.write.mode("overwrite").json(output_path)
    else:
        anonymized.show(truncate=False)


if __name__ == "__main__":
    main()
