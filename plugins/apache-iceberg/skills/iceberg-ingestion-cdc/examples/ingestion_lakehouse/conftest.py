import re
import uuid

import pytest

from common.kafka_io import create_topic, delete_topic
from common.lakehouse import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("lakehouse-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def ns(spark, request):
    """One isolated namespace per test module; tables are purged afterwards."""
    name = re.sub(r"\W", "_", request.module.__name__.split(".")[-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")
    yield name
    for row in spark.sql(f"SHOW TABLES IN {name}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {name}.{row.tableName} PURGE")
    spark.sql(f"DROP NAMESPACE IF EXISTS {name}")


@pytest.fixture
def topic(request):
    """A fresh 4-partition Kafka topic per test."""
    name = f"{re.sub(r'[^A-Za-z0-9]', '-', request.node.name)[:60]}-{uuid.uuid4().hex[:6]}"
    create_topic(name, partitions=4)
    yield name
    delete_topic(name)
