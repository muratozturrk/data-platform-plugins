"""Every lakehouse role is up and answers its protocol."""
from common.dremio import Dremio
from common.lakehouse import S3_ENDPOINT
from environment_health_check.environment import ROLES, check_environment, resolve


def test_every_role_answers_its_api(spark):
    Dremio().wait_until_ready()                     # Dremio is the slowest service to boot
    report = check_environment(spark)
    failed = {host: detail for host, (ok, detail) in report.items() if not ok}
    assert not failed, failed
    assert report["postgres"][1] == "3000 rows in fashion_sales"
    assert report["nessie"][1] == "default branch main, API v2"
    assert report["spark-iceberg"][1].startswith("Spark 3.5")
    assert "OK" in report["dremio"][1]


def test_services_are_reached_by_container_name_not_ip():
    """Some setups look up the S3 store's container IP because Docker DNS failed there. On a user-defined
    compose network every service name resolves, so the lab configures hostnames only."""
    assert "seaweedfs" in S3_ENDPOINT
    addresses = {r.host: resolve(r.host) for r in ROLES}
    assert all(ip.count(".") == 3 for ip in addresses.values())
    assert len(set(addresses.values())) == len(ROLES)          # one address per container
