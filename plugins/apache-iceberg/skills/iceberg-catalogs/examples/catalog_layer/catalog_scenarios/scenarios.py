"""Eight catalog-selection scenarios, encoded as requirement profiles."""
from dataclasses import dataclass

from requirements_probe.requirements import Requirement as R


@dataclass(frozen=True)
class Scenario:
    key: str
    title: str
    requirements: tuple[R, ...]
    expected_outcome: str
    rejected: tuple[str, ...] = ()     # options evaluated and turned down


SCENARIOS = [
    Scenario("hms_migration", "Mid-sized team migrating from Hive", (
        R("hive_compat", why="seamless compatibility with the existing HMS"),
        R("multi_engine", why="Iceberg tables across Spark and Flink"),
        R("self_managed", why="self-managed during the initial migration"),
        R("rest", must=False, why="path to a cloud-native, governed architecture"),
    ), "hive", ("hadoop",)),
    Scenario("aws_fintech", "Rapidly scaling cloud-native fintech startup", (
        R("managed"), R("aws_native"), R("multi_engine"), R("rbac"), R("audit"), R("rest"),
        R("cloud_agnostic", must=False, why="may expand beyond AWS later"),
    ), "glue", ("dremio", "snowflake_open_catalog")),
    Scenario("healthcare_residency", "Multinational healthcare enterprise, strict governance", (
        R("geo_distributed"), R("rbac"), R("audit"), R("multi_engine"), R("multi_asset"),
        R("rest", why="open APIs and extensibility"), R("federation"),
    ), "gravitino", ("lakekeeper", "nessie")),
    Scenario("saas_simplicity", "SaaS startup prioritizing operational simplicity", (
        R("managed"), R("aws_native", why="Athena, Redshift, Glue ETL"), R("low_ops"), R("rbac"),
    ), "glue", ("lakekeeper", "jdbc")),
    Scenario("multicloud_federation", "Large multicloud enterprise, federated governance", (
        R("federation"), R("geo_distributed", why="regions and clouds"), R("multi_engine"),
        R("multi_asset", why="multiple metadata types"), R("rbac"), R("rest"), R("oss"),
    ), "gravitino", ("nessie", "polaris")),
    Scenario("stress_test_clones", "Financial firm cloning production daily for stress tests", (
        R("versioning", why="zero-copy, branch-based clones"),
        R("multi_table_transactions", why="the whole environment, not one table"),
        R("multi_engine", why="existing Spark pipeline"),
    ), "nessie", ("glue", "polaris", "gravitino", "lakekeeper")),
    Scenario("legacy_query_federation", "Phased migration with query federation across legacy systems", (
        R("query_federation", why="Hive, Teradata, Oracle and Iceberg in one SQL interface"),
        R("rbac"), R("audit"), R("low_ops"), R("rest"),
    ), "dremio", ("hadoop", "jdbc", "polaris", "gravitino", "glue")),
    Scenario("python_first", "Lightweight Python-first lakehouse", (
        R("no_service", why="run from developer machines, no infrastructure"),
        R("python"), R("oss"), R("low_cost"),
        R("cloud_agnostic", must=False),
    ), "hadoop", ("polaris", "glue")),
]
