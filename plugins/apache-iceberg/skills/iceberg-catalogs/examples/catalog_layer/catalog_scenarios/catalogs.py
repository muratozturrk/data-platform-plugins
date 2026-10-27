"""Catalog capability facts as documented for each catalog option (a point-in-time snapshot).

Only capabilities documented for a catalog are set to True. This is a snapshot, not a current
product matrix: re-check vendors before a real decision.
"""

TRUE_KEYS = {
    # built into Iceberg, no service, metadata next to data, deprecated
    "hadoop": {"oss", "self_managed", "no_service", "python", "cloud_agnostic", "low_cost"},
    # reuses an existing Hive Metastore; no REST, limited RBAC; historical option
    "hive": {"oss", "self_managed", "hive_compat", "multi_engine", "cloud_agnostic"},
    # any JDBC database; ACID pointer updates; client-side catalog
    "jdbc": {"oss", "self_managed", "multi_engine", "cloud_agnostic", "python", "low_cost"},
    # REST, service principals + RBAC, credential vending, external catalogs (federation)
    "polaris": {"oss", "self_managed", "rest", "rbac", "credential_vending", "multi_engine",
                "cloud_agnostic", "federation", "multi_tenant"},
    # Git-like branches/tags, multi-table commits, REST, can vend credentials
    "nessie": {"oss", "self_managed", "rest", "versioning", "multi_table_transactions",
               "credential_vending", "multi_engine", "cloud_agnostic"},
    # federated, geodistributed metadata lake; tabular + files + AI models + topics
    "gravitino": {"oss", "self_managed", "rest", "rbac", "audit", "federation", "geo_distributed",
                  "multi_asset", "multi_engine", "cloud_agnostic"},
    # Rust, OpenFGA authz, credential vending, soft delete, change-approval hooks
    "lakekeeper": {"oss", "self_managed", "rest", "rbac", "credential_vending", "audit",
                   "multi_engine", "cloud_agnostic", "data_contracts", "python", "multi_tenant"},
    # managed, IAM + Lake Formation, CloudTrail, one catalog per account and region
    "glue": {"managed", "rest", "rbac", "audit", "aws_native", "multi_engine", "low_ops"},
    # Polaris-based, RBAC + row/column policies, lineage, automated maintenance,
    #        Dremio engine federates queries across legacy sources
    "dremio": {"managed", "rest", "rbac", "fine_grained", "audit", "credential_vending",
               "multi_engine", "cloud_agnostic", "query_federation", "auto_maintenance", "low_ops"},
    # Polaris-based managed service, RBAC, credential vending, billed per request
    "snowflake_open_catalog": {"managed", "rest", "rbac", "credential_vending", "multi_engine",
                               "cloud_agnostic", "low_ops"},
    # Databricks governance for Delta and Iceberg
    "unity": {"managed", "rest", "rbac", "audit", "multi_engine", "multi_format", "low_ops"},
}

STATUS = {"hadoop": "deprecated", "hive": "legacy"}


def catalog_facts() -> dict[str, dict]:
    """{catalog: {capability: True, ..., "status": ...}} (built-in, open-source services, managed services)."""
    return {name: {**{k: True for k in keys}, **({"status": STATUS[name]} if name in STATUS else {})}
            for name, keys in TRUE_KEYS.items()}
