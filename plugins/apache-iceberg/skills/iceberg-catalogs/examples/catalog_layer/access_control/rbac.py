"""Catalog-based access control on Apache Polaris: RBAC + credential vending.

Model:
  principal (service identity) -> principal role -> catalog role -> privileges on catalog/namespace/table
The catalog checks every request against these grants and vends S3 credentials scoped to the
table location and to read or read/write, depending on the privilege.
"""
from __future__ import annotations

from dataclasses import dataclass

import boto3
import pyarrow as pa
from botocore.exceptions import ClientError

from common.iceberg_http import IcebergRest
from common.lakehouse import ROOT_CLIENT, S3_ENDPOINT, split_uri
from common.polaris import PolarisAdmin, pyiceberg_client

READER = ["NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_LIST", "TABLE_READ_PROPERTIES",
          "TABLE_READ_DATA"]
WRITER = READER + ["TABLE_CREATE", "TABLE_WRITE_PROPERTIES", "TABLE_WRITE_DATA", "TABLE_DROP"]


@dataclass
class Principal:
    name: str
    client_id: str
    client_secret: str

    def engine(self, catalog: str):
        return pyiceberg_client(catalog, self.client_id, self.client_secret)

    def rest(self, catalog: str, vended: bool = True) -> IcebergRest:
        return IcebergRest.polaris(catalog, self.client_id, self.client_secret, vended_credentials=vended)


@dataclass
class Domain:
    catalog: str
    engineer: Principal
    analyst: Principal
    roles: tuple[str, ...]


def provision(admin: PolarisAdmin, catalog: str, suffix: str) -> Domain:
    """Sales domain: engineers write sales.*, analysts read sales.*, nobody else reads hr.*."""
    admin.create_catalog(catalog)
    seed(catalog)                     # grants on a namespace need the namespace to exist
    engineer = Principal(f"etl_svc_{suffix}", *admin.create_principal(f"etl_svc_{suffix}"))
    analyst = Principal(f"bi_analyst_{suffix}", *admin.create_principal(f"bi_analyst_{suffix}"))

    for prole, crole, privileges, principal in (
            (f"data_engineer_{suffix}", "sales_writer", WRITER, engineer),
            (f"analyst_{suffix}", "sales_reader", READER, analyst)):
        admin.create_principal_role(prole)
        admin.assign_principal_role(principal.name, prole)
        admin.create_catalog_role(catalog, crole)
        for p in privileges:
            admin.grant(catalog, crole, {"type": "namespace", "namespace": ["sales"], "privilege": p})
        admin.assign_catalog_role(prole, catalog, crole)
    return Domain(catalog, engineer, analyst, (f"data_engineer_{suffix}", f"analyst_{suffix}"))


def seed(catalog: str) -> None:
    """Admin creates sales.orders and hr.salaries with a few rows (through vended credentials)."""
    admin_engine = pyiceberg_client(catalog, *ROOT_CLIENT)
    for ns, table, data in (
            ("sales", "orders", pa.table({"order_id": pa.array([1, 2, 3], pa.int64()),
                                          "amount": pa.array([10.0, 20.0, 30.0])})),
            ("hr", "salaries", pa.table({"employee_id": pa.array([1, 2], pa.int64()),
                                         "salary": pa.array([90000.0, 120000.0])}))):
        admin_engine.create_namespace(ns)
        admin_engine.create_table(f"{ns}.{table}", schema=data.schema).append(data)


def teardown(admin: PolarisAdmin, domain: Domain) -> None:
    admin.drop_catalog_cascade(domain.catalog)
    for p in (domain.engineer, domain.analyst):
        admin.drop_principal(p.name)
    for r in domain.roles:
        admin.drop_principal_role(r)


def vended_s3(load_result: dict):
    """boto3 client built ONLY from the credentials in a LoadTable response."""
    cfg = dict(load_result.get("config", {}))
    for cred in load_result.get("storage-credentials", []):
        cfg |= cred["config"]
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name="us-east-1",
                        aws_access_key_id=cfg["s3.access-key-id"],
                        aws_secret_access_key=cfg["s3.secret-access-key"],
                        aws_session_token=cfg["s3.session-token"]), cfg


def can(s3, op: str, uri: str) -> bool:
    bucket, key = split_uri(uri)
    try:
        if op == "read":
            s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        else:
            s3.put_object(Bucket=bucket, Key=key, Body=b"x")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("AccessDenied", "403"):
            return False
        raise
