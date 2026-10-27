"""Connect Dremio to the Nessie catalog and query the Spark-written Iceberg table.

`connect_dremio` does the "Add Source > Nessie" form (general, storage, connection properties) as one API call.
"""
import os

from common.dremio import Dremio

SOURCE = "nessie"

# Add Source > Nessie: General settings, Storage settings, connection properties.
NESSIE_SOURCE_CONFIG = {
    "nessieEndpoint": "http://nessie:19120/api/v2",
    "nessieAuthType": "NONE",
    "storageProvider": "AWS",
    "awsRootPath": "warehouse",
    "credentialType": "ACCESS_KEY",
    "awsAccessKey": os.getenv("AWS_ACCESS_KEY_ID", "admin"),             # local dev default
    "awsAccessSecret": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),   # local dev default
    "secure": False,                                       # "Encrypt connection" unchecked
    "propertyList": [
        {"name": "fs.s3a.path.style.access", "value": "true"},
        {"name": "fs.s3a.endpoint", "value": "seaweedfs:8333"},   # MinIO setups: minio:9000
        {"name": "dremio.s3.compat", "value": "true"},
    ],
}


def connect_dremio() -> Dremio:
    """First-run setup: wait for boot, create the admin user, log in, add the Nessie source."""
    dremio = Dremio()
    dremio.wait_until_ready()
    dremio.bootstrap_admin()
    dremio.login()
    dremio.create_source(SOURCE, "NESSIE", NESSIE_SOURCE_CONFIG)
    return dremio


def snapshots(dremio: Dremio, table: str) -> list[dict]:
    """SELECT * FROM TABLE(table_snapshot('nessie.sales.fashion_sales'))."""
    return dremio.sql(f"SELECT * FROM TABLE(table_snapshot('{table}')) ORDER BY committed_at")


def count_at_snapshot(dremio: Dremio, table: str, snapshot_id: int) -> int:
    """SELECT * FROM <table> AT SNAPSHOT '<id>'. Dremio expects the id as a string literal."""
    return dremio.sql(f"SELECT count(*) AS n FROM {table} AT SNAPSHOT '{snapshot_id}'")[0]["n"]


def data_files_scanned(dremio: Dremio, sql: str) -> int:
    """How many data files Dremio actually opened for a query, read from the job profile.

    In the plan, the manifest-scan operator (SPLIT_GEN_MANIFEST_SCAN) evaluates the filter against each
    file's min/max stats and emits one split per surviving file; DATA_FILE_SCAN reads only those.
    The manifest scan's record count in the profile is therefore the number of files left after pruning.
    """
    job_id = dremio.submit(sql)
    dremio.wait_job(job_id)
    profile = dremio.session.get(f"{dremio.url}/apiv2/profiles/{job_id}.json").json()
    scan_line = next(line for line in profile["plan"].splitlines() if "Type=[SPLIT_GEN_MANIFEST_SCAN]" in line)
    major, op_id = (int(x) for x in scan_line.split()[0].split("-"))
    return sum(i.get("records", 0)
               for fragment in profile["fragmentProfile"] if fragment["majorFragmentId"] == major
               for minor in fragment["minorFragmentProfile"]
               for op in minor["operatorProfile"] if op["operatorId"] == op_id
               for i in op.get("inputProfile", []))
