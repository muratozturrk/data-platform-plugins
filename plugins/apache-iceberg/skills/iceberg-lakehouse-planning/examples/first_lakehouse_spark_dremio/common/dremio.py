"""Minimal Dremio client: the UI steps (first admin user, add catalog source, SQL editor)
done through Dremio's REST API, plus Arrow Flight queries (the protocol Superset uses).

API reference: https://docs.dremio.com/current/reference/api/ (catalog, sql, job endpoints).
"""
import os
import time

import requests
from pyarrow import flight

DREMIO_URL = os.getenv("DREMIO_URL", "http://dremio:9047")
DREMIO_FLIGHT = os.getenv("DREMIO_FLIGHT", "grpc+tcp://dremio:32010")
ADMIN_USER = os.getenv("DREMIO_USER", "admin")
ADMIN_PASSWORD = os.getenv("DREMIO_PASSWORD", "password1")   # local dev default; Dremio requires letters and digits


class DremioError(RuntimeError):
    pass


class Dremio:
    def __init__(self, url: str = DREMIO_URL):
        self.url = url.rstrip("/")
        self.session = requests.Session()

    # -- setup ---------------------------------------------------------------------------------
    def wait_until_ready(self, timeout_s: int = 300) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                if self.session.get(f"{self.url}/apiv2/server_status", timeout=5).ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(3)
        raise DremioError("Dremio did not become ready")

    def bootstrap_admin(self) -> None:
        """'Create an admin account' on first visit. Idempotent."""
        r = self.session.put(f"{self.url}/apiv2/bootstrap/firstuser",
                             headers={"Authorization": "_dremionull"},
                             json={"userName": ADMIN_USER, "firstName": "Lake", "lastName": "Admin",
                                   "email": "admin@example.com", "createdAt": int(time.time() * 1000),
                                   "password": ADMIN_PASSWORD})
        if not r.ok and "already" not in r.text.lower():
            raise DremioError(f"bootstrap failed: {r.status_code} {r.text}")

    def login(self, user: str = ADMIN_USER, password: str = ADMIN_PASSWORD) -> "Dremio":
        r = self.session.post(f"{self.url}/apiv2/login", json={"userName": user, "password": password})
        r.raise_for_status()
        self.session.headers["Authorization"] = f"_dremio{r.json()['token']}"
        return self

    def source(self, name: str) -> dict | None:
        r = self.session.get(f"{self.url}/api/v3/catalog/by-path/{name}")
        return r.json() if r.ok else None

    def create_source(self, name: str, source_type: str, config: dict) -> dict:
        """'Add Source'. Idempotent: an existing source is returned unchanged."""
        existing = self.source(name)
        if existing:
            return existing
        r = self.session.post(f"{self.url}/api/v3/catalog",
                              json={"entityType": "source", "name": name, "type": source_type,
                                    "config": config})
        if not r.ok:
            raise DremioError(f"create source failed: {r.status_code} {r.text}")
        return r.json()

    # -- SQL -----------------------------------------------------------------------------------
    def submit(self, sql: str) -> str:
        r = self.session.post(f"{self.url}/api/v3/sql", json={"sql": sql})
        if not r.ok:
            raise DremioError(f"{r.status_code} {r.text}")
        return r.json()["id"]

    def wait_job(self, job_id: str, timeout_s: int = 180) -> dict:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            job = self.session.get(f"{self.url}/api/v3/job/{job_id}").json()
            if job["jobState"] == "COMPLETED":
                return job
            if job["jobState"] in ("FAILED", "CANCELED"):
                raise DremioError(job.get("errorMessage", job["jobState"]))
            time.sleep(0.5)
        raise DremioError(f"job {job_id} timed out")

    def sql(self, sql: str) -> list[dict]:
        """Run SQL like the Dremio SQL editor and return all rows (REST pages of 500)."""
        job_id = self.submit(sql)
        job = self.wait_job(job_id)
        rows, offset = [], 0
        while offset < job.get("rowCount", 0):
            page = self.session.get(f"{self.url}/api/v3/job/{job_id}/results",
                                    params={"offset": offset, "limit": 500}).json()
            rows += page["rows"]
            offset += 500
        return rows

    def job(self, sql: str) -> dict:
        """Run SQL and return the finished job record (row count, state) instead of rows."""
        return self.wait_job(self.submit(sql))



def flight_query(sql: str, user: str = ADMIN_USER, password: str = ADMIN_PASSWORD,
                 location: str = DREMIO_FLIGHT):
    """Arrow Flight query, the transport behind Superset's `dremio+flight://` URI. Returns a pyarrow Table."""
    client = flight.FlightClient(location)
    token = client.authenticate_basic_token(user, password)
    options = flight.FlightCallOptions(headers=[token])
    info = client.get_flight_info(flight.FlightDescriptor.for_command(sql), options)
    return client.do_get(info.endpoints[0].ticket, options).read_all()
