"""Spice OSS runtime from Python over Arrow Flight (what the spicepy SDK wraps)."""
import os
import time

import requests
from pyarrow import flight

FLIGHT_URL = os.getenv("SPICE_FLIGHT_URL", "grpc://spice:50051")
HTTP_URL = os.getenv("SPICE_HTTP_URL", "http://spice:8090")


def wait_ready(datasets: set[str], timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            status = {d["name"]: d["status"] for d in
                      requests.get(f"{HTTP_URL}/v1/datasets?status=true", timeout=5).json()}
            if all(status.get(d) == "Ready" for d in datasets):
                return
        except (requests.RequestException, ValueError):
            pass
        time.sleep(2)
    raise TimeoutError(f"Spice datasets not ready: {datasets}")


def query(sql: str):
    """client.query(sql).read_pandas() in spicepy is this Flight round trip."""
    client = flight.FlightClient(FLIGHT_URL)
    info = client.get_flight_info(flight.FlightDescriptor.for_command(sql))
    return client.do_get(info.endpoints[0].ticket).read_all()
