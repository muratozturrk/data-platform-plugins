"""The three Polaris servers of the deployment stack and a helper to restart them."""
from __future__ import annotations

import os
import time
from dataclasses import dataclass

import docker
import requests


@dataclass(frozen=True)
class Server:
    service: str
    root: tuple[str, str]

    @property
    def base(self) -> str:
        return f"http://{self.service}:8181"

    @property
    def mgmt(self) -> str:
        return f"{self.base}/api/management/v1"

    @property
    def catalog_api(self) -> str:
        return f"{self.base}/api/catalog"

    def token(self, client_id: str | None = None, secret: str | None = None, scope: str = "PRINCIPAL_ROLE:ALL") -> str:
        r = requests.post(f"{self.catalog_api}/v1/oauth/tokens", timeout=30, data={
            "grant_type": "client_credentials", "client_id": client_id or self.root[0],
            "client_secret": secret or self.root[1], "scope": scope})
        r.raise_for_status()
        return r.json()["access_token"]

    def headers(self, bearer: str | None = None) -> dict:
        return {"Authorization": f"Bearer {bearer or self.token()}", "Content-Type": "application/json",
                "Accept": "application/json"}

    def ready(self) -> bool:
        try:
            return requests.get(f"http://{self.service}:8182/q/health/ready", timeout=5).status_code == 200
        except requests.RequestException:
            return False


QUICKSTART = Server("polaris-quickstart", ("root_user", "my_secret_id"))        # quickstart image, FILE storage enabled
MEM = Server("polaris-mem", ("root", "s3cr3t"))                    # 1.8.0 defaults, in-memory
DURABLE = Server("polaris-durable", ("root", "s3cr3t"))            # 1.8.0 + PostgreSQL


def restart(server: Server, timeout_s: int = 180) -> float:
    """Restart the server's container through the Docker API; return seconds until ready again."""
    client = docker.from_env()
    project = os.getenv("COMPOSE_PROJECT", "polaris-deploy")
    [container] = client.containers.list(filters={"label": [f"com.docker.compose.project={project}",
                                                            f"com.docker.compose.service={server.service}"]})
    t0 = time.time()
    container.restart(timeout=20)
    while time.time() - t0 < timeout_s:
        if server.ready():
            return round(time.time() - t0, 1)
        time.sleep(2)
    raise TimeoutError(f"{server.service} not ready after {timeout_s}s")


def image_of(server: Server) -> str:
    client = docker.from_env()
    project = os.getenv("COMPOSE_PROJECT", "polaris-deploy")
    [container] = client.containers.list(filters={"label": [f"com.docker.compose.project={project}",
                                                            f"com.docker.compose.service={server.service}"]})
    return container.attrs["Config"]["Image"]
