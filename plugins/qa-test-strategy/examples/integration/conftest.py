"""Fast-band integration fixtures with Testcontainers (Python). Session-scoped containers, per-test unique names."""
import json, os, time, uuid, pytest, httpx
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer
from testcontainers.kafka import KafkaContainer

@pytest.fixture(scope="session")
def net():
    with Network() as n:
        yield n

@pytest.fixture(scope="session")
def postgres(net):
    with PostgresContainer("quay.io/debezium/postgres:16", username="hkm", password="hkm", dbname="erp").with_network(net).with_network_aliases("postgres") as pg:
        yield pg

@pytest.fixture(scope="session")
def kafka(net):
    with KafkaContainer("confluentinc/cp-kafka:7.6.0").with_kraft().with_network(net).with_network_aliases("kafka") as k:
        yield k

@pytest.fixture(scope="session")
def karapace(net, kafka):
    c = (DockerContainer("ghcr.io/aiven-open/karapace:4.1.0").with_network(net).with_network_aliases("karapace")
         .with_env("KARAPACE_BOOTSTRAP_URI", "kafka:9092").with_env("KARAPACE_REGISTRY_HOST", "0.0.0.0").with_env("KARAPACE_REGISTRY_PORT", "8081")
         .with_exposed_ports(8081).with_command("python3 -m karapace.schema_registry_main"))
    with c:
        wait_for_logs(c, "Karapace", timeout=60)
        yield f"http://{c.get_container_host_ip()}:{c.get_exposed_port(8081)}"

@pytest.fixture(scope="session")
def debezium(net, kafka, postgres):
    c = (DockerContainer("quay.io/debezium/connect:3.1").with_network(net).with_network_aliases("connect")
         .with_env("BOOTSTRAP_SERVERS", "kafka:9092").with_env("GROUP_ID", "1")
         .with_env("CONFIG_STORAGE_TOPIC", "_connect_configs").with_env("OFFSET_STORAGE_TOPIC", "_connect_offsets").with_env("STATUS_STORAGE_TOPIC", "_connect_status")
         .with_exposed_ports(8083))
    with c:
        url = f"http://{c.get_container_host_ip()}:{c.get_exposed_port(8083)}"
        for _ in range(60):
            try:
                if httpx.get(f"{url}/connectors").status_code == 200: break
            except httpx.HTTPError: pass
            time.sleep(1)
        yield url

@pytest.fixture(scope="session")
def nessie(net):
    c = (DockerContainer("ghcr.io/projectnessie/nessie:0.108.3").with_network(net).with_network_aliases("nessie")
         .with_env("NESSIE_VERSION_STORE_TYPE", "IN_MEMORY").with_exposed_ports(19120))
    with c:
        wait_for_logs(c, "Listening on", timeout=60)
        yield f"http://{c.get_container_host_ip()}:{c.get_exposed_port(19120)}/api/v2"

@pytest.fixture(scope="session")
def keycloak(net):
    realm = os.path.join(os.path.dirname(__file__), "fixtures", "platform-realm.json")
    c = (DockerContainer("quay.io/keycloak/keycloak:26.2").with_network(net).with_network_aliases("keycloak")
         .with_env("KC_BOOTSTRAP_ADMIN_USERNAME", "admin").with_env("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin").with_env("KC_HEALTH_ENABLED", "true")
         .with_volume_mapping(realm, "/opt/keycloak/data/import/platform-realm.json").with_exposed_ports(8080).with_command("start-dev --import-realm"))
    with c:
        wait_for_logs(c, "Running the server", timeout=120)
        yield f"http://{c.get_container_host_ip()}:{c.get_exposed_port(8080)}"

@pytest.fixture(scope="session")
def trino(net, postgres, nessie):
    cfg = os.path.join(os.path.dirname(__file__), "fixtures", "trino-etc")   # catalogs: erp (postgresql), lake (iceberg/nessie), memory; file access control
    c = (DockerContainer("trinodb/trino:484").with_network(net).with_network_aliases("trino")
         .with_volume_mapping(cfg, "/etc/trino").with_exposed_ports(8080))
    with c:
        url = f"http://{c.get_container_host_ip()}:{c.get_exposed_port(8080)}"
        for _ in range(90):
            try:
                if httpx.get(f"{url}/v1/info").json().get("starting") is False: break
            except Exception: pass
            time.sleep(1)
        yield url

def token(keycloak, client_id, secret):
    r = httpx.post(f"{keycloak}/realms/platform/protocol/openid-connect/token",
                   data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": secret})
    r.raise_for_status(); return r.json()["access_token"]

@pytest.fixture
def institution_a(keycloak): return {"id": "t-acme", "token": token(keycloak, "svc-t-acme", "secret")}
@pytest.fixture
def institution_b(keycloak): return {"id": "t-globex", "token": token(keycloak, "svc-t-globex", "secret")}
@pytest.fixture
def unique(): return uuid.uuid4().hex[:8]
