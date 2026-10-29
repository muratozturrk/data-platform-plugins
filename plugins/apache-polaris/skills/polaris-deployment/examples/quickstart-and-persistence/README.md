# Polaris Quickstart and Persistence Examples

Three Polaris servers side by side: an older quickstart image with quickstart settings, Polaris
1.8.0 with defaults (in-memory), and Polaris 1.8.0 on PostgreSQL bootstrapped by the admin tool.
All 13 tests passed on a fresh stack (about 62 s of test time, container restarts included).

| Test package | What it proves |
|---|---|
| `deployment/` | The quickstart server comes up with its bootstrap credentials; in-memory persistence loses every catalog on restart (ready again after 11 s); relational-jdbc on PostgreSQL keeps it (ready after 13 s); the durable server needs no `POLARIS_BOOTSTRAP_CREDENTIALS` because the root principal lives in the database |
| `catalogs/` | Token request; a FILE catalog needs `ALLOW_INSECURE_STORAGE_TYPES` + FILE in `SUPPORTED_CATALOG_STORAGE_TYPES` (1.8.0 defaults: 400 `Unsupported storage type: FILE`); a placeholder role ARN is rejected by 1.1.0-SNAPSHOT but accepted by 1.8.0; FILE writes fail until the table directories are writable for uid 10000 |
| `access_setup/` | The first-user setup (catalog, catalog role, principal, principal role, two assignments, grant) as 11 HTTP calls and as `quickstart_env/bootstrap.py`, each ending with the new principal writing rows; non-existent privilege names (`CATALOG_MANAGE_PRIVILEGES`, `TABLE_DELETE`) return 400 |

`quickstart_env/` holds the two helper scripts many quickstart environments ship:
`bootstrap.py` (the setup sequence against `localhost:8181`) and `icebergdata/table_setup.py`
(creates `<namespace>/<table>/metadata` and `data` with mode 777). The tests run them unchanged,
only replacing `localhost:8181` with the service name.

## Stack

| Service | Image | Role |
|---|---|---|
| `polaris-quickstart` | `apache/polaris:1.1.0-incubating-SNAPSHOT` | Quickstart settings: `root_user` / `my_secret_id`, FILE storage enabled, shared data volume |
| `polaris-mem` | `apache/polaris:latest` (1.8.0) | Defaults: in-memory persistence, no FILE storage |
| `polaris-durable` + `postgres` + `polaris-bootstrap` | `apache/polaris:latest`, `postgres:16-alpine`, `apache/polaris-admin-tool:latest` | relational-jdbc persistence, root principal created once by the admin tool |
| `runner` | `python:3.11-slim` + requests, PyIceberg 0.9.1, Docker SDK 7.1.0 | Tests; restarts Polaris containers through `/var/run/docker.sock` |

## How to Run

```bash
docker compose -p polaris-deploy up -d --build
docker compose -p polaris-deploy exec runner python -m pytest -v
docker compose -p polaris-deploy down -v
```

Use the project name `polaris-deploy` (the runner finds containers by the compose project label,
see `COMPOSE_PROJECT` in `docker-compose.yml`). On Git Bash for Windows prefix `docker compose exec`
with `MSYS_NO_PATHCONV=1`.

## Layout

```
docker-compose.yml           three Polaris servers, PostgreSQL, admin-tool bootstrap, runner
config/postgres-init.sql     creates polaris_schema (needed before the admin tool can bootstrap)
common/servers.py            Server objects (QUICKSTART, MEM, DURABLE), token(), restart(), image_of()
quickstart_env/              bootstrap.py, icebergdata/table_setup.py
conftest.py                  uid / measure / example_rows fixtures
deployment/                  test_deployment.py
catalogs/                    test_catalogs.py
access_setup/                test_access_setup.py
runner/Dockerfile            test runner image
```

The suite restarts the quickstart server before each access-setup path, so run it on a stack
that nothing else is using.
