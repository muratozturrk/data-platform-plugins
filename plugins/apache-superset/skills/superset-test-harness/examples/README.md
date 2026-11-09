# superset-test-harness – examples
- `docker-compose.test.yml` – full harness: Superset web/worker/beat (platform image), PostgreSQL, Redis, Trino, Keycloak (test realm import), Mailpit
- `superset_config_test.py` – test overrides on top of the production config
- `conftest.py` – compose lifecycle (Testcontainers `DockerCompose`), `superset_client` (login/CSRF/as_user/guest token/chart-data), `seeded` fixture (two institutions via assets + RLS), reset
- `seed_institutions.sh` – bootstrap + `superset-cli sync native` + `import-rls` + embedded registration
- `test_assets_import_idempotent.py` – sync twice, UUIDs/counts stable
- `test_alerts_reports.py` – report executes, email lands in Mailpit, archive object written with snapshot ref
- `test_cache_isolation.py` – per-institution cache keys, TTL
