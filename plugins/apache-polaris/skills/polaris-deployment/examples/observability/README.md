# Polaris Observability and Readiness Examples

Polaris 1.8.0 wired to Prometheus and an OpenTelemetry Collector, plus the server's own
production-readiness checks. These 8 tests are part of a suite that passed in full on a fresh stack.

| Test package | What it proves |
|---|---|
| `observability/` | Metrics on the management port (`:8182/q/metrics` 200, `:8282` refused); `polaris.metrics.tags.*` added to every metric and `application` overridable; Prometheus scrape of `polaris-obs:8182` gives `up = 1`; OTLP spans carry `service.name`, `deployment.environment`, `polaris.realm`, `polaris.request.id`; the request ID comes from `X-Request-ID` (not `Polaris-Request-Id`); `quarkus.log.console.json=true` writes no JSON while `quarkus.log.console.json.enabled=true` does, with `mdc` = requestId, realmId and custom keys; `/deployments/logs/polaris.log` exists |
| `readiness/` | At startup Polaris logs `Production readiness checks failed!` and names each offending option (in-memory `polaris.persistence.type`, `polaris.realm-context.require-header=false`, insecure storage) |

## Stack

| Service | Image | Role |
|---|---|---|
| `polaris-obs` | `apache/polaris:latest` (1.8.0) | Metrics tags, OTLP traces, MDC fields, older JSON-log property |
| `polaris-hard` | `apache/polaris:latest` (1.8.0) | Hardened settings (RSA token broker, rate limit, flags) with the current JSON-log property |
| `keys` | runner image | One-shot: RSA key pair into the `secrets` volume |
| `otel-collector` | `otel/opentelemetry-collector:0.116.1` | OTLP gRPC receiver on 4317, debug exporter |
| `prometheus` | `prom/prometheus:v2.55.1` | Scrapes `/q/metrics` every 5 s |
| `runner` | `python:3.11-slim` + requests, Docker SDK, cryptography | Tests; reads container logs through `/var/run/docker.sock` |

## How to Run

```bash
docker compose -p polaris-ops up -d --build
docker compose -p polaris-ops exec runner python -m pytest -v
docker compose -p polaris-ops down -v
```

Use the project name `polaris-ops`: `conftest.py` finds containers by that compose project label.
On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`.

## Layout

```
docker-compose.yml           polaris-obs, polaris-hard, otel-collector, prometheus, keys, runner
config/make_keys.py          RSA key pair for the rsa-key-pair token broker
config/otel-collector.yaml   OTLP receiver, debug exporter
config/prometheus.yml        scrape config for the management port
conftest.py                  token(), container(), measure fixture
observability/               test_observability.py
readiness/                   test_readiness.py
runner/Dockerfile            test runner image
```

The tuning side of `polaris-hard` (token broker, rate limiting, feature flags) is tested in the
`polaris-configuration-and-cli` skill's examples.
