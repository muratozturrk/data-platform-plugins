# Observability and Production Readiness

## Concept

Polaris is a Quarkus application, so its signals come from Quarkus extensions:

- **Metrics**: Micrometer in Prometheus format at `/q/metrics` on the management port **8182**,
  with extra tags from `polaris.metrics.tags.*` (default tag `application=Polaris`).
- **Traces**: OpenTelemetry, off unless `quarkus.otel.sdk.disabled=false`, exported over OTLP.
- **Logs**: console and a rotating file, with MDC fields (request ID, realm ID, trace IDs,
  custom keys from `polaris.log.mdc.*`) and optional JSON output.
- **Readiness checks**: at startup Polaris 1.8.0 checks its own configuration against
  production rules and logs every offending option.

## Configuration Reference (Polaris 1.8.0)

| Signal | Property / endpoint | Older docs say | Measured on 1.8.0 |
|---|---|---|---|
| Metrics endpoint | `:8182/q/metrics` | `:8282/q/metrics` | 8182: 200; 8282: connection refused |
| Metric tags | `polaris.metrics.tags.<key>=<value>` | same | Added to every metric; `application` overridable |
| Traces on | `quarkus.otel.sdk.disabled=false` | same | Spans reach the collector |
| Trace endpoint | `quarkus.otel.exporter.otlp.traces.endpoint=http://otel-collector:4317` | same | OTLP gRPC |
| Resource attributes | `quarkus.otel.resource.attributes=service.name=polaris,deployment.environment=prod` | same | Present on spans |
| Request ID header | `X-Request-ID` (`polaris.log.request-id-header-name`) | `Polaris-Request-Id` | Only `X-Request-ID` is picked up |
| JSON console logs | `quarkus.log.console.json.enabled=true` | `quarkus.log.console.json=true` | Older form: 0 JSON lines |
| Custom MDC | `polaris.log.mdc.<key>=<value>` | same | Visible in JSON output, not in the plain format |
| Log file | `/deployments/logs/polaris.log` | `./logs` | Present; rotation defaults 10 MB, 14 backups, daily |
| Start despite failed checks | `polaris.readiness.ignore-severe-issues=true` | not mentioned | Logged as accepted |

## Worked Example: Prometheus + OTLP Collector

```yaml
  polaris-obs:
    image: apache/polaris:latest
    environment:
      quarkus.otel.sdk.disabled: "false"
      quarkus.otel.exporter.otlp.traces.endpoint: http://otel-collector:4317
      quarkus.otel.resource.attributes: service.name=polaris,deployment.environment=prod
      polaris.metrics.tags.environment: prod
      polaris.metrics.tags.region: us-west-2
      polaris.metrics.tags.application: my-polaris-instance
      polaris.log.mdc.environment: prod
      quarkus.log.console.json.enabled: "true"
```

`prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'polaris'
    metrics_path: '/q/metrics'
    scrape_interval: 5s
    static_configs:
      - targets: ['polaris-obs:8182']
```

Measured: `up{job="polaris"} = 1`, 2 series with `environment="prod"`. A sample line:
`http_server_requests_seconds_count{application="my-polaris-instance",environment="prod",method="GET",outcome="SUCCESS",region="us-west-2",status="200",uri="/api/management/v1/catalogs"} 5.0`.
Spans carried `service.name=polaris`, `deployment.environment=prod`, `polaris.realm` and
`polaris.request.id`. Plain log lines look like
`[io.qua.htt.access-log] [xreq-0,POLARIS] [<trace-id>,,<span-id>,true]` (request ID, realm, trace
context). A JSON record has `mdc = {"requestId": "json-req-1", "realmId": "POLARIS", "environment": "prod"}`.

## Readiness Checks

Polaris 1.8.0 logs `Production readiness checks failed! Check the warnings below.` followed by one
line per offending option, for example:

- `The realm context resolver is configured to map requests without a realm header to the default realm. Offending configuration option: 'polaris.realm-context.require-header'.`
- `The current metastore is intended for tests only. Offending configuration option: 'polaris.persistence.type'.`
- insecure storage types enabled

and then a note that severe issues were accepted because `polaris.readiness.ignore-severe-issues=true`
was set. Treat this list as the built-in hardening checklist: fix each option instead of setting
the ignore flag in production.

## Production Checklist

| Area | Setting | Where |
|---|---|---|
| Durable metadata | `polaris.persistence.type=relational-jdbc`, admin-tool bootstrap | [containers-and-persistence.md](containers-and-persistence.md) |
| Storage | No insecure types; STS vending with scoped roles | [storage-and-first-catalog.md](storage-and-first-catalog.md) |
| Tenancy | `polaris.realm-context.require-header=true` | `polaris-configuration-and-cli` skill |
| Token signing | `rsa-key-pair` from mounted key files for several replicas | `polaris-configuration-and-cli` skill |
| Overload protection | `polaris.rate-limiter.filter.type=default` + token bucket; task limits | `polaris-configuration-and-cli` skill |
| Signals | `/q/metrics` scraped internally, OTLP traces, JSON logs with MDC | this file |
| Verification | Startup log free of readiness warnings; token request smoke test | this file |

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Prometheus target down on 8282 | Metrics live on the management port | Scrape `:8182/q/metrics` |
| No JSON logs | `quarkus.log.console.json=true` (older name) | `quarkus.log.console.json.enabled=true` |
| Request IDs missing from logs and spans | Client sends `Polaris-Request-Id` | Send `X-Request-ID` (or set `polaris.log.request-id-header-name`) |
| Custom MDC key not in log lines | Plain format does not print it | Use JSON output |
| `/q/metrics` reachable from outside | Management port published | Keep 8182 on the internal network |
| Secrets in logs | Config-loader DEBUG logging | Do not enable DEBUG for config loading in production |
| Readiness warnings ignored for good | `ignore-severe-issues=true` left in place | Fix the options, then remove the flag |

## Related

- [containers-and-persistence.md](containers-and-persistence.md), [storage-and-first-catalog.md](storage-and-first-catalog.md).
- `polaris-configuration-and-cli` skill: rate limiting, token broker, realms and feature flags.
