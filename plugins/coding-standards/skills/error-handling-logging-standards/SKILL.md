---
name: error-handling-logging-standards
description: Cross-language standard for errors, retries, logging, tracing and metrics in platform code and pipelines: exception hierarchy, error contracts, structured logs with tenant/run context, log levels, PII/secret hygiene, OpenTelemetry. Use when writing any code that can fail or emit telemetry.
---

# Error Handling and Logging Standards

## Errors
| # | Rule |
|---|---|
| 1 | One exception hierarchy per package rooted in `PlatformError(code, message, hint, details)`; codes stable `{domain}.{condition}` (see api-naming) |
| 2 | Fail fast on programmer errors (assertions, invalid state); handle expected failures (I/O, upstream, validation) explicitly |
| 3 | Catch narrowly; never bare `except`/`catch (e)` without rethrow; re-raise with cause (`raise X from e`, `new Error(msg, {cause})`) |
| 4 | Translate at boundaries: plugin/engine errors → platform errors with context (tenant, pipeline, run, step); never leak stack traces or internal names to API clients |
| 5 | Retries only for transient classes, with exponential backoff + jitter, bounded attempts, idempotent operations only; circuit breaker for shared dependencies |
| 6 | Partial failure in pipelines is explicit: quarantine/dead-letter with reason, never silent row drops; `onFailure: block|warn|quarantine` from the manifest |
| 7 | Validation errors list all problems with path and fix hint, not just the first |
| 8 | No exceptions for control flow; no returning `None`/`null` to signal failure in public APIs |

## Logging
| # | Rule |
|---|---|
| 1 | Structured JSON logs; fields `ts, level, logger, msg, tenant_id, workspace_id, pipeline, run_id, step, trace_id, span_id`; context bound once at the edge and propagated (structlog contextvars / OpenTelemetry baggage) |
| 2 | Levels: `DEBUG` developer detail (off in prod), `INFO` state changes and milestones (run started/finished, N rows written), `WARNING` degraded but continuing, `ERROR` operation failed and needs attention, `CRITICAL` platform-level outage; no `INFO` inside per-row loops |
| 3 | Message is a constant template; variables as fields (`log.info("run_finished", rows=n)`), never f-strings |
| 4 | Never log secrets, tokens, credentials, full PII; mask by field name allow-list; payload samples truncated and tagged |
| 5 | One log line per event; no multi-line banners; errors logged once at the handling site with `exc_info` |
| 6 | Metrics via OpenTelemetry: counters `{component}_{thing}_total`, histograms `_seconds`/`_bytes`, labels include `tenant`, never high-cardinality ids |
| 7 | Traces: one span per pipeline step and per external call; span names `{component}.{operation}` |
| 8 | Audit events (who did what to which resource) are a separate, immutable stream, not application logs |

## Anti-patterns
- `except Exception: logger.error("error")` with no context and no re-raise
- Retrying non-idempotent writes
- Logging the whole DataFrame/record
- `print` debugging left in; log levels used for formatting

Sources: Google SRE Book (monitoring, "The Four Golden Signals"); OpenTelemetry semantic conventions; structlog docs; Michael Nygard, *Release It!* 2e (stability patterns: timeouts, circuit breaker, bulkheads); Martin, *Clean Code* ch. 7 Error Handling; OWASP Logging Cheat Sheet.
