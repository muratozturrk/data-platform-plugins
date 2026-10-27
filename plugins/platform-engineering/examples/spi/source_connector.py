"""SourceConnector SPI v1 – the only thing the core knows about sources."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol, runtime_checkable

SPI_VERSION = "v1"

@dataclass(frozen=True)
class TenantContext:
    tenant_id: str
    project_id: str
    environment: str
    plan: str
    region: str
    tz: str = "UTC"
    principal: str = "system"

@dataclass
class Capabilities:
    incremental: bool = False
    schema_discovery: bool = False
    pushdown_filters: bool = False

@dataclass
class ReadRequest:
    cursor: dict[str, Any] | None
    options: dict[str, Any] = field(default_factory=dict)

@runtime_checkable
class SourceConnector(Protocol):
    name: str
    version: str
    spi_version: str

    def config_schema(self) -> dict[str, Any]:
        """JSON Schema for the plugin's own `options` block."""

    def capabilities(self) -> Capabilities: ...

    def discover(self, ctx: TenantContext, options: dict[str, Any]) -> list[str]:
        """Return discoverable streams/tables for this tenant."""

    def schema(self, ctx: TenantContext, stream: str, options: dict[str, Any]) -> dict[str, Any]: ...

    def read(self, ctx: TenantContext, stream: str, req: ReadRequest) -> Iterator[tuple[list[dict], dict]]:
        """Yield (batch, next_cursor). Must only touch resources resolved for ctx."""
