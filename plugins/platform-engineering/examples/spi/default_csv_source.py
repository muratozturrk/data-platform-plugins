"""Built-in default plugin: preserves existing behaviour so extraction from core is a no-op for users."""
from __future__ import annotations
import csv
from typing import Any, Iterator
from source_connector import Capabilities, ReadRequest, SourceConnector, TenantContext, SPI_VERSION

class CsvSource:  # implements SourceConnector
    name = "csv-source"
    version = "1.0.0"
    spi_version = SPI_VERSION

    def __init__(self, storage):           # StorageProvider injected by the core
        self._storage = storage

    def config_schema(self) -> dict[str, Any]:
        return {"type": "object", "required": ["path"],
                "properties": {"path": {"type": "string"}, "delimiter": {"type": "string", "default": ","}}}

    def capabilities(self) -> Capabilities:
        return Capabilities(schema_discovery=True)

    def discover(self, ctx: TenantContext, options: dict[str, Any]) -> list[str]:
        return [options["path"]]

    def schema(self, ctx: TenantContext, stream: str, options: dict[str, Any]) -> dict[str, Any]:
        with self._storage.open(ctx, stream) as f:          # path resolved per tenant, never built here
            header = next(csv.reader(f, delimiter=options.get("delimiter", ",")))
        return {"fields": [{"name": h, "type": "string"} for h in header]}

    def read(self, ctx: TenantContext, stream: str, req: ReadRequest) -> Iterator[tuple[list[dict], dict]]:
        with self._storage.open(ctx, stream) as f:
            rows = list(csv.DictReader(f, delimiter=req.options.get("delimiter", ",")))
        yield rows, {}
