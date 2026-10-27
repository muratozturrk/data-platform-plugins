"""Contract test: run against EVERY SourceConnector implementation (built-in and third-party).
Ship this in platform.sdk.testing so plugin authors run it locally."""
import pytest
from importlib.metadata import entry_points
from source_connector import SourceConnector, TenantContext, ReadRequest, SPI_VERSION

CTX_A = TenantContext("tenant-a", "proj", "test", "team", "eu-central-1")
CTX_B = TenantContext("tenant-b", "proj", "test", "team", "eu-central-1")

def all_sources():
    return [ep.load() for ep in entry_points(group="platform.plugins.source")]

@pytest.mark.parametrize("plugin_cls", all_sources())
class TestSourceConnectorContract:
    def test_declares_identity(self, plugin_cls, storage):
        p = plugin_cls(storage)
        assert isinstance(p, SourceConnector)
        assert p.spi_version == SPI_VERSION
        assert p.name and p.version

    def test_config_schema_is_json_schema(self, plugin_cls, storage):
        s = plugin_cls(storage).config_schema()
        assert s.get("type") == "object"

    def test_read_requires_tenant_context(self, plugin_cls, storage, sample_options):
        p = plugin_cls(storage)
        stream = p.discover(CTX_A, sample_options)[0]
        batches = list(p.read(CTX_A, stream, ReadRequest(cursor=None, options=sample_options)))
        assert batches

    def test_no_cross_tenant_access(self, plugin_cls, storage, sample_options):
        p = plugin_cls(storage)
        stream = p.discover(CTX_A, sample_options)[0]
        with pytest.raises(PermissionError):
            list(p.read(CTX_B, stream, ReadRequest(cursor=None, options=sample_options)))
