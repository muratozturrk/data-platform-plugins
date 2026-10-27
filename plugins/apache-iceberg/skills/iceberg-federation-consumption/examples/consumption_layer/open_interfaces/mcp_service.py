"""MCP server that lets an AI agent use the semantic layer, not raw SQL.

Built with the official MCP Python SDK (FastMCP): https://github.com/modelcontextprotocol/python-sdk
Tools are bound to one identity (in production: the OAuth principal of the MCP session); every
query runs on Trino as that user, so the row filters and masks of the SQL endpoint still apply.
"""
from mcp.server.fastmcp import FastMCP

from common.lakehouse import trino_connect, trino_query
from common.semantic import DIMENSIONS, GOVERNED_VIEW, METRICS, compile_query


def build_server(user: str, semantic_schema: str) -> FastMCP:
    mcp = FastMCP("lakehouse-semantic-layer")
    source = f"{semantic_schema}.{GOVERNED_VIEW}"

    @mcp.tool()
    def list_datasets() -> dict:
        """Datasets this caller may query (Trino filters SHOW TABLES by privileges)."""
        return {"datasets": [r[0] for r in trino_query(f"SHOW TABLES IN {semantic_schema}", user=user)]}

    @mcp.tool()
    def describe_metrics() -> dict:
        """Governed metric definitions and the dimensions they can be grouped by."""
        return {"metrics": {m.name: m.description for m in METRICS.values()}, "dimensions": sorted(DIMENSIONS)}

    @mcp.tool()
    def query_metrics(metrics: list[str], dimensions: list[str] | None = None, limit: int = 100) -> dict:
        """Compute governed metrics, optionally grouped by dimensions. No free-form SQL is accepted."""
        dims = dimensions or []
        sql = compile_query(metrics, dims, source) + f" LIMIT {min(int(limit), 1000)}"
        with trino_connect(user) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            names = [d[0] for d in cur.description]
            rows = [dict(zip(names, r)) for r in cur.fetchall()]
        return {"user": user, "rows": rows}

    return mcp
