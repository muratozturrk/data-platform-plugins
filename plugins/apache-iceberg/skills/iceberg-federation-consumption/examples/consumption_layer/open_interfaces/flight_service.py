"""Arrow Flight endpoint that streams Iceberg scans as Arrow record batches.

Pattern from the Arrow cookbook "Simple Parquet storage service with Arrow Flight" and the
pyarrow.flight middleware API: https://arrow.apache.org/cookbook/py/flight.html ,
https://arrow.apache.org/docs/python/flight.html
A Flight request is a JSON command: {"table", "columns", "filter", "snapshot_id"}.
Bearer tokens identify the caller; tenant users get their merchant filter added server-side.
"""
import json
import re
import threading

import pyarrow.flight as flight
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, parser
from pyiceberg.io.pyarrow import schema_to_pyarrow

TENANT_USER = re.compile(r"m[0-9]+")          # same convention as config/trino/rules.json


class BearerAuthFactory(flight.ServerMiddlewareFactory):
    def __init__(self, tokens: dict[str, str]):
        self.tokens = tokens

    def start_call(self, info, headers):
        value = (headers.get("authorization") or [""])[0]
        token = value.removeprefix("Bearer ").strip()
        if token not in self.tokens:
            raise flight.FlightUnauthenticatedError("missing or invalid bearer token")
        return CallerMiddleware(self.tokens[token])


class CallerMiddleware(flight.ServerMiddleware):
    def __init__(self, user: str):
        self.user = user


class IcebergFlightServer(flight.FlightServerBase):
    def __init__(self, catalog, tokens: dict[str, str], location: str = "grpc://0.0.0.0:0"):
        super().__init__(location, middleware={"auth": BearerAuthFactory(tokens)})
        self.catalog = catalog

    def _scan(self, context, command: bytes):
        req = json.loads(command)
        row_filter = parser.parse(req["filter"]) if req.get("filter") else AlwaysTrue()
        user = context.get_middleware("auth").user
        if TENANT_USER.fullmatch(user):                       # enforce tenant isolation on the server
            row_filter = And(row_filter, EqualTo("merchant_id", user))
        return self.catalog.load_table(req["table"]).scan(
            row_filter=row_filter, selected_fields=tuple(req.get("columns") or ["*"]),
            snapshot_id=req.get("snapshot_id"))

    def get_flight_info(self, context, descriptor):
        scan = self._scan(context, descriptor.command)
        schema = schema_to_pyarrow(scan.projection(), include_field_ids=False)
        endpoint = flight.FlightEndpoint(descriptor.command, [])   # same server serves the stream
        return flight.FlightInfo(schema, descriptor, [endpoint], -1, -1)

    def do_get(self, context, ticket):
        return flight.RecordBatchStream(self._scan(context, ticket.ticket).to_arrow_batch_reader())


def start(server: IcebergFlightServer) -> threading.Thread:
    t = threading.Thread(target=server.serve, daemon=True)
    t.start()
    return t


def fetch(port: int, token: str, table: str, columns=None, where=None, snapshot_id=None):
    """Client side: get_flight_info (schema, endpoints), then do_get the stream into one Arrow table."""
    client = flight.FlightClient(f"grpc://localhost:{port}")
    options = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])
    command = json.dumps({"table": table, "columns": columns, "filter": where, "snapshot_id": snapshot_id})
    info = client.get_flight_info(flight.FlightDescriptor.for_command(command), options)
    table_ = client.do_get(info.endpoints[0].ticket, options).read_all()
    client.close()
    return info.schema, table_
