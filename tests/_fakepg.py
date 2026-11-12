"""Offline in-memory PostgreSQL wire peer (no sockets, no server).

It drives the *real* asyncpg Cython protocol through an in-memory
asyncio transport, so prepare-cache invalidation, transaction status and
pool reset behaviour can be replayed without a PostgreSQL instance.

Only the subset of the wire protocol used by the scenarios in
test_offline_role_path_cache.py is implemented.
"""

import asyncio
import struct


# ---------------------------------------------------------------- wire utils
def msg(mtype: bytes, payload: bytes = b"") -> bytes:
    return mtype + struct.pack("!I", len(payload) + 4) + payload


def cstr(s) -> bytes:
    if isinstance(s, str):
        s = s.encode()
    return s + b"\x00"


def startup_reply(parameters):
    out = b""
    for k, v in parameters.items():
        out += msg(b"S", cstr(k) + cstr(v))
    out += msg(b"R", struct.pack("!I", 0))          # AuthenticationOk
    out += msg(b"K", struct.pack("!II", 1234, 5678))  # BackendKeyData
    out += msg(b"Z", b"I")                            # ReadyForQuery: idle
    return out


def error_response(fields: dict) -> bytes:
    payload = b""
    for code, value in fields.items():
        payload += code.encode() + cstr(value)
    payload += b"\x00"
    return msg(b"E", payload)


def row_description(columns):
    # columns: list of (name, table_oid, col_num, type_oid, size, mod, fmt)
    payload = struct.pack("!H", len(columns))
    for name, toid, cnum, typoid, size, mod, fmt in columns:
        payload += cstr(name)
        payload += struct.pack("!IhIhih", toid, cnum, typoid, size, mod, fmt)
    return msg(b"T", payload)


def data_row_text(values):
    # values: list of str or None, text format
    payload = struct.pack("!H", len(values))
    for v in values:
        if v is None:
            payload += struct.pack("!i", -1)
        else:
            b = v if isinstance(v, bytes) else str(v).encode()
            payload += struct.pack("!i", len(b)) + b
    return msg(b"D", payload)


def data_row_binary(values):
    # values: list of bytes (already encoded) or None
    payload = struct.pack("!H", len(values))
    for v in values:
        if v is None:
            payload += struct.pack("!i", -1)
        else:
            payload += struct.pack("!i", len(v)) + v
    return msg(b"D", payload)


def command_complete(tag: str) -> bytes:
    return msg(b"C", cstr(tag))


def ready(status: bytes) -> bytes:
    return msg(b"Z", status)


# ------------------------------------------------------------- fake transport
class FakeTransport(asyncio.Transport):
    def __init__(self, protocol, server):
        self._protocol = protocol
        self._server = server
        self._closed = False

    def write(self, data):
        self._server.feed_client(data)

    def writelines(self, seq):
        for chunk in seq:
            self.write(chunk)

    def close(self):
        if not self._closed:
            self._closed = True

    def abort(self):
        self.close()

    def is_closing(self):
        return self._closed

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def get_extra_info(self, name, default=None):
        if name == "socket":
            return None
        if name == "ssl_object":
            return None
        return default


# ------------------------------------------------------------- client decoder
class ClientReader:
    def __init__(self, data: bytes):
        self.buf = data
        self.messages = []
        while self.buf:
            mtype = self.buf[:1]
            (length,) = struct.unpack("!I", self.buf[1:5])
            body = bytes(self.buf[5:1 + length])
            self.messages.append((mtype, body))
            self.buf = self.buf[1 + length:]


# ----------------------------------------------------------------- fake server
class FakePgServer:
    """Scriptable PG peer.

    Models one unqualified table ``items`` visible via search_path in two
    schemas with incompatible column types:

        schema_a.items(id int4)          -> binary int4 rows
        schema_b.items(id text)          -> text rows

    A prepared statement keeps its original plan. When search_path changes
    and the stale plan is bound, PostgreSQL raises 0A000 from
    RevalidateCachedQuery ("cached plan must not change result type").
    """

    INT4 = 23
    TEXT = 25

    def __init__(self):
        self.transport = None
        self.protocol = None
        self.search_path = "schema_a"
        self.prepared = {}            # stmt_name -> query
        # (query, path-at-prepare-time) -> current result type signature
        self.plan_signature = {}
        self.events = []              # observed frontend message types
        self._startup_seen = False

    # asyncio plumbing -------------------------------------------------------
    def attach(self, loop, protocol):
        self.protocol = protocol
        self.transport = FakeTransport(protocol, self)
        protocol.connection_made(self.transport)
        return self.transport

    def feed_client(self, data: bytes):
        loop = asyncio.get_running_loop()
        loop.call_soon(self._process, data)

    def _send(self, chunks):
        if isinstance(chunks, (bytes, bytearray)):
            chunks = [chunks]
        self.protocol.data_received(b"".join(chunks))

    # message processing -----------------------------------------------------
    def _process(self, data: bytes):
        # StartupMessage has no type byte: int32 length, int32 protocol
        # version 196608, then key/value NUL strings and a final NUL.
        if (not self._startup_seen
                and len(data) >= 8
                and data[4:8] == struct.pack("!I", 196608)):
            (length,) = struct.unpack("!I", data[:4])
            self._startup_seen = True
            self._send(startup_reply({
                "server_version": "16.0.0",
                "client_encoding": "UTF8",
                "DateStyle": "ISO, MDY",
                "standard_conforming_strings": "on",
            }))
            rest = data[length:]
            if rest:
                self._process_typed(rest)
            return
        self._process_typed(data)

    def _process_typed(self, data: bytes):
        reader = ClientReader(data)
        for mtype, body in reader.messages:
            self.events.append(mtype)
            self._route(mtype, body)

    def _route(self, mtype, body):
        handler = {
            b"P": self._on_parse,
            b"D": self._on_describe,
            b"B": self._on_bind,
            b"E": self._on_execute,
            b"S": self._on_sync,
            b"H": self._on_flush,
            b"Q": self._on_simple_query,
            b"C": self._on_close,
            b"X": self._on_terminate,
        }.get(mtype)
        if handler is None:
            raise AssertionError(f"fake server: unhandled message {mtype!r}")
        handler(body)

    # helpers ----------------------------------------------------------------
    @staticmethod
    def _read_cstr(buf, pos):
        end = buf.index(b"\x00", pos)
        return buf[pos:end], end + 1

    def _current_columns(self):
        if self.search_path == "schema_a":
            return [("id", 16384, 1, self.INT4, 4, -1, 1)]
        return [("id", 16385, 1, self.TEXT, -1, -1, 0)]

    def _row_for_path(self):
        if self.search_path == "schema_a":
            return data_row_binary([struct.pack("!i", 42)])
        return data_row_text(["fourty-two"])

    def _signature(self):
        return tuple(c[3] for c in self._current_columns())

    # frontend message handlers ---------------------------------------------
    def _on_parse(self, body):
        pos = 0
        name, pos = self._read_cstr(body, pos)
        query, pos = self._read_cstr(body, pos)
        name = name.decode()
        query = query.decode()
        self.prepared[name] = query
        self.plan_signature[(query, self.search_path)] = self._signature()
        self._send(msg(b"1"))  # ParseComplete

    def _on_describe(self, body):
        kind = body[:1]
        if kind == b"S":
            # ParameterDescription (none) + RowDescription for the table
            # currently visible through search_path.
            self._send([
                msg(b"t", struct.pack("!H", 0)),
                row_description(self._current_columns()),
            ])
        else:
            self._send(msg(b"n"))  # NoData for portal

    def _on_bind(self, body):
        self._send(msg(b"2"))  # BindComplete

    def _on_execute(self, body):
        # The bound statement's plan was fixed at prepare time. We emulate
        # plan revalidation failure when the result type would change.
        query = self._bound_query_for(body)
        stale = self._is_stale(query)
        if stale:
            # An error inside an open transaction block puts that block into
            # the aborted state, exactly as PostgreSQL's ReadyForQuery byte
            # 'E' (PQTRANS_INERROR) reports.
            if self._txn:
                self._aborted = True
            self._send(error_response({
                "S": "ERROR",
                "C": "0A000",
                "M": "cached plan must not change result type",
                "F": "plancache.c",
                "R": "RevalidateCachedQuery",
            }))
            return
        self._send([self._row_for_path(), command_complete("SELECT 1")])

    def _bound_query_for(self, body):
        # Find most recently parsed query; sufficient for single-portal tests.
        for q in reversed(list(self.prepared.values())):
            return q
        return ""

    def _is_stale(self, query):
        sig = self.plan_signature.get((query, self.search_path))
        if sig is None:
            # statement prepared under a different search_path
            for (q, path), old_sig in self.plan_signature.items():
                if q == query:
                    return old_sig != self._signature()
            return False
        return False

    def _on_sync(self, body):
        if self._aborted:
            status = b"E"
        else:
            status = b"T" if self._txn else b"I"
        self._send(ready(status))

    def _on_flush(self, body):
        return

    def _on_simple_query(self, body):
        # A simple Query message may carry several semicolon-separated
        # statements (e.g. the pool reset query ends with "RESET ALL;").
        # PostgreSQL returns one CommandComplete per statement but a single
        # ReadyForQuery only after the whole Query message finishes.
        raw = body[:-1].decode()
        statements = [part.strip() for part in raw.split(";") if part.strip()]
        chunks = []
        for statement in statements:
            norm = " ".join(statement.split()).lower()
            chunks.append(command_complete(self._apply_simple(norm)))
        if self._aborted:
            status = b"E"
        else:
            status = b"T" if self._txn else b"I"
        chunks.append(ready(status))
        self._send(chunks)

    def _apply_simple(self, norm):
        if norm.startswith("set search_path"):
            self.search_path = norm.split(
                "to", 1)[1].strip().rstrip(";").strip()
            return "SET"
        if norm.startswith("set role"):
            self.role = norm.split("to", 1)[1].strip().rstrip(";").strip()
            return "SET"
        if norm.startswith("begin"):
            self._txn = True
            self._aborted = False
            return "BEGIN"
        if norm.startswith("commit"):
            self._txn = False
            self._aborted = False
            return "COMMIT"
        if norm.startswith("rollback"):
            self._txn = False
            self._aborted = False
            return "ROLLBACK"
        if norm.startswith("reset all"):
            self.search_path = "schema_a"
            self.role = None
            return "RESET"
        if norm.startswith("select pg_advisory_unlock_all"):
            return "SELECT"
        if norm.startswith("close all") or norm.startswith("unlisten"):
            return "SET"
        return "SET"

    def _on_close(self, body):
        kind = body[:1]
        name, _ = self._read_cstr(body, 1)
        name = name.decode()
        if kind == b"S":
            self.prepared.pop(name, None)
        self._send(msg(b"3"))  # CloseComplete

    def _on_terminate(self, body):
        self.transport.close()
        loop = asyncio.get_running_loop()
        loop.call_soon(self.protocol.connection_lost, None)

    role = None
    _txn = False
    _aborted = False
