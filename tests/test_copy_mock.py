# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


"""COPY failure-recovery tests driven by an in-process mock backend.

These tests do not require a running PostgreSQL cluster.  Instead they
speak just enough of the PostgreSQL frontend/backend wire protocol over
an already-connected :func:`socket.socketpair` (so they also work in
sandboxes that forbid binding a local address) to observe exactly what
asyncpg does when a COPY operation has to be aborted.

The focus is the connection's fate after a failed COPY, mirroring the
real-world situation where an application wants to write an audit record
on the same connection right after a bulk load blew up:

* an input iterator raising mid ``COPY ... FROM STDIN`` (COPY IN);
* an output consumer being cancelled mid ``COPY ... TO STDOUT``
  (COPY OUT);
* the server rejecting the COPY or erroring while it is in flight;
* any of the above happening inside an explicit transaction.

The mock backend is *event driven*: every client message is parsed in
``data_received`` and handed to a handler that emits the reply
immediately.  That removes any dependence on the relative scheduling of
the client coroutine and the server task, which matters because asyncpg
pipelining means ``CopyData`` / ``CopyFail`` can already be queued before
the client has processed the backend's ``CopyInResponse``.

For each case we record every client-to-server message (in particular
``CopyFail`` and ``Sync``) in an append-only history and then assert one
of the two legal endings:

1. asyncpg keeps reading until ``ReadyForQuery`` and the connection is
   usable again with the correct transaction status; or
2. asyncpg gives up on the protocol state and tears the connection down,
   in which case further queries must fail loudly instead of corrupting
   the next command.
"""

import asyncio
import platform
import socket
import struct
import unittest

from asyncpg import connect_utils
from asyncpg import exceptions as apg_errors
from asyncpg import protocol as apg_proto
from asyncpg.connection import Connection


_MOCK_ADDR = "\x00mock-copy.sock"
SKIP_REASON = "requires an AF_UNIX socketpair (POSIX selector loop)"
_GENERIC_ERROR = {"S": "ERROR", "C": "57014", "M": "canceling statement"}


# ---------------------------------------------------------------------------
# Wire-protocol framing helpers (backend -> frontend).
# ---------------------------------------------------------------------------

def _frame(mtype, payload=b""):
    return mtype + struct.pack("!I", len(payload) + 4) + payload


def _cstr(value):
    if isinstance(value, str):
        value = value.encode()
    return value + b"\x00"


def _error_response(fields):
    payload = b"".join(code.encode() + _cstr(msg)
                       for code, msg in fields.items()) + b"\x00"
    return _frame(b"E", payload)


def _ready_for_query(status):
    return _frame(b"Z", status)


def _parameter_status(name, value):
    return _frame(b"S", _cstr(name) + _cstr(value))


def _auth_ok():
    return _frame(b"R", struct.pack("!I", 0))


def _backend_key_data(pid=4242, secret=9999):
    return _frame(b"K", struct.pack("!II", pid, secret))


def _command_complete(tag):
    return _frame(b"C", _cstr(tag))


def _copy_in_response():
    # Global format byte (0 = text), column count (0); no per-column fmts.
    return _frame(b"G", struct.pack("!ibh", 0, 0, 0))


def _copy_out_response():
    return _frame(b"H", struct.pack("!ibh", 0, 0, 0))


def _copy_data(payload):
    return _frame(b"d", payload)


_PARSE_COMPLETE = _frame(b"1")
_BIND_COMPLETE = _frame(b"2")
_NO_DATA = _frame(b"n")


class _Message:
    __slots__ = ("mtype", "body")

    def __init__(self, mtype, body):
        self.mtype = mtype
        self.body = body

    def __repr__(self):
        return "_Message({!r}, {!r})".format(self.mtype, self.body[:32])


class _MockPGServer(asyncio.Protocol):
    """Event-driven fake backend.

    ``handler(message, server)`` is invoked synchronously for every fully
    parsed client message and is responsible for calling :meth:`send`.
    The raw message stream is always retained in :attr:`history`.
    """

    def __init__(self, handler):
        self._handler = handler
        self._buffer = b""
        self.history = []
        self.transport = None
        self.closed = asyncio.get_event_loop().create_future()
        # Arbitrary per-scenario state the handler can use.
        self.ctx = {}

    def data_received(self, data):
        self._buffer += data
        while self._buffer:
            if len(self._buffer) < 5:
                return
            if self._buffer[0] < 32:
                # StartupMessage / CancelRequest carry no type byte.
                length = struct.unpack("!I", self._buffer[0:4])[0]
                if len(self._buffer) < length:
                    return
                body = self._buffer[4:length]
                self._buffer = self._buffer[length:]
                code = struct.unpack("!I", body[0:4])[0]
                mtype = b"STARTUP" if code == 196608 else b"CANCEL"
            else:
                mtype = self._buffer[:1]
                length = struct.unpack("!I", self._buffer[1:5])[0]
                if len(self._buffer) < 1 + length:
                    return
                body = self._buffer[5:1 + length]
                self._buffer = self._buffer[1 + length:]
            message = _Message(mtype, body)
            self.history.append(message)
            self._handler(message, self)

    def connection_lost(self, exc):
        if not self.closed.done():
            self.closed.set_result(exc)

    def send(self, data):
        if self.transport is not None:
            self.transport.write(data)

    def send_later(self, data):
        # Defer the reply by one loop iteration.  This matters when a
        # client message triggers an internal state transition (e.g.
        # CopyFail -> PROTOCOL_CANCELLED): answering inside the same
        # data_received() batch would make asyncpg parse the reply before
        # it has finished moving into the new state.
        async def _emit():
            await asyncio.sleep(0)
            self.send(data)

        asyncio.ensure_future(_emit())

    def close(self):
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    def connection_made(self, transport):
        self.transport = transport


# ---------------------------------------------------------------------------
# Reusable, ordering-independent message handlers.
# ---------------------------------------------------------------------------

def _handle_handshake(message, server):
    # Reply to the StartupMessage with a standard ready-to-query sequence.
    if message.mtype != b"STARTUP":
        return False
    server.send(_auth_ok())
    server.send(_parameter_status("server_version", "16.0.0"))
    server.send(_parameter_status("client_encoding", "UTF8"))
    server.send(_parameter_status("DateStyle", "ISO, MDY"))
    server.send(_parameter_status("TimeZone", "UTC"))
    server.send(_parameter_status("integer_datetimes", "on"))
    server.send(_backend_key_data())
    server.send(_ready_for_query(b"I"))
    server.ctx["handshook"] = True
    return True


def _handle_audit_query(message, server, tag="INSERT 0 1", rfq=b"I"):
    """Answer the post-COPY audit command, in either query flavor."""
    if message.mtype == b"Q":
        server.send(_command_complete(tag))
        server.send(_ready_for_query(rfq))
        return True
    if message.mtype == b"P":
        # Extended query: Parse -> (expect D/H), Bind -> Execute -> Sync.
        server.ctx["extended"] = True
        return False
    if server.ctx.get("extended"):
        if message.mtype == b"H":
            server.send(_PARSE_COMPLETE)
            server.send(_NO_DATA)
        elif message.mtype == b"B":
            server.send(_BIND_COMPLETE)
        elif message.mtype == b"E":
            server.send(_command_complete(tag))
        elif message.mtype == b"S":
            server.send(_ready_for_query(rfq))
            server.ctx["extended"] = False
            return True
    return False


# ---------------------------------------------------------------------------
# Harness: wire a real asyncpg Connection to the mock over a socketpair.
# ---------------------------------------------------------------------------

def _make_params(*, command_timeout=None):
    _, params, config = connect_utils._parse_connect_arguments(
        dsn=None, host=_MOCK_ADDR, port=None, user="u",
        password=None, passfile=None, database="d",
        command_timeout=command_timeout, statement_cache_size=0,
        max_cached_statement_lifetime=0,
        max_cacheable_statement_size=0, ssl=None, direct_tls=None,
        server_settings=None, target_session_attrs=None,
        krbsrvname=None, gsslib=None, service=None, servicefile=None)
    return params, config


async def _start(handler, *, command_timeout=None):
    loop = asyncio.get_running_loop()
    client_sock, server_sock = socket.socketpair(
        socket.AF_UNIX, socket.SOCK_STREAM)
    client_sock.setblocking(False)
    server_sock.setblocking(False)

    server_transport, server = await loop.connect_accepted_socket(
        lambda: _MockPGServer(handler), server_sock)
    server._transport_obj = server_transport
    server._client_sock = client_sock

    params, config = _make_params(command_timeout=command_timeout)
    connected = loop.create_future()
    proto = apg_proto.Protocol(
        _MOCK_ADDR, connected, params, apg_proto.Record, loop)
    transport, _ = await loop.create_connection(
        lambda: proto, sock=client_sock)
    await connected
    con = Connection(proto, transport, loop, _MOCK_ADDR, config, params)
    proto.set_connection(con)
    return con, server


async def _shutdown(con, server):
    try:
        con.terminate()
    except Exception:
        pass
    try:
        server.close()
    except Exception:
        pass
    transport = getattr(server, "_transport_obj", None)
    if transport is not None and not transport.is_closing():
        transport.abort()
    client_sock = getattr(server, "_client_sock", None)
    if client_sock is not None:
        try:
            client_sock.close()
        except OSError:
            pass
    await asyncio.sleep(0)


def _client_types(server):
    return [m.mtype for m in server.history]


class _CancelController:
    """Patch point for the auxiliary CancelRequest connection.

    A real backend accepts the cancel on a second socket and then answers
    the cancellation on the main connection.  Binding a second address is
    unavailable here, so we emulate the two outcomes that matter to the
    main connection's state machine: a delivered request, or a failed one
    (which must make asyncpg discard the connection).
    """

    def __init__(self):
        self.mode = "ok"

    async def __call__(self, **kwargs):
        if self.mode == "fail":
            raise OSError("simulated cancel-connection failure")


_cancel_controller = _CancelController()
_REAL_CANCEL = connect_utils._cancel


@unittest.skipIf(
    not hasattr(socket, "AF_UNIX") or platform.system() == "Windows",
    SKIP_REASON)
class TestCopyFailureRecovery(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        _cancel_controller.mode = "ok"
        # Redirect the auxiliary cancel connection only for these tests so
        # the rest of the suite keeps the real implementation.
        connect_utils._cancel = _cancel_controller

    def tearDown(self):
        connect_utils._cancel = _REAL_CANCEL
        self.loop.close()
        asyncio.set_event_loop(None)

    def run_coro(self, coro):
        return self.loop.run_until_complete(
            asyncio.wait_for(coro, timeout=30))

    def finish(self, con, server):
        self.loop.run_until_complete(_shutdown(con, server))

    def wait_until_closed(self, con, timeout=2.0):
        async def _poll():
            deadline = self.loop.time() + timeout
            while not con.is_closed() and self.loop.time() < deadline:
                await asyncio.sleep(0.01)
            return con.is_closed()

        self.assertTrue(self.loop.run_until_complete(_poll()))

    # -- per-scenario handlers ------------------------------------------

    def _copy_in_handler(self, *, after_fail="rfq", rfq_status=b"I",
                         error_fields=None):
        """COPY IN whose input stream is aborted by the client.

        after_fail:
          * "rfq":  backend answers CopyFail with Error + ReadyForQuery and
                    then services the follow-up audit query;
          * "half": backend sends Error but never ReadyForQuery;
          * "drop": backend closes the transport after CopyFail.
        """
        error_fields = error_fields or _GENERIC_ERROR

        def handler(message, server):
            if _handle_handshake(message, server):
                return
            if message.mtype == b"Q" and b"COPY" in message.body:
                # The COPY ... FROM STDIN simple query.
                server.ctx["copy_active"] = True
                server.send(_copy_in_response())
            elif message.mtype == b"d":
                # Streamed CopyData: nothing to say until the client ends.
                return
            elif message.mtype == b"f":
                server.ctx["copy_fail"] = message
                server.ctx["copy_active"] = False
                if after_fail == "drop":
                    server.close()
                else:
                    # Defer so asyncpg reaches PROTOCOL_CANCELLED first.
                    if after_fail == "rfq":
                        server.send_later(
                            _error_response(error_fields)
                            + _ready_for_query(rfq_status))
                        server.ctx["ready_for_audit"] = True
                    else:  # "half": ErrorResponse without ReadyForQuery.
                        server.send_later(_error_response(error_fields))
            elif message.mtype == b"c":
                server.ctx["copy_done"] = message
            elif server.ctx.get("ready_for_audit"):
                _handle_audit_query(message, server)

        return handler

    def _copy_out_handler(self, *, mode="rfq", data_chunks=2):
        """COPY OUT whose consumer rejects the first data it receives."""

        def handler(message, server):
            if _handle_handshake(message, server):
                return
            if message.mtype == b"Q" and b"COPY" in message.body:
                server.send(_copy_out_response())
                for index in range(data_chunks):
                    server.send(
                        _copy_data("chunk{}\n".format(index).encode()))
                # Let the client consume the batch, run the failing
                # consumer and move into the cancelling state before the
                # backend's cancellation acknowledgement arrives.
                if mode == "drop":
                    async def _drop():
                        await asyncio.sleep(0.05)
                        server.close()
                    asyncio.ensure_future(_drop())
                elif mode == "rfq":
                    server.send_later(
                        _error_response(_GENERIC_ERROR)
                        + _ready_for_query(b"I"))
                    server.ctx["ready_for_audit"] = True
                else:  # half: ErrorResponse without ReadyForQuery.
                    server.send_later(_error_response(_GENERIC_ERROR))
            elif server.ctx.get("ready_for_audit"):
                _handle_audit_query(message, server)

        return handler

    def _copy_in_server_error_handler(self, *, sqlstate="23514",
                                      rfq_status=b"I"):
        """Backend accepts COPY, then errors while data is streaming."""

        def handler(message, server):
            if _handle_handshake(message, server):
                return
            if message.mtype == b"Q" and b"COPY" in message.body:
                server.send(_copy_in_response())
                # The constraint violation is reported after the COPY is
                # established; defer it so asyncpg is already in the
                # COPY_IN_DATA sub-protocol when the error arrives.
                server.ctx["error_sent"] = True
                server.send_later(
                    _error_response({
                        "S": "ERROR", "C": sqlstate,
                        "M": "new row violates check constraint"})
                    + _ready_for_query(rfq_status))
                server.ctx["ready_for_audit"] = True
            elif message.mtype in (b"d", b"f", b"c"):
                server.ctx.setdefault("trailing", []).append(message.mtype)
            elif server.ctx.get("ready_for_audit"):
                _handle_audit_query(message, server)

        return handler

    def _transaction_handler(self, *, rollback=True):
        """Explicit transaction whose COPY fails; backend tracks xact state."""

        def handler(message, server):
            if _handle_handshake(message, server):
                return
            if message.mtype == b"Q" and b"BEGIN" in message.body:
                server.send(_command_complete("BEGIN"))
                server.send(_ready_for_query(b"T"))  # in transaction
                server.ctx["in_xact"] = True
            elif message.mtype == b"Q" and b"COPY" in message.body:
                server.send(_copy_in_response())
                server.ctx["copy_active"] = True
            elif message.mtype == b"d":
                return
            elif message.mtype == b"f":
                server.ctx["copy_fail"] = message
                # Defer so asyncpg reaches PROTOCOL_CANCELLED before the
                # failed-transaction ReadyForQuery('E') is parsed.
                server.send_later(
                    _error_response({
                        "S": "ERROR", "C": "25P02",
                        "M": "current transaction is aborted"})
                    + _ready_for_query(b"E"))  # failed transaction
            elif message.mtype == b"Q" and b"ROLLBACK" in message.body:
                server.send(_command_complete("ROLLBACK"))
                server.send(_ready_for_query(b"I"))
                server.ctx["in_xact"] = False
                server.ctx["ready_for_audit"] = True
            elif server.ctx.get("ready_for_audit"):
                _handle_audit_query(message, server)

        return handler

    # -- helpers --------------------------------------------------------

    @staticmethod
    async def _failing_copy_in_source():
        yield b"row1\n"
        yield b"row2\n"
        yield b"row3\n"
        raise RuntimeError("input generator exploded")

    class _ConsumerAbort(Exception):
        pass

    async def _failing_consumer(self, _buffer):
        raise self._ConsumerAbort("consumer refuses more data")

    async def _audit_insert(self, con, server):
        # Trigger the follow-up command; the event-driven handler answers.
        return await con.execute("INSERT INTO audit_log VALUES (1)")

    # ------------------------------------------------------------------
    # Scenario A: input iterator raises after some blocks were streamed.
    # ------------------------------------------------------------------

    def test_copy_in_iterator_error_sends_copyfail_and_recovers(self):
        class _Boom(Exception):
            pass

        async def source():
            yield b"row1\n"
            yield b"row2\n"
            raise _Boom("input generator exploded")

        con, server = self.run_coro(_start(self._copy_in_handler()))
        try:
            with self.assertRaises(_Boom):
                self.run_coro(con.copy_to_table("data_t", source=source()))
            types_ = _client_types(server)
            self.assertIn(b"d", types_)       # blocks were streamed
            self.assertIn(b"f", types_)       # CopyFail was sent
            self.assertNotIn(b"c", types_)    # never a clean CopyDone
            self.assertIsNotNone(server.ctx.get("copy_fail"))
            self.assertTrue(server.ctx["copy_fail"].body.startswith(
                b"COPY aborted due to an exception in input generator"))
            self.assertFalse(con.is_closed())
            # The follow-up audit insert succeeds on the same connection,
            # and the session is back to idle (no leaked transaction).
            tag = self.run_coro(self._audit_insert(con, server))
            self.assertEqual(tag, "INSERT 0 1")
            self.assertFalse(con._protocol.is_in_transaction())
        finally:
            self.finish(con, server)

    def test_copy_in_half_response_eventually_marks_connection_dead(self):
        # ErrorResponse but no ReadyForQuery: the cancellation cannot be
        # acknowledged, so the command timeout terminates the stuck
        # connection and the next query cannot run on it.
        con, server = self.run_coro(_start(
            self._copy_in_handler(after_fail="half"),
            command_timeout=0.3))
        try:
            with self.assertRaises(RuntimeError):
                self.run_coro(con.copy_to_table(
                    "data_t", source=self._failing_copy_in_source()))
            with self.assertRaises(
                    (apg_errors.InterfaceError, asyncio.TimeoutError)):
                self.loop.run_until_complete(asyncio.wait_for(
                    con.execute("INSERT INTO audit_log VALUES (1)"),
                    timeout=1))
            # Once the cancellation deadline passes the connection is
            # discarded rather than left half-open.
            self.loop.run_until_complete(asyncio.sleep(0.05))
            self.assertTrue(con.is_closed())
        finally:
            self.finish(con, server)

    def test_copy_in_cancel_connection_failure_terminates(self):
        _cancel_controller.mode = "fail"
        con, server = self.run_coro(_start(self._copy_in_handler()))
        try:
            with self.assertRaises(RuntimeError):
                self.run_coro(con.copy_to_table(
                    "data_t", source=self._failing_copy_in_source()))
            self.loop.run_until_complete(asyncio.sleep(0.05))
            # A failed auxiliary cancel leaves protocol state uncertain:
            # asyncpg must tear the connection down rather than reuse it.
            self.assertTrue(con.is_closed())
            with self.assertRaises(apg_errors.InterfaceError):
                self.run_coro(con.execute("INSERT INTO audit_log VALUES(1)"))
        finally:
            self.finish(con, server)

    def test_copy_in_server_drop_is_reported_as_connection_loss(self):
        con, server = self.run_coro(_start(
            self._copy_in_handler(after_fail="drop")))
        try:
            with self.assertRaises(Exception):
                self.run_coro(con.copy_to_table(
                    "data_t", source=self._failing_copy_in_source()))
            self.wait_until_closed(con)
        finally:
            self.finish(con, server)

    # ------------------------------------------------------------------
    # Scenario B: output consumer is cancelled during COPY OUT.
    # ------------------------------------------------------------------

    def test_copy_out_consumer_cancel_recovers(self):
        con, server = self.run_coro(_start(
            self._copy_out_handler(mode="rfq")))
        try:
            with self.assertRaises(self._ConsumerAbort):
                self.run_coro(con.copy_from_table(
                    "data_t", output=self._failing_consumer))
            # COPY OUT has no CopyFail: the client abandons reading and
            # requests cancellation, recovering on ReadyForQuery.
            self.assertNotIn(b"f", _client_types(server))
            self.assertFalse(con.is_closed())
            tag = self.run_coro(self._audit_insert(con, server))
            self.assertEqual(tag, "INSERT 0 1")
            self.assertFalse(con._protocol.is_in_transaction())
        finally:
            self.finish(con, server)

    def test_copy_out_half_response_marks_connection_unusable(self):
        con, server = self.run_coro(_start(
            self._copy_out_handler(mode="half"),
            command_timeout=0.3))
        try:
            with self.assertRaises(self._ConsumerAbort):
                self.run_coro(con.copy_from_table(
                    "data_t", output=self._failing_consumer))
            with self.assertRaises(
                    (apg_errors.InterfaceError, asyncio.TimeoutError)):
                self.loop.run_until_complete(asyncio.wait_for(
                    con.execute("INSERT INTO audit_log VALUES (1)"),
                    timeout=2))
            self.assertTrue(con.is_closed())
        finally:
            self.finish(con, server)

    def test_copy_out_server_drop_tears_connection_down(self):
        # When the peer vanishes, the transport reports connection loss;
        # asyncpg must surface it and refuse to reuse the connection.
        con, server = self.run_coro(_start(
            self._copy_out_handler(mode="drop")))
        try:
            with self.assertRaises(self._ConsumerAbort):
                self.run_coro(con.copy_from_table(
                    "data_t", output=self._failing_consumer))
            self.wait_until_closed(con)
        finally:
            self.finish(con, server)

    def test_copy_out_cancel_failure_tears_connection_down(self):
        _cancel_controller.mode = "fail"
        con, server = self.run_coro(_start(
            self._copy_out_handler(mode="rfq")))
        try:
            with self.assertRaises(self._ConsumerAbort):
                self.run_coro(con.copy_from_table(
                    "data_t", output=self._failing_consumer))
            self.loop.run_until_complete(asyncio.sleep(0.05))
            self.assertTrue(con.is_closed())
        finally:
            self.finish(con, server)

    # ------------------------------------------------------------------
    # Scenario C: the server errors while the COPY is in flight.
    # ------------------------------------------------------------------

    @staticmethod
    async def _rows():
        for index in range(5):
            yield "row{}\n".format(index).encode()

    def test_server_error_during_copy_in_raises_postgres_error(self):
        con, server = self.run_coro(
            _start(self._copy_in_server_error_handler()))
        try:
            with self.assertRaises(apg_errors.PostgresError) as ctx:
                self.run_coro(
                    con.copy_to_table("data_t", source=self._rows()))
            self.assertEqual(ctx.exception.sqlstate, "23514")
            self.assertFalse(con.is_closed())
            tag = self.run_coro(self._audit_insert(con, server))
            self.assertEqual(tag, "INSERT 0 1")
            self.assertFalse(con._protocol.is_in_transaction())
        finally:
            self.finish(con, server)

    # ------------------------------------------------------------------
    # Scenario D: failure inside an explicit transaction.
    # ------------------------------------------------------------------

    class _TxBoom(Exception):
        pass

    async def _tx_source(self):
        yield b"row1\n"
        raise self._TxBoom("generator died")

    def test_copy_failure_in_transaction_rolls_back_and_recovers(self):

        async def body():
            con, server = await _start(self._transaction_handler())
            try:
                # Let the exception propagate out of the block so the
                # managed transaction issues ROLLBACK in __aexit__.
                with self.assertRaises(self._TxBoom):
                    async with con.transaction():
                        self.assertTrue(con._protocol.is_in_transaction())
                        await con.copy_to_table(
                            "data_t", source=self._tx_source())
                # ROLLBACK cleared the failed-transaction status.
                self.assertFalse(con._protocol.is_in_transaction())
                self.assertFalse(con.is_closed())
                self.assertIn(b"f", _client_types(server))
                tag = await self._audit_insert(con, server)
                self.assertEqual(tag, "INSERT 0 1")
            finally:
                await _shutdown(con, server)

        self.run_coro(body())

    def test_failed_transaction_status_is_visible_until_rollback(self):

        async def body():
            # No automatic ROLLBACK servicing: inspect the RFQ('E') state.
            con, server = await _start(
                self._transaction_handler(rollback=False))
            tx = con.transaction()
            try:
                await tx.start()
                with self.assertRaises(self._TxBoom):
                    await con.copy_to_table(
                        "data_t", source=self._tx_source())
                await asyncio.sleep(0.02)
                # Backend reported ReadyForQuery('E'): still inside a
                # failed transaction, but the connection itself is usable.
                self.assertTrue(con._protocol.is_in_transaction())
                self.assertFalse(con.is_closed())
            finally:
                await _shutdown(con, server)

        self.run_coro(body())


if __name__ == "__main__":
    unittest.main()
