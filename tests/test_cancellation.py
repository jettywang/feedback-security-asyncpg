# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncio
import socket
import ssl
import struct
import types
from unittest import mock

import asyncpg

from asyncpg import _testbase as tb
from asyncpg import connect_utils
from asyncpg import connection as pg_connection


class TestCancellation(tb.ConnectedTestCase):

    async def test_cancellation_01(self):
        st1000 = await self.con.prepare('SELECT 1000')

        async def test0():
            val = await self.con.execute('SELECT 42')
            self.assertEqual(val, 'SELECT 1')

        async def test1():
            val = await self.con.fetchval('SELECT 42')
            self.assertEqual(val, 42)

        async def test2():
            val = await self.con.fetchrow('SELECT 42')
            self.assertEqual(val, (42,))

        async def test3():
            val = await self.con.fetch('SELECT 42')
            self.assertEqual(val, [(42,)])

        async def test4():
            val = await self.con.prepare('SELECT 42')
            self.assertEqual(await val.fetchval(), 42)

        async def test5():
            self.assertEqual(await st1000.fetchval(), 1000)

        async def test6():
            self.assertEqual(await st1000.fetchrow(), (1000,))

        async def test7():
            self.assertEqual(await st1000.fetch(), [(1000,)])

        async def test8():
            cur = await st1000.cursor()
            self.assertEqual(await cur.fetchrow(), (1000,))

        for test in {test0, test1, test2, test3, test4, test5,
                     test6, test7, test8}:

            # Leave room for loaded CI runners while still detecting a
            # cancellation that waits for the 20-second pg_sleep to finish.
            with self.subTest(testfunc=test), self.assertRunUnder(5):
                st = await self.con.prepare('SELECT pg_sleep(20)')
                task = self.loop.create_task(st.fetch())
                await asyncio.sleep(0.05)
                task.cancel()

                with self.assertRaises(asyncio.CancelledError):
                    await task

                async with self.con.transaction():
                    await test()

    async def test_cancellation_02(self):
        st = await self.con.prepare('SELECT 1')
        task = self.loop.create_task(st.fetch())
        await asyncio.sleep(0.05)
        task.cancel()
        self.assertEqual(await task, [(1,)])

    async def test_cancellation_03(self):
        with self.assertRaises(asyncpg.InFailedSQLTransactionError):
            async with self.con.transaction():
                task = self.loop.create_task(
                    self.con.fetch('SELECT pg_sleep(20)'))
                await asyncio.sleep(0.05)
                task.cancel()

                with self.assertRaises(asyncio.CancelledError):
                    await task

                await self.con.fetch('SELECT generate_series(0, 100)')

        self.assertEqual(
            await self.con.fetchval('SELECT 42'),
            42)

    async def test_cancellation_04(self):
        await self.con.fetchval('SELECT pg_sleep(0)')
        waiter = asyncio.Future()
        self.con._cancel_current_command(waiter)
        await waiter
        self.assertEqual(await self.con.fetchval('SELECT 42'), 42)


class _FakeSocket:
    family = socket.AF_INET

    def __init__(self):
        self.options = []

    def setsockopt(self, *args):
        self.options.append(args)


class _FakeTransport:
    def __init__(self):
        self.writes = []
        self.closed = False
        self._closing = False

    def write(self, data):
        self.writes.append(bytes(data))

    def close(self):
        self.closed = True
        self._closing = True

    def is_closing(self):
        return self._closing

    def get_extra_info(self, name, default=None):
        if name == 'socket':
            return _FakeSocket()
        return default


class _FakeProtocol:
    def __init__(self, *, backend_pid=1111, backend_secret=2222):
        self.backend_pid = backend_pid
        self.backend_secret = backend_secret
        self.aborted = False

    def get_settings(self):
        return types.SimpleNamespace(server_version='16.0')

    def is_connected(self):
        return not self.aborted

    def is_in_transaction(self):
        return False

    def abort(self):
        self.aborted = True


def _make_params(**overrides):
    values = dict(
        user='user',
        password=None,
        database='db',
        ssl=None,
        sslmode=connect_utils.SSLMode.disable,
        ssl_negotiation=connect_utils.SSLNegotiation.postgres,
        server_settings=None,
        target_session_attrs=connect_utils.SessionAttribute.any,
        krbsrvname=None,
        gsslib='gssapi',
    )
    values.update(overrides)
    return connect_utils._ConnectionParameters(**values)


def _make_config(**overrides):
    values = dict(
        command_timeout=None,
        statement_cache_size=100,
        max_cached_statement_lifetime=None,
        max_cacheable_statement_size=1024,
    )
    values.update(overrides)
    return connect_utils._ClientConfiguration(**values)


class TestCancelRequestTargeting(tb.TestCase):
    """CancelRequest targeting with connection doubles (no real server)."""

    def _make_connection(self, *, addr=('db2', 5433), params=None,
                         cancel_params=None, protocol=None,
                         transport=None):
        con = pg_connection.Connection(
            protocol or _FakeProtocol(),
            transport or _FakeTransport(),
            self.loop,
            addr,
            _make_config(),
            params or _make_params(),
        )
        if cancel_params is not None:
            con._cancel_params = cancel_params
        self.addCleanup(con.terminate)
        return con

    def _patch_cancel(self):
        calls = []

        async def fake_cancel(**kwargs):
            calls.append(kwargs)

        return calls, mock.patch.object(connect_utils, '_cancel',
                                        fake_cancel)

    async def test_cancel_uses_selected_addr_and_negotiated_params(self):
        # The input params asked for sslmode=prefer, but the negotiated
        # transport fell back to plaintext; the CancelRequest must use
        # the negotiated params and the actually selected address.
        negotiated = _make_params()
        input_params = _make_params(
            ssl=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
            sslmode=connect_utils.SSLMode.prefer)
        con = self._make_connection(
            addr=('db2', 5433), params=input_params,
            cancel_params=negotiated)
        calls, patcher = self._patch_cancel()
        waiter = self.loop.create_future()
        with patcher:
            con._cancel_current_command(waiter)
            await waiter

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['addr'], ('db2', 5433))
        self.assertIs(calls[0]['params'], negotiated)
        self.assertEqual(calls[0]['backend_pid'], 1111)
        self.assertEqual(calls[0]['backend_secret'], 2222)
        self.assertFalse(con._cancellations)

    async def test_cancel_target_is_snapshotted(self):
        con = self._make_connection(addr=('db1', 5432))
        snapshot_addr = con._addr
        snapshot_params = con._cancel_params
        calls, patcher = self._patch_cancel()
        waiter = self.loop.create_future()
        with patcher:
            con._cancel_current_command(waiter)
            # Mutating the connection after the cancellation was
            # requested must not redirect the CancelRequest.
            con._addr = ('standby', 5433)
            con._cancel_params = _make_params()
            await waiter

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['addr'], snapshot_addr)
        self.assertIs(calls[0]['params'], snapshot_params)

    async def test_cancel_not_sent_after_backend_disconnect(self):
        transport = _FakeTransport()
        con = self._make_connection(transport=transport)
        calls, patcher = self._patch_cancel()
        waiter = self.loop.create_future()
        with patcher:
            con._cancel_current_command(waiter)
            # The backend connection drops before the background
            # cancellation task gets to run.
            transport.close()
            await waiter

        self.assertEqual(calls, [])

    async def test_cancel_not_sent_after_connection_takeover(self):
        con = self._make_connection()
        calls, patcher = self._patch_cancel()
        waiter = self.loop.create_future()
        with patcher:
            con._cancel_current_command(waiter)
            # A new connection takes over before the background
            # cancellation task runs: the old cancel key must not be
            # sent to the new backend.
            con._protocol = _FakeProtocol(backend_pid=9999,
                                          backend_secret=8888)
            con._addr = ('standby', 5433)
            await waiter

        self.assertEqual(calls, [])

    async def test_cancel_not_sent_after_terminate(self):
        con = self._make_connection()
        calls, patcher = self._patch_cancel()
        waiter = self.loop.create_future()
        with patcher:
            con._cancel_current_command(waiter)
            con.terminate()
            # Let the cancelled background task settle.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        self.assertEqual(calls, [])
        self.assertTrue(con._aborted)
        self.assertFalse(con._cancellations)

    async def test_cancel_on_terminated_connection_completes(self):
        con = self._make_connection()
        con.terminate()
        calls, patcher = self._patch_cancel()
        waiter = self.loop.create_future()
        ack = self.loop.create_future()
        with patcher:
            con._cancel_current_command(waiter, ack)
            # The cancel task must not hang waiting for an
            # acknowledgement that will never arrive.
            await asyncio.wait_for(waiter, 1)

        self.assertEqual(calls, [])
        self.assertFalse(con._cancellations)

    async def test_cancel_failure_terminates_connection(self):
        con = self._make_connection()

        async def failing_cancel(**kwargs):
            raise OSError('cancel connection failed')

        waiter = self.loop.create_future()
        with mock.patch.object(connect_utils, '_cancel', failing_cancel):
            con._cancel_current_command(waiter)
            await waiter

        # A failed CancelRequest must not be treated as a successfully
        # cancelled query: the original connection is terminated and its
        # query fails instead of completing normally.
        self.assertTrue(con._aborted)
        self.assertFalse(con._cancellations)

    async def test_cancel_connection_reset_is_tolerated(self):
        con = self._make_connection()

        async def resetting_cancel(**kwargs):
            raise ConnectionResetError('reset by server')

        waiter = self.loop.create_future()
        with mock.patch.object(connect_utils, '_cancel', resetting_cancel):
            con._cancel_current_command(waiter)
            await waiter

        self.assertFalse(con._aborted)


class TestCancelRequestTransport(tb.TestCase):
    """TLS/direct negotiation strategy of the cancel connection."""

    def _run_cancel(self, *, addr, params, pid=10, secret=20):
        return connect_utils._cancel(
            loop=self.loop, addr=addr, params=params,
            backend_pid=pid, backend_secret=secret)

    def _tcp_connector(self, recorded, *, fail=None, lost_exc=None):
        transports = []

        async def create_connection(protocol_factory, *args, **kwargs):
            recorded.append((args, kwargs))
            if fail is not None:
                raise fail
            proto = protocol_factory()
            transport = _FakeTransport()
            transports.append(transport)
            if lost_exc is not None:
                self.loop.call_soon(proto.connection_lost, lost_exc)
            else:
                self.loop.call_soon(proto.on_disconnect.set_result, True)
            return transport, proto

        create_connection.transports = transports
        return create_connection

    async def test_cancel_request_plain_tcp(self):
        recorded = []
        connector = self._tcp_connector(recorded)
        with mock.patch.object(self.loop, 'create_connection', connector):
            await self._run_cancel(addr=('db1', 5444),
                                   params=_make_params())

        self.assertEqual(len(recorded), 1)
        args, kwargs = recorded[0]
        self.assertEqual(args, ('db1', 5444))
        self.assertNotIn('ssl', kwargs)
        transport = connector.transports[0]
        self.assertTrue(transport.closed)
        msg, = transport.writes
        self.assertEqual(struct.unpack('!llll', msg),
                         (16, 80877102, 10, 20))

    async def test_cancel_request_unix_socket(self):
        recorded = []

        async def create_unix_connection(protocol_factory, path):
            recorded.append(path)
            proto = protocol_factory()
            transport = _FakeTransport()
            self.loop.call_soon(proto.on_disconnect.set_result, True)
            return transport, proto

        with mock.patch.object(self.loop, 'create_unix_connection',
                               create_unix_connection):
            await self._run_cancel(addr='/tmp/.s.PGSQL.5432',
                                   params=_make_params())

        self.assertEqual(recorded, ['/tmp/.s.PGSQL.5432'])

    async def test_cancel_request_starttls(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        cases = (
            (connect_utils.SSLMode.require, False),
            (connect_utils.SSLMode.prefer, True),
        )
        for sslmode, expected_advisory in cases:
            with self.subTest(sslmode=sslmode):
                recorded = []

                async def fake_ssl_connection(protocol_factory, host, port,
                                              *, loop, ssl_context,
                                              ssl_is_advisory):
                    recorded.append((host, port, ssl_context,
                                     ssl_is_advisory))
                    proto = protocol_factory()
                    transport = _FakeTransport()
                    loop.call_soon(proto.on_disconnect.set_result, True)
                    return transport, proto

                params = _make_params(
                    ssl=ctx, sslmode=sslmode,
                    ssl_negotiation=connect_utils.SSLNegotiation.postgres)
                with mock.patch.object(connect_utils,
                                       '_create_ssl_connection',
                                       fake_ssl_connection):
                    await self._run_cancel(addr=('db1', 5444),
                                           params=params)

                self.assertEqual(
                    recorded, [('db1', 5444, ctx, expected_advisory)])

    async def test_cancel_request_direct_tls(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        recorded = []
        connector = self._tcp_connector(recorded)
        params = _make_params(
            ssl=ctx, sslmode=connect_utils.SSLMode.require,
            ssl_negotiation=connect_utils.SSLNegotiation.direct)

        async def no_starttls(*args, **kwargs):
            raise AssertionError(
                'STARTTLS must not be used for a direct TLS connection')

        with mock.patch.object(self.loop, 'create_connection',
                               connector), \
                mock.patch.object(connect_utils, '_create_ssl_connection',
                                  no_starttls):
            await self._run_cancel(addr=('db1', 5444), params=params)

        self.assertEqual(len(recorded), 1)
        args, kwargs = recorded[0]
        self.assertEqual(args, ('db1', 5444))
        self.assertIs(kwargs['ssl'], ctx)

    async def test_cancel_request_connect_error_propagates(self):
        connector = self._tcp_connector(
            [], fail=ConnectionRefusedError('refused'))
        with mock.patch.object(self.loop, 'create_connection', connector):
            with self.assertRaises(ConnectionRefusedError):
                await self._run_cancel(addr=('db1', 5444),
                                       params=_make_params())

    async def test_cancel_request_transport_error_propagates(self):
        connector = self._tcp_connector([], lost_exc=OSError('boom'))
        with mock.patch.object(self.loop, 'create_connection', connector):
            with self.assertRaises(OSError):
                await self._run_cancel(addr=('db1', 5444),
                                       params=_make_params())

    async def test_cancel_request_reset_propagates(self):
        # Connection._cancel treats a reset after the CancelRequest as
        # delivered, but connect_utils._cancel must not hide it as a
        # clean disconnect.
        connector = self._tcp_connector(
            [], lost_exc=ConnectionResetError('reset'))
        with mock.patch.object(self.loop, 'create_connection', connector):
            with self.assertRaises(ConnectionResetError):
                await self._run_cancel(addr=('db1', 5444),
                                       params=_make_params())
