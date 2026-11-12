# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

"""Offline replay of SET ROLE / search_path cache interactions.

These tests never open a socket and never start PostgreSQL.  They drive the
real asyncpg Cython protocol through the in-memory peer in ``_fakepg`` so the
behaviour can be replayed anywhere (CI without a database, an air-gapped
review machine, etc.).

The modelled database has one unqualified table visible through
``search_path`` in two schemas with incompatible column types::

    schema_a.items(id int4)   -> binary int4
    schema_b.items(id text)   -> text

Switching ``search_path`` after a statement has been prepared makes the
stale server plan fail with SQLSTATE 0A000 raised from
``RevalidateCachedQuery``, which asyncpg maps to
:class:`~asyncpg.exceptions.InvalidCachedStatementError`.
"""

import asyncio
import unittest

from asyncpg import connect_utils
from asyncpg import exceptions
from asyncpg.connection import Connection
from asyncpg.protocol import Record
from asyncpg.protocol import protocol
from asyncpg import pool as apg_pool

from tests import _fakepg


QUERY = "SELECT id FROM items"


def _config(statement_cache_size=100):
    return connect_utils._ClientConfiguration(
        command_timeout=None,
        statement_cache_size=statement_cache_size,
        max_cached_statement_lifetime=0,
        max_cacheable_statement_size=1024 * 15,
    )


def _params():
    return connect_utils._ConnectionParameters(
        user="alice",
        password=None,
        database="appdb",
        ssl=None,
        sslmode=None,
        ssl_negotiation=None,
        server_settings=None,
        target_session_attrs=None,
        krbsrvname=None,
        gsslib=None,
    )


async def connect_fake(loop, server, *, statement_cache_size=100):
    connected = loop.create_future()
    proto = protocol.Protocol(
        ("fake", 0), connected, _params(), Record, loop)
    transport = server.attach(loop, proto)
    await connected
    con = Connection(
        proto, transport, loop, ("fake", 0),
        _config(statement_cache_size), _params())
    proto.set_connection(con)
    return con


class OfflineCacheTests(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(None)

    def run_coro(self, coro):
        return self.loop.run_until_complete(coro)

    async def _connect(self, **kwargs):
        self.server = _fakepg.FakePgServer()
        return await connect_fake(self.loop, self.server, **kwargs)

    # 1. ordinary fetch: cache hit, then transparent re-prepare -----------
    def test_fetch_cache_hit_and_silent_reprepare(self):
        async def scenario():
            con = await self._connect()
            try:
                first = await con.fetchrow(QUERY)
                self.assertEqual(tuple(first), (42,))
                cached = list(con._stmt_cache.iter_statements())
                self.assertEqual(len(cached), 1)
                self.assertFalse(cached[0].closed)

                # A second identical fetch must reuse the cached plan: no new
                # Parse message reaches the server.
                parses_before = self.server.events.count(b"P")
                await con.fetchrow(QUERY)
                self.assertEqual(self.server.events.count(b"P"),
                                 parses_before)

                await con.execute("SET search_path TO schema_b")
                # The stale plan now fails with 0A000; outside a transaction
                # asyncpg drops the cache, re-prepares once and retries.
                second = await con.fetchrow(QUERY)
                self.assertEqual(tuple(second), ("fourty-two",))

                refreshed = list(con._stmt_cache.iter_statements())
                self.assertTrue(all(s.closed is False for s in refreshed))
                return self.server
            finally:
                await con.close()

        server = self.run_coro(scenario())
        # One failed Bind/Execute round trip and a fresh Parse are observable.
        self.assertIn(b"E", server.events)
        self.assertGreaterEqual(server.events.count(b"P"), 2)

    # 2. same invalidation inside a transaction is fatal for the txn ------
    def test_invalidation_inside_transaction_raises(self):
        async def scenario():
            con = await self._connect()
            try:
                await con.fetchrow(QUERY)
                await con.execute("SET search_path TO schema_b")

                async with con.transaction():
                    self.assertTrue(con._protocol.is_in_transaction())
                    with self.assertRaises(
                            exceptions.InvalidCachedStatementError):
                        await con.fetchrow(QUERY)
                    # The server has marked the block aborted; the protocol
                    # mirrors ReadyForQuery status byte 'E' and still reports
                    # itself as "in a transaction" (PQTRANS_INERROR).
                    self.assertTrue(con._protocol.is_in_transaction())

                # Leaving the block rolled the failed transaction back, so the
                # connection is idle again and the cache had been dropped.
                self.assertFalse(con._protocol.is_in_transaction())

                # A fresh fetch works: the statement is re-prepared on demand
                # against the now-visible text-typed table.
                row = await con.fetchrow(QUERY)
                self.assertEqual(tuple(row), ("fourty-two",))
            finally:
                await con.close()

        self.run_coro(scenario())

    # 3. explicit prepare() handle is NOT in the LRU cache and never
    #    retries or self-repairs on plan invalidation ---------------------
    def test_explicit_prepare_handle_is_not_reused_after_change(self):
        async def scenario():
            con = await self._connect()
            try:
                stmt = await con.prepare(QUERY)
                # Explicit prepare uses use_cache=False: nothing enters the
                # per-connection LRU statement cache.
                self.assertEqual(len(con._stmt_cache), 0)
                self.assertFalse(stmt._state.closed)

                await con.execute("SET search_path TO schema_b")
                # There is no transparent retry for an explicit handle:
                # PreparedStatement.__do_execute only special-cases
                # OutdatedSchemaCacheError. InvalidCachedStatementError (0A000)
                # propagates straight to the caller.
                with self.assertRaises(
                        exceptions.InvalidCachedStatementError):
                    await stmt.fetch()
                # Unlike OutdatedSchemaCacheError, the state is NOT marked
                # closed; the stale handle simply keeps failing against the
                # revalidated plan until the caller drops it.
                self.assertFalse(stmt._state.closed)
                with self.assertRaises(
                        exceptions.InvalidCachedStatementError):
                    await stmt.fetchrow()

                # The correct recovery is to discard the old handle and
                # prepare again; the new statement describes the text column.
                rebuilt = await con.prepare(QUERY)
                self.assertEqual(tuple(await rebuilt.fetchrow()),
                                 ("fourty-two",))
            finally:
                await con.close()

        self.run_coro(scenario())

    # 4. SET ROLE is session state: pool release runs RESET ALL ----------
    def test_pool_release_resets_role_and_search_path(self):
        async def scenario():
            con = await self._connect()
            try:
                await con.fetchrow(QUERY)
                self.assertEqual(len(con._stmt_cache), 1)

                await con.execute("SET ROLE TO tenant_a")
                await con.execute("SET search_path TO schema_b")
                self.assertEqual(self.server.role, "tenant_a")
                self.assertEqual(self.server.search_path, "schema_b")

                # This is exactly what PoolConnectionHolder._release() invokes
                # before handing the physical connection to another acquirer.
                await con.reset()

                # get_reset_query() emits RESET ALL on a standard PostgreSQL
                # server; the peer models it as restoring session defaults.
                self.assertIsNone(self.server.role)
                self.assertEqual(self.server.search_path, "schema_a")
                # RESET ALL does not DEALLOCATE prepared statements, so the
                # client-side LRU cache entries survive a pool round trip.
                self.assertEqual(len(con._stmt_cache), 1)
            finally:
                await con.close()

        self.run_coro(scenario())

    # 5. an open transaction is rolled back on release --------------------
    def test_pool_release_rolls_back_open_transaction(self):
        async def scenario():
            con = await self._connect()
            try:
                async with con.transaction():
                    self.assertTrue(con._protocol.is_in_transaction())
                    # Simulate a forgotten block at release time. Connection
                    # ._reset() (called by the pool holder) issues ROLLBACK.
                    await con._reset()
                    self.assertFalse(con._protocol.is_in_transaction())
            finally:
                await con.close()

        self.run_coro(scenario())

    # 6. cache disabled (statement_cache_size=0): anonymous statements ----
    def test_disabled_cache_uses_anonymous_statements(self):
        async def scenario():
            con = await self._connect(statement_cache_size=0)
            try:
                self.assertFalse(con._stmt_cache_enabled)
                await con.fetchrow(QUERY)
                self.assertEqual(len(con._stmt_cache), 0)
                # Every execution re-parses an anonymous statement.
                await con.fetchrow(QUERY)
                self.assertGreaterEqual(self.server.events.count(b"P"), 2)
            finally:
                await con.close()

        self.run_coro(scenario())

    # 7. pool-wide invalidation: one bad connection clears the caches of
    #    every connection in the pool (mirrors test_cache_invalidation.py
    #    test_prepare_cache_invalidation_in_pool, fully offline) -----------
    def test_pool_wide_cache_invalidation_and_release_reset(self):
        async def scenario():
            loop = asyncio.get_running_loop()
            # The pool opens two separate protocol/connection pairs, each with
            # its own scriptable backend peer.
            servers = []

            async def fake_connect(*args, **kwargs):
                peer = _fakepg.FakePgServer()
                servers.append(peer)
                return await connect_fake(
                    loop, peer,
                    statement_cache_size=kwargs.get(
                        "statement_cache_size", 100))

            p = apg_pool.Pool(
                min_size=2, max_size=2, max_queries=999999,
                max_inactive_connection_lifetime=0,
                loop=loop,
                connection_class=Connection,
                record_class=Record,
                connect=fake_connect)
            try:
                await p._async__init__()
                con1 = await p.acquire()
                con2 = await p.acquire()
                await con1.fetchrow(QUERY)
                await con2.fetchrow(QUERY)
                self.assertEqual(len(con1._stmt_cache), 1)
                self.assertEqual(len(con2._stmt_cache), 1)

                # Schema switch invalidates the cached plan everywhere.
                await con1.execute("SET search_path TO schema_b")
                row = await con1.fetchrow(QUERY)
                self.assertEqual(tuple(row), ("fourty-two",))

                # _drop_global_statement_cache delegated to the pool clears
                # the local cache of *all* live connections, including con2.
                self.assertEqual(len(con1._stmt_cache), 1)
                self.assertEqual(len(con2._stmt_cache), 0)

                # Releasing runs Connection.reset() (RESET ALL) before the
                # physical connection becomes available to another acquirer,
                # so role/search_path never leak across borrowers.
                await p.release(con2)
                await p.release(con1)
                for peer in servers:
                    self.assertEqual(peer.search_path, "schema_a")
            finally:
                await p.close()

        self.run_coro(scenario())


if __name__ == "__main__":
    unittest.main()
