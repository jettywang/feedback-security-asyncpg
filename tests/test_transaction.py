# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import asyncpg

from asyncpg import _testbase as tb


class TestTransaction(tb.ConnectedTestCase):

    async def test_transaction_regular(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())
        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaises(ZeroDivisionError):
            async with tr as with_tr:
                self.assertIs(self.con._top_xact, tr)
                self.assertTrue(self.con.is_in_transaction())

                # We don't return the transaction object from __aenter__,
                # to make it harder for people to use '.rollback()' and
                # '.commit()' from within an 'async with' block.
                self.assertIsNone(with_tr)

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                1 / 0

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_nested(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaises(ZeroDivisionError):
            async with tr:
                self.assertIs(self.con._top_xact, tr)
                self.assertTrue(self.con.is_in_transaction())

                await self.con.execute('''
                    CREATE TABLE mytab (a int);
                ''')

                async with self.con.transaction():
                    self.assertIs(self.con._top_xact, tr)
                    self.assertTrue(self.con.is_in_transaction())

                    await self.con.execute('''
                        INSERT INTO mytab (a) VALUES (1), (2);
                    ''')

                self.assertIs(self.con._top_xact, tr)
                self.assertTrue(self.con.is_in_transaction())

                with self.assertRaises(ZeroDivisionError):
                    in_tr = self.con.transaction()
                    async with in_tr:

                        self.assertIs(self.con._top_xact, tr)
                        self.assertTrue(self.con.is_in_transaction())

                        await self.con.execute('''
                            INSERT INTO mytab (a) VALUES (3), (4);
                        ''')

                        1 / 0

                st = await self.con.prepare('SELECT * FROM mytab;')

                recs = []
                async for rec in st.cursor():
                    recs.append(rec)

                self.assertEqual(len(recs), 2)
                self.assertEqual(recs[0][0], 1)
                self.assertEqual(recs[1][0], 2)

                self.assertIs(self.con._top_xact, tr)
                self.assertTrue(self.con.is_in_transaction())

                1 / 0

        self.assertIs(self.con._top_xact, None)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.PostgresError,
                                    '"mytab" does not exist'):
            await self.con.prepare('''
                SELECT * FROM mytab
            ''')

    async def test_transaction_interface_errors(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction(readonly=True, isolation='serializable')
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot start; .* already started'):
            async with tr:
                await tr.start()

        self.assertTrue(repr(tr).startswith(
            '<asyncpg.Transaction state:rolledback serializable readonly'))

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot start; .* already rolled back'):
            async with tr:
                pass

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot manually commit.*async with'):
            async with tr:
                await tr.commit()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot manually rollback.*async with'):
            async with tr:
                await tr.rollback()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        tr = self.con.transaction()
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot enter context:.*async with'):
            async with tr:
                async with tr:
                    pass

    async def test_transaction_within_manual_transaction(self):
        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

        await self.con.execute('BEGIN')

        tr = self.con.transaction()
        self.assertIsNone(self.con._top_xact)
        self.assertTrue(self.con.is_in_transaction())

        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'cannot use Connection.transaction'):
            await tr.start()

        with self.assertLoopErrorHandlerCalled(
                'Resetting connection with an active transaction'):
            await self.con.reset()

        self.assertIsNone(self.con._top_xact)
        self.assertFalse(self.con.is_in_transaction())

    async def test_isolation_level(self):
        await self.con.reset()
        default_isolation = await self.con.fetchval(
            'SHOW default_transaction_isolation'
        )
        isolation_levels = {
            None: default_isolation,
            'read_committed': 'read committed',
            'read_uncommitted': 'read uncommitted',
            'repeatable_read': 'repeatable read',
            'serializable': 'serializable',
        }
        set_sql = 'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL '
        get_sql = 'SHOW TRANSACTION ISOLATION LEVEL'
        for tx_level in isolation_levels:
            for conn_level in isolation_levels:
                with self.subTest(conn=conn_level, tx=tx_level):
                    if conn_level:
                        await self.con.execute(
                            set_sql + isolation_levels[conn_level]
                        )
                    level = await self.con.fetchval(get_sql)
                    self.assertEqual(level, isolation_levels[conn_level])
                    async with self.con.transaction(isolation=tx_level):
                        level = await self.con.fetchval(get_sql)
                        self.assertEqual(
                            level,
                            isolation_levels[tx_level or conn_level],
                        )
                    await self.con.reset()

    async def test_nested_isolation_level(self):
        set_sql = 'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL '
        isolation_levels = {
            'read_committed': 'read committed',
            'read_uncommitted': 'read uncommitted',
            'repeatable_read': 'repeatable read',
            'serializable': 'serializable',
        }
        for inner in [None] + list(isolation_levels):
            for outer, outer_sql_level in isolation_levels.items():
                for implicit in [False, True]:
                    with self.subTest(
                        implicit=implicit, outer=outer, inner=inner,
                    ):
                        if implicit:
                            await self.con.execute(set_sql + outer_sql_level)
                            outer_level = None
                        else:
                            outer_level = outer

                        async with self.con.transaction(isolation=outer_level):
                            if inner and outer != inner:
                                with self.assertRaisesRegex(
                                    asyncpg.InterfaceError,
                                    'current {!r} != outer {!r}'.format(
                                        inner, outer
                                    )
                                ):
                                    async with self.con.transaction(
                                            isolation=inner,
                                    ):
                                        pass
                            else:
                                async with self.con.transaction(
                                        isolation=inner,
                                ):
                                    pass

    async def test_nested_readonly_deferrable(self):
        modes = ['readonly', 'deferrable']
        for mode in modes:
            for outer_mode in [False, True]:
                for inner_mode in [False, True]:
                    with self.subTest(
                        mode=mode, outer=outer_mode, inner=inner_mode,
                    ):
                        kwargs = {mode: outer_mode}
                        async with self.con.transaction(**kwargs):
                            kwargs = {mode: inner_mode}
                            if inner_mode and not outer_mode:
                                with self.assertRaisesRegex(
                                    asyncpg.InterfaceError,
                                    'nested transaction cannot be {}'.format(
                                        mode),
                                ):
                                    async with self.con.transaction(
                                            **kwargs):
                                        pass
                            else:
                                async with self.con.transaction(**kwargs):
                                    pass

    async def test_nested_readonly_implicit(self):
        # A nested readonly transaction is allowed when the outer
        # transaction is read-only via the session default.
        await self.con.execute(
            'SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY')
        try:
            async with self.con.transaction():
                async with self.con.transaction(readonly=True):
                    pass
        finally:
            await self.con.reset()


class FakeProtocol:
    def __init__(self, in_transaction):
        self._in_transaction = in_transaction

    def is_in_transaction(self):
        return self._in_transaction


class FakeConnection:
    """A stand-in for Connection that records executed SQL."""

    def __init__(self, *, in_transaction=False,
                 transaction_isolation='read committed',
                 transaction_read_only='off',
                 transaction_deferrable='off'):
        self._pool_release_ctr = 0
        self._top_xact = None
        self._protocol = FakeProtocol(in_transaction)
        self._transaction_isolation = transaction_isolation
        self._transaction_read_only = transaction_read_only
        self._transaction_deferrable = transaction_deferrable
        self._uid = 0
        # Each entry is (query, top_xact) where top_xact is the
        # connection's _top_xact at the moment the query was executed.
        self.query_log = []
        self.show_log = []

    def is_closed(self):
        return False

    def _get_unique_id(self, prefix):
        self._uid += 1
        return '__asyncpg_{}_{:x}__'.format(prefix, self._uid)

    async def execute(self, query, *args, **kwargs):
        self.query_log.append((query, self._top_xact))
        return 'OK'

    async def fetchval(self, query, *args, **kwargs):
        self.show_log.append(query)
        if query == 'SHOW transaction_isolation;':
            return self._transaction_isolation
        elif query == 'SHOW transaction_read_only;':
            return self._transaction_read_only
        elif query == 'SHOW transaction_deferrable;':
            return self._transaction_deferrable
        raise AssertionError('unexpected query: {!r}'.format(query))

    def queries(self):
        return [query for query, _ in self.query_log]


class TestTransactionNestingWithFakeConnection(tb.TestCase):

    def make_transaction(self, con, isolation=None, readonly=False,
                         deferrable=False):
        # Mirror what Connection.transaction() does.
        return asyncpg.transaction.Transaction(
            con, isolation, readonly, deferrable)

    async def test_fake_nested_readonly_rejected(self):
        con = FakeConnection()
        outer = self.make_transaction(con)
        await outer.start()
        self.assertIs(con._top_xact, outer)

        inner = self.make_transaction(con, readonly=True)
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'nested transaction cannot be readonly'):
            await inner.start()

        # The rejection must happen before any SAVEPOINT is created,
        # and the connection's transaction stack must be untouched.
        self.assertEqual(con.queries(), ['BEGIN;'])
        self.assertIs(con._top_xact, outer)
        self.assertEqual(con.show_log, ['SHOW transaction_read_only;'])

        # The outer transaction is still fully functional.
        await outer.commit()
        self.assertEqual(con.queries(), ['BEGIN;', 'COMMIT;'])
        self.assertIsNone(con._top_xact)

    async def test_fake_nested_readonly_allowed_in_readonly_outer(self):
        con = FakeConnection()
        outer = self.make_transaction(con, readonly=True, deferrable=True)
        await outer.start()

        inner = self.make_transaction(con, readonly=True, deferrable=True)
        await inner.start()
        self.assertIs(con._top_xact, outer)

        await inner.commit()
        await outer.commit()

        self.assertEqual(con.queries(), [
            'BEGIN READ ONLY DEFERRABLE;',
            'SAVEPOINT __asyncpg_savepoint_1__;',
            'RELEASE SAVEPOINT __asyncpg_savepoint_1__;',
            'COMMIT;',
        ])
        # No SHOW queries are needed when the outer transaction
        # explicitly carries the requested modes.
        self.assertEqual(con.show_log, [])
        self.assertIsNone(con._top_xact)

    async def test_fake_nested_readonly_allowed_by_session_default(self):
        con = FakeConnection(transaction_read_only='on')
        async with self.make_transaction(con):
            async with self.make_transaction(con, readonly=True):
                pass

        self.assertEqual(con.queries(), [
            'BEGIN;',
            'SAVEPOINT __asyncpg_savepoint_1__;',
            'RELEASE SAVEPOINT __asyncpg_savepoint_1__;',
            'COMMIT;',
        ])
        self.assertEqual(con.show_log, ['SHOW transaction_read_only;'])
        self.assertIsNone(con._top_xact)

    async def test_fake_nested_deferrable_rejected(self):
        con = FakeConnection()
        outer = self.make_transaction(con, readonly=True)
        await outer.start()

        inner = self.make_transaction(con, deferrable=True)
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'nested transaction cannot be deferrable'):
            await inner.start()

        self.assertEqual(con.queries(), ['BEGIN READ ONLY;'])
        self.assertIs(con._top_xact, outer)

        await outer.rollback()
        self.assertEqual(con.queries(), ['BEGIN READ ONLY;', 'ROLLBACK;'])
        self.assertIsNone(con._top_xact)

    async def test_fake_nested_isolation_mismatch_rejected(self):
        con = FakeConnection()
        outer = self.make_transaction(con, isolation='serializable')
        await outer.start()

        inner = self.make_transaction(con, isolation='read_committed')
        with self.assertRaisesRegex(
                asyncpg.InterfaceError,
                "current 'read_committed' != outer 'serializable'"):
            await inner.start()

        self.assertEqual(
            con.queries(), ['BEGIN ISOLATION LEVEL SERIALIZABLE;'])
        self.assertIs(con._top_xact, outer)

        await outer.commit()
        self.assertEqual(con.queries(), [
            'BEGIN ISOLATION LEVEL SERIALIZABLE;',
            'COMMIT;',
        ])
        self.assertIsNone(con._top_xact)

    async def test_fake_audit_helper_in_readwrite_transaction(self):
        # The audit helper scenario: a readonly helper nested in a
        # read-write `async with` transaction must fail loudly, and
        # the outer transaction must still exit normally.
        con = FakeConnection()

        async def audit_helper():
            async with self.make_transaction(con, readonly=True):
                pass  # pragma: no cover

        outer = self.make_transaction(con)
        async with outer:
            self.assertIs(con._top_xact, outer)
            with self.assertRaisesRegex(asyncpg.InterfaceError,
                                        'cannot be readonly'):
                await audit_helper()
            # The outer transaction is unaffected by the rejection.
            self.assertIs(con._top_xact, outer)

        self.assertEqual(con.queries(), ['BEGIN;', 'COMMIT;'])
        self.assertIsNone(con._top_xact)

    async def test_fake_manual_transaction_rejected(self):
        con = FakeConnection(in_transaction=True)
        tr = self.make_transaction(con, readonly=True)
        with self.assertRaisesRegex(asyncpg.InterfaceError,
                                    'manually started transaction'):
            await tr.start()

        # No SQL may be issued by asyncpg in a manually started
        # transaction.
        self.assertEqual(con.queries(), [])
        self.assertEqual(con.show_log, [])
        self.assertIsNone(con._top_xact)
