.. _asyncpg-role-search-path-cache:

==================================================
``SET ROLE`` / ``search_path`` and statement reuse
==================================================

This note explains what asyncpg keeps, discards and rebuilds when one
physical connection changes ``SET ROLE`` or ``search_path`` and then
executes a statement that was prepared earlier. It is written for the
multi-tenant pattern where two schemas contain a table with the same
name but *different column types*, for example::

    CREATE SCHEMA tenant_a;
    CREATE SCHEMA tenant_b;
    CREATE TABLE tenant_a.accounts(id int,      payload int);
    CREATE TABLE tenant_b.accounts(id int,      payload text);

A prepared statement is bound to the objects that were visible through
``search_path`` at prepare time. If ``search_path`` later makes a
different, type-incompatible table visible, PostgreSQL rejects the stale
plan with SQLSTATE ``0A000`` raised from ``RevalidateCachedQuery``
("cached plan must not change result type"). asyncpg maps that to
:class:`~asyncpg.exceptions.InvalidCachedStatementError`.

The rules below describe how the three layers -- the per-connection LRU
statement cache, explicit prepared-statement handles, and the pool --
react to that error.

Three cache paths
-----------------

1. Implicit cache used by ``fetch`` / ``fetchrow`` / ``execute``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Connection.fetch()`` and friends resolve the query through
``Connection._get_statement()``. Statements are keyed by the exact query
text, the record class and the codec flag, and held in an ordered-dict
LRU cache (``Connection._stmt_cache``). On a cache hit no ``Parse``
message is sent; the cached server-side statement name is reused.

When execution raises ``InvalidCachedStatementError``,
``Connection._do_execute()``:

* calls ``Connection._drop_global_statement_cache()``, which, for a pooled
  connection, clears the local statement cache of **every** live
  connection in the pool (``Pool._drop_statement_cache()``);
* retries **once** with a fresh prepare, but only when no transaction is
  in progress and the retry budget is still available;
* re-raises immediately when the connection is inside a transaction.

So an ordinary ``fetch`` outside a transaction recovers transparently.
The *connection* stays open and reusable; only the cached statement is
replaced.

2. Explicit ``Connection.prepare()`` handles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Connection.prepare()`` calls ``_get_statement(..., use_cache=False)``.
The returned :class:`~asyncpg.PreparedStatement` therefore never enters
the LRU cache; it is owned by the caller and holds its own reference to
the low-level statement state.

``PreparedStatement.__do_execute()`` has no transparent retry. It only
special-cases
:class:`~asyncpg.exceptions.OutdatedSchemaCacheError`
(a composite/type codec mismatch), on which it reloads schema state and
marks its own state closed. An ``InvalidCachedStatementError`` (the
``search_path``/plan-revalidation case) propagates straight to the
caller. The handle is *not* marked closed, but it keeps failing against
the revalidated plan until it is dropped. Recovery is to discard the old
handle and call ``prepare()`` again.

3. Pool release and session state
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Returning a connection runs ``PoolConnectionHolder._release()``, which
calls ``Connection.reset()``. The default reset query from
``Connection.get_reset_query()`` is, on a standard PostgreSQL server::

    SELECT pg_advisory_unlock_all();
    CLOSE ALL;
    UNLISTEN *;
    RESET ALL;

``RESET ALL`` restores session GUCs, including ``search_path`` and the
role set by ``SET ROLE``, to their defaults. It does **not**
``DEALLOCATE`` prepared statements, so both the server-side prepared
statements and asyncpg's client-side LRU entries survive a pool round
trip. If a transaction is still open, ``Connection._reset()`` first
issues ``ROLLBACK`` (and logs an "active transaction" warning for
non-managed transactions). If reset itself fails with an I/O error, the
holder terminates the connection rather than returning a broken one.

Integration contract
--------------------

Reusable without rebuilding
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* The physical **connection** after a transparent cache invalidation:
  a non-transactional ``fetch`` failure is retried automatically and the
  connection remains healthy.
* The **LRU cache entry for unchanged queries**: entries whose resolved
  objects and result types did not change are reused with no new
  ``Parse``. Cache size and lifetime are controlled by
  ``statement_cache_size`` and ``max_cached_statement_lifetime``.
* A **freshly prepared** :class:`~asyncpg.PreparedStatement` after the
  schema has settled; prepare against the intended ``search_path`` and
  keep the handle only while that role/path context is active.

Must be released or discarded
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* An explicit :class:`~asyncpg.PreparedStatement` whose underlying
  objects changed: it does not repair itself. Drop the Python handle and
  prepare a new statement; do not cache it across tenants.
* A borrowed **pool connection** before changing role/path context is
  visible to anyone else: always return it through the pool so
  ``reset()`` runs. Never hand a connection that ran ``SET ROLE`` or
  ``SET search_path`` to another code path without releasing it first.
* Connections whose ``reset()`` raised: the pool already terminates
  them; do not attempt to reuse such a connection yourself.

Must be reopened
~~~~~~~~~~~~~~~~

* Any **transaction** in which ``InvalidCachedStatementError`` was
  raised: the server has placed the block in the aborted state
  (``PQTRANS_INERROR``). Roll back (the ``async with con.transaction()``
  block does this automatically on exception) and start a new
  transaction; statements issued before rollback only succeed up to the
  failed one and the whole block must be retried.
* A transaction left open at pool release: ``reset()`` rolls it back, so
  work that must survive has to be committed before release.

Choosing a safe pattern
-----------------------

* Prefer the implicit cache for short queries and let asyncpg re-prepare
  on ``search_path`` changes outside transactions.
* Set the context immediately after acquiring a connection and run all of
  that tenant's work before releasing it::

      async with pool.acquire() as con:
          await con.execute("SET ROLE TO $1", tenant_role)
          await con.execute("SET search_path TO $1", tenant_schema)
          # ... all queries for this tenant here ...
  # release() runs RESET ALL, clearing role and search_path.

* Avoid long-lived explicit prepared statements that outlive a role/path
  switch. If a statement must be reused across tenants, fully-qualify the
  tables (``tenant_a.accounts``) so the plan does not depend on
  ``search_path``.
* For transactional multi-statement work, treat an
  ``InvalidCachedStatementError`` as fatal for the transaction and retry
  the whole unit of work after rollback.

Offline replay
--------------

The behaviour is replayed without a PostgreSQL server or any socket by
``tests/test_offline_role_path_cache.py`` together with the in-memory
wire peer in ``tests/_fakepg.py``. The peer drives the real asyncpg
Cython protocol through an in-process transport and covers:

* cache hit and silent re-prepare after a ``search_path`` switch;
* the fatal-vs-transparent difference between in-transaction and
  out-of-transaction invalidation;
* explicit ``prepare()`` handles staying out of the LRU cache and
  failing instead of repairing themselves;
* ``RESET ALL`` on pool release restoring role/path while keeping the
  statement cache;
* rollback of a transaction left open at release;
* pool-wide cache clearing across two live connections.
