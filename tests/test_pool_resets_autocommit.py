"""Regression tests for ``close_db_connection`` resetting per-conn state.

Some callers in ``src/backtesting`` set ``conn.autocommit = True`` on a
checked-out conn for their lifetime — that's the right pattern for
long-running write paths (``runner.execute_run``, ``worker.py``) where
per-statement writes must be immediately visible to API polls. But
psycopg2 conn attributes persist on the object across pool checkouts, so
without an explicit reset on return, the next caller — usually a reader
expecting transactional semantics — silently inherits ``autocommit=True``.
Behavior then depends on pool-checkout order rather than on the caller's
own code.

These tests pin that ``close_db_connection`` resets ``autocommit`` to
``False`` (psycopg2's default) before re-parking the conn in the pool,
so every checkout starts from a known state. The reset is also
defense-in-depth against the writer pattern being copied to a new site
that forgets to reset it itself.
"""

import pytest
import psycopg2
from psycopg2 import extensions

from src.database import connection as db_connection_module


class _FakePool:
    def __init__(self):
        self.returned = []  # list of (conn, close_kwarg) tuples

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))


class _FakeConn:
    def __init__(self, *, autocommit=False, tx_status=extensions.TRANSACTION_STATUS_IDLE):
        self.autocommit = autocommit
        self._tx_status = tx_status
        self.closed = 0
        self.rollback_calls = 0

    def get_transaction_status(self):
        return self._tx_status

    def rollback(self):
        self.rollback_calls += 1
        self._tx_status = extensions.TRANSACTION_STATUS_IDLE


@pytest.fixture
def swap_pool(monkeypatch):
    def _swap(fake):
        monkeypatch.setattr(db_connection_module, "_connection_pool", fake)
    return _swap


# ---------------------------------------------------------------------------
# autocommit reset on conn return
# ---------------------------------------------------------------------------


def test_close_resets_autocommit_true_back_to_false(swap_pool):
    """A writer that set autocommit=True must NOT leak the flag onto the
    pooled conn. The next checkout — possibly a reader — gets a clean
    autocommit=False, the psycopg2 default."""
    pool = _FakePool()
    swap_pool(pool)
    conn = _FakeConn(autocommit=True, tx_status=extensions.TRANSACTION_STATUS_IDLE)

    db_connection_module.close_db_connection(conn)

    assert conn.autocommit is False
    # And the conn was returned to the pool (not discarded).
    assert pool.returned == [(conn, False)]


def test_close_leaves_autocommit_false_alone(swap_pool):
    """No-op when autocommit is already False (psycopg2 default). The
    reset is conditional so we don't churn the attribute on every
    return."""
    pool = _FakePool()
    swap_pool(pool)
    conn = _FakeConn(autocommit=False, tx_status=extensions.TRANSACTION_STATUS_IDLE)

    db_connection_module.close_db_connection(conn)

    assert conn.autocommit is False


def test_close_resets_autocommit_after_rolling_back_intrans(swap_pool):
    """A writer that set autocommit=True then somehow ended up INTRANS
    on return (rare but possible — e.g. set autocommit=True after a
    SELECT had already opened a txn). The pool must both roll back the
    leftover txn AND reset the autocommit flag, so the next checkout
    is in a clean state."""
    pool = _FakePool()
    swap_pool(pool)
    conn = _FakeConn(autocommit=True, tx_status=extensions.TRANSACTION_STATUS_INTRANS)

    db_connection_module.close_db_connection(conn)

    assert conn.rollback_calls == 1
    assert conn.autocommit is False
    assert conn._tx_status == extensions.TRANSACTION_STATUS_IDLE


def test_close_discards_dead_conn_without_touching_autocommit(swap_pool):
    """A conn libpq marks ``closed`` is evicted (close=True) — no
    attribute access on a dead conn, which could raise. The autocommit
    reset is in the live branch; the dead branch must not touch it."""
    pool = _FakePool()
    swap_pool(pool)
    conn = _FakeConn(autocommit=True)
    conn.closed = 2  # mimic libpq closed state

    db_connection_module.close_db_connection(conn)

    # Discarded — pool got close=True.
    assert pool.returned == [(conn, True)]


def test_close_handles_status_check_failure_without_attr_error(swap_pool):
    """If ``get_transaction_status`` raises (e.g. broken conn), the
    whole live-branch try block bails out and the conn is discarded.
    The autocommit reset is inside the same try, so a raising conn
    must not bypass the discard path or leave the function unwound."""
    pool = _FakePool()
    swap_pool(pool)

    class _RaisingConn(_FakeConn):
        def get_transaction_status(self):
            raise psycopg2.InterfaceError("connection broken")

    conn = _RaisingConn(autocommit=True)
    db_connection_module.close_db_connection(conn)

    # Discarded due to the InterfaceError — close=True.
    assert pool.returned == [(conn, True)]