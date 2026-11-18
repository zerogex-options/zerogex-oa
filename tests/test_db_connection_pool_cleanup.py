import pytest
import psycopg2
from psycopg2 import extensions

from src.database import connection as db_connection_module


class _FakePool:
    def __init__(self, conns_to_hand_out=None):
        self.returned = []  # list of (conn, close_kwarg) tuples
        self._to_hand = list(conns_to_hand_out or [])

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))

    def getconn(self):
        if not self._to_hand:
            raise AssertionError("FakePool ran out of conns to hand out")
        return self._to_hand.pop(0)


class _FakeConn:
    def __init__(self, tx_status=extensions.TRANSACTION_STATUS_IDLE, closed=0):
        self._tx_status = tx_status
        self.closed = closed
        self.rollback_calls = 0

    def get_transaction_status(self):
        if self.closed:
            raise psycopg2.InterfaceError("connection already closed")
        return self._tx_status

    def rollback(self):
        self.rollback_calls += 1
        self._tx_status = extensions.TRANSACTION_STATUS_IDLE


@pytest.fixture
def swap_pool(monkeypatch):
    """Temporarily replace the module-level pool with a fake for one test."""

    def _swap(fake):
        monkeypatch.setattr(db_connection_module, "_connection_pool", fake)

    return _swap


# -- close_db_connection ----------------------------------------------------


def test_close_db_connection_rolls_back_non_idle_tx(swap_pool):
    pool = _FakePool()
    swap_pool(pool)
    conn = _FakeConn(tx_status=extensions.TRANSACTION_STATUS_INTRANS)

    db_connection_module.close_db_connection(conn)

    assert conn.rollback_calls == 1
    assert pool.returned == [(conn, False)]  # parked, not closed


def test_close_db_connection_skips_rollback_for_idle_tx(swap_pool):
    pool = _FakePool()
    swap_pool(pool)
    conn = _FakeConn(tx_status=extensions.TRANSACTION_STATUS_IDLE)

    db_connection_module.close_db_connection(conn)

    assert conn.rollback_calls == 0
    assert pool.returned == [(conn, False)]


def test_close_db_connection_evicts_already_closed_conn(swap_pool):
    """A conn libpq already marks `closed` must be evicted (close=True), not re-parked.

    Without this, the next getconn() hands the same dead conn back and we
    enter the SSL-died-flap loop that motivated this change.
    """
    pool = _FakePool()
    swap_pool(pool)
    conn = _FakeConn(closed=2)  # 2 == server-side close per psycopg2

    db_connection_module.close_db_connection(conn)

    assert pool.returned == [(conn, True)]


def test_close_db_connection_evicts_conn_whose_status_check_raises(swap_pool):
    """If get_transaction_status / rollback throws, the conn is unusable — evict."""
    pool = _FakePool()
    swap_pool(pool)

    class _DyingConn:
        closed = 0  # libpq hasn't noticed yet

        def get_transaction_status(self):
            raise psycopg2.OperationalError("SSL connection has been closed unexpectedly")

        def rollback(self):  # pragma: no cover - not reached
            raise AssertionError("rollback should not be called after status raised")

    conn = _DyingConn()
    db_connection_module.close_db_connection(conn)

    assert pool.returned == [(conn, True)]


def test_close_db_connection_no_op_when_pool_or_conn_missing(swap_pool):
    swap_pool(None)
    # Must not raise even with no pool.
    db_connection_module.close_db_connection(_FakeConn())

    pool = _FakePool()
    swap_pool(pool)
    db_connection_module.close_db_connection(None)
    assert pool.returned == []


# -- get_db_connection ------------------------------------------------------


def test_get_db_connection_returns_healthy_conn(swap_pool):
    healthy = _FakeConn()
    pool = _FakePool(conns_to_hand_out=[healthy])
    swap_pool(pool)

    got = db_connection_module.get_db_connection()

    assert got is healthy
    assert pool.returned == []  # nothing discarded


def test_get_db_connection_discards_closed_conn_and_retries(swap_pool):
    """If the pool hands back a stale (closed) conn, eat it and try once more."""
    dead = _FakeConn(closed=2)
    healthy = _FakeConn()
    pool = _FakePool(conns_to_hand_out=[dead, healthy])
    swap_pool(pool)

    got = db_connection_module.get_db_connection()

    assert got is healthy
    assert pool.returned == [(dead, True)]  # dead conn evicted with close=True


def test_get_db_connection_raises_after_two_dead_conns(swap_pool):
    """Cap retries so a fully-poisoned pool surfaces a clear error rather than looping."""
    dead1 = _FakeConn(closed=2)
    dead2 = _FakeConn(closed=2)
    pool = _FakePool(conns_to_hand_out=[dead1, dead2])
    swap_pool(pool)

    with pytest.raises(psycopg2.OperationalError, match="healthy DB connection"):
        db_connection_module.get_db_connection()

    assert pool.returned == [(dead1, True), (dead2, True)]


# -- driver and server disagree (2026-11-17 API incident) --------------------


class _DriverFaithfulConn:
    """A conn whose ``rollback()`` behaves like psycopg2's real one.

    psycopg2 sends ROLLBACK only when the DRIVER opened the transaction
    (``conn.status == STATUS_BEGIN``); otherwise ``rollback()`` returns without
    touching the server. ``get_transaction_status()`` reports the SERVER's
    state. ``_FakeConn`` above assumes a rollback always lands, which is
    exactly the assumption that failed in production.
    """

    def __init__(self, *, server, driver):
        self.closed = 0
        self.autocommit = False
        self.server = server
        self.status = driver
        self.rollback_calls = 0

    def get_transaction_status(self):
        return self.server

    def rollback(self):
        self.rollback_calls += 1
        if self.status == extensions.STATUS_BEGIN:
            self.server = extensions.TRANSACTION_STATUS_IDLE
            self.status = extensions.STATUS_READY


@pytest.mark.parametrize(
    "server",
    [extensions.TRANSACTION_STATUS_INERROR, extensions.TRANSACTION_STATUS_INTRANS],
)
def test_close_db_connection_discards_conn_whose_rollback_is_a_no_op(swap_pool, caplog, server):
    """The server holds a transaction the driver doesn't know about, so the
    rollback sends nothing. Re-parking it failed every later borrower's first
    statement with "current transaction is aborted" until the API restarted;
    it must be closed instead."""
    pool = _FakePool()
    swap_pool(pool)
    conn = _DriverFaithfulConn(server=server, driver=extensions.STATUS_READY)

    with caplog.at_level("WARNING", logger=db_connection_module.logger.name):
        db_connection_module.close_db_connection(conn)

    assert conn.rollback_calls == 1
    assert conn.server == server  # the rollback really did nothing
    assert pool.returned == [(conn, True)]
    messages = [r.getMessage() for r in caplog.records]
    name = {
        extensions.TRANSACTION_STATUS_INERROR: "INERROR",
        extensions.TRANSACTION_STATUS_INTRANS: "INTRANS",
    }[server]
    assert any(
        f"still in a transaction after rollback (server {name}, was {name}; driver READY)" in m
        for m in messages
    )
    # The old message claimed a rollback that never happened.
    assert not any(m.startswith("Rolled back open transaction") for m in messages)


def test_close_db_connection_reparks_conn_the_driver_can_roll_back(swap_pool, caplog):
    """The ordinary case is unchanged: the driver opened the transaction, its
    rollback reaches the server, and the conn goes back to the pool."""
    pool = _FakePool()
    swap_pool(pool)
    conn = _DriverFaithfulConn(
        server=extensions.TRANSACTION_STATUS_INERROR, driver=extensions.STATUS_BEGIN
    )

    with caplog.at_level("WARNING", logger=db_connection_module.logger.name):
        db_connection_module.close_db_connection(conn)

    assert conn.server == extensions.TRANSACTION_STATUS_IDLE
    assert pool.returned == [(conn, False)]
    assert [r.getMessage() for r in caplog.records] == [
        "Rolled back open transaction before returning DB connection"
    ]
