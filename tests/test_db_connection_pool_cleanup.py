from psycopg2 import extensions

from src.database import connection as db_connection_module


class _FakePool:
    def __init__(self):
        self.returned = []

    def putconn(self, conn):
        self.returned.append(conn)


class _FakeConn:
    def __init__(self, tx_status):
        self._tx_status = tx_status
        self.rollback_calls = 0

    def get_transaction_status(self):
        return self._tx_status

    def rollback(self):
        self.rollback_calls += 1
        self._tx_status = extensions.TRANSACTION_STATUS_IDLE


def test_close_db_connection_rolls_back_non_idle_tx():
    pool = _FakePool()
    conn = _FakeConn(extensions.TRANSACTION_STATUS_INTRANS)
    original_pool = db_connection_module._connection_pool
    db_connection_module._connection_pool = pool
    try:
        db_connection_module.close_db_connection(conn)
    finally:
        db_connection_module._connection_pool = original_pool

    assert conn.rollback_calls == 1
    assert pool.returned == [conn]


def test_close_db_connection_skips_rollback_for_idle_tx():
    pool = _FakePool()
    conn = _FakeConn(extensions.TRANSACTION_STATUS_IDLE)
    original_pool = db_connection_module._connection_pool
    db_connection_module._connection_pool = pool
    try:
        db_connection_module.close_db_connection(conn)
    finally:
        db_connection_module._connection_pool = original_pool

    assert conn.rollback_calls == 0
    assert pool.returned == [conn]
