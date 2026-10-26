"""build_snapshot ends the transaction it opens (14 Oct schema-apply freeze).

An engine tick or replay that kept one transaction across many snapshots held
share locks on underlying_quotes for as long as it ran, and a schema ALTER
queued behind it froze every quote reader. These pin the rule in
``_snapshot_transaction``: end a transaction we opened, run an autocommit
connection inside a transaction (its SAVEPOINTs need one), and leave a
caller's open transaction alone.
"""

from psycopg2.extensions import TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS

from src.tradeworkz.context import _snapshot_transaction


class _Conn:
    def __init__(self, autocommit=False, status=TRANSACTION_STATUS_IDLE):
        self.autocommit = autocommit
        self.status = status
        self.rollbacks = 0
        self.autocommit_seen_inside = None

    def get_transaction_status(self):
        return self.status

    def rollback(self):
        self.rollbacks += 1


def test_idle_connection_transaction_is_ended():
    conn = _Conn()
    with _snapshot_transaction(conn):
        pass
    assert conn.rollbacks == 1


def test_idle_connection_transaction_is_ended_on_error():
    conn = _Conn()
    try:
        with _snapshot_transaction(conn):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert conn.rollbacks == 1


def test_autocommit_connection_runs_in_a_transaction_and_is_restored():
    conn = _Conn(autocommit=True)
    with _snapshot_transaction(conn):
        conn.autocommit_seen_inside = conn.autocommit
    assert conn.autocommit_seen_inside is False
    assert conn.autocommit is True
    assert conn.rollbacks == 1


def test_caller_transaction_is_left_alone():
    conn = _Conn(status=TRANSACTION_STATUS_INTRANS)
    with _snapshot_transaction(conn):
        pass
    assert conn.rollbacks == 0


def test_connection_without_status_is_left_alone():
    class _Bare:
        def rollback(self):  # pragma: no cover - must not be called
            raise AssertionError("rolled back a connection it does not own")

    with _snapshot_transaction(_Bare()):
        pass
