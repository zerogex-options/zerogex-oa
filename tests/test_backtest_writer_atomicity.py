"""Regression tests for atomic ownership-check + write in backtest configs.

Pre-fix, ``configs.update_config`` and ``configs.delete_config`` ran the
ownership SELECT and the subsequent UPDATE/DELETE under
``conn.autocommit = True``. Each statement committed independently, so a
concurrent owner change (or delete) of the same row between the check
and the write could slip past the ownership guard:

    T1: SELECT end_user WHERE id = 7    -> 'alice'    (T1 thinks it owns 7)
    T2: UPDATE end_user='bob' WHERE id=7                (ownership transferred)
    T1: UPDATE … WHERE id = 7                           (T1's UPDATE proceeds)

Small attack surface, but a real correctness gap.

Fix: both functions now use ``db_connection()`` (one transaction) with
``SELECT … FOR UPDATE`` on the ownership check. The row lock blocks any
concurrent writer until T1 either commits or rolls back, so the
check-then-write is atomic.

These tests pin the SQL shape (the FOR UPDATE locking clause and the
transactional wrapper) since the racy version would type-check and
behave identically in single-threaded test runs.
"""

import inspect
import json

import pytest

from src.backtesting import configs as configs_mod


class _Cur:
    """Cursor that returns successive scripted fetchone() values."""

    def __init__(self, fetch_results):
        self._results = list(fetch_results)
        self._i = 0
        self.executed: list[tuple] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        row = self._results[self._i]
        self._i += 1
        return row

    def fetchall(self):
        return self._results[self._i]


class _Conn:
    """Fake psycopg2 conn — supports the context-manager commit/rollback."""

    def __init__(self, fetch_results):
        self.autocommit = False
        self._cur = _Cur(fetch_results)
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self):
        return self._cur

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


@pytest.fixture
def patched(monkeypatch):
    """Install a fake conn for both the writer path (configs_mod imports)
    and the context-manager path (resolves names from source module)."""
    from src.database import connection as conn_module

    def install(fetch_results):
        conn = _Conn(fetch_results)
        monkeypatch.setattr(configs_mod, "get_db_connection", lambda: conn)
        monkeypatch.setattr(configs_mod, "close_db_connection", lambda c: None)
        monkeypatch.setattr(conn_module, "get_db_connection", lambda: conn)
        monkeypatch.setattr(conn_module, "close_db_connection", lambda c: None)
        return conn

    return install


def _summary_row(cid=1):
    # id, name, underlying, share_token, created_at, updated_at
    from datetime import datetime, timezone
    ts = datetime(2026, 6, 22, tzinfo=timezone.utc)
    return (cid, "My Config", "SPY", "tok123", ts, ts)


# ---------------------------------------------------------------------------
# update_config
# ---------------------------------------------------------------------------


def test_update_config_locks_row_for_update(patched):
    """The ownership SELECT must use ``FOR UPDATE`` so the row is locked
    against any concurrent update or delete until the UPDATE in the
    same transaction completes. Without this, the autocommit pattern
    can have ownership change between SELECT and UPDATE — small race
    window, but a real bypass of the owner-only guard."""
    conn = patched([("alice",), _summary_row()])
    out = configs_mod.update_config(1, end_user="alice", name="New name")

    assert out is not None
    select_sql = conn._cur.executed[0][0]
    assert "FOR UPDATE" in select_sql
    assert "WHERE id = %s" in select_sql


def test_update_config_runs_in_a_single_transaction(patched):
    """SELECT and UPDATE must share a transaction so the row lock holds
    across both. ``db_connection()`` commits at clean exit; if it were
    skipped (e.g. someone reverting to ``conn.autocommit = True``) the
    FOR UPDATE lock would be released between statements and the race
    would reopen."""
    conn = patched([("alice",), _summary_row()])
    configs_mod.update_config(1, end_user="alice", name="New name")

    # Context manager committed once on clean exit.
    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    # Crucially: autocommit was NOT re-enabled inside the function.
    assert conn.autocommit is False


def test_update_config_owner_mismatch_returns_none_without_writing(patched):
    """Foreign owner: bail before the UPDATE. The FOR UPDATE lock is
    released by the implicit rollback at function exit (the second
    fetch is never reached, so test fixture only needs the SELECT row).
    """
    conn = patched([("alice",)])
    out = configs_mod.update_config(1, end_user="bob", name="hacked")

    assert out is None
    # Only the SELECT FOR UPDATE ran — no UPDATE was attempted.
    assert len(conn._cur.executed) == 1
    assert "SELECT end_user" in conn._cur.executed[0][0]


def test_update_config_missing_row_returns_none(patched):
    """Missing row: ``fetchone()`` returns None, function returns None,
    no UPDATE attempted."""
    conn = patched([None])
    out = configs_mod.update_config(999, end_user="alice", name="x")

    assert out is None
    assert len(conn._cur.executed) == 1


# ---------------------------------------------------------------------------
# delete_config
# ---------------------------------------------------------------------------


def test_delete_config_locks_row_for_update(patched):
    """Same race rationale as update_config: lock the row across the
    ownership check and the DELETE so a concurrent ownership change
    can't make the DELETE land on someone else's row."""
    conn = patched([("alice",), None])  # SELECT row, DELETE returns nothing
    ok = configs_mod.delete_config(1, end_user="alice")

    assert ok is True
    select_sql = conn._cur.executed[0][0]
    assert "FOR UPDATE" in select_sql


def test_delete_config_runs_in_a_single_transaction(patched):
    conn = patched([("alice",), None])
    configs_mod.delete_config(1, end_user="alice")

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert conn.autocommit is False


def test_delete_config_owner_mismatch_returns_false(patched):
    conn = patched([("alice",)])
    ok = configs_mod.delete_config(1, end_user="bob")

    assert ok is False
    assert len(conn._cur.executed) == 1


def test_delete_config_missing_row_returns_false(patched):
    conn = patched([None])
    ok = configs_mod.delete_config(999, end_user="alice")

    assert ok is False


# ---------------------------------------------------------------------------
# Cross-check: save_config intentionally untouched
# ---------------------------------------------------------------------------


def test_save_config_pattern_intentionally_untouched():
    """``save_config`` is a single INSERT — no ownership check, no race,
    no atomicity concern. It keeps the ``conn.autocommit = True``
    pattern (also working, also IDLE-returning to the pool). If a future
    edit migrates it for "consistency," that's a deliberate scope
    decision — flag it and review, don't drift incidentally."""
    src = inspect.getsource(configs_mod.save_config)
    assert "conn.autocommit = True" in src
    assert "with db_connection()" not in src
    assert "FOR UPDATE" not in src