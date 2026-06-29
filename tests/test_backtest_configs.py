"""Tests for saved & shareable backtest configs (Phase 6, src/backtesting.configs).

These exercise the owner-scoping logic and SQL shape against a scripted fake
connection — no database required (mirrors test_backtest_worker's approach).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.backtesting import configs as configs_mod

_TS = datetime(2026, 6, 22, tzinfo=timezone.utc)


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
    def __init__(self, fetch_results):
        self.autocommit = False
        self._cur = _Cur(fetch_results)

    def cursor(self):
        return self._cur

    # Reader helpers in configs now route through the ``db_connection()``
    # context manager (commits on success, rolls back on exception) instead
    # of the raw ``get_db_connection`` / ``close_db_connection`` pair. The
    # context manager invokes ``conn.commit()`` on clean exit, so the fake
    # conn needs harmless no-op commit/rollback hooks. The bool methods
    # below are never asserted by these tests — they exist only so the
    # context manager doesn't AttributeError.
    def commit(self):
        pass

    def rollback(self):
        pass


@pytest.fixture
def patched(monkeypatch):
    """Install a fake connection and return a setter for its scripted rows."""
    from src.database import connection as conn_module

    holder = {}

    def install(fetch_results):
        conn = _Conn(fetch_results)
        holder["conn"] = conn
        # Writers in configs still use the raw get/close pair (autocommit
        # pattern) — patch them on the configs module's imported names so
        # the unchanged save/update/delete paths keep using this fake conn.
        monkeypatch.setattr(configs_mod, "get_db_connection", lambda: conn)
        monkeypatch.setattr(configs_mod, "close_db_connection", lambda c: None)
        # Readers go through ``db_connection()`` (defined in
        # ``src.database.connection``), which resolves ``get_db_connection``
        # and ``close_db_connection`` against the source module — not the
        # caller's bound imports. Patch the source too so both paths land
        # on the same fake conn.
        monkeypatch.setattr(conn_module, "get_db_connection", lambda: conn)
        monkeypatch.setattr(conn_module, "close_db_connection", lambda c: None)
        return conn

    return install


def _summary_row(cid=1, owner="alice"):
    # id, name, underlying, share_token, created_at, updated_at[, spec, end_user]
    return (cid, "My Config", "SPY", "tok123", _TS, _TS)


def test_save_returns_summary_with_token(patched):
    conn = patched([_summary_row()])
    out = configs_mod.save_config({"underlying": "SPY"}, name="My Config",
                                  underlying="SPY", end_user="alice")
    assert out["id"] == 1
    assert out["share_token"] == "tok123"
    assert out["underlying"] == "SPY"
    sql = " ".join(conn._cur.executed[0][0].split())
    assert "INSERT INTO backtest_configs" in sql


def test_list_uses_user_filter(patched):
    conn = patched([[_summary_row(), _summary_row(cid=2)]])
    out = configs_mod.list_configs(end_user="alice")
    assert [c["id"] for c in out] == [1, 2]
    assert conn._cur.executed[0][1] == ("alice", 100)


def test_list_anonymous_filters_null_owner(patched):
    conn = patched([[]])
    configs_mod.list_configs(end_user=None)
    sql = " ".join(conn._cur.executed[0][0].split())
    assert "end_user IS NULL" in sql


def test_get_config_owner_match_includes_spec(patched):
    row = (*_summary_row(), {"underlying": "SPY"}, "alice")
    patched([row])
    out = configs_mod.get_config(1, end_user="alice")
    assert out is not None
    assert out["spec"] == {"underlying": "SPY"}


def test_get_config_foreign_owner_returns_none(patched):
    row = (*_summary_row(), {"underlying": "SPY"}, "alice")
    patched([row])
    assert configs_mod.get_config(1, end_user="mallory") is None


def test_get_config_anonymous_readable(patched):
    row = (*_summary_row(), {"underlying": "SPY"}, None)
    patched([row])
    assert configs_mod.get_config(1, end_user="anyone") is not None


def test_delete_foreign_owner_refused(patched):
    conn = patched([("alice",)])
    assert configs_mod.delete_config(1, end_user="mallory") is False
    # Only the ownership SELECT ran — never a DELETE.
    assert all("DELETE" not in e[0] for e in conn._cur.executed)


def test_delete_owner_runs_delete(patched):
    conn = patched([("alice",)])
    assert configs_mod.delete_config(1, end_user="alice") is True
    assert any("DELETE FROM backtest_configs" in e[0] for e in conn._cur.executed)


def test_delete_missing_returns_false(patched):
    patched([None])
    assert configs_mod.delete_config(99, end_user="alice") is False


def test_update_owner_sets_name_and_spec(patched):
    conn = patched([("alice",), _summary_row()])
    out = configs_mod.update_config(1, end_user="alice", name="Renamed",
                                    spec_dict={"underlying": "QQQ"}, underlying="QQQ")
    assert out is not None
    update_sql = " ".join(conn._cur.executed[1][0].split())
    assert "UPDATE backtest_configs SET" in update_sql
    assert "updated_at = NOW()" in update_sql


def test_update_foreign_owner_returns_none(patched):
    conn = patched([("alice",)])
    assert configs_mod.update_config(1, end_user="mallory", name="x") is None
    # The ownership SELECT now uses ``FOR UPDATE`` to lock the row across
    # the check+write (configs.update_config docstring explains the race).
    # The previous ``"UPDATE" not in e[0]`` substring check would now
    # spuriously match the row-lock clause, so assert the actual UPDATE
    # statement on the ``backtest_configs`` table never ran instead.
    assert all("UPDATE backtest_configs SET" not in e[0] for e in conn._cur.executed)


def test_shared_lookup_by_token(patched):
    conn = patched([("Shared", "SPY", {"underlying": "SPY"})])
    out = configs_mod.get_shared_config("tok123")
    assert out == {"name": "Shared", "underlying": "SPY", "spec": {"underlying": "SPY"}}
    assert conn._cur.executed[0][1] == ("tok123",)


def test_shared_missing_returns_none(patched):
    patched([None])
    assert configs_mod.get_shared_config("nope") is None
