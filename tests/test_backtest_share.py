"""Tests for shareable run reports (src/backtesting/queries share_* helpers).

Owner-scoping, "only completed runs are shareable", token idempotency, and the
public sanitization (no owner, no custom-strategy conditions) — all against a
scripted fake connection, no database.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.backtesting import queries as q

_TS = datetime(2026, 6, 22, tzinfo=timezone.utc)


class _Cur:
    def __init__(self, fetch_results):
        self._results = list(fetch_results)
        self._i = 0
        self.executed: list = []

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
        self._cur = _Cur(fetch_results)

    def cursor(self):
        return self._cur

    def commit(self):
        pass

    def rollback(self):
        pass


@pytest.fixture
def patched(monkeypatch):
    from src.database import connection as conn_module

    def install(fetch_results):
        conn = _Conn(fetch_results)
        monkeypatch.setattr(conn_module, "get_db_connection", lambda: conn)
        monkeypatch.setattr(conn_module, "close_db_connection", lambda c: None)
        return conn

    return install


# ----------------------------------------------------------------------
# share_run
# ----------------------------------------------------------------------


def test_share_run_mints_token_for_completed(patched):
    conn = patched([("alice", "completed", None)])
    token = q.share_run(1, end_user="alice")
    assert isinstance(token, str) and len(token) >= 8
    # A SELECT then an UPDATE that sets the token.
    update_sql = " ".join(conn._cur.executed[1][0].split())
    assert "UPDATE backtest_runs SET share_token" in update_sql
    assert conn._cur.executed[1][1][0] == token


def test_share_run_returns_existing_token(patched):
    conn = patched([("alice", "completed", "tok_existing")])
    token = q.share_run(1, end_user="alice")
    assert token == "tok_existing"
    assert len(conn._cur.executed) == 1  # no UPDATE — idempotent


def test_share_run_rejects_non_owner(patched):
    patched([("bob", "completed", None)])
    assert q.share_run(1, end_user="alice") is None


def test_share_run_rejects_incomplete(patched):
    patched([("alice", "running", None)])
    assert q.share_run(1, end_user="alice") is None


def test_share_run_missing_is_none(patched):
    patched([None])
    assert q.share_run(999, end_user="alice") is None


# ----------------------------------------------------------------------
# get_shared_run — sanitization
# ----------------------------------------------------------------------


def _run_row(spec):
    # id, underlying, start_date, end_date, summary, created_at, spec
    return (7, "SPY", date(2026, 5, 1), date(2026, 6, 1), {"n_trades": 42}, _TS, spec)


def test_shared_run_redacts_custom_conditions_and_owner(patched):
    spec = {
        "underlying": "SPY",
        "sizing": {"capital": 25000, "risk_per_trade_pct": 2},
        "fill_model": {"slippage_pct": 0.01, "commission_per_contract": 0.65},
        "exit": {"profit_target_pct": 0.75},
        "strategy": {
            "direction": "bearish",
            "structure": "vertical",
            "conditions": [{"field": "net_gex", "op": "<", "value": 0}],  # the edge
        },
    }
    patched([_run_row(spec)])
    out = q.get_shared_run("tok")
    assert out["summary"] == {"n_trades": 42}
    assert out["is_custom"] is True
    assert out["structure"] == "vertical"
    assert out["capital"] == 25000
    # The user's secret sauce and identity are NOT exposed.
    assert "conditions" not in str(out)
    assert "end_user" not in out
    assert "strategy" not in out  # only the label/flags, never the raw block


def test_shared_run_labels_patterns(patched):
    spec = {"patterns": ["gamma_flip_break", "call_wall_fade"], "sizing": {"capital": 10000}}
    patched([_run_row(spec)])
    out = q.get_shared_run("tok")
    assert out["is_custom"] is False
    assert "gamma_flip_break" in out["strategy_label"]


def test_shared_run_missing_is_none(patched):
    patched([None])
    assert q.get_shared_run("nope") is None


# ----------------------------------------------------------------------
# get_shared_equity
# ----------------------------------------------------------------------


def test_shared_equity_returns_curve(patched):
    patched([(7,), [(_TS, 10100.0, -1.2), (_TS, 10250.0, 0.0)]])
    curve = q.get_shared_equity("tok")
    assert len(curve) == 2
    assert curve[0]["equity"] == 10100.0
    assert curve[1]["drawdown_pct"] == 0.0


def test_shared_equity_unknown_token_is_none(patched):
    patched([None])
    assert q.get_shared_equity("nope") is None
