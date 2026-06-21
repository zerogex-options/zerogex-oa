"""Tests for the dedicated backtest worker (Phase 4, src/backtesting)."""

from __future__ import annotations

from src.backtesting import worker as worker_mod
from src.backtesting.runner import claim_next_queued_run, requeue_stale_runs


class _Cur:
    def __init__(self, fetch_result, rowcount=0):
        self._fetch = fetch_result
        self.rowcount = rowcount
        self.executed: list[tuple] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetch


class _Conn:
    def __init__(self, fetch_result=None, rowcount=0):
        self._cur = _Cur(fetch_result, rowcount)

    def cursor(self):
        return self._cur


def test_claim_returns_id_and_uses_skip_locked():
    conn = _Conn(fetch_result=(42,))
    assert claim_next_queued_run(conn) == 42
    sql = conn._cur.executed[0][0]
    assert "FOR UPDATE SKIP LOCKED" in " ".join(sql.split())
    assert "status = 'running'" in " ".join(sql.split())


def test_claim_returns_none_when_queue_empty():
    assert claim_next_queued_run(_Conn(fetch_result=None)) is None


def test_requeue_stale_returns_rowcount():
    conn = _Conn(rowcount=3)
    assert requeue_stale_runs(conn, older_than_minutes=30) == 3
    sql = " ".join(conn._cur.executed[0][0].split())
    assert "status = 'queued'" in sql and "interval" in sql.lower()


def test_worker_processes_one_run_then_stops(monkeypatch):
    executed: list[int] = []
    claims = iter([7, None])

    w = worker_mod.BacktestWorker(poll_interval=0.0)
    monkeypatch.setattr(w, "_recover_stale", lambda: None)
    monkeypatch.setattr(w, "_claim", lambda: next(claims, None))

    def _fake_execute(run_id):
        executed.append(run_id)
        w.running = False  # stop after the first real run

    monkeypatch.setattr(worker_mod, "execute_run", _fake_execute)
    w.run()
    assert executed == [7]
