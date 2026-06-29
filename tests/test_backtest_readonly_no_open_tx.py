"""Regression tests for the backtest API read path not leaking open transactions.

The backtest read endpoints (``/api/backtest/meta``, ``/api/backtest/runs``,
``/api/backtest/runs/{id}``, ``/api/backtest/runs/{id}/trades``,
``/api/backtest/configs``, ``/api/backtest/sweeps/{id}``, …) are polled
by the Backtesting UI on a sub-10-second cadence. They go through the
synchronous psycopg2 pool via ``asyncio.to_thread``.

psycopg2 is not in autocommit mode by default, so even a plain ``SELECT``
opens an implicit transaction. The original pattern was:

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT ...")
        ...
    finally:
        close_db_connection(conn)

…which returned the conn to the pool still in ``TRANSACTION_STATUS_INTRANS``.
``close_db_connection`` then defensively rolled it back and emitted

    WARNING - Rolled back open transaction before returning DB connection

…on every poll. The warning is real (a leaked txn would hold locks and bloat
table churn) but the rollback noise drowned the signal it was meant to
surface.

The fix: read-only callers go through the existing ``db_connection()``
context manager, which commits on success (no-op for read-only queries
but transitions the conn to IDLE) and rolls back on exception. These tests
pin both invariants.
"""

import importlib
import inspect
import re

import pytest
import psycopg2
from psycopg2 import extensions

from src.database import connection as db_connection_module


# ---------------------------------------------------------------------------
# Source-level contract: every read-only backtest helper uses db_connection()
# ---------------------------------------------------------------------------
#
# Brittle-by-design: if a future edit drops back to the manual
# ``get_db_connection()`` / ``close_db_connection()`` pair the polling
# warning returns. The tests below would fail loudly, with a message
# pointing at the canonical context manager.


_READONLY_TARGETS = [
    ("src.backtesting.queries", ["get_run", "list_runs", "get_trades", "get_equity"]),
    ("src.backtesting.configs", ["list_configs", "get_config", "get_shared_config"]),
    ("src.backtesting.sweeps", ["get_sweep", "list_sweeps"]),
    ("src.api.routers.backtest", ["_build_meta_sync"]),
]


@pytest.mark.parametrize("module_path, fn_names", _READONLY_TARGETS)
def test_readonly_backtest_helpers_use_db_connection_context_manager(
    module_path, fn_names
):
    """Every read-only helper on the backtest API path must source its conn
    from ``db_connection()`` so the txn state is reset to IDLE on return.
    Raw ``get_db_connection()`` plus ``close_db_connection()`` leaves the
    conn ``INTRANS`` and triggers the polling-frequency warning."""
    module = importlib.import_module(module_path)
    for fn_name in fn_names:
        fn = getattr(module, fn_name)
        src = inspect.getsource(fn)
        assert re.search(r"with\s+db_connection\(\)\s+as\s+\w+", src), (
            f"{module_path}.{fn_name} must acquire its conn via "
            "`with db_connection() as conn:` — see "
            "test_backtest_readonly_no_open_tx module docstring for why."
        )
        assert "get_db_connection()" not in src, (
            f"{module_path}.{fn_name} still calls the bare get_db_connection() — "
            "this leaks an open SELECT txn back to the pool and re-triggers "
            "the noisy polling warning. Use `with db_connection() as conn:`."
        )


# ---------------------------------------------------------------------------
# Runtime contract: db_connection() leaves the conn IDLE on success
# ---------------------------------------------------------------------------


class _FakePool:
    def __init__(self):
        self.returned = []

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))


class _FakeConn:
    """Minimal psycopg2 conn stand-in tracking transaction-state transitions."""

    def __init__(self):
        # Mirrors a fresh checkout: pool returns a conn that's currently
        # IDLE, and the caller's first execute() flips it to INTRANS.
        self._tx_status = extensions.TRANSACTION_STATUS_IDLE
        self.closed = 0
        self.commit_calls = 0
        self.rollback_calls = 0

    # close_db_connection's contract:
    def get_transaction_status(self):
        return self._tx_status

    def commit(self):
        self.commit_calls += 1
        self._tx_status = extensions.TRANSACTION_STATUS_IDLE

    def rollback(self):
        self.rollback_calls += 1
        self._tx_status = extensions.TRANSACTION_STATUS_IDLE

    # Lets the caller simulate "ran a SELECT", which is what psycopg2 does
    # implicitly the first time .execute() runs without autocommit.
    def begin_implicit_tx(self):
        self._tx_status = extensions.TRANSACTION_STATUS_INTRANS


@pytest.fixture
def patched_pool(monkeypatch):
    pool = _FakePool()
    monkeypatch.setattr(db_connection_module, "_connection_pool", pool)

    def _hand_out(conn):
        monkeypatch.setattr(
            db_connection_module, "get_db_connection", lambda: conn
        )

    return pool, _hand_out


def test_db_connection_commits_on_success_so_close_does_not_warn(
    patched_pool, caplog
):
    """``db_connection()`` must commit at clean exit so the conn returns to
    the pool IDLE and ``close_db_connection`` finds nothing to roll back.
    If commit is dropped, the polling-frequency warning comes back."""
    _pool, hand_out = patched_pool
    conn = _FakeConn()
    hand_out(conn)

    import logging
    caplog.set_level(logging.WARNING, logger="src.database.connection")

    with db_connection_module.db_connection() as got:
        # Simulate the implicit txn that any psycopg2 SELECT opens.
        got.begin_implicit_tx()
        assert got is conn

    assert conn.commit_calls == 1
    assert conn.rollback_calls == 0
    assert conn._tx_status == extensions.TRANSACTION_STATUS_IDLE

    # And — the regression — no rollback-on-return warning was emitted.
    rollback_warnings = [
        r for r in caplog.records
        if "Rolled back open transaction" in r.getMessage()
    ]
    assert rollback_warnings == [], (
        "db_connection() left the conn INTRANS; close_db_connection rolled "
        "it back and re-emitted the polling-frequency warning that this fix "
        "is meant to silence."
    )


def test_db_connection_rolls_back_on_exception(patched_pool):
    """Exceptions inside the ``with`` block must rollback (not commit) and
    re-raise — the existing semantics that ``get_db_connection`` callers
    were doing manually, and the reason it's safe to swap them in."""
    _pool, hand_out = patched_pool
    conn = _FakeConn()
    hand_out(conn)

    with pytest.raises(RuntimeError, match="boom"):
        with db_connection_module.db_connection() as got:
            got.begin_implicit_tx()
            raise RuntimeError("boom")

    assert conn.commit_calls == 0
    assert conn.rollback_calls == 1
    assert conn._tx_status == extensions.TRANSACTION_STATUS_IDLE


def test_writers_using_autocommit_pattern_are_intentionally_untouched():
    """The backtest write helpers (``save_config``, ``update_config``,
    ``delete_config``, ``create_run``, ``mark_run_*``, ``record_trade``,
    ``record_equity_point``, sweep-create) use ``conn.autocommit = True``
    instead of the context manager. That pattern is correct for writes
    that don't need multi-statement atomicity — each statement
    auto-commits and the conn returns IDLE on its own — so it does NOT
    trigger the polling warning either. Leaving them alone keeps this
    diff focused on the reader pattern.

    This test exists to document the deliberate non-change: if a future
    edit unifies the two patterns, it should be a separate, scoped
    decision (multi-statement writes would need real txns, not autocommit)
    rather than incidental drift from this fix.
    """
    from src.backtesting import configs as configs_module

    src = inspect.getsource(configs_module.save_config)
    assert "conn.autocommit = True" in src
    assert "with db_connection()" not in src