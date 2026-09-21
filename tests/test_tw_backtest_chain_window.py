"""The replay window's coverage probe must be cheap and must fail safe.

`_chain_window` clamps a screen to the instants that can actually be priced.
It reads ``option_chains_archive``, which is retention-EXEMPT and grows
forever — and which has no index with ``timestamp`` leading (PK is
``(option_symbol, timestamp)``, the only other index is
``(underlying, timestamp)``). An unqualified global min/max there seq-scans
the table, gets slower every night, and on 2026-09-14 exceeded the 90-second
``statement_timeout`` and blocked every screen outright.

So the probe is a bounded loose index scan with a hot-table fallback, and
these tests pin the two properties that keep it safe rather than the SQL text.
"""

from __future__ import annotations

from datetime import datetime, timezone

import psycopg2.errors
import pytest

from src.tradeworkz import backtest as bt

_HOT_LO = datetime(2026, 6, 15, 13, 30, tzinfo=timezone.utc)
_HOT_HI = datetime(2026, 9, 12, 20, 0, tzinfo=timezone.utc)
_ARCH_LO = datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
_ARCH_HI = datetime(2026, 9, 12, 1, 54, tzinfo=timezone.utc)


class _Cursor:
    """Records every statement; answers the two reads the probe makes."""

    def __init__(self, *, archive_rows=None, archive_raises=None, savepoint_raises=False):
        self.executed: list[str] = []
        self._archive_rows = archive_rows
        self._archive_raises = archive_raises
        self._savepoint_raises = savepoint_raises
        self._last = None

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        self.executed.append(text)
        if self._savepoint_raises and text.startswith("SAVEPOINT"):
            raise psycopg2.errors.InvalidTransactionState("no transaction")
        if "FROM option_chains " in f"{text} " and "archive" not in text:
            self._last = (_HOT_LO, _HOT_HI)
            return
        if "option_chains_archive" in text and text.startswith("WITH RECURSIVE"):
            if self._archive_raises is not None:
                raise self._archive_raises
            self._last = self._archive_rows
            return
        if "to_regclass" in text:
            self._last = (True,)
            return
        self._last = None

    def fetchone(self):
        return self._last


class _Conn:
    def __init__(self, cursor):
        self._cur = cursor

    def cursor(self):
        return self._cur


def _conn(**kw):
    return _Conn(_Cursor(**kw))


# ---------------------------------------------------------------------------
# The fast path
# ---------------------------------------------------------------------------


def test_window_spans_hot_and_archive():
    conn = _conn(archive_rows=(_ARCH_LO, _ARCH_HI))
    lo, hi = bt._chain_window(conn)
    assert lo == _ARCH_LO  # archive reaches further back
    assert hi == _HOT_HI  # hot table is fresher


def test_archive_probe_never_issues_an_unqualified_global_min_max():
    """The regression. A bare aggregate over the archive is the seq scan."""
    conn = _conn(archive_rows=(_ARCH_LO, _ARCH_HI))
    bt._chain_window(conn)
    for sql in conn._cur.executed:
        if "option_chains_archive" in sql and ("MIN(" in sql or "MAX(" in sql):
            assert "WHERE" in sql or "WITH RECURSIVE" in sql, sql


def test_archive_probe_walks_underlyings_off_the_existing_index():
    conn = _conn(archive_rows=(_ARCH_LO, _ARCH_HI))
    bt._chain_window(conn)
    probe = next(s for s in conn._cur.executed if s.startswith("WITH RECURSIVE"))
    # Endpoints are taken per-underlying, which the (underlying, timestamp)
    # index serves as a seek rather than a scan.
    assert "a.underlying = s.underlying" in probe
    assert "a.underlying > s.underlying" in probe


# ---------------------------------------------------------------------------
# Failing safe
# ---------------------------------------------------------------------------


def test_a_timed_out_archive_probe_degrades_to_the_hot_window():
    """A slow archive must not block the screen entirely.

    The clamp is an optimisation: losing archive reach costs depth, while
    raising here costs the whole run.
    """
    conn = _conn(archive_raises=psycopg2.errors.QueryCanceled("statement timeout"))
    lo, hi = bt._chain_window(conn)
    assert (lo, hi) == (_HOT_LO, _HOT_HI)


def test_a_failed_probe_rolls_the_savepoint_back():
    """A timed-out statement poisons the transaction unless rolled back."""
    conn = _conn(archive_raises=psycopg2.errors.QueryCanceled("statement timeout"))
    bt._chain_window(conn)
    assert any(s.startswith("ROLLBACK TO SAVEPOINT") for s in conn._cur.executed)


def test_the_statement_timeout_override_is_always_rolled_back():
    """SET LOCAL survives RELEASE but not ROLLBACK.

    Releasing on the happy path would leave the 10s ceiling in force for the
    rest of the transaction and kill the replay's legitimately slow reads, so
    the probe must roll back even when it succeeds.
    """
    conn = _conn(archive_rows=(_ARCH_LO, _ARCH_HI))
    bt._chain_window(conn)
    stmts = conn._cur.executed
    assert any("statement_timeout" in s for s in stmts)
    set_at = next(i for i, s in enumerate(stmts) if "statement_timeout" in s)
    rollback_at = next(i for i, s in enumerate(stmts) if s.startswith("ROLLBACK TO SAVEPOINT"))
    assert rollback_at > set_at, "override outlived the probe"


def test_the_probe_is_bounded():
    assert 0 < bt._ARCHIVE_WINDOW_TIMEOUT_MS <= 30_000


def test_no_archive_table_leaves_the_hot_window_untouched(monkeypatch):
    monkeypatch.setattr(bt, "_archive_available", lambda conn: False)
    conn = _conn()
    assert bt._chain_window(conn) == (_HOT_LO, _HOT_HI)
    assert not any("option_chains_archive" in s for s in conn._cur.executed)


def test_an_empty_archive_is_not_treated_as_coverage():
    conn = _conn(archive_rows=(None, None))
    assert bt._chain_window(conn) == (_HOT_LO, _HOT_HI)


def test_a_savepoint_that_cannot_be_taken_degrades_quietly():
    conn = _conn(savepoint_raises=True)
    assert bt._chain_window(conn) == (_HOT_LO, _HOT_HI)


@pytest.mark.parametrize(
    "rows,expected_lo",
    [((_ARCH_LO, _ARCH_HI), _ARCH_LO), ((None, None), _HOT_LO)],
)
def test_archive_only_widens_never_narrows(rows, expected_lo):
    conn = _conn(archive_rows=rows)
    lo, _hi = bt._chain_window(conn)
    assert lo == expected_lo
