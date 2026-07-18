"""Tests for the cash-index session-open phantom repair logic.

The DB path needs a live connection, but the pure logic — the OHLC
reconstruction, symbol resolution, the shared SQL, and the dry-run vs execute
branching — is exercised here with fakes (mirroring test_underlying_backfill.py,
which pins the same kind of DB-adjacent contract without a real Postgres). The
values below are the real SPX bars from 2026-07-13..17.
"""

from __future__ import annotations

import contextlib
from datetime import date, datetime, timezone

from src.tools.cash_index_open_repair import (
    _SELECT_CANDIDATES_SQL,
    _SINGLE_BAR_REPAIR_SQL,
    _TARGETS_CTE,
    _UPDATE_SQL,
    default_cash_index_symbols,
    reconstruct_session_open,
    repair,
    repair_one_session_open,
    resolve_symbols,
)


# ----------------------------------------------------------------------
# reconstruct_session_open — the OHLC rebuild (no prior close needed)
# ----------------------------------------------------------------------
def test_reconstruct_gap_down_strips_phantom_open_and_high():
    # 7/17 SPX: stale rotation value is the open AND the high (gap down).
    # Real data = close 7447.71 and low 7441.62; rebuild anchors on close.
    assert reconstruct_session_open(7533.77, 7533.77, 7441.62, 7447.71) == (
        7447.71,  # open -> close
        7447.71,  # high -> close (phantom high dropped)
        7441.62,  # low  -> real low preserved
    )


def test_reconstruct_gap_up_strips_phantom_open_and_low():
    # 7/14 SPX: stale value is the open AND the low (gap up); real high kept.
    assert reconstruct_session_open(7515.34, 7538.83, 7515.34, 7533.00) == (
        7533.00,  # open -> close
        7538.83,  # high -> real high preserved
        7533.00,  # low  -> close (phantom low dropped)
    )


def test_reconstruct_gap_up_collapses_when_only_close_is_real():
    # 7/13 SPX: O=L=7547.53 (stale), H=C=7557.94. The real low is masked, so the
    # bar honestly collapses to the close level.
    assert reconstruct_session_open(7547.53, 7557.94, 7547.53, 7557.94) == (
        7557.94,
        7557.94,
        7557.94,
    )


def test_reconstruct_returns_none_when_open_inside_range():
    # 7/15 SPX: open is strictly inside [low, high] -> small overnight move, no
    # visible phantom, left untouched.
    assert reconstruct_session_open(7574.73, 7576.39, 7569.59, 7572.01) is None


def test_reconstruct_returns_none_on_flat_bar():
    # open == close -> nothing gapped, nothing to fix.
    assert reconstruct_session_open(7500.0, 7500.0, 7500.0, 7500.0) is None


# ----------------------------------------------------------------------
# Symbol resolution
# ----------------------------------------------------------------------
def test_default_symbols_are_the_cash_indexes():
    assert default_cash_index_symbols() == ["DJX", "NDX", "RUT", "SPX"]


def test_resolve_symbols_defaults_to_all_cash_indexes():
    assert resolve_symbols(None) == ["DJX", "NDX", "RUT", "SPX"]
    assert resolve_symbols("   ") == ["DJX", "NDX", "RUT", "SPX"]


def test_resolve_symbols_splits_comma_and_whitespace_and_dedups():
    assert resolve_symbols("SPX,NDX") == ["SPX", "NDX"]
    assert resolve_symbols("SPX NDX") == ["SPX", "NDX"]
    assert resolve_symbols("spx, spx  ndx") == ["SPX", "NDX"]


def test_resolve_symbols_drops_non_cash_indexes():
    # SPY is an ETF with a real pre-market open — never a phantom, never touched.
    assert resolve_symbols("SPX,SPY") == ["SPX"]
    assert resolve_symbols("SPY") == []


# ----------------------------------------------------------------------
# SQL contract — self-contained detection (open is an extreme of a gapped bar)
# ----------------------------------------------------------------------
def test_targets_cte_pins_detection_and_scope():
    norm = " ".join(_TARGETS_CTE.split())
    # Phantom signature: the stale open is an extreme, and the bar gapped.
    assert "(open = high OR open = low) AND open <> close" in norm
    # Only the 09:30 ET open bar, DST-correct via the tz conversion.
    assert "(timezone('America/New_York', timestamp))::time = TIME '09:30:00'" in norm
    # Symbol + optional date bounds are parameterized.
    assert "symbol = ANY(%(symbols)s)" in norm
    assert "%(start)s::date" in norm and "%(end)s::date" in norm
    # Rebuild uses GREATEST/LEAST over the non-stale values.
    assert "GREATEST(" in norm and "LEAST(" in norm
    # No prior-close lookup anymore.
    assert "prior_close" not in norm and "LATERAL" not in norm


def test_select_and_update_share_the_cte():
    cte = " ".join(_TARGETS_CTE.split())
    assert cte in " ".join(_SELECT_CANDIDATES_SQL.split())
    assert cte in " ".join(_UPDATE_SQL.split())
    upd = " ".join(_UPDATE_SQL.split())
    assert "UPDATE underlying_quotes u" in upd
    assert "open = t.new_open" in upd and "high = t.new_high" in upd and "low = t.new_low" in upd


def test_single_bar_repair_sql_targets_one_bar():
    norm = " ".join(_SINGLE_BAR_REPAIR_SQL.split())
    assert "UPDATE underlying_quotes" in norm
    assert "SET open = close" in norm
    assert "WHERE symbol = %(symbol)s AND timestamp = %(ts)s" in norm
    assert "(open = high OR open = low) AND open <> close" in norm


# ----------------------------------------------------------------------
# repair_one_session_open — the single-bar path the ingester calls
# ----------------------------------------------------------------------
class _FakeCursor:
    def __init__(self, rows=None, rowcount=0):
        self._rows = rows or []
        self.rowcount = rowcount
        self.calls = []  # (sql, params)

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur
        self.commits = 0

    def cursor(self):
        return self._cur

    def commit(self):
        self.commits += 1


def test_repair_one_session_open_executes_single_bar_update():
    cur = _FakeCursor(rowcount=1)
    conn = _FakeConn(cur)
    ts = datetime(2026, 7, 17, 13, 30, tzinfo=timezone.utc)
    n = repair_one_session_open(conn, "SPX", ts)

    assert n == 1
    assert len(cur.calls) == 1
    sql, params = cur.calls[0]
    assert "UPDATE underlying_quotes" in sql
    assert params == {"symbol": "SPX", "ts": ts}


# ----------------------------------------------------------------------
# repair() dry-run vs execute branching (fake DB)
# ----------------------------------------------------------------------
def _factory(cur):
    @contextlib.contextmanager
    def _cm():
        yield _FakeConn(cur)

    return _cm


_TS = datetime(2026, 7, 17, 13, 30, tzinfo=timezone.utc)  # 09:30 ET on an EDT day
# (symbol, timestamp, old_open, old_high, old_low, close, new_open, new_high, new_low)
_TARGET_ROW = ("SPX", _TS, 7533.77, 7533.77, 7441.62, 7447.71, 7447.71, 7447.71, 7441.62)


def test_repair_dry_run_selects_but_does_not_update():
    cur = _FakeCursor(rows=[_TARGET_ROW], rowcount=0)
    candidates, updated = repair(
        ["SPX"], date(2026, 7, 1), date(2026, 7, 17), dry_run=True, conn_factory=_factory(cur)
    )

    assert updated == 0
    assert len(candidates) == 1
    row = candidates[0]
    assert row["old_open"] == 7533.77
    assert (row["new_open"], row["new_high"], row["new_low"]) == (7447.71, 7447.71, 7441.62)
    # Only the SELECT ran — no UPDATE in dry-run.
    assert len(cur.calls) == 1
    assert "UPDATE" not in cur.calls[0][0]
    assert cur.calls[0][1] == {
        "symbols": ["SPX"],
        "start": date(2026, 7, 1),
        "end": date(2026, 7, 17),
    }


def test_repair_execute_runs_update_after_select():
    cur = _FakeCursor(rows=[_TARGET_ROW], rowcount=1)
    candidates, updated = repair(["SPX"], dry_run=False, conn_factory=_factory(cur))

    assert updated == 1
    assert len(candidates) == 1
    assert len(cur.calls) == 2
    assert "UPDATE underlying_quotes" in cur.calls[1][0]
    assert cur.calls[1][1] == {"symbols": ["SPX"], "start": None, "end": None}


def test_repair_execute_skips_update_when_nothing_to_fix():
    cur = _FakeCursor(rows=[], rowcount=0)
    candidates, updated = repair(["SPX"], dry_run=False, conn_factory=_factory(cur))

    assert candidates == [] and updated == 0
    assert len(cur.calls) == 1
    assert "UPDATE" not in cur.calls[0][0]
