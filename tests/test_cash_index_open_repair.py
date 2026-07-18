"""Tests for the cash-index session-open phantom repair tool.

The DB path needs a live connection, but the pure logic — the OHLC
reconstruction, symbol resolution, the shared SQL (detection + rebuild), and the
dry-run vs execute branching — is exercised here with fakes (mirroring
test_underlying_backfill.py, which pins the same kind of DB-adjacent contract
without a real Postgres).
"""

from __future__ import annotations

import contextlib
from datetime import date, datetime, timezone

from src.tools.cash_index_open_repair import (
    _SELECT_CANDIDATES_SQL,
    _TARGETS_CTE,
    _UPDATE_SQL,
    default_cash_index_symbols,
    reconstruct_session_open,
    repair,
    resolve_symbols,
)


# ----------------------------------------------------------------------
# reconstruct_session_open — the OHLC rebuild
# ----------------------------------------------------------------------
def test_reconstruct_gap_down_strips_phantom_open_and_high():
    # Jul 17 SPX: prior close 7533.77 carried into open AND high (gap down).
    # Real data = close 7447.71 and low 7441.62; rebuild anchors on close.
    assert reconstruct_session_open(7533.77, 7533.77, 7441.62, 7447.71, 7533.77) == (
        7447.71,  # open  -> close
        7447.71,  # high  -> close (phantom high dropped)
        7441.62,  # low   -> real low preserved
    )


def test_reconstruct_gap_up_strips_phantom_open_and_low():
    # Prior close carried into open AND low (gap up): real high is preserved.
    assert reconstruct_session_open(7515.34, 7538.83, 7515.34, 7533.00, 7515.34) == (
        7533.00,  # open -> close
        7538.83,  # high -> real high preserved
        7533.00,  # low  -> close (phantom low dropped)
    )


def test_reconstruct_open_strictly_outside_range_keeps_real_extremes():
    # Phantom open above the whole real bar; high/low are both genuine here.
    assert reconstruct_session_open(7530.0, 7490.0, 7440.0, 7480.0, 7530.0) == (
        7480.0,  # open -> close
        7490.0,  # high preserved
        7440.0,  # low preserved
    )


def test_reconstruct_open_within_range_only_fixes_open():
    # open == prior close but the first minute traded both sides of it, so the
    # extremes are real — only the open is nudged to the close.
    assert reconstruct_session_open(7574.73, 7576.39, 7569.59, 7572.01, 7574.73) == (
        7572.01,
        7576.39,
        7569.59,
    )


def test_reconstruct_returns_none_when_not_a_phantom():
    # open != prior close -> a genuine open, never touched.
    assert reconstruct_session_open(7500.0, 7510.0, 7495.0, 7505.0, 7533.77) is None
    # flat no-op: open == prior close == close -> nothing to fix.
    assert reconstruct_session_open(7500.0, 7500.0, 7500.0, 7500.0, 7500.0) is None


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
# SQL contract — detection by prior close, rebuild by GREATEST/LEAST
# ----------------------------------------------------------------------
def test_targets_cte_pins_detection_and_scope():
    norm = " ".join(_TARGETS_CTE.split())
    # Detection: open equals the prior session's close, and not a flat no-op.
    assert "uq.open = pc.prior_close" in norm
    assert "uq.open <> uq.close" in norm
    # Prior close comes from the most recent row strictly before the bar.
    assert "p.timestamp < ob.timestamp" in norm and "ORDER BY p.timestamp DESC" in norm
    # Only the 09:30 ET open bar, DST-correct via the tz conversion.
    assert "(timezone('America/New_York', timestamp))::time = TIME '09:30:00'" in norm
    # Symbol + optional date bounds are parameterized.
    assert "symbol = ANY(%(symbols)s)" in norm
    assert "%(start)s::date" in norm and "%(end)s::date" in norm
    # Rebuild uses GREATEST/LEAST over the non-phantom values.
    assert "GREATEST(" in norm and "LEAST(" in norm


def test_select_and_update_share_the_cte():
    cte = " ".join(_TARGETS_CTE.split())
    assert cte in " ".join(_SELECT_CANDIDATES_SQL.split())
    assert cte in " ".join(_UPDATE_SQL.split())
    upd = " ".join(_UPDATE_SQL.split())
    # The UPDATE writes the reconstructed open/high/low from the CTE.
    assert "UPDATE underlying_quotes u" in upd
    assert "open = t.new_open" in upd
    assert "high = t.new_high" in upd
    assert "low = t.new_low" in upd


# ----------------------------------------------------------------------
# repair() dry-run vs execute branching (fake DB)
# ----------------------------------------------------------------------
class _FakeCursor:
    def __init__(self, rows, rowcount):
        self._rows = rows
        self.rowcount = rowcount
        self.calls = []  # (sql, params)

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def commit(self):
        pass


def _factory(cur):
    @contextlib.contextmanager
    def _cm():
        yield _FakeConn(cur)

    return _cm


_TS = datetime(2026, 7, 17, 13, 30, tzinfo=timezone.utc)  # 09:30 ET on an EDT day
# (symbol, timestamp, old_open, old_high, old_low, close, prior_close, new_open, new_high, new_low)
_TARGET_ROW = ("SPX", _TS, 7533.77, 7533.77, 7441.62, 7447.71, 7533.77, 7447.71, 7447.71, 7441.62)


def test_repair_dry_run_selects_but_does_not_update():
    cur = _FakeCursor(rows=[_TARGET_ROW], rowcount=0)
    candidates, updated = repair(
        ["SPX"], date(2026, 7, 1), date(2026, 7, 17), dry_run=True, conn_factory=_factory(cur)
    )

    assert updated == 0
    assert len(candidates) == 1
    row = candidates[0]
    assert row["symbol"] == "SPX"
    assert row["old_open"] == 7533.77 and row["prior_close"] == 7533.77
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
    # A clean DB is never written to — SELECT only, no UPDATE.
    assert len(cur.calls) == 1
    assert "UPDATE" not in cur.calls[0][0]
