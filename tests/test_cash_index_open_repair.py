"""Tests for the cash-index session-open phantom repair tool.

The DB path needs a live connection, but the pure logic — phantom detection,
symbol resolution, the shared SQL predicate, and the dry-run vs execute
branching — is exercised here with fakes (mirroring test_underlying_backfill.py,
which pins the same kind of DB-adjacent contract without a real Postgres).
"""

from __future__ import annotations

import contextlib
from datetime import date, datetime, timezone

from src.tools.cash_index_open_repair import (
    _PHANTOM_PREDICATE,
    _SELECT_CANDIDATES_SQL,
    _UPDATE_SQL,
    default_cash_index_symbols,
    is_phantom_session_open,
    repair,
    resolve_symbols,
)


# ----------------------------------------------------------------------
# Phantom detection
# ----------------------------------------------------------------------
def test_is_phantom_open_flags_out_of_range_open():
    # Gap-down day: prior-close open above the bar's own high.
    assert is_phantom_session_open(7530.0, 7490.0, 7440.0) is True
    # Gap-up day: prior-close open below the bar's own low.
    assert is_phantom_session_open(7400.0, 7490.0, 7440.0) is True


def test_is_phantom_open_accepts_in_range_open():
    # A genuine first print is within [low, high] — not a phantom.
    assert is_phantom_session_open(7460.0, 7490.0, 7440.0) is False
    # Edges are in-range (the first print can be the high or the low).
    assert is_phantom_session_open(7490.0, 7490.0, 7440.0) is False
    assert is_phantom_session_open(7440.0, 7490.0, 7440.0) is False


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
# SQL contract — the dry-run SELECT and the UPDATE must share one predicate
# ----------------------------------------------------------------------
def test_predicate_pins_phantom_and_session_open_and_scope():
    norm = " ".join(_PHANTOM_PREDICATE.split())
    # Phantom signature: open outside the bar's own [low, high].
    assert "(open > high OR open < low)" in norm
    # Only the 09:30 ET open bar, DST-correct via the tz conversion.
    assert "(timezone('America/New_York', timestamp))::time = TIME '09:30:00'" in norm
    # Symbol + optional date bounds are parameterized.
    assert "symbol = ANY(%(symbols)s)" in norm
    assert "%(start)s::date" in norm and "%(end)s::date" in norm


def test_select_and_update_share_the_predicate():
    pred = " ".join(_PHANTOM_PREDICATE.split())
    assert pred in " ".join(_SELECT_CANDIDATES_SQL.split())
    assert pred in " ".join(_UPDATE_SQL.split())
    # The repair replaces the phantom open with the bar's own close.
    assert "SET open = close" in " ".join(_UPDATE_SQL.split())


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
_PHANTOM_ROW = ("SPX", _TS, 7530.0, 7490.0, 7440.0, 7480.0)


def test_repair_dry_run_selects_but_does_not_update():
    cur = _FakeCursor(rows=[_PHANTOM_ROW], rowcount=0)
    candidates, updated = repair(
        ["SPX"], date(2026, 7, 1), date(2026, 7, 17), dry_run=True, conn_factory=_factory(cur)
    )

    assert updated == 0
    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "SPX"
    assert candidates[0]["open"] == 7530.0 and candidates[0]["close"] == 7480.0
    # Only the SELECT ran — no UPDATE in dry-run.
    assert len(cur.calls) == 1
    assert "SELECT" in cur.calls[0][0] and "UPDATE" not in cur.calls[0][0]
    # Params are threaded through verbatim.
    assert cur.calls[0][1] == {
        "symbols": ["SPX"],
        "start": date(2026, 7, 1),
        "end": date(2026, 7, 17),
    }


def test_repair_execute_runs_update_after_select():
    cur = _FakeCursor(rows=[_PHANTOM_ROW], rowcount=1)
    candidates, updated = repair(["SPX"], dry_run=False, conn_factory=_factory(cur))

    assert updated == 1
    assert len(candidates) == 1
    # SELECT first, then the UPDATE.
    assert len(cur.calls) == 2
    assert "SELECT" in cur.calls[0][0]
    assert "UPDATE underlying_quotes" in cur.calls[1][0]
    # No date bounds -> None passed through (predicate short-circuits them).
    assert cur.calls[1][1] == {"symbols": ["SPX"], "start": None, "end": None}


def test_repair_execute_skips_update_when_nothing_to_fix():
    cur = _FakeCursor(rows=[], rowcount=0)
    candidates, updated = repair(["SPX"], dry_run=False, conn_factory=_factory(cur))

    assert candidates == [] and updated == 0
    # A clean DB is never written to — SELECT only, no UPDATE.
    assert len(cur.calls) == 1
    assert "SELECT" in cur.calls[0][0]
