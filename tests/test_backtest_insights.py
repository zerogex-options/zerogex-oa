"""Tests for the pattern-insights read path (/api/backtest/insights/patterns).

Covers ``_derive_pattern_economics`` (the PF / expectancy / avg-win/loss math)
and ``get_pattern_insights`` (the DB read, with a scripted connection so no
database is required). The router itself is a one-liner over to_thread and is
exercised indirectly through ``get_pattern_insights``.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.backtesting import queries


# ----------------------------------------------------------------------
# _derive_pattern_economics
# ----------------------------------------------------------------------


def _econ(n=20, w=10, ll=10, gw=200.0, gl=100.0):
    return queries._derive_pattern_economics(n, w, ll, gw, gl)


def test_derive_basic_winners_and_losers():
    e = _econ(n=20, w=10, ll=10, gw=200.0, gl=100.0)
    assert e["net_pnl"] == pytest.approx(100.0)
    assert e["profit_factor"] == pytest.approx(2.0)
    assert e["expectancy"] == pytest.approx(5.0)
    assert e["avg_win_pnl"] == pytest.approx(20.0)
    assert e["avg_loss_pnl"] == pytest.approx(10.0)


def test_derive_returns_all_nones_when_gross_pnls_missing():
    # Touch-source rows persist NULL for the dollar columns; the helper must
    # not invent fake economics from the counts alone.
    e = queries._derive_pattern_economics(20, 10, 10, None, None)
    assert all(e[k] is None for k in (
        "net_pnl", "profit_factor", "expectancy", "avg_win_pnl", "avg_loss_pnl",
    ))


def test_derive_pf_none_when_no_losses():
    # PF is undefined (not ∞) when there were no losing trades, so the JSON
    # payload stays representable end-to-end.
    e = queries._derive_pattern_economics(5, 5, 0, 250.0, 0.0)
    assert e["profit_factor"] is None
    assert e["net_pnl"] == pytest.approx(250.0)
    assert e["expectancy"] == pytest.approx(50.0)
    assert e["avg_win_pnl"] == pytest.approx(50.0)
    assert e["avg_loss_pnl"] is None       # no losses to average


def test_derive_pf_zero_when_all_lose():
    # Every trade lost: PF is 0/gross_loss = 0 (numerator is 0, not undefined).
    e = queries._derive_pattern_economics(5, 0, 5, 0.0, 250.0)
    assert e["profit_factor"] == pytest.approx(0.0)
    assert e["net_pnl"] == pytest.approx(-250.0)
    assert e["avg_win_pnl"] is None


def test_derive_handles_zero_resolved_count():
    e = queries._derive_pattern_economics(0, 0, 0, 0.0, 0.0)
    assert e["expectancy"] is None
    # PF over (0/0) is undefined ⇒ None; net is 0; avgs are None.
    assert e["profit_factor"] is None
    assert e["avg_win_pnl"] is None
    assert e["avg_loss_pnl"] is None


# ----------------------------------------------------------------------
# get_pattern_insights — uses a scripted connection (no DB)
# ----------------------------------------------------------------------


class _Cur:
    """Scripted cursor — records executed SQL and returns canned rows."""

    def __init__(self, rows):
        self._rows = rows
        self.executed: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._cur = _Cur(rows)

    def cursor(self):
        return self._cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def _row():
    # Build a row tuple matching the SELECT column order in get_pattern_insights:
    # pattern, underlying, window_start, window_end,
    # n_emitted, n_resolved, n_target_hit, n_stop_hit,
    # hit_rate, proposed_base, gross_win_pnl, gross_loss_pnl,
    # source, computed_at
    def make(
        pattern="p", underlying="SPY",
        ws=date(2026, 4, 1), we=date(2026, 6, 1),
        n_emitted=20, n_resolved=20, n_wins=10, n_losses=10,
        hit_rate=0.5, proposed_base=0.50,
        gross_win_pnl=200.0, gross_loss_pnl=100.0,
        source="option_pnl",
        computed_at=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
    ):
        return (
            pattern, underlying, ws, we,
            n_emitted, n_resolved, n_wins, n_losses,
            hit_rate, proposed_base, gross_win_pnl, gross_loss_pnl,
            source, computed_at,
        )
    return make


def test_insights_returns_one_row_per_pair_with_derived_econ(monkeypatch, _row):
    conn = _Conn([
        _row(pattern="a", underlying="QQQ", gross_win_pnl=300.0, gross_loss_pnl=100.0),
        _row(pattern="b", underlying="SPY", gross_win_pnl=50.0,  gross_loss_pnl=200.0),
    ])
    monkeypatch.setattr(queries, "db_connection", lambda: conn)
    rows = queries.get_pattern_insights(source="option_pnl")
    assert len(rows) == 2
    by_pair = {(r["pattern"], r["underlying"]): r for r in rows}
    a = by_pair[("a", "QQQ")]
    assert a["net_pnl"] == pytest.approx(200.0)
    assert a["profit_factor"] == pytest.approx(3.0)
    assert a["expectancy"] == pytest.approx(10.0)
    # Window dates round-trip through ISO.
    assert a["window_start"] == "2026-04-01"
    assert a["window_end"] == "2026-06-01"
    # Source comes through verbatim.
    assert a["source"] == "option_pnl"


def test_insights_default_orders_by_net_pnl_desc(monkeypatch, _row):
    # Three rows with mixed economics. Default order = winners first, biggest
    # net first, then size as tiebreak. None-net rows go last.
    conn = _Conn([
        _row(pattern="winner", gross_win_pnl=400.0, gross_loss_pnl=100.0),       # net +300
        _row(pattern="big_loser", gross_win_pnl=50.0, gross_loss_pnl=500.0),     # net −450
        _row(pattern="no_econ", gross_win_pnl=None, gross_loss_pnl=None),         # net None
    ])
    monkeypatch.setattr(queries, "db_connection", lambda: conn)
    out = queries.get_pattern_insights(source="option_pnl")
    assert [r["pattern"] for r in out] == ["winner", "big_loser", "no_econ"]


def test_insights_query_is_scoped_by_source(monkeypatch, _row):
    conn = _Conn([_row()])
    monkeypatch.setattr(queries, "db_connection", lambda: conn)
    queries.get_pattern_insights(source="underlying_touch")
    sql, params = conn._cur.executed[0]
    assert "source = %s" in " ".join(sql.split())
    assert params[0] == "underlying_touch"


def test_insights_unknown_source_falls_back_to_option_pnl(monkeypatch, _row):
    conn = _Conn([_row()])
    monkeypatch.setattr(queries, "db_connection", lambda: conn)
    queries.get_pattern_insights(source="bogus")
    _, params = conn._cur.executed[0]
    assert params[0] == "option_pnl"


def test_insights_filters_by_underlying(monkeypatch, _row):
    conn = _Conn([_row()])
    monkeypatch.setattr(queries, "db_connection", lambda: conn)
    queries.get_pattern_insights(source="option_pnl", underlying="qqq")
    sql, params = conn._cur.executed[0]
    flat = " ".join(sql.split())
    assert "AND underlying = %s" in flat
    # Underlying is upper-cased before the bind so the table's canonical form
    # is hit regardless of how the request was capitalized.
    assert params == ["option_pnl", "QQQ"]


def test_insights_touch_source_passes_through_null_econ(monkeypatch, _row):
    # A touch row has NULL gross_win_pnl / gross_loss_pnl; the helper must
    # surface that as None on every derived field, not zeros.
    conn = _Conn([_row(source="underlying_touch", gross_win_pnl=None, gross_loss_pnl=None)])
    monkeypatch.setattr(queries, "db_connection", lambda: conn)
    rows = queries.get_pattern_insights(source="underlying_touch")
    assert rows[0]["net_pnl"] is None
    assert rows[0]["profit_factor"] is None
    assert rows[0]["expectancy"] is None
    assert rows[0]["gross_win_pnl"] is None
    assert rows[0]["gross_loss_pnl"] is None
