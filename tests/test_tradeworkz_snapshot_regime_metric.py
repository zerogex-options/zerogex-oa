"""Regression: MarketSnapshot.net_gex must read gex_summary.net_gex_at_spot.

Two different columns on ``gex_summary`` measure "net GEX":

  * ``total_net_gex``   — sum of per-strike net GEX across the whole
                          chain, unweighted. Far-OTM long-dated strikes
                          get counted at full weight. Can drift sign
                          relative to at-spot when a heavy long-dated
                          tail dominates.
  * ``net_gex_at_spot`` — the DTE horizon-occupancy-weighted value of
                          the spot-shift gamma profile at the current
                          spot. The regime-correct headline figure
                          that /gex-heatmap and every other GEX-
                          consuming page on the site displays.

The analytics engine explicitly documents this at
``src/analytics/main_engine.py::_calculate_gex_summary``:
    "net_gex_at_spot is the regime-correct headline figure;
     total_net_gex is retained as the raw chain aggregate.
     Downstream consumers must not treat them as interchangeable."

TradeWorkz initially read ``total_net_gex`` and classified regime off
that. On days like 2026-07-09 where the two disagreed in sign around
10 AM ET, every regime-gated bot ran on the wrong signal — the fleet
went 100% bearish while the site was showing positive γ. This test
locks the fix in.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from src.tradeworkz.context import build_snapshot


class _FakeCursor:
    """Fake psycopg2 cursor that returns pre-scripted rows for each SQL."""

    def __init__(self):
        # Each entry is (matcher, rows_to_return). matcher is a substring
        # test on the SQL — order matters, first match wins.
        self._script: List[Tuple[str, List[tuple]]] = []
        self._pending: List[tuple] = []
        self.last_gex_sql: Optional[str] = None

    def program(self, sql_substring: str, rows: Iterable[tuple]) -> None:
        self._script.append((sql_substring, list(rows)))

    def execute(self, sql: str, params: Any = None) -> None:
        for matcher, rows in self._script:
            if matcher in sql:
                self._pending = list(rows)
                if "FROM gex_summary" in sql:
                    self.last_gex_sql = sql
                return
        self._pending = []

    def fetchone(self):
        return self._pending[0] if self._pending else None

    def fetchall(self):
        return list(self._pending)


class _FakeConn:
    def __init__(self):
        self._cursor = _FakeCursor()

    def cursor(self):
        return self._cursor


def _build_scripted_conn(
    total_net_gex: Optional[float],
    net_gex_at_spot: Optional[float],
) -> _FakeConn:
    """Wire up a fake conn that scripts every SELECT build_snapshot issues."""
    conn = _FakeConn()
    cur = conn.cursor()
    ts = datetime.now(timezone.utc)
    # underlying_quotes latest tick (timestamp, close)
    cur.program(
        "SELECT timestamp, close\n        FROM underlying_quotes",
        [(ts, 746.0)],
    )
    # underlying_quotes session bounds (MIN low, MAX high, sess_open)
    cur.program(
        "SELECT MIN(low), MAX(high)",
        [(739.0, 750.0, 745.0)],
    )
    # gex_summary — this is the row the test cares about. The SELECT
    # order is: net_gex_at_spot, gamma_flip_point, max_pain,
    # put_call_ratio, call_wall, put_wall, max_gamma_strike,
    # total_net_gex, call_wall_strength, put_wall_strength.
    cur.program(
        "FROM gex_summary",
        [
            (
                net_gex_at_spot,
                745.5,
                745.0,
                1.0,
                750.0,
                740.0,
                745.0,
                total_net_gex,
                1.0e9,
                8.0e8,
            )
        ],
    )
    # vix_bars latest
    cur.program("FROM vix_bars", [(17.0,)])
    # underlying_vwap_deviation latest
    cur.program(
        "FROM underlying_vwap_deviation",
        [(746.5, -0.1)],
    )
    # recent underlying closes (30 rows)
    cur.program(
        "SELECT close FROM underlying_quotes\n        WHERE symbol",
        [(746.0,) for _ in range(30)],
    )
    return conn


def test_snap_net_gex_reads_net_gex_at_spot_not_total_net_gex():
    """The exact 2026-07-09 morning scenario: two metrics disagree in sign.
    The regime must follow net_gex_at_spot (positive) — the value the
    rest of the site shows — NOT total_net_gex (negative)."""
    conn = _build_scripted_conn(
        total_net_gex=-6.27e8,
        net_gex_at_spot=+3.2e9,
    )
    snap = build_snapshot(conn, "SPY")
    assert snap is not None
    assert snap.net_gex == pytest.approx(3.2e9), (
        f"snap.net_gex should reflect net_gex_at_spot (+3.2B), got "
        f"{snap.net_gex}. If this equals -6.27e8, the SELECT is reading "
        f"total_net_gex again — regime will misclassify on any day the "
        f"two columns disagree in sign."
    )
    assert snap.gex_regime() in {"positive_strong", "positive_weak"}


def test_snap_net_gex_regime_matches_site_when_both_positive():
    """Sanity: on a day both columns agree, regime is still positive."""
    conn = _build_scripted_conn(
        total_net_gex=+8.0e9,
        net_gex_at_spot=+3.2e9,
    )
    snap = build_snapshot(conn, "SPY")
    assert snap is not None
    assert snap.net_gex == pytest.approx(3.2e9)
    assert snap.gex_regime() in {"positive_strong", "positive_weak"}


def test_snap_net_gex_regime_negative_when_at_spot_negative():
    """Negative at-spot → negative regime, regardless of the raw
    chain aggregate value."""
    conn = _build_scripted_conn(
        total_net_gex=+2.0e9,  # would look "positive strong" if we used this
        net_gex_at_spot=-8.0e8,
    )
    snap = build_snapshot(conn, "SPY")
    assert snap is not None
    assert snap.net_gex == pytest.approx(-8.0e8)
    assert snap.gex_regime() in {"negative_weak", "negative_strong"}


def test_gex_select_column_order_is_at_spot_first():
    """The SQL text itself must SELECT net_gex_at_spot as the first
    projected column so the tuple positional indexing stays correct.
    If someone later reorders the SELECT to put total_net_gex first
    to match an old draft, this test fails BEFORE anything looks at
    the resulting snapshot values."""
    conn = _build_scripted_conn(
        total_net_gex=+1.0,
        net_gex_at_spot=+1.0,
    )
    build_snapshot(conn, "SPY")
    sql = conn.cursor().last_gex_sql or ""
    normalised = " ".join(sql.split())
    assert "SELECT net_gex_at_spot," in normalised, (
        "gex_summary SELECT must project net_gex_at_spot as the FIRST "
        "column so the tuple indexing in build_snapshot keeps mapping "
        f"gx[0] to the regime metric. Got: {normalised[:200]}"
    )
