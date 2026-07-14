"""Per-symbol GEX normalization: percentile math + symbol-relative regime.

The regime split used to be an absolute dollar line (``net_gex >= 1.5e9``
== strong), which only makes sense at the scale it was tuned for. These
tests lock in the replacement — ``net_gex_pctile`` from the symbol's own
``gex_historical_stats`` distribution — and that it degrades safely to the
old absolute thresholds when no distribution is available.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import pytest

from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.gex_stats import (
    fetch_net_gex_pctile,
    percentile_rank,
    tod_bucket_for,
)

# A plausible per-symbol distribution of net_gex_at_spot.
DIST = dict(p05=-1.0e9, p25=-2.0e8, p50=5.0e8, p75=1.5e9, p95=3.0e9)


# ----------------------------------------------------------------------
# percentile_rank
# ----------------------------------------------------------------------


def test_percentile_rank_hits_known_knots():
    assert percentile_rank(DIST["p50"], **DIST) == pytest.approx(50.0)
    assert percentile_rank(DIST["p25"], **DIST) == pytest.approx(25.0)
    assert percentile_rank(DIST["p75"], **DIST) == pytest.approx(75.0)


def test_percentile_rank_interpolates_interior():
    midway = (DIST["p50"] + DIST["p75"]) / 2.0  # halfway p50..p75
    assert percentile_rank(midway, **DIST) == pytest.approx(62.5)


def test_percentile_rank_clamps_extremes():
    assert percentile_rank(9.9e9, **DIST) == pytest.approx(100.0)
    assert percentile_rank(-9.9e9, **DIST) == pytest.approx(0.0)


def test_percentile_rank_degenerate_and_missing():
    # Flat distribution -> ambiguous -> 50.
    assert percentile_rank(5.0, 1.0, 1.0, 1.0, 1.0, 1.0) == 50.0
    # Fewer than two usable knots -> None.
    assert percentile_rank(5.0, None, None, 3.0, None, None) is None


# ----------------------------------------------------------------------
# tod_bucket_for
# ----------------------------------------------------------------------


def test_tod_bucket_for_rth_boundaries():
    def utc(h, m):
        return datetime(2026, 7, 14, h, m, tzinfo=timezone.utc)

    assert tod_bucket_for(utc(13, 30)) == 0    # 09:30 ET
    assert tod_bucket_for(utc(13, 35)) == 1    # 09:35 ET
    assert tod_bucket_for(utc(19, 55)) == 77   # 15:55 ET
    assert tod_bucket_for(utc(20, 0)) == -1    # 16:00 ET -> outside
    assert tod_bucket_for(utc(13, 29)) == -1   # 09:29 ET -> pre-open


# ----------------------------------------------------------------------
# fetch_net_gex_pctile (fake conn)
# ----------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, row: Optional[tuple]):
        self._row = row
        self.executed = False

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed = True
        assert "gex_historical_stats" in sql

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row: Optional[tuple]):
        self._cur = _FakeCursor(row)

    def cursor(self):
        return self._cur


TS = datetime(2026, 7, 14, 15, 30, tzinfo=timezone.utc)


def test_fetch_pctile_interpolates_from_row():
    conn = _FakeConn((DIST["p05"], DIST["p25"], DIST["p50"], DIST["p75"], DIST["p95"]))
    pct = fetch_net_gex_pctile(conn, "SPY", DIST["p75"], TS)
    assert pct == pytest.approx(75.0)


def test_fetch_pctile_none_when_no_row():
    assert fetch_net_gex_pctile(_FakeConn(None), "SPY", 1.0e9, TS) is None


def test_fetch_pctile_none_when_net_gex_missing():
    conn = _FakeConn((0.0, 0.0, 0.0, 0.0, 0.0))
    assert fetch_net_gex_pctile(conn, "SPY", None, TS) is None


# ----------------------------------------------------------------------
# gex_regime — percentile-driven when available, absolute fallback otherwise
# ----------------------------------------------------------------------


def _snap(net_gex: float, pctile: Optional[float]) -> MarketSnapshot:
    return MarketSnapshot(
        underlying="SPX",
        timestamp=TS,
        spot=7530.0,
        net_gex=net_gex,
        net_gex_pctile=pctile,
    )


def test_regime_percentile_positive_strong_vs_weak():
    assert _snap(1.0e9, 90.0).gex_regime() == "positive_strong"
    assert _snap(1.0e9, 40.0).gex_regime() == "positive_weak"


def test_regime_percentile_negative_strong_vs_weak():
    assert _snap(-1.0e9, 10.0).gex_regime() == "negative_strong"
    assert _snap(-1.0e9, 60.0).gex_regime() == "negative_weak"


def test_regime_falls_back_to_absolute_without_pctile():
    assert _snap(2.0e9, None).gex_regime() == "positive_strong"
    assert _snap(5.0e8, None).gex_regime() == "positive_weak"
    assert _snap(-2.0e9, None).gex_regime() == "negative_strong"
    assert _snap(-8.0e8, None).gex_regime() == "negative_weak"


def test_regime_is_symbol_relative_not_absolute():
    """The whole point: magnitude is judged per-symbol, not by a fixed dollar line.

    A huge SPX-scale reading that is *low for SPX* is only 'weak'; a tiny
    reading that is *high for its symbol* is 'strong'. The old absolute
    1.5e9 rule (shown for contrast) would get both backwards.
    """
    # 2e10 >> 1.5e9 (absolute rule: "strong") but 20th pct for SPX -> weak.
    assert _snap(2.0e10, 20.0).gex_regime() == "positive_weak"
    # 3e8 < 1.5e9 (absolute rule: "weak") but 92nd pct for its symbol -> strong.
    assert _snap(3.0e8, 92.0).gex_regime() == "positive_strong"
