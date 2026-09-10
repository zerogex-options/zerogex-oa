"""API tests for the Spread Monitor router (/api/market/spreads/*).

Mocks the three DatabaseManager reads so no live Postgres is needed, and
pins the behaviours a surface would silently get wrong:

* a contract with no bid is COUNTED and not measured — the failure a width
  statistic cannot express;
* the put/call split survives to the response instead of being blended;
* a missing history rollup degrades to "no verdict", never to an error or a
  fabricated middle-of-the-road percentile;
* a symbol whose chain cannot be read appears in the comparison as an
  explicit "unavailable" row rather than being dropped or zeroed.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient


SESSION_DATE = date(2026, 9, 10)
SNAPSHOT_TS = datetime(2026, 9, 10, 18, 0, tzinfo=timezone.utc)
SPOT = 6800.0


def _build_app(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    return dbmod


def _contract(strike, option_type, bid, ask, symbol_prefix="SPXW", dte=0):
    return {
        "option_symbol": f"{symbol_prefix} 260910{option_type}{int(strike)}",
        "strike": strike,
        "option_type": option_type,
        "expiration": date(2026, 9, 10 + dte),
        "bid": bid,
        "ask": ask,
        "open_interest": 100,
        "volume": 10,
        "snapshot_ts": SNAPSHOT_TS,
        "session_date": SESSION_DATE,
    }


def _chain() -> Dict[str, Any]:
    """Tight calls, wide puts, and one put with no bid at all."""
    rows = [
        _contract(SPOT, "C", 20.00, 20.40),
        _contract(SPOT + 20, "C", 12.00, 12.30),
        _contract(SPOT - 20, "P", 18.00, 24.00),
        _contract(SPOT - 40, "P", 12.00, 16.00),
        _contract(SPOT - 100, "P", 0.00, 4.00),
        _contract(SPOT, "C", 30.00, 30.50, dte=2),
    ]
    return {
        "spot_price": SPOT,
        "spot_timestamp": SNAPSHOT_TS,
        "snapshot_ts": SNAPSHOT_TS,
        "session_date": SESSION_DATE,
        "rows": rows,
    }


def _history_rows(option_type: str, values: List[float]) -> List[Dict[str, Any]]:
    from src.config import SPREAD_STATS_DTE_MAX, SPREAD_STATS_MONEYNESS_BAND_PCT

    return [
        {
            "trading_date": date(2026, 8, 1 + i),
            "option_type": option_type,
            "spot_price": SPOT,
            "dte_max": int(SPREAD_STATS_DTE_MAX),
            "moneyness_band_pct": float(SPREAD_STATS_MONEYNESS_BAND_PCT),
            "contract_count": 100,
            "tradable_count": 95,
            "two_sided_pct": 95.0,
            "zero_bid_pct": 5.0,
            "crossed_or_locked_pct": 0.0,
            "median_spread": 0.5,
            "median_relative_spread_pct": value,
            "p90_relative_spread_pct": value * 2,
            "median_spread_bps_underlying": 7.0,
            "p90_spread_bps_underlying": 14.0,
            "total_open_interest": 10_000,
            "total_volume": 500,
            "source_timestamp": SNAPSHOT_TS,
        }
        for i, value in enumerate(values)
    ]


class _StubDb:
    """Only the three reads the router makes; anything else is a hard error."""

    def __init__(self, chain, history, series):
        self._chain = chain
        self._history = history
        self._series = series

    async def get_spread_snapshot_chain(self, symbol, dte_max, band):
        if callable(self._chain):
            return self._chain(symbol, dte_max, band)
        return self._chain

    async def get_daily_spread_history(self, symbol, option_type, days):
        if callable(self._history):
            return self._history(symbol, option_type, days)
        return self._history or []

    async def get_spread_intraday_series(self, *args, **kwargs):
        return self._series or []


def _client(monkeypatch, *, chain=_chain, history=None, series=None) -> TestClient:
    """App with the router's db dependency overridden and its cache cleared.

    The module-level response cache is keyed by query params only, so a
    fixture change with the same params would otherwise be served the
    previous test's body.
    """
    _build_app(monkeypatch)
    from src.api.main import app
    from src.api.routers import spread_liquidity as mod

    stub = _StubDb(chain() if chain is _chain else chain, history, series)
    app.dependency_overrides[mod.get_db] = lambda: stub
    mod._cache.clear()
    return TestClient(app)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


def test_snapshot_returns_the_put_call_split_unblended(monkeypatch):
    client = _client(monkeypatch)
    body = client.get("/api/market/spreads?symbol=SPX").json()

    assert body["symbol"] == "SPX"
    assert body["spot_price"] == SPOT
    puts = body["puts"]["median_relative_spread_pct"]
    calls = body["calls"]["median_relative_spread_pct"]
    assert puts > calls
    # The ratio is the headline the thread is about: puts are the expensive
    # side to trade.
    assert body["put_call_width_ratio"] > 1.0


def test_no_bid_put_is_counted_but_not_measured(monkeypatch):
    client = _client(monkeypatch)
    puts = client.get("/api/market/spreads?symbol=SPX").json()["puts"]

    assert puts["contract_count"] == 3
    assert puts["tradable_count"] == 2
    assert puts["zero_bid_pct"] > 0
    assert puts["median_relative_spread_pct"] is not None


def test_snapshot_reports_the_scope_it_measured(monkeypatch):
    client = _client(monkeypatch)
    scope = client.get(
        "/api/market/spreads?symbol=SPX&dte_max=7&moneyness_band_pct=5"
    ).json()["scope"]

    assert scope["dte_max"] == 7
    assert scope["moneyness_band_pct"] == 5.0
    assert scope["strike_low"] < SPOT < scope["strike_high"]
    assert scope["contract_count"] == 6


def test_snapshot_carries_the_quoted_not_effective_disclosure(monkeypatch):
    client = _client(monkeypatch)
    body = client.get("/api/market/spreads?symbol=SPX").json()
    assert body["basis"] == "quoted_nbbo"
    assert "not effective spreads" in body["disclosure"]


def test_expirations_are_broken_out_nearest_first(monkeypatch):
    client = _client(monkeypatch)
    slices = client.get("/api/market/spreads?symbol=SPX").json()["by_expiration"]
    assert [s["dte"] for s in slices] == [0, 2]
    assert slices[0]["puts"]["contract_count"] == 3
    assert slices[1]["calls"]["contract_count"] == 1


def test_moneyness_curves_are_returned_per_option_type(monkeypatch):
    client = _client(monkeypatch)
    body = client.get("/api/market/spreads?symbol=SPX").json()
    assert sum(b["contract_count"] for b in body["puts_by_moneyness"]) == 3
    assert sum(b["contract_count"] for b in body["calls_by_moneyness"]) == 3


def test_empty_chain_is_404_not_a_zeroed_reading(monkeypatch):
    client = _client(monkeypatch, chain=None)
    assert client.get("/api/market/spreads?symbol=SPX").status_code == 404


# ---------------------------------------------------------------------------
# Historical context
# ---------------------------------------------------------------------------


def test_history_ranks_todays_reading_against_the_window(monkeypatch):
    def history(symbol, option_type, days):
        if option_type == "P":
            return _history_rows("P", [1.0, 1.2, 1.1, 1.3, 1.15])
        if option_type == "C":
            return _history_rows("C", [1.0, 1.1, 1.05, 1.2, 1.1])
        return _history_rows("A", [1.0, 1.1, 1.05, 1.2, 1.1])

    client = _client(monkeypatch, history=history)
    body = client.get("/api/market/spreads?symbol=SPX").json()

    assert body["history"]["sessions"] == 5
    # The fixture chain's puts are far wider than any day in the window.
    assert body["history"]["puts_percentile"] == 100.0
    assert body["history"]["puts_vs_window_ratio"] > 1.0


def test_no_history_yields_no_verdict_rather_than_a_middling_one(monkeypatch):
    """A fresh deployment must not render as "an ordinary day"."""
    client = _client(monkeypatch, history=[])
    assert client.get("/api/market/spreads?symbol=SPX").json()["history"] is None


def test_history_rows_measured_under_a_different_scope_are_excluded(monkeypatch):
    """A percentile across two populations ranks the populations."""

    def history(symbol, option_type, days):
        rows = _history_rows(option_type, [1.0, 1.2, 1.1])
        for row in rows:
            row["moneyness_band_pct"] = 25.0  # a different band entirely
        return rows

    client = _client(monkeypatch, history=history)
    assert client.get("/api/market/spreads?symbol=SPX").json()["history"] is None


def test_todays_own_row_is_excluded_from_its_own_window(monkeypatch):
    """Ranking a reading against itself drags it toward the middle."""

    def history(symbol, option_type, days):
        rows = _history_rows(option_type, [1.0, 1.2, 1.1])
        rows[-1]["trading_date"] = SESSION_DATE
        return rows

    client = _client(monkeypatch, history=history)
    assert client.get("/api/market/spreads?symbol=SPX").json()["history"]["sessions"] == 2


def test_a_broken_rollup_does_not_take_the_page_down(monkeypatch):
    """A partially-migrated deployment still serves the live reading."""

    def history(symbol, option_type, days):
        raise RuntimeError('relation "daily_spread_stats" does not exist')

    client = _client(monkeypatch, history=history)
    body = client.get("/api/market/spreads?symbol=SPX").json()
    assert body["history"] is None
    assert body["puts"]["median_relative_spread_pct"] is not None


def test_history_can_be_skipped_entirely(monkeypatch):
    client = _client(monkeypatch, history=lambda *a, **k: _history_rows("P", [1.0]))
    body = client.get("/api/market/spreads?symbol=SPX&history_days=0").json()
    assert body["history"] is None


# ---------------------------------------------------------------------------
# Cross-symbol comparison
# ---------------------------------------------------------------------------


def test_compare_returns_a_row_per_symbol(monkeypatch):
    client = _client(monkeypatch)
    body = client.get("/api/market/spreads/compare?symbols=SPX,NDX").json()
    assert [r["symbol"] for r in body["rows"]] == ["SPX", "NDX"]
    for row in body["rows"]:
        assert row["puts"]["median_relative_spread_pct"] is not None
        assert row["unavailable"] is None


def test_unreadable_symbol_is_flagged_not_dropped_or_zeroed(monkeypatch):
    """A missing row reads as "not compared"; a zeroed one as "perfectly tight"."""

    def chain(symbol, dte_max, band):
        return None if symbol == "NDX" else _chain()

    client = _client(monkeypatch, chain=chain)
    rows = {
        r["symbol"]: r
        for r in client.get("/api/market/spreads/compare?symbols=SPX,NDX").json()["rows"]
    }
    assert rows["NDX"]["unavailable"] == "no quoted chain"
    assert rows["NDX"]["puts"] is None
    assert rows["SPX"]["unavailable"] is None


def test_compare_rejects_an_unbounded_symbol_list(monkeypatch):
    client = _client(monkeypatch)
    many = ",".join(["SPX"] * 9)
    assert client.get(f"/api/market/spreads/compare?symbols={many}").status_code == 400
    assert client.get("/api/market/spreads/compare?symbols=").status_code == 400


# ---------------------------------------------------------------------------
# Intraday series
# ---------------------------------------------------------------------------


def _series_row(option_type, bucket, median_rel, zero_bid=0, total=100):
    return {
        "bucket_start": datetime(2026, 9, 10, bucket, 0, tzinfo=timezone.utc),
        "anchor_ts": datetime(2026, 9, 10, bucket, 14, tzinfo=timezone.utc),
        "spot": SPOT,
        "option_type": option_type,
        "contract_count": total,
        "tradable_count": total - zero_bid,
        "zero_bid_count": zero_bid,
        "crossed_or_locked_count": 0,
        "median_spread": 0.5,
        "median_relative_spread_pct": median_rel,
        "p90_relative_spread_pct": median_rel * 2,
        "total_open_interest": 5000,
        "total_volume": 200,
    }


def test_series_folds_call_and_put_rows_into_one_bar(monkeypatch):
    series = [
        _series_row("C", 14, 1.5),
        _series_row("P", 14, 4.0, zero_bid=10),
        _series_row("C", 15, 1.6),
        _series_row("P", 15, 6.0, zero_bid=20),
    ]
    client = _client(monkeypatch, series=series)
    bars = client.get("/api/market/spreads/series?symbol=SPX").json()["bars"]

    assert len(bars) == 2
    assert bars[0]["calls"]["median_relative_spread_pct"] == 1.5
    assert bars[0]["puts"]["median_relative_spread_pct"] == 4.0
    assert bars[1]["puts"]["zero_bid_pct"] == 20.0


def test_series_derives_bps_from_the_bucket_spot(monkeypatch):
    """Spot is constant inside a bucket, so the conversion is exact."""
    client = _client(monkeypatch, series=[_series_row("P", 14, 4.0)])
    bar = client.get("/api/market/spreads/series?symbol=SPX").json()["bars"][0]
    assert bar["puts"]["median_spread_bps_underlying"] == round(
        10_000 * 0.5 / SPOT, 3
    )


def test_empty_session_is_an_empty_series_not_an_error(monkeypatch):
    client = _client(monkeypatch, series=[])
    response = client.get("/api/market/spreads/series?symbol=SPX")
    assert response.status_code == 200
    assert response.json()["bars"] == []


# ---------------------------------------------------------------------------
# Daily history endpoint
# ---------------------------------------------------------------------------


def test_history_endpoint_returns_rows_oldest_first(monkeypatch):
    client = _client(
        monkeypatch, history=lambda *a, **k: _history_rows("P", [1.0, 1.2, 1.4])
    )
    body = client.get("/api/market/spreads/history?symbol=SPX&option_type=P").json()
    dates = [r["trading_date"] for r in body["rows"]]
    assert dates == sorted(dates)
    assert body["option_type"] == "P"


def test_history_endpoint_rejects_an_unknown_option_type(monkeypatch):
    client = _client(monkeypatch, history=[])
    assert (
        client.get("/api/market/spreads/history?symbol=SPX&option_type=X").status_code
        == 422
    )


def test_history_endpoint_on_an_unseeded_deployment_returns_empty(monkeypatch):
    client = _client(monkeypatch, history=[])
    response = client.get("/api/market/spreads/history?symbol=SPX")
    assert response.status_code == 200
    assert response.json()["rows"] == []
