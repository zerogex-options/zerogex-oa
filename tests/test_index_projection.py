"""Unit tests for the futures-implied cash-index spot projection."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.jobs.index_projection import ImpliedIndexSpot, implied_index_spot

ET = ZoneInfo("America/New_York")

# 08:30 ET is inside the overnight futures-display window (18:00 -> 09:30);
# 12:30 ET is inside the cash session (no projection).
_PREMARKET = datetime(2026, 7, 8, 8, 30, tzinfo=ET)
_CASH_SESSION = datetime(2026, 7, 8, 12, 30, tzinfo=ET)


class _FakeDB:
    def __init__(self, *, fut=None, prev=None):
        self._fut = fut if fut is not None else {
            "close": 6050.0, "reference_close": 6000.0, "future_symbol": "@ES",
        }
        self._prev = prev if prev is not None else {"previous_close": 5980.0}

    async def get_latest_future_quote(self, symbol, session_start=None):
        return self._fut

    async def get_previous_close(self, symbol):
        return self._prev


@pytest.mark.asyncio
async def test_projects_spx_with_constant_basis():
    r = await implied_index_spot(_FakeDB(), "SPX", at=_PREMARKET)
    assert isinstance(r, ImpliedIndexSpot)
    # 5980 cash close + (6050 - 6000) overnight = 6030.
    assert r.implied_price == pytest.approx(6030.0)
    assert r.gap_points == pytest.approx(50.0)
    assert r.gap_pct == pytest.approx(50.0 / 6000.0)
    assert r.future_symbol == "@ES"
    assert r.as_audit()["source"] == "futures_implied"


@pytest.mark.asyncio
async def test_no_projection_during_cash_session():
    assert await implied_index_spot(_FakeDB(), "SPX", at=_CASH_SESSION) is None


@pytest.mark.asyncio
async def test_no_projection_for_non_cash_index():
    # SPY / QQQ are ETFs with live overnight prints — never projected.
    assert await implied_index_spot(_FakeDB(), "SPY", at=_PREMARKET) is None
    assert await implied_index_spot(_FakeDB(), "QQQ", at=_PREMARKET) is None


@pytest.mark.asyncio
async def test_no_projection_when_future_feed_empty():
    # Empty dict (falsy) and None both mean "no futures bar yet".
    assert await implied_index_spot(_FakeDB(fut={}), "SPX", at=_PREMARKET) is None
    empty = _FakeDB()
    empty._fut = None
    assert await implied_index_spot(empty, "SPX", at=_PREMARKET) is None


@pytest.mark.asyncio
async def test_no_projection_when_reference_close_missing():
    db = _FakeDB(fut={"close": 6050.0, "reference_close": None, "future_symbol": "@ES"})
    assert await implied_index_spot(db, "SPX", at=_PREMARKET) is None


@pytest.mark.asyncio
async def test_no_projection_when_prior_cash_close_missing():
    db = _FakeDB()
    db._prev = None  # get_previous_close returns None
    assert await implied_index_spot(db, "SPX", at=_PREMARKET) is None
    db2 = _FakeDB(prev={"previous_close": 0})
    assert await implied_index_spot(db2, "SPX", at=_PREMARKET) is None


@pytest.mark.asyncio
async def test_negative_overnight_move_projects_lower():
    db = _FakeDB(fut={"close": 5960.0, "reference_close": 6000.0, "future_symbol": "@ES"})
    r = await implied_index_spot(db, "SPX", at=_PREMARKET)
    assert r.implied_price == pytest.approx(5980.0 - 40.0)  # 5940
    assert r.gap_points == pytest.approx(-40.0)
