"""``session`` on an ES / NQ quote describes the MARKET, not the feed.

The distinction is not cosmetic. ``getPrimaryPriceChangeSummary`` does not
answer a "closed" session by merely declining to merge live ticks — it
REPLACES the headline price with ``current_session_close`` and its change
baseline with ``prior_session_close``. For a 23-hour instrument that turns a
stale feed into a confidently-wrong quote: the last 16:00 cash close, shown
as the live price, with that session's day change presented as today's.

This is exactly what shipped on 2026-08-24. The ES header read

    ES  $7692.00  +26.75 (+0.35%)

which is Friday's 16:00 mark and Friday's day change, while ES traded 7675.50
and ``/api/market/quote?symbol=ES`` had a live 7677.25 print in hand. The
quote endpoint had folded feed staleness into the session flag, so a feed the
API could see was working still read as a closed market.

Freshness is now its own signal (``stale`` / ``data_age_seconds``) so the
chart-tip merge can still refuse a dead feed without the session lying about
it.
"""

import asyncio
import datetime as dt
from unittest.mock import patch

import pytest

import src.api.main as main


def _quote(bar_age_seconds: float, *, cme_open: bool = True):
    """Run _native_futures_quote against a bar of a given age."""
    now = dt.datetime(2026, 8, 24, 12, 50, tzinfo=dt.timezone.utc)  # Mon 08:50 ET
    bar = {
        "timestamp": now - dt.timedelta(seconds=bar_age_seconds),
        "open": 7676.0,
        "high": 7678.0,
        "low": 7675.0,
        "close": 7677.25,
        "up_volume": 10,
        "down_volume": 8,
    }

    class _FrozenNow(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    class _DB:
        async def get_latest_future_quote(self, index_symbol, session_start=None):
            return dict(bar)

    with (
        patch.object(main, "_db", lambda: _DB()),
        patch.object(main, "is_futures_session_open", lambda *a, **k: cme_open),
        patch.object(main, "datetime", _FrozenNow),
    ):
        return asyncio.run(main._native_futures_quote("ES", "SPX"))


# --- the regression --------------------------------------------------------


def test_a_stale_feed_does_not_report_the_market_as_closed():
    """The bug. A 40-minute-old bar during an open CME session read "closed",
    and the header answered that by publishing Friday's cash close."""
    q = _quote(bar_age_seconds=40 * 60)
    assert q.session == "open"


def test_a_stale_feed_still_serves_the_observed_print():
    """A late ES print beats a three-day-old one. The price must be the
    future's own last bar, never a session close."""
    q = _quote(bar_age_seconds=40 * 60)
    assert float(q.close) == pytest.approx(7677.25)


def test_staleness_is_reported_as_its_own_signal():
    """So the chart tip-candle merge can still refuse a dead feed — the
    concern the session flag was wrongly overloaded to carry."""
    q = _quote(bar_age_seconds=40 * 60)
    assert q.stale is True
    assert q.data_age_seconds == pytest.approx(2400, abs=1)


def test_a_fresh_feed_is_not_marked_stale():
    q = _quote(bar_age_seconds=45)
    assert q.session == "open"
    assert q.stale is False
    assert q.data_age_seconds == pytest.approx(45, abs=1)


def test_a_bar_one_minute_old_is_fresh():
    """Bars are start-of-minute stamped, so a healthy feed's newest bar is
    always ~1-2 minutes old. A threshold that called that stale would mark
    the feed dead on every request."""
    assert _quote(bar_age_seconds=90).stale is False


# --- the session still tracks the CME calendar -----------------------------


def test_the_session_closes_when_cme_actually_closes():
    """The 17:00-18:00 maintenance break and the weekend are real closes, and
    there the frozen-close reading is the correct one."""
    q = _quote(bar_age_seconds=45, cme_open=False)
    assert q.session == "closed"


def test_a_closed_market_with_a_fresh_bar_is_not_stale():
    """Freshness and the calendar are independent axes now."""
    q = _quote(bar_age_seconds=45, cme_open=False)
    assert q.stale is False
