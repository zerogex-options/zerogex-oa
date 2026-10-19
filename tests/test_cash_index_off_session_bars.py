"""A cash index stores no bar outside its cash session.

ThetaData's Market Value index endpoint answers ``market_price`` with no
timestamp, so the provider stamps each poll with its own time. After the close
every minute then became a new SPX / NDX bar at the closing level: a fake
overnight and pre-market tape, a WebSocket frame per bar (the header flipped
between ES and the SPX close), and a "fresh" price for the Greeks gate.
TradeStation's stream went quiet instead. ``_store_underlying`` now drops those
bars for every cash index, keeps the 16:00 minute that carries the settled
close, and leaves ETFs alone.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
import pytz

import src.market_calendar as mc
from src.ingestion.main_engine import IngestionEngine
from src.market_calendar import cash_index_bar_in_session

ET = pytz.timezone("America/New_York")


def _et(y, mo, d, h, mi, s=0):
    return ET.localize(datetime(y, mo, d, h, mi, s))


# 2026-10-19 is a Monday, 2026-10-17 a Saturday.
@pytest.mark.parametrize(
    ("bucket", "expected"),
    [
        (_et(2026, 10, 19, 4, 0), False),
        (_et(2026, 10, 19, 9, 29), False),
        (_et(2026, 10, 19, 9, 30), True),
        (_et(2026, 10, 19, 12, 0), True),
        (_et(2026, 10, 19, 15, 59), True),
        # The closing minute carries the post-auction settled level.
        (_et(2026, 10, 19, 16, 0), True),
        (_et(2026, 10, 19, 16, 1), False),
        (_et(2026, 10, 19, 20, 0), False),
        (_et(2026, 10, 17, 12, 0), False),
    ],
)
def test_window_is_the_cash_session_with_its_closing_minute(bucket, expected):
    assert cash_index_bar_in_session(bucket) is expected


def test_window_accepts_utc_buckets():
    # Buckets reach the gate in UTC. 20:00Z is 16:00 EDT, 20:01Z is 16:01.
    assert cash_index_bar_in_session(datetime(2026, 10, 19, 20, 0, tzinfo=pytz.UTC))
    assert not cash_index_bar_in_session(datetime(2026, 10, 19, 20, 1, tzinfo=pytz.UTC))


def test_half_day_closes_at_13_00(monkeypatch):
    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {date(2026, 11, 27)})
    assert cash_index_bar_in_session(_et(2026, 11, 27, 13, 0))
    assert not cash_index_bar_in_session(_et(2026, 11, 27, 13, 1))


def test_holiday_has_no_session(monkeypatch):
    monkeypatch.setattr(mc, "NYSE_HOLIDAYS", {date(2026, 11, 26)})
    assert not cash_index_bar_in_session(_et(2026, 11, 26, 12, 0))


# ---------------------------------------------------------------------------
# _store_underlying
# ---------------------------------------------------------------------------


def _engine(monkeypatch, symbol):
    e = IngestionEngine.__new__(IngestionEngine)
    e.db_symbol = symbol
    e._db_backoff_until = 0.0
    e._session_open_repaired = None
    e._last_underlying_signature = None
    e.latest_underlying_price = None
    e.latest_underlying_timestamp = None
    e.underlying_bars_stored = 0
    monkeypatch.setattr(e, "_log_parity_signature", lambda *a, **k: None)
    monkeypatch.setattr(e, "_repair_session_open_if_needed", lambda *a, **k: None)
    written: list = []
    monkeypatch.setattr(e, "_upsert_underlying_quote", lambda payload: written.append(payload))
    return e, written


def _poll(ts, close=6650.0):
    """A Market Value mark as the provider hands it over: OHLC all the mark,
    stamped with the poll's own time."""
    return {
        "timestamp": ts,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "up_volume": None,
        "down_volume": None,
        "volume": None,
    }


@pytest.mark.parametrize("symbol", ["SPX", "NDX"])
@pytest.mark.parametrize(
    "polled_at",
    [
        _et(2026, 10, 19, 9, 14, 30),  # pre-market, when this was reported
        _et(2026, 10, 19, 16, 1, 30),  # the first minute after the close
        _et(2026, 10, 19, 23, 0, 0),  # overnight
        _et(2026, 10, 17, 12, 0, 0),  # weekend
    ],
)
def test_off_session_index_poll_is_not_stored(monkeypatch, symbol, polled_at):
    e, written = _engine(monkeypatch, symbol)
    e._store_underlying(_poll(polled_at))
    assert written == []
    # Nor does it refresh the price the Greeks freshness gate trusts.
    assert e.latest_underlying_price is None
    assert e.latest_underlying_timestamp is None


@pytest.mark.parametrize("symbol", ["SPX", "NDX"])
@pytest.mark.parametrize(
    "polled_at",
    [
        _et(2026, 10, 19, 10, 0, 30),
        # Floored with the 1 s close-stamp offset: 16:00:30 and 16:01:00 both
        # land in the 16:00 bucket, the one get_session_closes reads.
        _et(2026, 10, 19, 16, 0, 30),
        _et(2026, 10, 19, 16, 1, 0),
    ],
)
def test_in_session_index_poll_is_stored(monkeypatch, symbol, polled_at):
    e, written = _engine(monkeypatch, symbol)
    e._store_underlying(_poll(polled_at))
    assert len(written) == 1
    assert written[0]["symbol"] == symbol
    assert e.latest_underlying_price == 6650.0


def test_etf_is_not_gated(monkeypatch):
    # SPY trades extended hours; its after-hours bars are real.
    e, written = _engine(monkeypatch, "SPY")
    e._store_underlying(_poll(_et(2026, 10, 19, 18, 30, 0), close=665.0))
    assert len(written) == 1
