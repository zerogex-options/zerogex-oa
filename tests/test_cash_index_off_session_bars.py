"""A cash index stores bars for its session and closing minutes, and no re-sends.

Two things ThetaData does that TradeStation did not, both seen in production on
2026-10-19:

* Before the open the index snapshot answers with the LAST print and that
  print's own timestamp. It returned Friday's 16:04 SPX and 17:15 NDX prints on
  every poll, ingestion rewrote those rows right up to 09:29:59, and each
  rewrite went out as a live tick: the header flipped between ES and the SPX
  close. The provider now drops a print more than a few minutes old.
* After the close it keeps printing: SPX to 16:04, NDX to 17:15. TradeStation
  stored SPX through 16:01 and NDX through 16:00. ``_store_underlying`` keeps
  the session plus five closing minutes for every cash index, and leaves ETFs
  alone.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
import pytz

import src.market_calendar as mc
from src.ingestion.main_engine import IngestionEngine
from src.ingestion.providers.thetadata import ThetaDataProvider
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
        # The close minute is the bar get_session_closes reads.
        (_et(2026, 10, 19, 16, 0), True),
        # TradeStation stored SPX's 16:01 bar every day; ThetaData's last SPX
        # print lands at 16:04.
        (_et(2026, 10, 19, 16, 1), True),
        (_et(2026, 10, 19, 16, 4), True),
        (_et(2026, 10, 19, 16, 5), True),
        # NDX's tail to 17:15 is cut here.
        (_et(2026, 10, 19, 16, 6), False),
        (_et(2026, 10, 19, 17, 15), False),
        (_et(2026, 10, 17, 12, 0), False),
    ],
)
def test_window_is_the_session_plus_five_closing_minutes(bucket, expected):
    assert cash_index_bar_in_session(bucket) is expected


def test_window_accepts_utc_buckets():
    # Buckets reach the gate in UTC. 20:05Z is 16:05 EDT, 20:06Z is 16:06.
    assert cash_index_bar_in_session(datetime(2026, 10, 19, 20, 5, tzinfo=pytz.UTC))
    assert not cash_index_bar_in_session(datetime(2026, 10, 19, 20, 6, tzinfo=pytz.UTC))


def test_half_day_closes_at_13_00(monkeypatch):
    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {date(2026, 11, 27)})
    assert cash_index_bar_in_session(_et(2026, 11, 27, 13, 5))
    assert not cash_index_bar_in_session(_et(2026, 11, 27, 13, 6))


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
    """A Market Value mark as the provider hands it over: OHLC all the mark."""
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
        _et(2026, 10, 19, 9, 14, 30),  # pre-market
        _et(2026, 10, 19, 16, 6, 30),  # past the closing minutes
        _et(2026, 10, 19, 17, 15, 30),  # NDX's ThetaData tail
        _et(2026, 10, 19, 23, 0, 0),  # overnight
        _et(2026, 10, 17, 12, 0, 0),  # weekend
    ],
)
def test_off_session_index_bar_is_not_stored(monkeypatch, symbol, polled_at):
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
        # Floored with the 1 s close-stamp offset: 16:00:30 lands in the 16:00
        # bucket, 16:04:30 in 16:04, 16:06:00 still in 16:05.
        _et(2026, 10, 19, 16, 0, 30),
        _et(2026, 10, 19, 16, 4, 30),
        _et(2026, 10, 19, 16, 6, 0),
    ],
)
def test_session_and_closing_minute_bars_are_stored(monkeypatch, symbol, polled_at):
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


# ---------------------------------------------------------------------------
# ThetaData: a re-sent old index print is not a bar
# ---------------------------------------------------------------------------


class _IndexMarketValue:
    """The Market Value index endpoint, answering one print."""

    def __init__(self, timestamp):
        self.timestamp = timestamp

    def index_snapshot_market_value(self, **kw):
        return [{"market_price": 7659.48, "symbol": "SPX", "timestamp": self.timestamp}]


def _index_fetch(timestamp):
    provider = ThetaDataProvider(
        _IndexMarketValue(timestamp), stage="mv", market_value_endpoints=True
    )
    # Drive one poll without starting the poll thread.
    return provider.stream_underlying_bars("$SPXW.X", db_symbol="SPX")._fetch()


def _ago(**kw):
    # The vendor's timestamps arrive localised to America/New_York.
    return (datetime.now(timezone.utc) - timedelta(**kw)).astimezone(ET)


def test_last_session_print_resent_before_the_open_is_dropped():
    # Friday 16:04 read at Monday 09:14 is 65 hours old.
    assert _index_fetch(_ago(hours=65)) is None


def test_print_just_past_the_age_limit_is_dropped():
    assert _index_fetch(_ago(minutes=5, seconds=5)) is None


def test_live_print_is_a_bar():
    bar = _index_fetch(_ago(seconds=1))
    assert bar is not None
    assert bar.close == 7659.48


def test_print_with_no_timestamp_still_comes_through():
    # Stamped with the poll's own time; the engine's session window is what
    # keeps such a bar out after hours.
    assert _index_fetch(None) is not None
