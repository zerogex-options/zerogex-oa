"""Regression tests for StreamManager._yield_option_snapshot TimeStamp handling.

Before the fix, any option quote with TimeStamp='' was dropped at WARNING
level.  TradeStation routinely emits that for contracts that haven't trade-
printed in the current session — every far-OTM ETF strike pre-market, and
ALL cash-settled SPX/NDX contracts outside 09:30-16:00 ET.  The drop
starved option_chains during pre-market and downstream charts (GEX heatmap,
gamma flip) stayed blank until the cash session opened.

The fix uses the stream arrival time as the timestamp when the snapshot
carries a valid bid/ask/mid.  Quotes with neither timestamp NOR any quote
data are still dropped (nothing useful to write) but at DEBUG level.

Item #4 of the follow-up plan layers a second guard on top: cash-settled
(SPX/NDX/...) option quotes outside their RTH session are skipped before
the receive-time fallback runs, since those quotes carry stale data the
downstream cash-index query filter would have excluded anyway.

These now drive the method with provider ``OptionQuote`` objects rather
than TradeStation's raw JSON. The behaviour under test is unchanged --
what moved is WHERE the string ``TimeStamp``/``Bid``/``Ask`` is parsed:
each provider does it in its own module, because only that module knows
which spelling its feed uses. A quote whose time the feed sent empty,
garbled, or not at all arrives here identically, as ``timestamp=None``,
and the fallback this file exists to protect keys off exactly that.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

from src.ingestion.providers.base import OptionQuote
from src.ingestion.stream_manager import StreamManager

UTC = timezone.utc


def _q(symbol: str, *, ts=None, bid=None, ask=None, mid=None) -> OptionQuote:
    """One provider quote. ``ts=None`` is what an absent or unparseable
    feed timestamp becomes once the provider has had its say."""
    return OptionQuote(option_symbol=symbol, timestamp=ts, bid=bid, ask=ask, mid=mid)


def _bare_stream_manager(db_underlying: str = "QQQ") -> StreamManager:
    sm = StreamManager.__new__(StreamManager)
    sm.db_underlying = db_underlying
    sm._symbol_metadata = {
        "QQQ 260616C740": {
            "strike": 740.0,
            "expiration": date(2026, 6, 16),
            "option_type": "C",
        },
        "SPXW 260618C5300": {
            "strike": 5300.0,
            "expiration": date(2026, 6, 18),
            "option_type": "C",
        },
    }
    return sm


def test_valid_timestamp_is_used_as_is():
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": _q(
            "QQQ 260616C740", ts=datetime(2026, 6, 15, 13, 30, tzinfo=UTC), bid=1.20, ask=1.25
        )
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    ts = results[0]["timestamp"]
    assert ts is not None
    assert ts.year == 2026 and ts.month == 6 and ts.day == 15


def test_empty_timestamp_with_valid_bid_ask_falls_back_to_now():
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": _q("QQQ 260616C740", bid=1.20, ask=1.25)
    }
    before = datetime.now(timezone.utc)
    results = sm._yield_option_snapshot(state)
    after = datetime.now(timezone.utc)

    assert len(results) == 1
    row = results[0]
    ts = row["timestamp"]
    assert ts is not None
    # Fallback uses receive time -- must be inside the test's wall-clock window.
    assert before <= ts <= after
    # Bid/ask preserved -- this row IS usable downstream.
    assert row["bid"] == 1.20
    assert row["ask"] == 1.25
    assert row["mid"] == 1.225  # bid/ask midpoint computed since Mid is absent


def test_empty_timestamp_with_only_bid_still_recovered():
    sm = _bare_stream_manager()
    state = {
        # No ask, no mid -- a one-sided market still counts as a quote.
        "QQQ 260616C740": _q("QQQ 260616C740", bid=1.20)
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["timestamp"] is not None
    assert results[0]["bid"] == 1.20
    assert results[0]["ask"] is None


def test_empty_timestamp_with_only_mid_still_recovered():
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": _q("QQQ 260616C740", mid=1.225)
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["timestamp"] is not None
    assert results[0]["mid"] == 1.225


def test_empty_timestamp_and_no_quote_data_drops_silently():
    """Stream heartbeat / placeholder with neither timestamp nor quote -> drop."""
    sm = _bare_stream_manager()
    state = {
        # No bid, ask or mid -- nothing usable.
        "QQQ 260616C740": _q("QQQ 260616C740")
    }
    results = sm._yield_option_snapshot(state)
    assert results == []


def test_garbled_timestamp_with_valid_quote_falls_back_to_now():
    """A time the feed sent but that could not be read still falls back.

    The provider is what now decides a garbled timestamp is unusable, and
    it says so the only way the interface allows: ``timestamp=None``. So at
    THIS boundary garbled and absent are deliberately indistinguishable,
    and that is the point -- the fallback must not depend on knowing which
    it was. Whether each feed's own parser rejects the right strings is
    tested in that provider's module, against that vendor's spellings.
    """
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": _q("QQQ 260616C740", bid=1.20, ask=1.25)
    }
    before = datetime.now(timezone.utc)
    results = sm._yield_option_snapshot(state)
    after = datetime.now(timezone.utc)
    assert len(results) == 1
    ts = results[0]["timestamp"]
    assert before <= ts <= after


def test_unknown_symbol_in_state_is_skipped():
    """Quotes for symbols not in _symbol_metadata must not error out."""
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": _q(
            "QQQ 260616C740", ts=datetime(2026, 6, 15, 13, 30, tzinfo=UTC), bid=1.20, ask=1.25
        ),
        "QQQ 260616C999": _q(
            "QQQ 260616C999", ts=datetime(2026, 6, 15, 13, 30, tzinfo=UTC), bid=0.01, ask=0.05
        ),
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["option_symbol"] == "QQQ 260616C740"


# --- Item #4: skip cash-settled writes outside RTH -------------------------


def test_cash_settled_off_session_skips_whole_batch():
    """SPX option batch during pre-market -> skip entirely.

    The downstream cash-index session filter at query time already
    excludes off-session rows, so writing them is pure waste.
    """
    sm = _bare_stream_manager(db_underlying="SPX")
    state = {
        "SPXW 260618C5300": _q("SPXW 260618C5300", bid=10.50, ask=11.00)
    }
    # 08:30 ET on a weekday = pre-market for cash index (which trades 09:30-16:00).
    with patch(
        "src.ingestion.stream_manager.is_cash_index", return_value=True
    ), patch(
        "src.ingestion.stream_manager.is_underlying_active_session", return_value=False
    ):
        results = sm._yield_option_snapshot(state)
    assert results == []


def test_cash_settled_in_session_writes_normally():
    """SPX option batch during 09:30-16:00 ET -> normal write path."""
    sm = _bare_stream_manager(db_underlying="SPX")
    state = {
        "SPXW 260618C5300": _q(
            "SPXW 260618C5300",
            ts=datetime(2026, 6, 15, 13, 30, tzinfo=UTC),
            bid=10.50,
            ask=11.00,
        )
    }
    with patch(
        "src.ingestion.stream_manager.is_cash_index", return_value=True
    ), patch(
        "src.ingestion.stream_manager.is_underlying_active_session", return_value=True
    ):
        results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["option_symbol"] == "SPXW 260618C5300"


def test_etf_off_session_still_writes():
    """SPY/QQQ off-session must NOT be skipped -- they trade extended hours."""
    sm = _bare_stream_manager(db_underlying="QQQ")
    state = {
        # No timestamp -- forces the receive-time fallback.
        "QQQ 260616C740": _q("QQQ 260616C740", bid=1.20, ask=1.25)
    }
    # is_cash_index returns False for ETFs -> the cash-settled skip is bypassed
    # entirely.  is_underlying_active_session isn't even consulted for ETFs
    # in this path, but we set it False to prove ETFs aren't gated on it.
    with patch(
        "src.ingestion.stream_manager.is_cash_index", return_value=False
    ), patch(
        "src.ingestion.stream_manager.is_underlying_active_session", return_value=False
    ):
        results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["option_symbol"] == "QQQ 260616C740"


def test_cash_settled_off_session_with_empty_batch_no_op():
    """An empty drain on a cash-settled symbol off-session must not error."""
    sm = _bare_stream_manager(db_underlying="SPX")
    with patch(
        "src.ingestion.stream_manager.is_cash_index", return_value=True
    ), patch(
        "src.ingestion.stream_manager.is_underlying_active_session", return_value=False
    ):
        assert sm._yield_option_snapshot({}) == []
