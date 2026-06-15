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
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from src.ingestion.stream_manager import StreamManager


def _bare_stream_manager() -> StreamManager:
    sm = StreamManager.__new__(StreamManager)
    sm.db_underlying = "QQQ"
    sm._symbol_metadata = {
        "QQQ 260616C740": {
            "strike": 740.0,
            "expiration": date(2026, 6, 16),
            "option_type": "C",
        },
    }
    return sm


def test_valid_timestamp_is_used_as_is():
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": {
            "TimeStamp": "2026-06-15T13:30:00Z",
            "Bid": "1.20",
            "Ask": "1.25",
        }
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    ts = results[0]["timestamp"]
    assert ts is not None
    assert ts.year == 2026 and ts.month == 6 and ts.day == 15


def test_empty_timestamp_with_valid_bid_ask_falls_back_to_now():
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": {
            "TimeStamp": "",
            "Bid": "1.20",
            "Ask": "1.25",
        }
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
        "QQQ 260616C740": {
            "TimeStamp": "",
            "Bid": "1.20",
            # No Ask, no Mid -- a one-sided market still counts as a quote.
        }
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["timestamp"] is not None
    assert results[0]["bid"] == 1.20
    assert results[0]["ask"] is None


def test_empty_timestamp_with_only_mid_still_recovered():
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": {
            "TimeStamp": "",
            "Mid": "1.225",
        }
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["timestamp"] is not None
    assert results[0]["mid"] == 1.225


def test_empty_timestamp_and_no_quote_data_drops_silently():
    """Stream heartbeat / placeholder with neither timestamp nor quote -> drop."""
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": {
            "TimeStamp": "",
            # No Bid, Ask, or Mid -- nothing usable.
        }
    }
    results = sm._yield_option_snapshot(state)
    assert results == []


def test_garbled_timestamp_with_valid_quote_falls_back_to_now():
    """Non-empty but unparseable TimeStamp should still trigger the fallback."""
    sm = _bare_stream_manager()
    state = {
        "QQQ 260616C740": {
            "TimeStamp": "not-a-date",
            "Bid": "1.20",
            "Ask": "1.25",
        }
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
        "QQQ 260616C740": {"TimeStamp": "2026-06-15T13:30:00Z", "Bid": "1.20", "Ask": "1.25"},
        "QQQ 260616C999": {"TimeStamp": "2026-06-15T13:30:00Z", "Bid": "0.01", "Ask": "0.05"},
    }
    results = sm._yield_option_snapshot(state)
    assert len(results) == 1
    assert results[0]["option_symbol"] == "QQQ 260616C740"
