"""Tests for the ThetaData provider.

Written against the v3 gRPC client's signatures rather than a live
terminal, so these lock down the parts that are mine: symbol handling,
the join across three snapshot endpoints, sticky-field semantics, and the
capability declarations. The response column names are server-supplied and
must still be confirmed on first contact with a real terminal.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import threading

import time

import pytest

from src.ingestion.providers import available_providers
from src.ingestion.providers.base import ProviderCapabilityError
from src.ingestion.providers.thetadata import (
    ThetaDataProvider,
    _bar_from_row,
    _coerce_date,
    _normalise_strike,
    build_occ_symbol,
    parse_occ_symbol,
)

EXP = date(2026, 9, 18)


# ---------------------------------------------------------------------------
# Symbology
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "root,strike,right",
    [("SPY", 650.0, "C"), ("SPXW", 6500.0, "P"), ("QQQ", 580.5, "C"), ("NDXP", 24000.0, "P")],
)
def test_occ_symbol_roundtrip(root, strike, right):
    symbol = build_occ_symbol(root, EXP, strike, right)
    assert parse_occ_symbol(symbol) == (root, EXP, strike, right)


def test_occ_symbol_is_21_characters():
    """Anything else is not the OCC form and OPRA-derived feeds reject it."""
    assert len(build_occ_symbol("SPY", EXP, 650.0, "C")) == 21


def test_fractional_strikes_survive_the_roundtrip():
    """Half-dollar strikes are common on ETFs; truncating them silently
    reassigns gamma to the wrong strike."""
    symbol = build_occ_symbol("SPY", EXP, 650.5, "C")
    assert parse_occ_symbol(symbol)[2] == pytest.approx(650.5)


def test_unparseable_symbol_returns_none_rather_than_raising():
    """One malformed symbol must drop one contract, not abort a chain poll."""
    for bad in ("", "GARBAGE", "SPY 260918X00650000", "SPY 2609180650000"):
        assert parse_occ_symbol(bad) is None


def test_index_strikes_survive_normalisation():
    """The v3 feed sends strikes in dollars, so nothing may rescale them.

    This is a regression test for a real bug. _normalise_strike used to
    divide anything >= 1000 by a thousand, on the assumption that
    OPRA-derived feeds send thousandths. The live feed does not. SPY and
    QQQ strikes sit below 1000 and were unaffected, so the chain looked
    perfect -- while every SPX strike (6500 -> 6.50) and every NDX strike
    (25000 -> 25.00) failed to join back to the requested contracts and
    those chains came back empty.
    """
    assert _normalise_strike(650.0) == pytest.approx(650.0)
    assert _normalise_strike(6500.0) == pytest.approx(6500.0), "SPX strike rescaled"
    assert _normalise_strike(25000.0) == pytest.approx(25000.0), "NDX strike rescaled"
    assert _normalise_strike(None) is None


def test_coerce_date_accepts_the_shapes_the_api_returns():
    assert _coerce_date("2026-09-18") == EXP
    assert _coerce_date("20260918") == EXP
    assert _coerce_date(EXP) == EXP
    assert _coerce_date("nonsense") is None


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------


def test_registered_under_both_stages():
    names = available_providers()
    assert "thetadata" in names
    assert "thetadata_mv" in names


def test_futures_are_declared_unsupported():
    """ThetaData sells no CME product. This must fail at startup rather
    than writing an empty futures_quotes table."""
    provider = ThetaDataProvider(client=object())
    assert provider.capabilities.futures_bars is False
    with pytest.raises(ProviderCapabilityError):
        provider.stream_futures_bars("@ES")


def test_signed_volume_is_declared_unavailable():
    """Real-time equities are Nasdaq Basic, a fraction of consolidated
    volume, so the caller must classify from trades instead."""
    assert ThetaDataProvider._CAPABILITIES.signed_underlying_volume is False


def test_option_capabilities_are_declared():
    caps = ThetaDataProvider._CAPABILITIES
    assert caps.option_quotes
    assert caps.option_chain_discovery
    assert caps.option_open_interest
    assert caps.index_bars


# ---------------------------------------------------------------------------
# Chain fetch
# ---------------------------------------------------------------------------


class _FakeClient:
    """Records calls and returns canned frames for the three endpoints."""

    def __init__(self, *, quote_rows=None, ohlc_rows=None, oi_rows=None):
        self.calls = []
        self._quote_rows = quote_rows or []
        self._ohlc_rows = ohlc_rows or []
        self._oi_rows = oi_rows or []

    def option_snapshot_quote(self, **kw):
        self.calls.append(("quote", kw))
        return self._quote_rows

    def option_snapshot_ohlc(self, **kw):
        self.calls.append(("ohlc", kw))
        return self._ohlc_rows

    def option_snapshot_open_interest(self, **kw):
        self.calls.append(("open_interest", kw))
        return self._oi_rows


#: The exact row shape a live terminal returns, captured from
#: `make feed-probe PROVIDER=thetadata UNDERLYING=SPY` on 2026-09-14.
#: Strikes in DOLLARS, rights spelled "CALL"/"PUT", and a timezone-aware
#: quote timestamp. The earlier fixtures here were written from the wheel's
#: signatures and encoded strikes as thousandths, so they confirmed a guess
#: instead of the vendor -- which is how the index-strike bug survived.
QUOTE_TS = datetime(2026, 9, 11, 16, 14, 59, tzinfo=timezone(timedelta(hours=-4)))


def _chain_provider():
    quote_rows = [
        {
            "strike": 650.0,
            "right": "CALL",
            "bid": 1.20,
            "ask": 1.30,
            "bid_size": 5,
            "ask_size": 7,
            "timestamp": QUOTE_TS,
        },
        {
            "strike": 650.0,
            "right": "PUT",
            "bid": 0.80,
            "ask": 0.90,
            "bid_size": 3,
            "ask_size": 4,
            "timestamp": QUOTE_TS,
        },
        # A contract outside the requested set: expected with strike="*".
        {"strike": 999.0, "right": "CALL", "bid": 0.01, "ask": 0.02, "timestamp": QUOTE_TS},
    ]
    ohlc_rows = [{"strike": 650.0, "right": "CALL", "close": 1.25, "volume": 4210}]
    oi_rows = [{"strike": 650.0, "right": "CALL", "open_interest": 8123}]
    client = _FakeClient(quote_rows=quote_rows, ohlc_rows=ohlc_rows, oi_rows=oi_rows)
    return ThetaDataProvider(client), client


def test_chain_fetch_joins_three_endpoints():
    """bid/ask, last/volume and OI live on separate endpoints; a complete
    OptionQuote is the join."""
    provider, _ = _chain_provider()
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = provider.fetch_chain_state([call], include_open_interest=True)

    assert call in state
    assert state[call]["bid"] == pytest.approx(1.20)
    assert state[call]["ask"] == pytest.approx(1.30)
    assert state[call]["last"] == pytest.approx(1.25)
    assert state[call]["volume"] == 4210
    assert state[call]["open_interest"] == 8123


def test_chain_fetch_makes_one_call_per_expiration_not_per_contract():
    """strike="*" returns a whole expiration. Asking per contract would be
    thousands of calls for data that arrives in a handful."""
    provider, client = _chain_provider()
    symbols = [
        build_occ_symbol("SPY", EXP, strike, right)
        for strike in (650.0, 651.0, 652.0)
        for right in ("C", "P")
    ]
    provider.fetch_chain_state(symbols, include_open_interest=True)

    assert len(client.calls) == 3, "expected one call per endpoint, not per contract"
    assert {kind for kind, _ in client.calls} == {"quote", "ohlc", "open_interest"}
    for _, kwargs in client.calls:
        assert kwargs["strike"] == "*"
        assert kwargs["right"] == "both"


def test_open_interest_can_be_skipped():
    """OI settles once a day; polling it at quote cadence spends calls for
    nothing."""
    provider, client = _chain_provider()
    provider.fetch_chain_state(
        [build_occ_symbol("SPY", EXP, 650.0, "C")], include_open_interest=False
    )
    assert "open_interest" not in {kind for kind, _ in client.calls}


def test_contracts_outside_the_request_are_ignored():
    provider, _ = _chain_provider()
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = provider.fetch_chain_state([call], include_open_interest=True)
    assert build_occ_symbol("SPY", EXP, 999.0, "C") not in state


def test_one_failing_endpoint_does_not_lose_the_others():
    """A chain with quotes but no OI is degraded. A chain with nothing is
    an outage. The first must not become the second."""

    class _PartlyBroken(_FakeClient):
        def option_snapshot_open_interest(self, **kw):
            raise RuntimeError("entitlement error")

    client = _PartlyBroken(
        quote_rows=[{"strike": 650.0, "right": "CALL", "bid": 1.2, "ask": 1.3}],
        ohlc_rows=[{"strike": 650.0, "right": "CALL", "close": 1.25, "volume": 10}],
    )
    provider = ThetaDataProvider(client)
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = provider.fetch_chain_state([call], include_open_interest=True)

    assert state[call]["bid"] == pytest.approx(1.2)
    assert "open_interest" not in state[call]


def test_zero_prices_normalise_to_none():
    """A reported price of exactly zero means 'no quote'. Persisting it as
    a real zero hands the IV solver a free option."""
    client = _FakeClient(quote_rows=[{"strike": 650.0, "right": "CALL", "bid": 0, "ask": 0}])
    provider = ThetaDataProvider(client)
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = provider.fetch_chain_state([call], include_open_interest=False)
    assert state[call]["bid"] is None
    assert state[call]["ask"] is None


# ---------------------------------------------------------------------------
# Sticky merge
# ---------------------------------------------------------------------------


def test_sticky_open_interest_and_volume_survive_a_zero():
    """The single most repeated bug on the incumbent feed: a transient zero
    from a slow endpoint erasing the accumulated figure."""
    from src.ingestion.providers.thetadata import _PollingOptionQuoteStream

    call = build_occ_symbol("SPY", EXP, 650.0, "C")

    class _Swinging:
        def __init__(self):
            self.n = 0

        def fetch_chain_state(self, symbols, include_open_interest=True):
            self.n += 1
            if self.n == 1:
                return {call: {"bid": 1.0, "ask": 1.1, "volume": 500, "open_interest": 8000}}
            return {call: {"bid": 1.05, "ask": 1.15, "volume": 0, "open_interest": 0}}

    stream = _PollingOptionQuoteStream(
        _Swinging(), [call], poll_interval=0.01, oi_poll_interval=0.01
    )
    stream._poll(include_open_interest=True)
    stream._poll(include_open_interest=True)

    quote = stream.snapshot()[call]
    assert quote.volume == 500, "volume must not be erased by a zero"
    assert quote.open_interest == 8000, "OI must not be erased by a zero"
    assert quote.bid == pytest.approx(1.05), "prices must overwrite"


def test_drain_is_edge_triggered():
    """A watchdog has to distinguish 'no new data' from 'same data again'."""
    from src.ingestion.providers.thetadata import _PollingOptionQuoteStream

    call = build_occ_symbol("SPY", EXP, 650.0, "C")

    class _Static:
        def fetch_chain_state(self, symbols, include_open_interest=True):
            return {call: {"bid": 1.0, "ask": 1.1}}

    stream = _PollingOptionQuoteStream(_Static(), [call], poll_interval=0.01, oi_poll_interval=0.01)
    stream._poll(include_open_interest=False)
    assert set(stream.drain()) == {call}
    assert stream.drain() == {}


def test_no_greeks_are_requested():
    """ZeroGEX computes its own IV and Greeks. Paying for a surface the
    engine regenerates is the one clear waste in this vendor's catalogue."""
    provider, client = _chain_provider()
    provider.fetch_chain_state(
        [build_occ_symbol("SPY", EXP, 650.0, "C")], include_open_interest=True
    )
    assert not any("greeks" in kind for kind, _ in client.calls)


def test_quotes_carry_no_implied_volatility():
    provider, _ = _chain_provider()
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    quotes = provider.snapshot_option_quotes([call])
    assert quotes[call].implied_volatility is None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_strikes_require_an_expiration():
    """The endpoint takes one; guessing would silently return the wrong
    expiration's ladder."""

    class _C:
        def option_list_strikes(self, **kw):
            return []

    with pytest.raises(ValueError, match="expiration"):
        ThetaDataProvider(_C()).get_option_strikes("SPY", None)


def test_expirations_are_sorted_and_deduplicated():
    near = date.today() + timedelta(days=4)
    far = date.today() + timedelta(days=11)

    class _C:
        def option_list_expirations(self, **kw):
            return [
                {"expiration": far.isoformat()},
                {"expiration": near.isoformat()},
                {"expiration": near.strftime("%Y%m%d")},
            ]

    out = ThetaDataProvider(_C()).get_option_expirations("SPY")
    assert out == [near, far]


def test_expired_contracts_are_never_returned():
    """ThetaData answers expirations from its historical reference database.

    A live SPY query comes back with ~2,100 rows starting in 2012.
    Callers slice the front of this list to build a chain, so leaking
    history would hand them contracts that expired years ago and quote
    empty -- which looks exactly like an outage or a bad entitlement.
    """
    today = date.today()

    class _C:
        def option_list_expirations(self, **kw):
            return [
                {"expiration": "2012-06-01"},
                {"expiration": "2020-03-20"},
                {"expiration": (today - timedelta(days=1)).isoformat()},
                {"expiration": today.isoformat()},
                {"expiration": (today + timedelta(days=7)).isoformat()},
            ]

    out = ThetaDataProvider(_C()).get_option_expirations("SPY")
    # Today still trades (0DTE is the product), yesterday does not.
    assert out == [today, today + timedelta(days=7)]


def test_index_symbols_are_stripped_of_the_tradestation_decoration():
    """`$VIX.X` is TradeStation's spelling; ThetaData wants `VIX`."""
    seen = {}

    class _C:
        def index_snapshot_ohlc(self, **kw):
            seen.update(kw)
            return [{"close": 17.2}]

    provider = ThetaDataProvider(_C())
    stream = provider.stream_index_bars("$VIX.X", db_symbol="VIX")
    bar = stream._fetch()
    assert seen["symbol"] == "VIX"
    assert bar.close == pytest.approx(17.2)
    assert bar.symbol == "VIX"
    assert bar.up_volume is None


# ---------------------------------------------------------------------------
# Market Value selection
# ---------------------------------------------------------------------------


def test_market_value_endpoints_swap_the_quote_call():
    """Both selection mechanisms must work, because ThetaData's own
    answers disagree: support said "terminal stage", but the terminal's
    config.toml shows stage pointing at a server it labels unstable and
    testing-only."""
    calls = []

    class _C:
        def option_snapshot_quote(self, **kw):
            calls.append("quote")
            return []

        def option_snapshot_market_value(self, **kw):
            calls.append("market_value")
            return []

        def option_snapshot_ohlc(self, **kw):
            return []

        def option_snapshot_open_interest(self, **kw):
            return []

    symbol = build_occ_symbol("SPY", EXP, 650.0, "C")

    ThetaDataProvider(_C()).fetch_chain_state([symbol], include_open_interest=False)
    assert "quote" in calls and "market_value" not in calls

    calls.clear()
    ThetaDataProvider(_C(), market_value_endpoints=True).fetch_chain_state(
        [symbol], include_open_interest=False
    )
    assert "market_value" in calls and "quote" not in calls


def test_market_value_defaults_off():
    """The ordinary quote endpoint is the safe default: it is the one
    whose meaning is unambiguous."""
    assert ThetaDataProvider(object())._market_value_endpoints is False


# ---------------------------------------------------------------------------
# Findings from the first live terminal contact, 2026-09-14
# ---------------------------------------------------------------------------


def test_index_chains_round_trip_end_to_end():
    """An SPX chain must actually join, not just normalise.

    The unit test above covers _normalise_strike; this covers the path that
    broke. A rescaled strike still parses, so it fails silently at the
    `wanted` lookup and the whole expiration drops -- which reads as a
    missing entitlement rather than a unit bug.
    """
    for root, strike in (("SPXW", 6500.0), ("NDXP", 25000.0)):
        client = _FakeClient(
            quote_rows=[
                {
                    "strike": strike,
                    "right": "CALL",
                    "bid": 12.10,
                    "ask": 12.40,
                    "timestamp": QUOTE_TS,
                }
            ],
            ohlc_rows=[],
            oi_rows=[],
        )
        symbol = build_occ_symbol(root, EXP, strike, "C")
        state = ThetaDataProvider(client).fetch_chain_state([symbol], include_open_interest=False)
        assert symbol in state, f"{root} {strike} did not join"
        assert state[symbol]["bid"] == pytest.approx(12.10)


def test_quote_carries_the_vendors_timestamp_not_now():
    """Snapshots return the last quote whether or not the market is open.

    A probe run on Sunday returned quotes stamped the previous Friday at
    16:14. Stamping now() would record a two-day-old quote as current and
    defeat every staleness check downstream.
    """
    provider, _ = _chain_provider()
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = provider.fetch_chain_state([call], include_open_interest=False)

    stamped = state[call]["timestamp"]
    assert stamped is not None
    assert stamped == QUOTE_TS
    assert stamped.tzinfo is not None
    # Same instant, expressed in UTC.
    assert stamped.utcoffset() == timedelta(0)


def test_nothing_joining_is_reported_rather_than_returned_empty(caplog):
    """A unit or key mismatch must name itself.

    Rows arriving but joining to nothing is indistinguishable from a closed
    market or a missing entitlement unless the provider says so.
    """
    import logging

    from src.ingestion.providers import thetadata as mod

    mod._REPORTED_SHAPES.clear()
    client = _FakeClient(
        # Thousandths, as a feed that disagreed with this module would send.
        quote_rows=[{"strike": 650000, "right": "CALL", "bid": 1.2, "ask": 1.3}],
    )
    symbol = build_occ_symbol("SPY", EXP, 650.0, "C")
    with caplog.at_level(logging.WARNING):
        state = ThetaDataProvider(client).fetch_chain_state([symbol], include_open_interest=False)

    assert state.get(symbol, {}).get("bid") is None
    assert "no strike / right column matched" in caplog.text
    assert "'strike'" in caplog.text, "the columns that arrived must be named"


def test_the_unmapped_warning_fires_once_per_shape(caplog):
    """A poll loop must not emit this every five seconds."""
    import logging

    from src.ingestion.providers import thetadata as mod

    mod._REPORTED_SHAPES.clear()
    rows = [{"strike": 650000, "right": "CALL", "bid": 1.2, "ask": 1.3}]
    symbol = build_occ_symbol("SPY", EXP, 650.0, "C")
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            ThetaDataProvider(_FakeClient(quote_rows=rows)).fetch_chain_state(
                [symbol], include_open_interest=False
            )

    assert caplog.text.count("no strike / right column matched") == 1


# ---------------------------------------------------------------------------
# Symbol translation and parallel chain fetch
# ---------------------------------------------------------------------------


def test_index_symbol_is_not_the_option_root():
    """SPX's weekly chain is rooted SPXW; there is no index called SPXW.

    A live probe on $SPXW.X died with
    "No data found for: index_snapshot_ohlc(SPXW)". The option endpoints
    want the root, the index endpoint wants the index, and they differ for
    exactly the two underlyings this deployment cares most about.
    """
    from src.ingestion.providers.thetadata import index_symbol_for, option_root_for

    assert option_root_for("$SPXW.X") == "SPXW"
    assert index_symbol_for("$SPXW.X") == "SPX"
    assert option_root_for("$NDXP.X") == "NDXP"
    assert index_symbol_for("$NDXP.X") == "NDX"


def test_three_letter_indices_are_not_truncated():
    """The SPXW -> SPX rule must not turn VIX into VI."""
    from src.ingestion.providers.thetadata import index_symbol_for

    for symbol, expected in (
        ("$VIX.X", "VIX"),
        ("$VXN.X", "VXN"),
        ("$RUT.X", "RUT"),
        ("$SPX.X", "SPX"),
        ("SPY", "SPY"),
        ("QQQ", "QQQ"),
    ):
        assert index_symbol_for(symbol) == expected, symbol


def test_index_bars_query_the_index_not_the_root():
    """End-to-end: the symbol that reaches the client must be the index."""
    seen = {}

    class _C:
        def index_snapshot_ohlc(self, **kw):
            seen["symbol"] = kw.get("symbol")
            return []

    stream = ThetaDataProvider(_C()).stream_index_bars("$SPXW.X")
    stream.start()
    try:
        deadline = time.monotonic() + 3
        while "symbol" not in seen and time.monotonic() < deadline:
            time.sleep(0.02)
    finally:
        stream.stop()
    assert seen.get("symbol") == "SPX"


def test_chain_fetch_never_exceeds_the_account_concurrency_limit():
    """ThetaData allows 8 concurrent requests for this account.

    Exceeding it does not queue politely -- requests fail, and a failed bar
    poll then backs off exponentially. The bound has to hold across all the
    streams sharing one provider, not per call.
    """
    live = {"now": 0, "peak": 0}
    lock = threading.Lock()

    class _C:
        def _busy(self, **kw):
            with lock:
                live["now"] += 1
                live["peak"] = max(live["peak"], live["now"])
            time.sleep(0.05)
            with lock:
                live["now"] -= 1
            return []

        option_snapshot_quote = _busy
        option_snapshot_ohlc = _busy
        option_snapshot_open_interest = _busy

    provider = ThetaDataProvider(_C(), max_concurrency=3)
    symbols = [build_occ_symbol("SPY", EXP + timedelta(days=7 * i), 650.0, "C") for i in range(8)]
    try:
        provider.fetch_chain_state(symbols, include_open_interest=True)
    finally:
        provider.close()

    assert live["peak"] <= 3, f"ran {live['peak']} concurrent requests against a limit of 3"
    assert live["peak"] > 1, "did not parallelise at all"


def test_parallel_fetch_is_faster_than_sequential():
    """The whole point: a cycle must fit inside its poll interval."""

    class _C:
        def _slow(self, **kw):
            time.sleep(0.05)
            return []

        option_snapshot_quote = _slow
        option_snapshot_ohlc = _slow
        option_snapshot_open_interest = _slow

    symbols = [build_occ_symbol("SPY", EXP + timedelta(days=7 * i), 650.0, "C") for i in range(6)]

    serial = ThetaDataProvider(_C(), max_concurrency=1)
    started = time.monotonic()
    serial.fetch_chain_state(symbols, include_open_interest=True)
    serial_elapsed = time.monotonic() - started

    parallel = ThetaDataProvider(_C(), max_concurrency=6)
    started = time.monotonic()
    try:
        parallel.fetch_chain_state(symbols, include_open_interest=True)
    finally:
        parallel.close()
    parallel_elapsed = time.monotonic() - started

    assert (
        parallel_elapsed < serial_elapsed / 2
    ), f"parallel {parallel_elapsed:.2f}s vs serial {serial_elapsed:.2f}s"


def test_one_failing_endpoint_does_not_lose_the_others_in_parallel():
    """A chain with quotes but no OI is degraded; with nothing, an outage."""

    class _C:
        def option_snapshot_quote(self, **kw):
            return [{"strike": 650.0, "right": "CALL", "bid": 1.2, "ask": 1.3}]

        def option_snapshot_ohlc(self, **kw):
            return [{"strike": 650.0, "right": "CALL", "close": 1.25, "volume": 9}]

        def option_snapshot_open_interest(self, **kw):
            raise RuntimeError("entitlement check failed")

    provider = ThetaDataProvider(_C(), max_concurrency=4)
    symbol = build_occ_symbol("SPY", EXP, 650.0, "C")
    try:
        state = provider.fetch_chain_state([symbol], include_open_interest=True)
    finally:
        provider.close()

    assert state[symbol]["bid"] == pytest.approx(1.2)
    assert state[symbol]["volume"] == 9
    assert state[symbol].get("open_interest") is None


def test_close_is_idempotent():
    provider = ThetaDataProvider(_FakeClient(), max_concurrency=4)
    provider.close()
    provider.close()


def test_a_prior_sessions_volume_is_not_reported_as_todays():
    """option_snapshot_ohlc returns the last DAILY bar, not today's.

    A live SPX probe on Monday returned a contract whose only trade was the
    previous Friday: close 791.24, volume 1, timestamp Friday 12:43. That
    volume is Friday's.

    OptionQuote.volume is cumulative volume for THIS session and the engine
    differences successive snapshots to derive flow, so passing a stale
    figure through books old trades as today's and classifies them
    Lee-Ready into ask/bid flow. Worse, when the stale total exceeds
    today's first real print the engine reads the decrease as a vendor
    session reset, re-anchors to zero, and counts the whole stale total
    again.
    """
    stale = datetime(2026, 9, 11, 12, 43, tzinfo=timezone(timedelta(hours=-4)))
    client = _FakeClient(
        quote_rows=[
            {"strike": 6875.0, "right": "CALL", "bid": 738.1, "ask": 754.1, "timestamp": QUOTE_TS}
        ],
        ohlc_rows=[
            {
                "strike": 6875.0,
                "right": "CALL",
                "close": 791.24,
                "volume": 1,
                "timestamp": stale,
            }
        ],
    )
    symbol = build_occ_symbol("SPXW", EXP, 6875.0, "C")
    state = ThetaDataProvider(client).fetch_chain_state([symbol], include_open_interest=False)

    assert state[symbol]["volume"] == 0, "a prior session's volume leaked in as today's"
    # The last TRADE price stays: it is true whenever it happened, and the
    # IV solver needs it when there is no two-sided quote.
    assert state[symbol]["last"] == pytest.approx(791.24)


def test_todays_volume_is_kept():
    """The guard must not discard live volume."""
    now = datetime.now(timezone(timedelta(hours=-4)))
    client = _FakeClient(
        quote_rows=[
            {"strike": 650.0, "right": "CALL", "bid": 1.2, "ask": 1.3, "timestamp": QUOTE_TS}
        ],
        ohlc_rows=[
            {"strike": 650.0, "right": "CALL", "close": 1.25, "volume": 4210, "timestamp": now}
        ],
    )
    symbol = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = ThetaDataProvider(client).fetch_chain_state([symbol], include_open_interest=False)
    assert state[symbol]["volume"] == 4210


def test_prior_session_is_judged_in_the_feeds_own_timezone():
    """A UTC comparison would zero that afternoon's volume every evening.

    Rows arrive localised to America/New_York. The UTC date rolls at 20:00
    ET, four hours after the 16:00 close, so between 20:00 and midnight a
    UTC-date comparison marks the session that just ended as a prior one.
    """
    from src.ingestion.providers.thetadata import is_prior_session

    eastern = timezone(timedelta(hours=-4))
    now_et = datetime.now(eastern)
    assert is_prior_session(now_et) is False
    assert is_prior_session(now_et - timedelta(days=1)) is True

    # The evening window: a 16:00 ET bar judged at 21:00 ET the same day.
    # In UTC both are already "tomorrow" for `now` but not for the bar, so
    # a UTC comparison would call this a prior session. It is not.
    bar = datetime(2026, 9, 14, 16, 0, tzinfo=eastern)
    evening = datetime(2026, 9, 14, 21, 0, tzinfo=eastern)
    assert bar.date() == evening.date()
    assert bar.astimezone(timezone.utc).date() < evening.astimezone(timezone.utc).date()
    # Unknown or naive timestamps must not cause data to be discarded.
    assert is_prior_session(None) is False
    assert is_prior_session(datetime.now()) is False


# ---------------------------------------------------------------------------
# Market Value routing (ThetaData commercial answers, 2026-09-15)
# ---------------------------------------------------------------------------


class _MVClient:
    """A client exposing both realtime and Market Value endpoints."""

    def __init__(self, *, with_stock_mv=True):
        self.called = []
        if not with_stock_mv:
            del self.__class__.stock_snapshot_market_value

    def _bar(self, name, **kw):
        self.called.append(name)
        return [{"close": 100.0, "timestamp": datetime.now(timezone(timedelta(hours=-4)))}]

    def index_snapshot_ohlc(self, **kw):
        return self._bar("index_snapshot_ohlc", **kw)

    def index_snapshot_market_value(self, **kw):
        return self._bar("index_snapshot_market_value", **kw)

    def stock_snapshot_ohlc(self, **kw):
        return self._bar("stock_snapshot_ohlc", **kw)

    def stock_snapshot_market_value(self, **kw):
        return self._bar("stock_snapshot_market_value", **kw)


def _drain(stream, timeout=3.0):
    stream.start()
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            bar = stream.drain()
            if bar is not None:
                return bar
            time.sleep(0.02)
    finally:
        stream.stop()
    return None


def test_index_bars_honour_the_market_value_stage():
    """Index values are licensed separately from OPRA -- Cboe CGIF for SPX
    and VIX, Nasdaq GIDS for NDX.

    A Market Value deployment still calling the realtime index endpoint
    would carry exchange fees on half this deployment's underlyings, and
    the numbers would look entirely correct while doing it.
    """
    client = _MVClient()
    mv = ThetaDataProvider(client, stage="mv", market_value_endpoints=True, poll_interval=0.05)
    _drain(mv.stream_index_bars("$SPXW.X"))
    assert "index_snapshot_market_value" in client.called
    assert "index_snapshot_ohlc" not in client.called


def test_realtime_stage_still_uses_the_realtime_endpoints():
    client = _MVClient()
    rt = ThetaDataProvider(
        client, stage="realtime", market_value_endpoints=False, poll_interval=0.05
    )
    _drain(rt.stream_index_bars("$SPXW.X"))
    _drain(rt.stream_underlying_bars("SPY"))
    assert "index_snapshot_ohlc" in client.called
    assert "stock_snapshot_ohlc" in client.called
    assert not any(c.endswith("market_value") for c in client.called)


def test_underlying_bars_honour_the_market_value_stage():
    client = _MVClient()
    mv = ThetaDataProvider(client, stage="mv", market_value_endpoints=True, poll_interval=0.05)
    _drain(mv.stream_underlying_bars("SPY"))
    assert "stock_snapshot_market_value" in client.called


def test_a_missing_market_value_endpoint_raises_rather_than_falling_back():
    """Silently serving realtime inside a Market Value deployment would bill
    exchange fees on the feed that switched to Market Value to avoid them,
    and nothing downstream would show it."""

    class _NoStockMV:
        def stock_snapshot_ohlc(self, **kw):
            return []

    provider = ThetaDataProvider(_NoStockMV(), stage="mv", market_value_endpoints=True)
    with pytest.raises(ProviderCapabilityError, match="stock_snapshot_market_value"):
        provider._endpoint("stock_snapshot_market_value", "stock_snapshot_ohlc")


def test_the_probe_reports_which_market_value_endpoints_exist():
    class _Partial:
        def option_snapshot_market_value(self, **kw):
            return []

        def option_list_expirations(self, **kw):
            return []

    out = ThetaDataProvider(_Partial()).describe_columns("SPY")
    inventory = out["market_value_endpoints"]
    assert inventory["option_snapshot_market_value"] == "present"
    assert inventory["stock_snapshot_market_value"] == "ABSENT"
    assert inventory["index_snapshot_market_value"] == "ABSENT"


def test_two_stages_on_one_terminal_share_a_client(monkeypatch):
    """A second ThetaClient invalidates the first one's session.

    The terminal keeps ONE session, so two providers each constructing their
    own client kills the realtime vs Market Value comparison on its first
    sample: "Invalid session ID. This can occur if more than one terminal is
    running." Observed against a live terminal 2026-09-15.

    Sharing is sound because Market Value is selected per CALL for snapshots
    (the *_market_value endpoints), not per connection.
    """
    from src.ingestion.providers import thetadata as mod

    mod.reset_shared_clients()
    built = []

    def _factory():
        built.append(object())
        return built[-1]

    a = mod.shared_client(_factory, key=("host", "25503"))
    b = mod.shared_client(_factory, key=("host", "25503"))
    assert a is b
    assert len(built) == 1, "a second client would invalidate the first's session"

    # A genuinely different terminal still gets its own.
    c = mod.shared_client(_factory, key=("host", "25603"))
    assert c is not a
    assert len(built) == 2
    mod.reset_shared_clients()


def test_close_does_not_shut_a_client_another_provider_is_using():
    """Closing one stage must not break the other mid-comparison."""

    class _Client:
        closed = False

        def close(self):
            self.closed = True

    client = _Client()
    a = ThetaDataProvider(client, stage="realtime")
    b = ThetaDataProvider(client, stage="mv", market_value_endpoints=True)
    a.close()
    assert client.closed is False, "closed a client the other stage still needs"
    b.close()
    assert client.closed is False


def test_the_two_stages_report_different_provider_names():
    """A shared name makes a Market Value comparison measure nothing.

    The shadow tables key on (provider, option_symbol, captured_at). With
    both stages reporting "thetadata", the candidate's rows collided with
    the incumbent's and were dropped by ON CONFLICT DO NOTHING -- silently,
    since a conflict is not an error -- so the Market Value side was never
    persisted at all. feed_comparisons recorded both sides under the same
    name too, and the printed report labelled both columns "thetadata".
    Observed on a live run 2026-09-15.
    """

    class _C:
        pass

    assert ThetaDataProvider(_C(), stage="realtime").name == "thetadata"
    for mv in ("mv", "market_value", "marketvalue", "MarketValue", " MV "):
        assert ThetaDataProvider(_C(), stage=mv).name == "thetadata_mv", mv


def test_registry_builds_the_two_stages_under_distinct_names(monkeypatch):
    """The names the harness persists must differ, not just the CLI labels."""
    from src.ingestion.providers import get_provider, register_provider

    built = {}

    def _factory(**kw):
        stage = kw.get("stage", "realtime")

        class _C:
            pass

        p = ThetaDataProvider(_C(), stage=stage)
        built[stage] = p
        return p

    register_provider("_t_rt", lambda **kw: _factory(stage="realtime"))
    register_provider("_t_mv", lambda **kw: _factory(stage="mv"))
    assert get_provider("_t_rt").name != get_provider("_t_mv").name


# ---------------------------------------------------------------------------
# Market Value column names (live probe, 2026-09-15)
# ---------------------------------------------------------------------------

#: A real row from option_snapshot_market_value. The Market Value endpoints
#: answer with their own column names and carry no sizes.
MV_QUOTE_ROW = {
    "expiration": "2026-09-15",
    "market_ask": 47.19,
    "market_bid": 46.980000000000004,
    "market_price": 47.08,
    "right": "CALL",
    "strike": 710.0,
    "symbol": "SPY",
    "timestamp": datetime(2026, 9, 15, 13, 34, 37, tzinfo=timezone(timedelta(hours=-4))),
}


def test_market_value_quotes_are_read_not_dropped():
    """Accepting only "bid"/"ask" made every Market Value quote read as
    absent. The comparison then reported two-sided=0 while every other
    count looked healthy -- a feed returning 240 contracts and no prices."""
    from src.ingestion.providers.thetadata import _as_float, _pick

    assert _as_float(_pick(MV_QUOTE_ROW, "bid")) == pytest.approx(46.98)
    assert _as_float(_pick(MV_QUOTE_ROW, "ask")) == pytest.approx(47.19)
    assert _as_float(_pick(MV_QUOTE_ROW, "mid")) == pytest.approx(47.08)


def test_a_market_value_row_can_stand_in_for_a_spot_bar():
    """There is no Market Value OHLC endpoint, so spot comes from the mark.

    Without this the MV stage had no spot at all: every sample waited out
    its deadline and aborted with "no spot price available"."""
    from src.ingestion.providers.thetadata import _bar_from_row

    bar = _bar_from_row(MV_QUOTE_ROW, "SPY")
    assert bar is not None
    assert bar.close == pytest.approx(47.08)


def test_bars_carry_the_vendors_timestamp():
    """Same rule as the option quotes: these snapshots serve the last
    available value whether or not the market is open."""
    from src.ingestion.providers.thetadata import _bar_from_row

    bar = _bar_from_row(MV_QUOTE_ROW, "SPY")
    assert bar.timestamp == MV_QUOTE_ROW["timestamp"]
    assert bar.timestamp.utcoffset() == timedelta(0)


def test_a_market_value_chain_joins_and_prices():
    """End to end: the MV stage must produce two-sided quotes."""
    client = _FakeClient(quote_rows=[MV_QUOTE_ROW], ohlc_rows=[], oi_rows=[])
    client.option_snapshot_market_value = client.option_snapshot_quote

    provider = ThetaDataProvider(client, stage="mv", market_value_endpoints=True)
    symbol = build_occ_symbol("SPY", EXP, 710.0, "C")
    state = provider.fetch_chain_state([symbol], include_open_interest=False)

    assert state[symbol]["bid"] == pytest.approx(46.98)
    assert state[symbol]["ask"] == pytest.approx(47.19)
    assert state[symbol]["mid"] == pytest.approx(47.08)


def test_realtime_rows_still_have_no_vendor_mid():
    """Only Market Value rows carry a mark; elsewhere effective_mid() derives it."""
    provider, _ = _chain_provider()
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = provider.fetch_chain_state([call], include_open_interest=False)
    assert state[call].get("mid") is None


def test_stopping_a_bar_stream_does_not_wait_out_the_poll_interval():
    """Teardown was costing a full interval per sample.

    The poll loop slept with time.sleep, so stop() had to wait for the
    thread to wake before joining -- five seconds of dead time after the bar
    had already been delivered. A live probe reported 5.0s for spot against
    0.21s for the entire chain fetch.
    """

    class _C:
        def stock_snapshot_ohlc(self, **kw):
            return [{"close": 100.0, "timestamp": datetime.now(timezone(timedelta(hours=-4)))}]

    provider = ThetaDataProvider(_C(), poll_interval=30.0)
    stream = provider.stream_underlying_bars("SPY")
    stream.start()
    deadline = time.monotonic() + 3
    while stream.drain() is None and time.monotonic() < deadline:
        time.sleep(0.02)

    started = time.monotonic()
    stream.stop()
    assert time.monotonic() - started < 2.0, "stop() waited out the poll interval"


# ---------------------------------------------------------------------------
# Underlying bars: a Market Value row is a mark, not a bar
# ---------------------------------------------------------------------------

#: Exactly what a live terminal returned for QQQ on 2026-09-22.
_MV_STOCK_ROW = {
    "market_ask": 746.53,
    "market_bid": 746.5,
    "market_price": 746.51,
    "symbol": "QQQ",
    "timestamp": None,
}
_MV_INDEX_ROW = {"market_price": 14.33, "symbol": "VIX", "timestamp": None}
_REALTIME_STOCK_ROW = {
    "close": 746.52,
    "high": 746.6,
    "low": 741.0,
    "open": 741.005,
    "volume": 11145125,
    "symbol": "QQQ",
    "timestamp": None,
}


def test_a_market_value_mark_becomes_a_degenerate_bar():
    """The Market Value endpoints carry no open, high or low.

    Passed through as None they reach ``underlying_quotes`` as NULL, and
    the site's candles -- 1-minute rows aggregated to 5-minute OHLC -- lose
    their bodies and wicks. Seeding all three from the mark lets
    ``_upsert_underlying_quote`` build the real candle out of the sequence
    of polls (first-seen open, GREATEST high, LEAST low, last close).
    """
    bar = _bar_from_row(_MV_STOCK_ROW, "QQQ")
    assert bar is not None
    assert bar.open == bar.high == bar.low == bar.close == 746.51

    # Indices carry market_price alone -- no bid/ask either.
    index_bar = _bar_from_row(_MV_INDEX_ROW, "$VIX.X")
    assert index_bar.open == index_bar.high == index_bar.low == index_bar.close == 14.33


def test_a_realtime_row_keeps_its_own_ohlc():
    """stock_snapshot_ohlc does supply all three. Seeding must not clobber
    them -- this is the branch that tells a fallback from an overwrite."""
    bar = _bar_from_row(_REALTIME_STOCK_ROW, "QQQ")
    assert (bar.open, bar.high, bar.low, bar.close) == (741.005, 746.6, 741.0, 746.52)
    # The row's `volume` deliberately does NOT come through here. This
    # assertion used to read `== 11145125`, which pinned the row's running
    # DAILY total into a field meaning "volume traded during this bar" --
    # the two differ by three orders of magnitude at the close, and a VWAP
    # built on the wrong one still looks plausible. _SessionVolumeDelta
    # differences it in the stream instead; see the tests below.
    assert bar.volume is None


def test_a_zero_mark_yields_no_bar_at_all():
    """``_as_float`` maps a price of exactly 0 to None on purpose -- a zero
    quote means "no quote" -- so a zero close is an absent close and the
    row produces nothing, rather than a bar pinned at the origin."""
    assert _bar_from_row({"market_price": 0.0, "timestamp": None}, "QQQ") is None

    # And a zero in one leg of a real bar falls back to close rather than
    # dragging the candle to zero.
    bar = _bar_from_row({"close": 5.0, "open": 0.0, "high": 7.0, "timestamp": None}, "X")
    assert (bar.open, bar.high, bar.low) == (5.0, 7.0, 5.0)


def test_bars_can_poll_faster_than_chains(monkeypatch):
    """One underlying's bar is ONE call; its chain is expirations x
    endpoints. Sharing a single interval means either coarse candles or
    overlapping chain polls, and at a one-second bar poll the rate IS the
    wick resolution."""
    monkeypatch.setenv("THETADATA_POLL_SECONDS", "5")
    monkeypatch.setenv("THETADATA_BAR_POLL_SECONDS", "1")

    provider = ThetaDataProvider(object(), stage="mv")
    assert provider._poll_interval == 5.0
    assert provider._bar_poll_interval == 5.0, "explicit construction ignores the env"

    faster = ThetaDataProvider(object(), stage="mv", poll_interval=5.0, bar_poll_interval=1.0)
    assert faster._poll_interval == 5.0
    assert faster._bar_poll_interval == 1.0

    # Unset, bars inherit the chain cadence so existing deployments do not
    # silently change rate.
    same = ThetaDataProvider(object(), stage="mv", poll_interval=3.0)
    assert same._bar_poll_interval == 3.0

    # Storing the value is not the point -- the STREAM has to run at it.
    for stream in (
        faster.stream_underlying_bars("QQQ"),
        faster.stream_index_bars("$VIX.X"),
    ):
        assert stream._poll_interval == 1.0, "the bar stream must poll at the bar cadence"


# ---------------------------------------------------------------------------
# Session volume: cumulative in, per-bar out
# ---------------------------------------------------------------------------
def _et(h, m, s=0, day=22):
    return datetime(2026, 9, day, h, m, s, tzinfo=timezone(timedelta(hours=-4)))


def test_the_first_observation_anchors_rather_than_booking_the_whole_session():
    """A midday start sees a cumulative figure of millions. Reporting that
    as one minute's volume is a ~390x spike into every z-score reading the
    column -- and a plausible-looking one, which is worse than an obvious
    one. Anchor where we stand and report zero."""
    from src.ingestion.providers.thetadata import _SessionVolumeDelta

    d = _SessionVolumeDelta("QQQ")
    assert d.delta(11_145_125, _et(13, 4)) == 0
    # ... and the very next poll is a true delta off that anchor.
    assert d.delta(11_147_000, _et(13, 4, 30)) == 1875


def test_volume_accumulates_within_the_minute_and_resets_across_it():
    """The upsert is last-write-wins on the minute, so each poll must carry
    the whole minute so far -- not that poll's increment."""
    from src.ingestion.providers.thetadata import _SessionVolumeDelta

    d = _SessionVolumeDelta("QQQ")
    d.delta(1_000_000, _et(13, 4))
    assert d.delta(1_000_400, _et(13, 4, 20)) == 400
    assert d.delta(1_000_900, _et(13, 4, 40)) == 900, "running total for the minute, not 500"

    # New minute: the previous minute's LAST reading is this one's floor.
    assert d.delta(1_001_100, _et(13, 5, 5)) == 200
    assert d.delta(1_001_500, _et(13, 5, 45)) == 600


def test_a_session_rollover_is_not_a_step_backwards():
    """The cumulative counter restarts at the open. Having watched the
    previous session, the whole new figure belongs to the new session's
    first bar -- distinct from the cold-start case above, which cannot know
    that and anchors instead."""
    from src.ingestion.providers.thetadata import _SessionVolumeDelta

    d = _SessionVolumeDelta("QQQ")
    d.delta(1_000_000, _et(15, 59, day=22))
    d.delta(1_002_000, _et(15, 59, 30, day=22))
    assert d.delta(5_000, _et(9, 30, day=23)) == 5_000


def test_the_session_date_is_read_in_the_feeds_own_timezone():
    """is_prior_session's reason, applied here: the rows arrive localised to
    America/New_York and the UTC date rolls at 20:00 ET, four hours after
    the session closed. A UTC comparison would call 20:05 ET a new session
    every evening and re-anchor at zero, booking the whole day's cumulative
    volume as one after-hours bar."""
    from src.ingestion.providers.thetadata import _SessionVolumeDelta

    d = _SessionVolumeDelta("QQQ")
    d.delta(1_000_000, _et(19, 55))
    # 20:05 ET is the SAME session; in UTC it is already the next day.
    later = _et(20, 5)
    assert later.astimezone(timezone.utc).date() != later.date(), "fixture must span the roll"
    assert d.delta(1_000_300, later) == 300


def test_a_backwards_step_inside_a_session_is_unknown_not_zero():
    """Not a volume, and not something to invent a value for. Bar's own
    contract spells unknown as None."""
    from src.ingestion.providers.thetadata import _SessionVolumeDelta

    d = _SessionVolumeDelta("QQQ")
    d.delta(1_000_000, _et(13, 4))
    assert d.delta(999_000, _et(13, 5)) is None


def test_no_cumulative_figure_means_no_volume_claim():
    from src.ingestion.providers.thetadata import _SessionVolumeDelta

    d = _SessionVolumeDelta("QQQ")
    assert d.delta(None, _et(13, 4)) is None
    assert d.delta(1_000, None) is None


def test_a_bar_row_never_carries_the_cumulative_figure_as_a_per_bar_one():
    """_bar_from_row maps a row; the running daily total is not this bar's
    volume and must not reach Bar.volume wearing its name."""
    assert _bar_from_row(_REALTIME_STOCK_ROW, "QQQ").volume is None


class _FakeStockClient:
    """Answers the Market Value stock endpoint and the realtime OHLC one."""

    def __init__(self, cumulative):
        self.calls = []
        self._cumulative = list(cumulative)

    def stock_snapshot_market_value(self, **kw):
        self.calls.append(("market_value", kw))
        return [dict(_MV_STOCK_ROW)]

    def stock_snapshot_ohlc(self, **kw):
        self.calls.append(("ohlc", kw))
        volume, ts = self._cumulative.pop(0)
        return [{"close": 746.52, "volume": volume, "symbol": "QQQ", "timestamp": ts}]


def test_market_value_bars_fetch_volume_from_the_realtime_endpoint():
    """stock_snapshot_market_value answers market_bid / market_ask /
    market_price and nothing else, so the volume takes a second call -- the
    same standing as option_snapshot_ohlc, which this provider already makes
    unconditionally (F4)."""
    client = _FakeStockClient([(1_000_000, _et(13, 4)), (1_000_250, _et(13, 4, 30))])
    provider = ThetaDataProvider(client, stage="mv", market_value_endpoints=True)
    stream = provider.stream_underlying_bars("QQQ")

    first = stream._fetch()
    assert [k for k, _ in client.calls] == ["market_value", "ohlc"]
    assert first.close == pytest.approx(746.51), "price still comes from Market Value"
    assert first.volume == 0

    assert stream._fetch().volume == 250


def test_index_bars_report_no_volume_at_all():
    """A cash index is a calculation over its constituents, not something
    that trades. None is the fact here, not a gap -- index VWAP comes from
    an ETF proxy's volume instead.

    Asserted against a row that DOES carry a volume column, because
    index_snapshot_ohlc sends one (see the endpoint inventory in the
    module docstring) and it is meaningless: a running daily figure for a
    thing with no shares. A fixture without the column would pass this test
    whatever the code did.
    """

    class _Idx:
        def __init__(self):
            self.calls = []

        def index_snapshot_ohlc(self, **kw):
            self.calls.append("ohlc")
            return [
                {
                    "close": 14.33,
                    "volume": 9_999_999,
                    "symbol": "VIX",
                    "timestamp": _et(13, 4),
                }
            ]

        def stock_snapshot_ohlc(self, **kw):  # pragma: no cover - must not be called
            raise AssertionError("an index must not reach for an equity volume")

    client = _Idx()
    provider = ThetaDataProvider(client, stage="thetadata", market_value_endpoints=False)
    assert provider.stream_index_bars("$VIX.X")._fetch().volume is None

    # And on the Market Value path, where no volume arrives at all.
    class _MvIdx:
        def index_snapshot_market_value(self, **kw):
            return [dict(_MV_INDEX_ROW)]

    mv = ThetaDataProvider(_MvIdx(), stage="mv", market_value_endpoints=True)
    assert mv.stream_index_bars("$VIX.X")._fetch().volume is None
