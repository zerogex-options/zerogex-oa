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
