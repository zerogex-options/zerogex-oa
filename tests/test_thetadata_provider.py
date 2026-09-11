"""Tests for the ThetaData provider.

Written against the v3 gRPC client's signatures rather than a live
terminal, so these lock down the parts that are mine: symbol handling,
the join across three snapshot endpoints, sticky-field semantics, and the
capability declarations. The response column names are server-supplied and
must still be confirmed on first contact with a real terminal.
"""

from __future__ import annotations

from datetime import date

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


def test_strike_normalisation_handles_thousandths():
    """Feeds disagree on units. 650000 is $650, not $650,000."""
    assert _normalise_strike(650000) == pytest.approx(650.0)
    assert _normalise_strike(650.0) == pytest.approx(650.0)
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


def _chain_provider():
    quote_rows = [
        {"strike": 650000, "right": "C", "bid": 1.20, "ask": 1.30, "bid_size": 5, "ask_size": 7},
        {"strike": 650000, "right": "P", "bid": 0.80, "ask": 0.90, "bid_size": 3, "ask_size": 4},
        # A contract outside the requested set: expected with strike="*".
        {"strike": 999000, "right": "C", "bid": 0.01, "ask": 0.02},
    ]
    ohlc_rows = [{"strike": 650000, "right": "C", "close": 1.25, "volume": 4210}]
    oi_rows = [{"strike": 650000, "right": "C", "open_interest": 8123}]
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
        quote_rows=[{"strike": 650000, "right": "C", "bid": 1.2, "ask": 1.3}],
        ohlc_rows=[{"strike": 650000, "right": "C", "close": 1.25, "volume": 10}],
    )
    provider = ThetaDataProvider(client)
    call = build_occ_symbol("SPY", EXP, 650.0, "C")
    state = provider.fetch_chain_state([call], include_open_interest=True)

    assert state[call]["bid"] == pytest.approx(1.2)
    assert "open_interest" not in state[call]


def test_zero_prices_normalise_to_none():
    """A reported price of exactly zero means 'no quote'. Persisting it as
    a real zero hands the IV solver a free option."""
    client = _FakeClient(quote_rows=[{"strike": 650000, "right": "C", "bid": 0, "ask": 0}])
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
    class _C:
        def option_list_expirations(self, **kw):
            return [
                {"expiration": "2026-09-25"},
                {"expiration": "2026-09-18"},
                {"expiration": "20260918"},
            ]

    out = ThetaDataProvider(_C()).get_option_expirations("SPY")
    assert out == [date(2026, 9, 18), date(2026, 9, 25)]


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
