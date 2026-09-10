"""Tests for the market data provider abstraction.

Two things these tests are protecting:

1. **Zero behaviour change on the incumbent path.** The TradeStation
   provider must be a pure adapter. If it starts re-deriving values rather
   than passing the production accumulators' output through, a migration
   would be comparing the new feed against a subtly different incumbent.

2. **Capability gaps fail loudly.** No evaluated vendor covers all six
   symbol families, so asking a provider for a feed it does not carry has
   to raise at startup rather than write empty tables.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.ingestion.providers import (
    DEFAULT_PROVIDER,
    ProviderCapabilities,
    ProviderCapabilityError,
    available_providers,
    get_provider,
    register_provider,
)
from src.ingestion.providers.base import Bar, MarketDataProvider, OptionQuote
from src.ingestion.providers.stub import StubProvider
from src.ingestion.providers.tradestation import (
    TradeStationProvider,
    _normalise_option_quote,
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_default_provider_is_tradestation():
    """An unset environment must keep reading the incumbent feed."""
    assert DEFAULT_PROVIDER == "tradestation"


def test_registry_lists_known_providers():
    names = available_providers()
    assert "tradestation" in names
    assert "stub" in names


def test_unknown_provider_name_raises(monkeypatch):
    """A typo must be fatal, not a silent fallback.

    A cutover that appears to succeed while still reading the old feed is
    the worst possible failure mode here.
    """
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "databentoo")
    with pytest.raises(ValueError) as exc:
        get_provider()
    assert "databentoo" in str(exc.value)
    assert "registered providers" in str(exc.value)


def test_env_selects_provider(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "stub")
    provider = get_provider()
    assert provider.name == "stub"


def test_explicit_name_beats_env(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "tradestation")
    assert get_provider("stub").name == "stub"


def test_register_provider_roundtrip():
    class _Fake(StubProvider):
        name = "fake-vendor"

    register_provider("fake-vendor", lambda **kw: _Fake())
    try:
        assert get_provider("fake-vendor").name == "fake-vendor"
    finally:
        from src.ingestion.providers import _REGISTRY

        _REGISTRY.pop("fake-vendor", None)


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------


def test_capabilities_default_to_false():
    """Forgetting a flag must deny, never silently allow."""
    caps = ProviderCapabilities()
    for field in (
        "option_quotes",
        "option_chain_discovery",
        "option_open_interest",
        "underlying_bars",
        "index_bars",
        "futures_bars",
        "signed_underlying_volume",
    ):
        assert getattr(caps, field) is False


def test_capability_require_raises_with_guidance():
    caps = ProviderCapabilities(option_quotes=True)
    caps.require("option_quotes")  # supported: no raise
    with pytest.raises(ProviderCapabilityError) as exc:
        caps.require("futures_bars")
    assert "futures_bars" in str(exc.value)


def test_stub_denies_every_feed():
    provider = StubProvider()
    for call in (
        lambda: provider.stream_option_quotes(["X"]),
        lambda: provider.stream_underlying_bars("SPY"),
        lambda: provider.stream_index_bars("$VIX.X"),
        lambda: provider.stream_futures_bars("@ES"),
        lambda: provider.get_option_expirations("SPY"),
        lambda: provider.get_option_strikes("SPY"),
        lambda: provider.snapshot_option_quotes(["X"]),
    ):
        with pytest.raises(ProviderCapabilityError):
            call()


def test_stub_with_capability_reaches_not_implemented():
    """Capability granted but unimplemented is a different, clearer error."""
    provider = StubProvider(ProviderCapabilities(option_quotes=True))
    with pytest.raises(NotImplementedError):
        provider.stream_option_quotes(["X"])


def test_tradestation_declares_every_feed():
    """The incumbent is the only evaluated vendor covering all six families."""
    caps = TradeStationProvider._CAPABILITIES
    assert caps.option_quotes
    assert caps.option_chain_discovery
    assert caps.option_open_interest
    assert caps.underlying_bars
    assert caps.index_bars
    assert caps.futures_bars
    # And the one thing no alternative ships: UpVolume/DownVolume on the bar.
    assert caps.signed_underlying_volume


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def test_normalise_maps_every_persisted_field():
    q = _normalise_option_quote(
        "SPY 260918C650",
        {
            "Symbol": "SPY 260918C650",
            "Bid": "1.25",
            "Ask": "1.30",
            "Last": "1.28",
            "BidSize": "10",
            "AskSize": "12",
            "Volume": "4210",
            "OpenInterest": "8123",
            "ImpliedVolatility": "0.1842",
        },
    )
    assert q.option_symbol == "SPY 260918C650"
    assert q.bid == pytest.approx(1.25)
    assert q.ask == pytest.approx(1.30)
    assert q.last == pytest.approx(1.28)
    assert q.bid_size == 10
    assert q.ask_size == 12
    assert q.volume == 4210
    assert q.open_interest == 8123
    assert q.implied_volatility == pytest.approx(0.1842)


def test_absent_fields_normalise_to_none_not_zero():
    """An absent quote and a zero quote are different facts.

    Normalising a missing Bid to 0.0 would present a fabricated two-sided
    quote to the IV solver and the fill model.
    """
    q = _normalise_option_quote("X", {"Symbol": "X"})
    assert q.bid is None
    assert q.ask is None
    assert q.last is None
    assert q.mid is None
    assert q.volume is None
    assert q.open_interest is None
    assert q.implied_volatility is None


def test_open_interest_prefers_daily_but_skips_zero():
    """Mirrors the accumulator's sticky-OI rule.

    Stream deltas routinely carry DailyOpenInterest=0; taking it would
    zero the gamma weighting for that contract.
    """
    q = _normalise_option_quote(
        "X", {"Symbol": "X", "DailyOpenInterest": "0", "OpenInterest": "8123"}
    )
    assert q.open_interest == 8123

    q2 = _normalise_option_quote(
        "X", {"Symbol": "X", "DailyOpenInterest": "999", "OpenInterest": "8123"}
    )
    assert q2.open_interest == 999


def test_iv_falls_through_alternate_field_names():
    for key in ("ImpliedVolatility", "IV", "Volatility", "IVol"):
        q = _normalise_option_quote("X", {"Symbol": "X", key: "0.25"})
        assert q.implied_volatility == pytest.approx(0.25), key


def test_effective_mid_prefers_vendor_mid_then_midpoint():
    assert OptionQuote("X", mid=2.0, bid=1.0, ask=1.5).effective_mid() == 2.0
    assert OptionQuote("X", bid=1.0, ask=1.5).effective_mid() == pytest.approx(1.25)
    # One-sided or empty: no mid can be honestly derived.
    assert OptionQuote("X", bid=1.0).effective_mid() is None
    assert OptionQuote("X").effective_mid() is None
    # A zero vendor mid is treated as absent, matching the > 0 stickiness
    # convention used throughout the ingestion path.
    assert OptionQuote("X", mid=0.0, bid=1.0, ask=1.5).effective_mid() == pytest.approx(1.25)


def test_bar_signed_volume_none_is_distinct_from_zero():
    """None means the feed cannot tell you; 0 means it told you nothing traded."""
    unknown = Bar(symbol="SPY", timestamp=datetime.now(timezone.utc))
    assert unknown.up_volume is None
    quiet = Bar(symbol="SPY", timestamp=datetime.now(timezone.utc), up_volume=0, down_volume=0)
    assert quiet.up_volume == 0


# ---------------------------------------------------------------------------
# Adapter fidelity
# ---------------------------------------------------------------------------


class _FakeAccumulator:
    """Stands in for OptionStreamAccumulator with recorded interactions."""

    def __init__(self, state):
        self._state = state
        self.started_with = None
        self.stopped = False
        self.updates_received = 7

    def start(self, seed_from_rest=True):
        self.started_with = seed_from_rest

    def stop(self):
        self.stopped = True

    def is_alive(self):
        return True

    def snapshot(self):
        return dict(self._state)

    def drain(self):
        return dict(self._state)


def test_option_stream_adapter_passes_through_lifecycle():
    from src.ingestion.providers.tradestation import _OptionQuoteStreamAdapter

    acc = _FakeAccumulator({"X": {"Symbol": "X", "Bid": "1.0", "Ask": "1.2"}})
    stream = _OptionQuoteStreamAdapter(acc)

    stream.start(seed_from_snapshot=False)
    assert acc.started_with is False, "seed flag must reach the accumulator"

    snap = stream.snapshot()
    assert set(snap) == {"X"}
    assert isinstance(snap["X"], OptionQuote)
    assert snap["X"].bid == pytest.approx(1.0)

    assert stream.updates_received == 7
    assert stream.is_alive() is True

    stream.stop()
    assert acc.stopped is True


def test_option_stream_adapter_exposes_raw_accumulator():
    """The harness needs behaviour with no normalised equivalent."""
    from src.ingestion.providers.tradestation import _OptionQuoteStreamAdapter

    acc = _FakeAccumulator({})
    assert _OptionQuoteStreamAdapter(acc).raw is acc


def test_build_option_symbol_argument_order_is_corrected():
    """TradeStationClient takes option_type BEFORE strike, unlike the interface.

    Passing them straight through in interface order produces symbols like
    ``SPY 260918650.0C`` that quote as empty, which reads as a dead feed
    rather than as a bug.
    """
    calls = []

    class _FakeClient:
        def build_option_symbol(self, underlying, expiration, option_type, strike):
            calls.append((underlying, expiration, option_type, strike))
            return f"{underlying} {expiration:%y%m%d}{option_type}{strike:g}"

    provider = TradeStationProvider(_FakeClient())
    symbol = provider.build_option_symbol("SPY", date(2026, 9, 18), 650.0, "C")

    assert calls == [("SPY", date(2026, 9, 18), "C", 650.0)]
    assert symbol == "SPY 260918C650"


def test_snapshot_option_quotes_batches_requests():
    """The quotes endpoint puts symbols in the URL PATH; one call 414s.

    Batching mirrors OptionStreamAccumulator._seed_from_rest, which this
    call is reaching through the interface.
    """
    from src.config import OPTION_BATCH_SIZE

    batches = []

    class _FakeClient:
        def get_option_quotes(self, symbols):
            batches.append(list(symbols))
            return {"Quotes": [{"Symbol": s, "Bid": "1.0"} for s in symbols]}

    provider = TradeStationProvider(_FakeClient())
    symbols = [f"SPY 260918C{i}" for i in range(OPTION_BATCH_SIZE * 2 + 5)]
    out = provider.snapshot_option_quotes(symbols)

    assert len(batches) == 3, "expected three batches for 2x+5 symbols"
    assert all(len(b) <= OPTION_BATCH_SIZE for b in batches)
    assert len(out) == len(symbols)


def test_snapshot_survives_a_failing_batch():
    """One bad batch must not abort the seed; the stream backfills the rest."""
    from src.config import OPTION_BATCH_SIZE

    class _FlakyClient:
        def __init__(self):
            self.calls = 0

        def get_option_quotes(self, symbols):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("upstream 503")
            return {"Quotes": [{"Symbol": s, "Bid": "1.0"} for s in symbols]}

    provider = TradeStationProvider(_FlakyClient())
    symbols = [f"SPY 260918C{i}" for i in range(OPTION_BATCH_SIZE + 3)]
    out = provider.snapshot_option_quotes(symbols)

    assert 0 < len(out) < len(symbols)


def test_provider_is_abstract():
    """MarketDataProvider must not be instantiable without an implementation."""
    with pytest.raises(TypeError):
        MarketDataProvider()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# _BarChartStream — the one genuinely new reader
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for a streaming requests.Response."""

    def __init__(self, lines):
        self._lines = lines
        self.closed = False

    def raise_for_status(self):
        return None

    def iter_lines(self):
        yield from self._lines

    def close(self):
        self.closed = True


def _bar_stream(lines, monkeypatch):
    """Build a _BarChartStream whose single read consumes ``lines``."""
    from src.ingestion.providers import tradestation as ts_mod

    response = _FakeResponse(lines)
    monkeypatch.setattr(ts_mod._requests, "get", lambda *a, **kw: response)

    class _FakeAuth:
        def get_headers(self):
            return {}

    class _FakeClient:
        base_url = "https://example.invalid/v3"
        auth = _FakeAuth()

    stream = ts_mod._BarChartStream(
        _FakeClient(),
        "$VIX.X",
        "VIX",
        interval=5,
        unit="Minute",
        initial_barsback=10,
        poll_barsback=3,
        session_template="Default",
    )
    return stream, response


def test_barchart_stream_parses_sse_framed_lines(monkeypatch):
    """TradeStation frames some stream responses as SSE.

    Without stripping the prefix every line fails to parse and the stream
    looks alive while delivering nothing, which is the hardest kind of
    outage to diagnose.
    """
    stream, _ = _bar_stream(
        [
            b'data: {"TimeStamp":"2026-09-08T19:55:00Z","Open":"17.1",'
            b'"High":"17.4","Low":"17.0","Close":"17.2","TotalVolume":"0"}'
        ],
        monkeypatch,
    )
    stream._running = True
    stream._read_stream()

    bar = stream.drain()
    assert bar is not None, "SSE-framed bar was not parsed"
    assert bar.symbol == "VIX"
    assert bar.close == pytest.approx(17.2)


def test_barchart_stream_skips_heartbeats(monkeypatch):
    stream, _ = _bar_stream([b'{"Heartbeat":1,"Timestamp":"2026-09-08T19:55:00Z"}'], monkeypatch)
    stream._running = True
    stream._read_stream()
    assert stream.drain() is None
    assert stream.updates_received == 0


def test_barchart_stream_returns_on_auth_error(monkeypatch, caplog):
    """An expired token arrives as a payload on a 200, not an HTTP status.

    Reconnecting without naming it produces a silent backoff loop that
    reads like an upstream outage.
    """
    stream, _ = _bar_stream([b'{"Error":"Unauthorized","Message":"token expired"}'], monkeypatch)
    stream._running = True
    with caplog.at_level("WARNING"):
        stream._read_stream()
    assert stream.drain() is None
    assert any("auth error" in r.message.lower() for r in caplog.records)


def test_barchart_stream_drops_unparseable_timestamp(monkeypatch):
    """A misdated bar overwrites the current minute and corrupts the spot
    price the Greeks are computed against. Dropping is strictly safer."""
    stream, _ = _bar_stream(
        [b'{"TimeStamp":"not-a-date","Open":"1","High":"1","Low":"1","Close":"1"}'],
        monkeypatch,
    )
    stream._running = True
    stream._read_stream()
    assert stream.drain() is None


def test_barchart_stream_closes_the_response(monkeypatch):
    """The reader loop clears the slot but does not close the socket."""
    stream, response = _bar_stream([], monkeypatch)
    stream._running = True
    stream._read_stream()
    assert response.closed is True


def test_barchart_drain_is_edge_triggered(monkeypatch):
    """Second drain with no new data returns None, so a watchdog can tell
    'no new bar' from 'same bar again'."""
    stream, _ = _bar_stream(
        [
            b'{"TimeStamp":"2026-09-08T19:55:00Z","Open":"17.1","High":"17.4",'
            b'"Low":"17.0","Close":"17.2"}'
        ],
        monkeypatch,
    )
    stream._running = True
    stream._read_stream()
    assert stream.drain() is not None
    assert stream.drain() is None


def test_index_bar_has_no_signed_volume(monkeypatch):
    """An index VALUE feed carries no volume and no signed split.

    None, not 0: zero would claim the feed reported no trades.
    """
    stream, _ = _bar_stream(
        [
            b'{"TimeStamp":"2026-09-08T19:55:00Z","Open":"17.1","High":"17.4",'
            b'"Low":"17.0","Close":"17.2"}'
        ],
        monkeypatch,
    )
    stream._running = True
    stream._read_stream()
    bar = stream.drain()
    assert bar.up_volume is None
    assert bar.down_volume is None
    assert bar.volume is None
