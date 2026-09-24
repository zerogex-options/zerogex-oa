"""The REST half of StreamManager must go through the provider, not a client.

The streaming half was moved behind ``MarketDataProvider`` first; the REST
half -- the current price, the chain discovery, the symbol build, the day
rollover, the validation smoke test -- was not, and kept calling
``self.client`` directly. ``main_engine`` builds a TradeStationClient ONLY
for the TradeStation feed, deliberately, so that a new feed cannot depend on
credentials that are going away. Under any other provider ``self.client`` is
``None``.

The result was not a clean failure. ``initialize()`` raised
``'NoneType' object has no attribute 'get_stream_bars'``, the caller treats a
failed initialize as retryable, and with no backoff it retried as fast as the
CPU allowed: an observed run wrote 73 million log lines and a 7 GB file in
seven minutes against a disk shared with the live service.

These tests pin the four things that would let it back in.
"""

from __future__ import annotations

import io
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from src.ingestion.providers.base import Bar
from src.ingestion.stream_manager import StreamManager

STREAM_MANAGER_SRC = Path(__file__).resolve().parents[1] / "src" / "ingestion" / "stream_manager.py"


# ---------------------------------------------------------------------------
# 1. The whole class, scanned
# ---------------------------------------------------------------------------


def test_stream_manager_never_calls_a_vendor_client_directly():
    """No ``self.client.`` anywhere -- the scan that would have caught this.

    A source scan rather than a behavioural test on purpose: the failure
    mode is a call site that is never exercised until the one configuration
    where the client is ``None``, which is the configuration the unit tests
    do not run. Six of these existed; each was invisible until production.
    """
    source = io.open(STREAM_MANAGER_SRC, encoding="utf-8").read()
    assert "self.client." not in source, (
        "StreamManager reaches for a vendor client directly. Every feed-facing "
        "call must go through self.provider, or it is TradeStation-only and "
        "crashes with AttributeError on None under any other feed."
    )


# ---------------------------------------------------------------------------
# 2. The argument order that silently transposes
# ---------------------------------------------------------------------------


class _RecordingClient:
    """Stands in for TradeStationClient, recording the build call verbatim."""

    def __init__(self) -> None:
        self.build_calls: List[tuple] = []

    def build_option_symbol(
        self, underlying: str, expiration: date, option_type: str, strike: float
    ) -> str:
        # TradeStation's own order: option_type BEFORE strike.
        self.build_calls.append((underlying, expiration, option_type, strike))
        return f"{underlying} {expiration:%y%m%d}{option_type}{int(strike)}"


def test_build_option_symbol_is_not_transposed_through_the_provider():
    """strike and option_type swap places between the two interfaces.

    ``TradeStationClient.build_option_symbol`` takes option_type BEFORE
    strike; ``MarketDataProvider.build_option_symbol`` takes strike BEFORE
    option_type. Passing the call through positionally still runs, and
    produces symbols like ``SPY 260221450.0C`` that quote empty forever.
    """
    from src.ingestion.providers.tradestation import TradeStationProvider

    client = _RecordingClient()
    provider = TradeStationProvider(client)  # type: ignore[arg-type]

    provider.build_option_symbol(
        underlying="SPY", expiration=date(2026, 2, 21), strike=450.0, option_type="C"
    )

    assert client.build_calls == [
        ("SPY", date(2026, 2, 21), "C", 450.0)
    ], "the provider must hand the client option_type before strike"


def test_stream_manager_builds_symbols_with_keyword_arguments():
    """The call site itself must name the arguments, not rely on order."""
    source = io.open(STREAM_MANAGER_SRC, encoding="utf-8").read()
    idx = source.index("self.provider.build_option_symbol(")
    call = source[idx : source.index(")", idx)]
    for kw in ("underlying=", "expiration=", "strike=", "option_type="):
        assert kw in call, f"build_option_symbol call site must pass {kw} by keyword"


# ---------------------------------------------------------------------------
# 3. The current price, via the provider
# ---------------------------------------------------------------------------


def _bare_manager(provider: Any, underlying: str = "$SPXW.X", db: str = "SPX") -> StreamManager:
    sm = StreamManager.__new__(StreamManager)
    sm.provider = provider
    sm.underlying = underlying
    sm.db_underlying = db
    return sm


def test_underlying_bar_comes_from_the_provider_snapshot():
    provider = MagicMock()
    provider.snapshot_underlying_bar.return_value = Bar(
        symbol="$SPXW.X",
        timestamp=datetime(2026, 9, 24, 13, 30),
        open=6700.0,
        high=6710.0,
        low=6695.0,
        close=6705.5,
    )
    sm = _bare_manager(provider)

    row = sm._fetch_underlying_bar()

    provider.snapshot_underlying_bar.assert_called_once_with("$SPXW.X")
    assert row is not None
    assert row["close"] == 6705.5
    # Keyed on the DB spelling, not the vendor's, like every other row.
    assert row["symbol"] == "SPX"


def test_underlying_bar_returns_none_when_the_feed_has_nothing():
    provider = MagicMock()
    provider.snapshot_underlying_bar.return_value = None
    assert _bare_manager(provider)._fetch_underlying_bar() is None


def test_underlying_bar_without_a_close_is_not_a_bar():
    """The only caller formats close as a price; None there used to raise."""
    provider = MagicMock()
    provider.snapshot_underlying_bar.return_value = Bar(
        symbol="SPY", timestamp=datetime(2026, 9, 24, 13, 30), close=None
    )
    assert _bare_manager(provider, "SPY", "SPY")._fetch_underlying_bar() is None


def test_get_underlying_price_reads_the_close():
    provider = MagicMock()
    provider.snapshot_underlying_bar.return_value = Bar(
        symbol="SPY", timestamp=datetime(2026, 9, 24, 13, 30), close=764.07
    )
    assert _bare_manager(provider, "SPY", "SPY")._get_underlying_price() == 764.07


# ---------------------------------------------------------------------------
# 4. Validation now reads a dict of OptionQuote, not a vendor envelope
# ---------------------------------------------------------------------------


def _validating_manager(provider: Any, symbols: List[str]) -> StreamManager:
    sm = StreamManager.__new__(StreamManager)
    sm.provider = provider
    sm.tracked_option_symbols = symbols
    return sm


def test_validation_passes_when_the_provider_returns_a_quote():
    provider = MagicMock()
    provider.snapshot_option_quotes.return_value = {"SPY 260221C450": object()}
    sm = _validating_manager(provider, ["SPY 260221C450"])
    assert sm._validate_option_quote_symbol() is True
    provider.snapshot_option_quotes.assert_called_once_with(["SPY 260221C450"])


def test_validation_is_advisory_when_nothing_comes_back(caplog):
    """No error channel through the interface, so this cannot be fatal.

    A quiet pre-market open returns nothing for a perfectly good symbol.
    Failing the engine on that would spin the retry loop every morning.
    """
    provider = MagicMock()
    provider.snapshot_option_quotes.return_value = {}
    sm = _validating_manager(provider, ["SPY 260221C450"])
    with caplog.at_level("ERROR"):
        assert sm._validate_option_quote_symbol() is True
    assert "SPY 260221C450" in caplog.text, "the symbol tried must be greppable"


def test_validation_fails_with_no_symbols_to_try():
    assert _validating_manager(MagicMock(), [])._validate_option_quote_symbol() is False


# ---------------------------------------------------------------------------
# 5. ThetaData: a cash index is not a stock
# ---------------------------------------------------------------------------


class _EndpointRecorder:
    """A thetadata client that records which endpoint family was asked."""

    def __init__(self) -> None:
        self.calls: List[tuple] = []

    def _row(self, symbol: str) -> List[Dict[str, Any]]:
        return [
            {
                "symbol": symbol,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 6705.5,
                "timestamp": "2026-09-24T09:30:00",
            }
        ]

    def index_snapshot_ohlc(self, symbol: str, **kw: Any) -> List[Dict[str, Any]]:
        self.calls.append(("index", symbol))
        return self._row(symbol)

    def stock_snapshot_ohlc(self, symbol: str, **kw: Any) -> List[Dict[str, Any]]:
        self.calls.append(("stock", symbol))
        return self._row(symbol)


def _theta(client: Any):
    from src.ingestion.providers.thetadata import ThetaDataProvider

    return ThetaDataProvider(client)


@pytest.mark.parametrize(
    "symbol,family,root",
    [
        ("$SPXW.X", "index", "SPX"),
        ("$NDXP.X", "index", "NDX"),
        ("SPY", "stock", "SPY"),
        ("QQQ", "stock", "QQQ"),
    ],
)
def test_snapshot_underlying_bar_routes_indices_to_the_index_endpoint(symbol, family, root):
    """``SPXW`` is an option root, not an index -- there is no such security.

    The stock endpoint answers "No data found" for it, which is how SPX and
    NDX would have recorded nothing for a whole session while SPY and QQQ
    looked fine.
    """
    client = _EndpointRecorder()
    _theta(client).snapshot_underlying_bar(symbol)
    assert client.calls == [(family, root)]


def test_snapshot_underlying_bar_is_none_when_the_feed_has_nothing():
    class _Empty(_EndpointRecorder):
        def stock_snapshot_ohlc(self, symbol: str, **kw: Any) -> List[Dict[str, Any]]:
            self.calls.append(("stock", symbol))
            return []

    assert _theta(_Empty()).snapshot_underlying_bar("SPY") is None


@pytest.mark.parametrize(
    "symbol,family,root",
    [("$SPXW.X", "index", "SPX"), ("SPY", "stock", "SPY")],
)
def test_streamed_bars_route_indices_the_same_way(symbol, family, root):
    """The stream had the identical bug; one fix, both paths."""
    client = _EndpointRecorder()
    stream = _theta(client).stream_underlying_bars(symbol, db_symbol=root)
    # Drive one fetch without starting the poll thread.
    bar = stream._fetch()  # type: ignore[attr-defined]
    assert client.calls[0] == (family, root)
    assert bar is not None


def test_index_bars_carry_no_share_volume():
    """A cash index is a calculation, not something that trades."""
    client = _EndpointRecorder()
    bar = _theta(client).snapshot_underlying_bar("$SPXW.X")
    assert bar is not None
    assert bar.volume is None


# ---------------------------------------------------------------------------
# 6. TradeStation still behaves exactly as before
# ---------------------------------------------------------------------------


def test_tradestation_snapshot_converts_the_vendor_bar():
    from src.ingestion.providers.tradestation import TradeStationProvider

    client = MagicMock()
    client.get_stream_bars.return_value = {
        "Bars": [
            {
                "TimeStamp": "2026-09-24T13:30:00Z",
                "Open": "764.07",
                "High": "764.09",
                "Low": "764.00",
                "Close": "764.07",
                "TotalVolume": "112732",
                "UpVolume": "4694",
                "DownVolume": "108038",
            }
        ]
    }
    bar = TradeStationProvider(client).snapshot_underlying_bar("SPY")

    assert bar is not None
    assert bar.close == pytest.approx(764.07)
    assert bar.up_volume == 4694
    assert bar.down_volume == 108038
    # barsback=1 and no closed-market warning: the engine asks for this
    # outside market hours too, where a stale last bar is the right answer.
    kwargs = client.get_stream_bars.call_args.kwargs
    assert kwargs["barsback"] == 1
    assert kwargs["warn_if_closed"] is False


def test_tradestation_snapshot_is_none_on_an_empty_envelope():
    from src.ingestion.providers.tradestation import TradeStationProvider

    client = MagicMock()
    client.get_stream_bars.return_value = {"Bars": []}
    assert TradeStationProvider(client).snapshot_underlying_bar("SPY") is None


def test_tradestation_invalidate_delegates_to_the_client():
    from src.ingestion.providers.tradestation import TradeStationProvider

    client = MagicMock()
    TradeStationProvider(client).invalidate_strikes_cache()
    client.invalidate_strikes_cache.assert_called_once_with()


def test_a_provider_without_a_strike_cache_need_not_implement_invalidate():
    """The base default is a no-op, so ThetaData inherits it silently."""
    _theta(_EndpointRecorder()).invalidate_strikes_cache()


# ---------------------------------------------------------------------------
# 7. A failed initialize must not be retried flat out
# ---------------------------------------------------------------------------


def _engine_for_backoff(monkeypatch, fail: bool = True):
    """A bare IngestionEngine whose StreamManager fails to initialize."""
    from src.ingestion import main_engine as me

    class _FailingManager:
        def __init__(self, **kw: Any) -> None:
            pass

        def initialize(self) -> bool:
            return not fail

    slept: List[float] = []
    monkeypatch.setattr(me, "StreamManager", _FailingManager)
    monkeypatch.setattr(me, "is_engine_run_window", lambda: True)
    monkeypatch.setattr(me.time, "sleep", lambda s: slept.append(s))

    engine = me.IngestionEngine.__new__(me.IngestionEngine)
    engine.client = None
    engine.provider = MagicMock()
    engine.underlying = "$SPXW.X"
    engine.db_symbol = "SPX"
    engine.num_expirations = 3
    engine.strike_count_max = 40
    engine.strike_pct_range = 3.0
    engine.num_monthly_expirations = 0
    engine.monthly_underlying = None
    engine.running = True
    engine._initialize_failures = 0
    engine._active_stream_manager = None
    # run_streaming's finally-block flushes buffers on every exit, including
    # the failed-initialize one. Not what these tests are about.
    engine._flush_all_buffers = lambda: None  # type: ignore[method-assign]
    return engine, slept


def test_failed_initialize_backs_off_instead_of_spinning(monkeypatch):
    """Without this the retry is immediate and unbounded.

    The observed consequence was 73 million log lines and a 7 GB file in
    seven minutes, on a disk shared with the live service.
    """
    engine, slept = _engine_for_backoff(monkeypatch)

    for _ in range(4):
        engine.run_streaming()

    assert len(slept) == 4, "every failed initialize must pause before returning"
    assert slept == sorted(slept), "the delay must not shrink between failures"
    assert slept[0] > 0
    assert slept[-1] > slept[0], "repeated failure must widen the gap"


def test_backoff_is_capped(monkeypatch):
    engine, slept = _engine_for_backoff(monkeypatch)
    for _ in range(40):
        engine.run_streaming()
    assert max(slept) <= 120.0, "an unbounded delay would look like a hang"


def test_a_successful_initialize_clears_the_failure_count(monkeypatch):
    """Otherwise one bad morning penalises every restart for the rest of the day."""
    engine, slept = _engine_for_backoff(monkeypatch, fail=True)
    engine.run_streaming()
    engine.run_streaming()
    assert engine._initialize_failures == 2

    from src.ingestion import main_engine as me

    class _OkManager:
        def __init__(self, **kw: Any) -> None:
            pass

        def initialize(self) -> bool:
            return True

        def stream(self, max_iterations: Any = None):
            return iter(())

    monkeypatch.setattr(me, "StreamManager", _OkManager)
    engine.run_streaming()
    assert engine._initialize_failures == 0


# ---------------------------------------------------------------------------
# 8. The whole of initialize(), with no vendor client at all
# ---------------------------------------------------------------------------


class _ClientlessProvider:
    """Everything initialize() needs, and no ``client`` to fall back on.

    This is the production shape under any feed but TradeStation:
    ``main_engine`` passes ``client=None`` on purpose, so the new feed cannot
    depend on credentials that are being decommissioned.
    """

    def __init__(self) -> None:
        self.seen: List[str] = []

    def snapshot_underlying_bar(self, symbol: str) -> Optional[Bar]:
        self.seen.append("snapshot_underlying_bar")
        return Bar(symbol=symbol, timestamp=datetime(2026, 9, 24, 13, 30), close=6700.0)

    def get_option_expirations(self, underlying: str, strike_price: Any = None) -> List[date]:
        self.seen.append("get_option_expirations")
        return [date(2099, 1, 15), date(2099, 1, 16)]

    def get_option_strikes(self, underlying: str, expiration: Any = None) -> List[float]:
        self.seen.append("get_option_strikes")
        return [6690.0, 6700.0, 6710.0]

    def build_option_symbol(
        self, underlying: str, expiration: date, strike: float, option_type: str
    ) -> str:
        self.seen.append("build_option_symbol")
        return f"{underlying}{expiration:%y%m%d}{option_type}{strike:.0f}"

    def snapshot_option_quotes(self, option_symbols: Any) -> Dict[str, Any]:
        self.seen.append("snapshot_option_quotes")
        return {s: object() for s in option_symbols}

    def invalidate_strikes_cache(self) -> None:
        self.seen.append("invalidate_strikes_cache")


def test_initialize_completes_with_client_none():
    """The exact failure of 2026-09-24, as a test.

    Before the rewire this raised ``'NoneType' object has no attribute
    'get_stream_bars'`` on the first step, returned False, and was retried
    without pause until the disk filled.
    """
    provider = _ClientlessProvider()
    sm = StreamManager(
        client=None,  # type: ignore[arg-type]
        provider=provider,
        underlying="$SPXW.X",
        db_underlying="SPX",
        num_expirations=2,
        strike_count_max=40,
        strike_pct_range=3.0,
    )

    assert sm.client is None, "the premise: no vendor client exists under this feed"
    assert sm.initialize() is True

    assert sm.current_price == 6700.0
    assert len(sm.target_expirations) == 2
    assert sm.tracked_option_symbols, "a chain must have been built"
    # Every REST step went through the provider.
    for step in (
        "snapshot_underlying_bar",
        "get_option_expirations",
        "get_option_strikes",
        "build_option_symbol",
        "snapshot_option_quotes",
    ):
        assert step in provider.seen, f"initialize() never reached {step}"


def test_initialize_builds_calls_and_puts_at_every_strike():
    """Guards the transposition from the other side: the symbols themselves."""
    provider = _ClientlessProvider()
    sm = StreamManager(
        client=None,  # type: ignore[arg-type]
        provider=provider,
        underlying="$SPXW.X",
        db_underlying="SPX",
        num_expirations=1,
        strike_count_max=40,
        strike_pct_range=3.0,
    )
    assert sm.initialize() is True

    # The fake spells symbols <underlying><yymmdd><C|P><strike>, so a
    # transposed call would put the strike where the right belongs.
    for symbol in sm.tracked_option_symbols:
        assert symbol.endswith(("6690", "6700", "6710")), symbol
        assert "C" in symbol or "P" in symbol
    assert any("C" in s for s in sm.tracked_option_symbols)
    assert any("P" in s for s in sm.tracked_option_symbols)
