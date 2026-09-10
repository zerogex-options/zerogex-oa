"""TradeStation implementation of :class:`MarketDataProvider`.

This is an **adapter, not a rewrite**.  Every stream it hands back is one
of the accumulator classes that already run in production
(``OptionStreamAccumulator``, ``UnderlyingBarAccumulator``,
``_BarChartStream`` below), wrapped so its output arrives as normalised
:class:`OptionQuote` / :class:`Bar` records instead of raw TradeStation
dicts.  The reconnect logic, the sticky open-interest merge, the chunking
that keeps stream URLs under the 414 cliff, and the concurrency-header
bookkeeping are all the originals, untouched.

Consequently nothing here changes what production does.  ``StreamManager``
still builds ``OptionStreamAccumulator`` directly and still reads
``"Bid"`` / ``"DailyOpenInterest"`` keys; this module is exercised by the
comparison harness and by tests until the hot path is deliberately moved
over in a later change.

The one genuinely new piece is :class:`_BarChartStream`, which serves
``stream_index_bars`` and ``stream_futures_bars``.  Those two feeds are
today implemented as standalone child-process ingesters
(``volatility_index_ingester``, ``futures_underlying_ingester``) that own
their own ``requests`` loop *and* their own database writes.  Reusing them
here would drag the persistence layer into the provider, so this class
reimplements only the read half against the same endpoint, with the same
reconnect-and-backoff shape, and leaves the writing to the caller.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

import requests as _requests

from src.config import (
    API_REQUEST_TIMEOUT,
    DELAY_BETWEEN_BATCHES,
    OPTION_BATCH_SIZE,
    SESSION_TEMPLATE,
    _getenv_bool,
    _getenv_float,
    _getenv_int,
    _getenv_str,
)
from src.ingestion.providers.base import (
    Bar,
    BarStream,
    MarketDataProvider,
    OptionQuote,
    OptionQuoteStream,
    ProviderCapabilities,
)
from src.ingestion.stream_manager import (
    OptionStreamAccumulator,
    UnderlyingBarAccumulator,
    _stream_reconnect_delay,
)
from src.ingestion.tradestation_client import TradeStationClient
from src.utils import get_logger
from src.validation import safe_datetime, safe_float, safe_int

logger = get_logger(__name__)

# Same field-name candidates the accumulator merges on. Kept local rather
# than imported so a rename in stream_manager surfaces as a test failure
# here instead of silently changing normalisation.
_IV_KEYS = ("ImpliedVolatility", "IV", "Volatility", "IVol")
_OI_KEYS = ("DailyOpenInterest", "OpenInterest")

_STREAM_READ_TIMEOUT = _getenv_int("TS_STREAM_READ_TIMEOUT", 300)
_RECONNECT_HEALTHY_SECONDS = _getenv_float("TS_STREAM_RECONNECT_HEALTHY_SECONDS", 30.0)


def _is_auth_error_payload(payload: Dict[str, Any]) -> bool:
    """Best-effort detection of auth-expiry messages inside a stream payload.

    Mirrors the same helper in ``volatility_index_ingester``: TradeStation
    reports an expired token as a JSON payload on an otherwise-healthy
    200 response, so the HTTP status never reveals it.
    """
    text = " ".join(
        str(payload.get(k, "")) for k in ("Error", "Message", "Description", "Code")
    ).lower()
    return any(t in text for t in ("unauthorized", "401", "token", "forbidden"))


def _normalise_option_quote(symbol: str, raw: Dict[str, Any]) -> OptionQuote:
    """Convert one accumulator state dict into an :class:`OptionQuote`.

    Mirrors the field precedence the ingestion engine already applies:
    open interest prefers ``DailyOpenInterest`` over ``OpenInterest``, and
    IV is taken from the first populated candidate key.  ``safe_float`` /
    ``safe_int`` are reused so a malformed vendor value degrades to
    ``None`` exactly as it does on the live path rather than raising.
    """
    # NOTE: every conversion below passes ``default=None`` rather than
    # relying on safe_float/safe_int's 0.0/0 default. An absent field and a
    # genuine zero are different facts: a missing Bid means "this feed said
    # nothing", and normalising it to 0.0 would present a fabricated
    # two-sided quote to the IV solver and the fill model. The record's
    # Optional fields exist precisely to carry that distinction.
    open_interest = None
    for key in _OI_KEYS:
        if raw.get(key) is not None:
            candidate = safe_int(raw.get(key), None, field_name=key)
            if candidate:
                open_interest = candidate
                break

    implied_volatility = None
    for key in _IV_KEYS:
        if raw.get(key) is not None:
            candidate_iv = safe_float(raw.get(key), None, field_name=key)
            if candidate_iv:
                implied_volatility = candidate_iv
                break

    ts_raw = raw.get("TimeStamp")
    timestamp = safe_datetime(ts_raw, field_name="TimeStamp") if ts_raw else None

    return OptionQuote(
        option_symbol=symbol,
        timestamp=timestamp,
        bid=safe_float(raw.get("Bid"), None, field_name="Bid"),
        ask=safe_float(raw.get("Ask"), None, field_name="Ask"),
        last=safe_float(raw.get("Last"), None, field_name="Last"),
        mid=safe_float(raw.get("Mid"), None, field_name="Mid"),
        bid_size=safe_int(raw.get("BidSize"), None, field_name="BidSize"),
        ask_size=safe_int(raw.get("AskSize"), None, field_name="AskSize"),
        volume=safe_int(raw.get("Volume"), None, field_name="Volume"),
        open_interest=open_interest,
        implied_volatility=implied_volatility,
    )


class _OptionQuoteStreamAdapter(OptionQuoteStream):
    """Normalising wrapper around the production ``OptionStreamAccumulator``."""

    def __init__(self, accumulator: OptionStreamAccumulator):
        self._acc = accumulator

    def start(self, seed_from_snapshot: bool = True) -> None:
        self._acc.start(seed_from_rest=seed_from_snapshot)

    def stop(self) -> None:
        self._acc.stop()

    def is_alive(self) -> bool:
        return bool(self._acc.is_alive())

    def snapshot(self) -> Dict[str, OptionQuote]:
        return {sym: _normalise_option_quote(sym, raw) for sym, raw in self._acc.snapshot().items()}

    def drain(self) -> Dict[str, OptionQuote]:
        return {sym: _normalise_option_quote(sym, raw) for sym, raw in self._acc.drain().items()}

    @property
    def updates_received(self) -> int:
        return int(self._acc.updates_received)

    @property
    def raw(self) -> OptionStreamAccumulator:
        """Escape hatch to the underlying accumulator.

        The comparison harness uses this to reach behaviour that has no
        normalised equivalent (sticky-state carry across a strike
        recalibration, concurrency headers). Application code should not.
        """
        return self._acc


class _UnderlyingBarStreamAdapter(BarStream):
    """Normalising wrapper around the production ``UnderlyingBarAccumulator``."""

    def __init__(self, accumulator: UnderlyingBarAccumulator):
        self._acc = accumulator

    def start(self) -> None:
        self._acc.start()

    def stop(self) -> None:
        self._acc.stop()

    def is_alive(self) -> bool:
        return bool(self._acc.is_alive())

    def drain(self) -> Optional[Bar]:
        raw = self._acc.drain()
        if not raw:
            return None
        # The accumulator already emits DB-shaped lowercase keys and has
        # already resolved the minute bucket and volume carry-forward, so
        # this is a rename, not a re-derivation.
        return Bar(
            symbol=raw["symbol"],
            timestamp=raw["timestamp"],
            open=raw.get("open"),
            high=raw.get("high"),
            low=raw.get("low"),
            close=raw.get("close"),
            volume=raw.get("volume"),
            up_volume=raw.get("up_volume"),
            down_volume=raw.get("down_volume"),
        )

    @property
    def updates_received(self) -> int:
        return int(self._acc.updates_received)


class _BarChartStream(BarStream):
    """Persistent reader for ``marketdata/stream/barcharts/{symbol}``.

    Serves the index-value (VIX / VXN) and futures (@ES / @NQ) feeds.  The
    production child-process ingesters read the same endpoint but also own
    their upserts and retention pruning; this class is the read half only,
    so the provider stays a data source and persistence stays with the
    caller.

    Reconnect behaviour deliberately reuses ``_stream_reconnect_delay``
    from ``stream_manager``: exponential backoff with jitter, reset once a
    connection has stayed healthy.  A flat retry across many readers was
    the original cause of the per-account concurrent-stream exhaustion,
    and a second implementation with its own timing would reintroduce it.
    """

    def __init__(
        self,
        client: TradeStationClient,
        symbol: str,
        db_symbol: str,
        *,
        interval: int,
        unit: str,
        initial_barsback: int,
        poll_barsback: int,
        session_template: str,
        wakeup: Optional[threading.Event] = None,
    ):
        self._client = client
        self._symbol = symbol
        self._db_symbol = db_symbol
        self._interval = interval
        self._unit = unit
        self._initial_barsback = initial_barsback
        self._poll_barsback = poll_barsback
        self._session_template = session_template
        self._wakeup = wakeup

        self._bar: Optional[Bar] = None
        self._dirty = False
        self._seeded = False
        self._running = False
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._current_response: Optional[_requests.Response] = None
        self._response_lock = threading.Lock()
        self._updates_received = 0

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._reader_loop,
            daemon=True,
            name=f"barchart-stream-{self._db_symbol}",
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        with self._response_lock:
            if self._current_response is not None:
                try:
                    self._current_response.close()
                except Exception:
                    pass
                self._current_response = None
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                logger.warning(
                    "barchart stream thread for %s did not exit within 10s",
                    self._db_symbol,
                )

    def is_alive(self) -> bool:
        return bool(self._thread is not None and self._thread.is_alive())

    def drain(self) -> Optional[Bar]:
        with self._lock:
            if not self._dirty:
                return None
            self._dirty = False
            return self._bar

    @property
    def updates_received(self) -> int:
        return self._updates_received

    # -- internal ----------------------------------------------------------

    def _reader_loop(self) -> None:
        consecutive_failures = 0
        while self._running:
            opened_at = time.monotonic()
            try:
                self._read_stream()
            except Exception as e:  # noqa: BLE001 - reader must never die
                logger.warning("barchart stream %s error: %s", self._db_symbol, e)
            finally:
                with self._response_lock:
                    self._current_response = None
            if not self._running:
                break
            lifetime = time.monotonic() - opened_at
            if lifetime >= _RECONNECT_HEALTHY_SECONDS:
                consecutive_failures = 0
            consecutive_failures += 1
            time.sleep(_stream_reconnect_delay(consecutive_failures))

    def _read_stream(self) -> None:
        barsback = self._poll_barsback if self._seeded else self._initial_barsback
        url = f"{self._client.base_url}/marketdata/stream/barcharts/{self._symbol}"
        params = {
            "interval": str(self._interval),
            "unit": self._unit,
            "barsback": str(barsback),
            "sessiontemplate": self._session_template,
        }
        response = _requests.get(
            url,
            headers=self._client.auth.get_headers(),
            params=params,
            stream=True,
            timeout=(API_REQUEST_TIMEOUT, _STREAM_READ_TIMEOUT),
        )
        response.raise_for_status()
        with self._response_lock:
            self._current_response = response
        self._seeded = True

        try:
            for raw_line in response.iter_lines():
                if not self._running:
                    break
                if not raw_line:
                    continue
                line = (
                    raw_line.decode("utf-8", "replace")
                    if isinstance(raw_line, bytes)
                    else str(raw_line)
                )
                # TradeStation frames some stream responses as SSE. The
                # production ingesters strip this prefix; without it every
                # framed line fails to parse and the stream looks alive
                # while delivering nothing.
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except (ValueError, TypeError):
                    continue
                if not isinstance(payload, dict):
                    continue
                if payload.get("Heartbeat") is not None:
                    continue
                if payload.get("Error"):
                    # An expired token arrives as an in-band error payload,
                    # not an HTTP status. Reconnecting without saying so
                    # produces a silent backoff loop that looks like an
                    # upstream outage; name it so the log points at auth.
                    if _is_auth_error_payload(payload):
                        logger.warning(
                            "barchart stream %s: auth error in payload, "
                            "reconnecting for a fresh token: %s",
                            self._db_symbol,
                            str(payload)[:200],
                        )
                        return
                    continue
                self._merge_bar(payload)
        finally:
            # Return the connection rather than leaking it; the reader loop
            # only clears the slot, it does not close the socket.
            try:
                response.close()
            except Exception:
                pass

    def _merge_bar(self, raw: Dict[str, Any]) -> None:
        timestamp = safe_datetime(raw.get("TimeStamp"), field_name="TimeStamp")
        if not timestamp:
            # Same rule as the underlying accumulator: a bar with no
            # reliable timestamp has no correct place in the series, and
            # stamping it with now() would overwrite the current minute
            # with a misdated bar. Dropping is strictly safer.
            return
        bar = Bar(
            symbol=self._db_symbol,
            timestamp=timestamp,
            open=safe_float(raw.get("Open"), None, field_name="Open"),
            high=safe_float(raw.get("High"), None, field_name="High"),
            low=safe_float(raw.get("Low"), None, field_name="Low"),
            close=safe_float(raw.get("Close"), None, field_name="Close"),
            volume=safe_int(raw.get("TotalVolume"), None, field_name="TotalVolume"),
            # None, not 0: an index-value feed carries no volume at all, and
            # a futures feed carries no signed split. Zero would claim the
            # feed reported no trades, which is a different statement.
            up_volume=safe_int(raw.get("UpVolume"), None, field_name="UpVolume"),
            down_volume=safe_int(raw.get("DownVolume"), None, field_name="DownVolume"),
        )
        with self._lock:
            self._bar = bar
            self._dirty = True
            self._updates_received += 1
        if self._wakeup is not None:
            self._wakeup.set()


class TradeStationProvider(MarketDataProvider):
    """The incumbent feed, expressed through the provider interface.

    Constructed either from an existing :class:`TradeStationClient` (the
    normal case, so a process keeps ONE client and its shared stream/token
    bookkeeping) or from credentials via :meth:`from_env`.
    """

    name = "tradestation"

    _CAPABILITIES = ProviderCapabilities(
        option_quotes=True,
        option_chain_discovery=True,
        option_open_interest=True,
        underlying_bars=True,
        index_bars=True,
        futures_bars=True,
        # The one capability TradeStation has that the evaluated
        # alternatives do not: UpVolume/DownVolume arrive on the bar, so
        # the tick-test split needs no separate trade feed.
        signed_underlying_volume=True,
    )

    def __init__(self, client: TradeStationClient):
        self._client = client

    @classmethod
    def from_env(cls, *, futures_credentials: bool = False) -> "TradeStationProvider":
        """Build from environment credentials.

        ``futures_credentials`` selects the CME-entitled username, which
        exists because entitlements attach to a username rather than to
        the API application. See ``docs/runbooks/es_nq_futures_rollout.md``.
        """
        import os

        if futures_credentials:
            from src.config import futures_tradestation_credentials

            client_id, client_secret, refresh_token = futures_tradestation_credentials()
        else:
            client_id = os.getenv("TRADESTATION_CLIENT_ID", "")
            client_secret = os.getenv("TRADESTATION_CLIENT_SECRET", "")
            refresh_token = os.getenv("TRADESTATION_REFRESH_TOKEN", "")
        return cls(
            TradeStationClient(
                client_id,
                client_secret,
                refresh_token,
                sandbox=_getenv_bool("TRADESTATION_USE_SANDBOX", False),
            )
        )

    @property
    def capabilities(self) -> ProviderCapabilities:
        return self._CAPABILITIES

    @property
    def client(self) -> TradeStationClient:
        """The wrapped client, for callers that still speak TradeStation."""
        return self._client

    # -- streams -----------------------------------------------------------

    def stream_option_quotes(
        self,
        option_symbols: Sequence[str],
        *,
        wakeup: Any = None,
        max_symbols_per_connection: Optional[int] = None,
    ) -> OptionQuoteStream:
        self._CAPABILITIES.require("option_quotes")
        return _OptionQuoteStreamAdapter(
            OptionStreamAccumulator(
                self._client,
                list(option_symbols),
                wakeup=wakeup,
                max_symbols_per_connection=max_symbols_per_connection,
            )
        )

    def stream_underlying_bars(
        self,
        symbol: str,
        *,
        db_symbol: Optional[str] = None,
        interval: int = 1,
        unit: str = "Minute",
        wakeup: Any = None,
    ) -> BarStream:
        self._CAPABILITIES.require("underlying_bars")
        if interval != 1 or unit != "Minute":
            # UnderlyingBarAccumulator hard-codes the 1-minute bar stream
            # the ingestion engine buckets on. Anything else must go
            # through _BarChartStream so the caller gets what it asked for
            # instead of silently receiving 1-minute bars.
            return self._barchart_stream(
                symbol,
                db_symbol or symbol,
                interval=interval,
                unit=unit,
                initial_barsback=_getenv_int("TS_BARCHART_INITIAL_BARSBACK", 160),
                poll_barsback=_getenv_int("TS_BARCHART_POLL_BARSBACK", 3),
                wakeup=wakeup,
            )
        return _UnderlyingBarStreamAdapter(
            UnderlyingBarAccumulator(
                self._client,
                symbol,
                db_symbol or symbol,
                session_template=SESSION_TEMPLATE,
                wakeup=wakeup,
            )
        )

    def stream_index_bars(
        self,
        symbol: str,
        *,
        db_symbol: Optional[str] = None,
        interval: int = 5,
        unit: str = "Minute",
        initial_barsback: int = 160,
        poll_barsback: int = 3,
    ) -> BarStream:
        self._CAPABILITIES.require("index_bars")
        return self._barchart_stream(
            symbol,
            db_symbol or symbol,
            interval=interval,
            unit=unit,
            initial_barsback=initial_barsback,
            poll_barsback=poll_barsback,
        )

    def stream_futures_bars(
        self,
        symbol: str,
        *,
        db_symbol: Optional[str] = None,
        interval: int = 1,
        unit: str = "Minute",
        initial_barsback: int = 960,
        poll_barsback: int = 3,
    ) -> BarStream:
        self._CAPABILITIES.require("futures_bars")
        return self._barchart_stream(
            symbol,
            db_symbol or symbol,
            interval=interval,
            unit=unit,
            initial_barsback=initial_barsback,
            poll_barsback=poll_barsback,
            session_template=_getenv_str("FUTURES_SESSION_TEMPLATE", "Default"),
        )

    def _barchart_stream(
        self,
        symbol: str,
        db_symbol: str,
        *,
        interval: int,
        unit: str,
        initial_barsback: int,
        poll_barsback: int,
        session_template: Optional[str] = None,
        wakeup: Optional[threading.Event] = None,
    ) -> BarStream:
        return _BarChartStream(
            self._client,
            symbol,
            db_symbol,
            interval=interval,
            unit=unit,
            initial_barsback=initial_barsback,
            poll_barsback=poll_barsback,
            session_template=session_template or SESSION_TEMPLATE,
            wakeup=wakeup,
        )

    # -- discovery and snapshots -------------------------------------------

    def get_option_expirations(
        self, underlying: str, strike_price: Optional[float] = None
    ) -> List[date]:
        self._CAPABILITIES.require("option_chain_discovery")
        return self._client.get_option_expirations(underlying, strike_price)

    def get_option_strikes(self, underlying: str, expiration: Optional[str] = None) -> List[float]:
        self._CAPABILITIES.require("option_chain_discovery")
        return self._client.get_option_strikes(underlying, expiration)

    def snapshot_option_quotes(self, option_symbols: Sequence[str]) -> Dict[str, OptionQuote]:
        self._CAPABILITIES.require("option_open_interest")
        targets = list(option_symbols)
        out: Dict[str, OptionQuote] = {}
        # The quotes endpoint embeds the symbol list in the URL PATH, so a
        # single call across a full chain exceeds the ~25KB 414 cliff.
        # Batch at OPTION_BATCH_SIZE and pace between batches, exactly as
        # OptionStreamAccumulator._seed_from_rest already does -- this call
        # is that same REST seed, reached through the interface.
        for i in range(0, len(targets), OPTION_BATCH_SIZE):
            batch = targets[i : i + OPTION_BATCH_SIZE]
            try:
                raw = self._client.get_option_quotes(batch)
            except Exception as e:  # noqa: BLE001 - a bad batch must not
                # abort the whole seed; the stream backfills what is missed.
                logger.warning("option quote snapshot batch failed: %s", e)
                continue
            quotes = raw.get("Quotes", []) if isinstance(raw, dict) else []
            for q in quotes:
                if not isinstance(q, dict):
                    continue
                symbol = q.get("Symbol")
                if symbol:
                    out[symbol] = _normalise_option_quote(symbol, q)
            if DELAY_BETWEEN_BATCHES > 0:
                time.sleep(DELAY_BETWEEN_BATCHES)
        return out

    def build_option_symbol(
        self,
        underlying: str,
        expiration: date,
        strike: float,
        option_type: str,
    ) -> str:
        # NOTE the argument order: TradeStationClient.build_option_symbol
        # takes option_type BEFORE strike, the reverse of this interface.
        # Passing them positionally in interface order silently produces
        # symbols like "SPY 260221450.0C" that quote as empty.
        return self._client.build_option_symbol(underlying, expiration, option_type, strike)

    def close(self) -> None:
        self._client.close_all_streams()
