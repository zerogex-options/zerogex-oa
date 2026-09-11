"""ThetaData implementation of :class:`MarketDataProvider`.

Written against the v3 gRPC client (``thetadata`` on PyPI, 1.0.10) during
the September 2026 evaluation.  Four things about this vendor shape the
implementation, and none of them are obvious from the interface:

**1. There is no streaming API on this client.**  Every method is a
snapshot or a history query; the package exposes no ``stream_*`` at all.
So the stream objects below are polling loops.  That is a better fit than
it sounds: the ingestion engine already samples its accumulators every
five seconds and buckets to one minute, and ThetaData's stated limit is
8 concurrent calls with *no cap on total calls*, so a once-per-interval
chain poll sits well inside it.

**2. Snapshots are per-expiration, not per-contract.**
``option_snapshot_quote(symbol, expiration, strike="*", right="both")``
returns the whole chain for one expiration in a single call.  Requesting
4,000 contracts individually would be absurd here; instead the requested
symbols are grouped by (root, expiration), one call is made per group, and
the result is filtered back down.  Three expirations across four
underlyings is roughly a dozen calls per cycle.

**3. Quotes, open interest and last/volume are three separate endpoints.**
``option_snapshot_quote`` carries bid/ask, ``option_snapshot_open_interest``
carries OI, and ``option_snapshot_ohlc`` carries last and volume.  A full
``OptionQuote`` is a join across all three.  OI is fetched on a slower
cadence than quotes because it only settles once a day, which keeps the
call count down without losing anything.

**4. Real-time equities are Nasdaq Basic, not consolidated.**  The client
defaults ``stock_snapshot_quote(venue="nqb")``.  Nasdaq Basic tracks SPY
within pennies, which is all the Black-Scholes spot input needs, but it
sees only a fraction of consolidated volume — so this provider declares
``signed_underlying_volume=False`` and the caller must not treat its
volume as a full-market figure.

**Market Value.**  ThetaData sells a "Market Value" feed that adjusts each
quote's bid and ask by up to a cent, which they characterise as a derived
product carrying no exchange fees.  Per their support (2026-09-11), the
toggle is *terminal-level*: you run a second Theta Terminal configured to
that stage and point a client at its port.  It is therefore selected here
by ``mdds_port``, not by calling a different method — note that the
``*_snapshot_market_value`` endpoints on this client are a separate thing
and are deliberately NOT used for that purpose (see
``_MARKET_VALUE_ENDPOINT_NOTE`` below).

They also confirmed the adjustment never *introduces* a crossed quote and
never takes a price to zero, and that every quote is adjusted.  Nothing in
this module attempts to undo the adjustment: recovering the true quote
would reconstruct licensed exchange data and defeat the only reason the
feed is usable without an exchange licence.  Do not add averaging of
repeated polls of a static quote here, however tempting it is as a noise
reduction — that is the same thing by another name.

**Unverified against the live API.**  This was written from the wheel's
signatures, not from a running terminal.  The response column names in
particular come from the server at runtime rather than from any constant
in the package, so :data:`_FIELD_CANDIDATES` lists plausible spellings and
takes the first that appears.  Confirm them on first contact and prune the
lists.
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.ingestion.providers.base import (
    Bar,
    BarStream,
    MarketDataProvider,
    OptionQuote,
    OptionQuoteStream,
    ProviderCapabilities,
)
from src.utils import get_logger

logger = get_logger(__name__)

__all__ = ["ThetaDataProvider"]

#: Why the ``*_snapshot_market_value`` endpoints are not how Market Value is
#: selected. "Market value" is overloaded: in options vernacular it usually
#: means a mark or theoretical valuation, and ThetaData exposes
#: ``option_snapshot_market_value`` / ``index_snapshot_market_value``
#: alongside the ordinary quote endpoints. Their support described the
#: fee-exempt product as a *terminal stage* instead, which implies it
#: perturbs every endpoint rather than living behind one. Selecting by port
#: is therefore the reading that matches what they said. If that turns out
#: to be wrong, the fix is one line in :meth:`ThetaDataProvider._quote_call`.
_MARKET_VALUE_ENDPOINT_NOTE = __doc__

#: Response column names are server-supplied, so each logical field lists
#: candidate spellings and the first present one wins. Deliberately explicit
#: rather than fuzzy-matching: a silent mismap here would put an ask price
#: in a bid column and nothing downstream would notice.
_FIELD_CANDIDATES: Dict[str, Tuple[str, ...]] = {
    "bid": ("bid", "bid_price", "best_bid"),
    "ask": ("ask", "ask_price", "best_ask"),
    "bid_size": ("bid_size", "bid_sz"),
    "ask_size": ("ask_size", "ask_sz"),
    "last": ("close", "last", "price", "last_price"),
    "volume": ("volume", "vol"),
    "open_interest": ("open_interest", "oi"),
    "strike": ("strike",),
    "right": ("right", "option_type", "cp"),
    "expiration": ("expiration", "expiry"),
    "symbol": ("symbol", "root", "underlying"),
    "open": ("open",),
    "high": ("high",),
    "low": ("low",),
    "close": ("close",),
    "ms_of_day": ("ms_of_day", "ms", "time"),
    "date": ("date",),
}

#: Strikes arrive in thousandths on OPRA-derived feeds. A strike of 650
#: reported as 650000 is the single most likely unit bug in this module,
#: so the conversion is centralised and range-checked rather than inlined.
_STRIKE_THOUSANDTHS_THRESHOLD = 1000.0

_OCC_RE = re.compile(
    r"^(?P<root>[A-Z0-9.]{1,6})\s*"
    r"(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})"
    r"(?P<cp>[CP])(?P<strike>\d{8})$"
)


def parse_occ_symbol(symbol: str) -> Optional[Tuple[str, date, float, str]]:
    """Split an OCC 21-character symbol into its parts.

    Returns ``(root, expiration, strike, right)`` or ``None`` when the
    symbol does not parse. ``None`` rather than a raise because a single
    malformed symbol should drop one contract, not abort a chain poll.
    """
    match = _OCC_RE.match(symbol.strip().replace(" ", ""))
    if not match:
        return None
    try:
        expiration = date(
            2000 + int(match.group("yy")), int(match.group("mm")), int(match.group("dd"))
        )
    except ValueError:
        return None
    return (
        match.group("root"),
        expiration,
        int(match.group("strike")) / 1000.0,
        match.group("cp"),
    )


def build_occ_symbol(root: str, expiration: date, strike: float, right: str) -> str:
    """Inverse of :func:`parse_occ_symbol`, in the canonical 21-char form."""
    cp = "C" if right.upper().startswith("C") else "P"
    return f"{root.upper():<6}" f"{expiration:%y%m%d}{cp}{int(round(float(strike) * 1000)):08d}"


def _rows(frame: Any) -> List[Dict[str, Any]]:
    """Normalise whatever the client returns into a list of dicts.

    The client hands back Polars or Pandas depending on how it was
    constructed, and older paths return a plain ``dict`` of column lists.
    Handling all three keeps this module from caring which.
    """
    if frame is None:
        return []
    if isinstance(frame, list):
        return [r for r in frame if isinstance(r, dict)]
    for attr in ("to_dicts", "to_dict"):
        fn = getattr(frame, attr, None)
        if fn is None:
            continue
        try:
            out = fn(orient="records") if attr == "to_dict" else fn()
        except TypeError:
            try:
                out = fn()
            except Exception:  # noqa: BLE001
                continue
        except Exception:  # noqa: BLE001
            continue
        if isinstance(out, list):
            return [r for r in out if isinstance(r, dict)]
        if isinstance(out, dict):
            keys = list(out)
            if keys and isinstance(out[keys[0]], (list, tuple)):
                n = len(out[keys[0]])
                return [{k: out[k][i] for k in keys} for i in range(n)]
    if isinstance(frame, dict):
        keys = list(frame)
        if keys and isinstance(frame[keys[0]], (list, tuple)):
            n = len(frame[keys[0]])
            return [{k: frame[k][i] for k in keys} for i in range(n)]
    return []


def _pick(row: Dict[str, Any], field: str) -> Any:
    """First present candidate spelling for ``field``, else ``None``."""
    for key in _FIELD_CANDIDATES.get(field, (field,)):
        if key in row and row[key] is not None:
            return row[key]
    return None


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    # A feed reporting a price of exactly 0 usually means "no quote", and
    # persisting it as a real zero would hand the IV solver a free option.
    return out if out != 0 else None


def _as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalise_strike(value: Any) -> Optional[float]:
    """Strike in dollars, whether the feed sent dollars or thousandths."""
    raw = _as_float(value)
    if raw is None:
        return None
    return raw / 1000.0 if raw >= _STRIKE_THOUSANDTHS_THRESHOLD else raw


# ---------------------------------------------------------------------------
# Streams (polling — this client exposes no streaming API)
# ---------------------------------------------------------------------------


class _PollingOptionQuoteStream(OptionQuoteStream):
    """Background poller that keeps a chain's latest state.

    Deliberately mirrors ``OptionStreamAccumulator``'s semantics rather
    than inventing its own, because those semantics are load-bearing and
    were learned the hard way on the incumbent feed:

    * **Open interest and volume are sticky.** They only ever move on a
      positive value. OI settles once a day and volume is cumulative, so a
      transient zero from a slow endpoint must not erase the accumulated
      figure.
    * **Prices always overwrite.** A stale bid is worse than no bid.
    * **Drain is edge-triggered**, so a watchdog can distinguish "no new
      data" from "same data again".
    """

    def __init__(
        self,
        provider: "ThetaDataProvider",
        option_symbols: Sequence[str],
        *,
        poll_interval: float,
        oi_poll_interval: float,
        wakeup: Optional[threading.Event] = None,
    ):
        self._provider = provider
        self._symbols = list(option_symbols)
        self._poll_interval = poll_interval
        self._oi_poll_interval = oi_poll_interval
        self._wakeup = wakeup

        self._state: Dict[str, Dict[str, Any]] = {}
        self._dirty: set = set()
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._updates_received = 0
        self._last_oi_poll = 0.0

    # -- lifecycle ---------------------------------------------------------

    def start(self, seed_from_snapshot: bool = True) -> None:
        if seed_from_snapshot:
            # One synchronous pass so the first caller poll sees a populated
            # chain rather than only whatever ticked in the first interval.
            try:
                self._poll(include_open_interest=True)
            except Exception as e:  # noqa: BLE001
                logger.warning("thetadata seed poll failed: %s", e)
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="thetadata-option-poll"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                logger.warning("thetadata option poll thread did not exit in 10s")

    def is_alive(self) -> bool:
        return bool(self._thread is not None and self._thread.is_alive())

    def snapshot(self) -> Dict[str, OptionQuote]:
        with self._lock:
            return {s: _to_quote(s, st) for s, st in self._state.items()}

    def drain(self) -> Dict[str, OptionQuote]:
        with self._lock:
            out = {s: _to_quote(s, self._state[s]) for s in self._dirty if s in self._state}
            self._dirty = set()
            return out

    @property
    def updates_received(self) -> int:
        return self._updates_received

    # -- internal ----------------------------------------------------------

    def _loop(self) -> None:
        consecutive_failures = 0
        while self._running:
            started = time.monotonic()
            try:
                include_oi = (started - self._last_oi_poll) >= self._oi_poll_interval
                self._poll(include_open_interest=include_oi)
                if include_oi:
                    self._last_oi_poll = started
                consecutive_failures = 0
            except Exception as e:  # noqa: BLE001 - a poller must not die
                consecutive_failures += 1
                logger.warning(
                    "thetadata poll failed (%d consecutive): %s",
                    consecutive_failures,
                    e,
                )
            # Back off on sustained failure so a dead terminal is not hammered,
            # capped so recovery is still prompt once it returns.
            delay = self._poll_interval * min(2**consecutive_failures, 16)
            elapsed = time.monotonic() - started
            time.sleep(max(0.0, min(delay, 60.0) - elapsed))

    def _poll(self, *, include_open_interest: bool) -> None:
        fetched = self._provider.fetch_chain_state(
            self._symbols, include_open_interest=include_open_interest
        )
        if not fetched:
            return
        with self._lock:
            for symbol, incoming in fetched.items():
                prior = self._state.get(symbol, {})
                merged = dict(prior)
                for key in ("bid", "ask", "bid_size", "ask_size", "last", "timestamp"):
                    if incoming.get(key) is not None:
                        merged[key] = incoming[key]
                # Sticky: only ever move on a positive value.
                for key in ("volume", "open_interest"):
                    value = incoming.get(key)
                    if value is not None and value > 0:
                        merged[key] = value
                self._state[symbol] = merged
                self._dirty.add(symbol)
                self._updates_received += 1
        if self._wakeup is not None:
            self._wakeup.set()


def _to_quote(symbol: str, state: Dict[str, Any]) -> OptionQuote:
    return OptionQuote(
        option_symbol=symbol,
        timestamp=state.get("timestamp"),
        bid=state.get("bid"),
        ask=state.get("ask"),
        last=state.get("last"),
        mid=None,  # derived on demand by OptionQuote.effective_mid()
        bid_size=state.get("bid_size"),
        ask_size=state.get("ask_size"),
        volume=state.get("volume"),
        open_interest=state.get("open_interest"),
        # ThetaData sells Greeks and IV as separate endpoints. ZeroGEX
        # computes its own, so they are deliberately not fetched: paying
        # for a surface the engine regenerates is the one clear waste in
        # this vendor's catalogue.
        implied_volatility=None,
    )


class _PollingBarStream(BarStream):
    """Background poller for one underlying or index symbol."""

    def __init__(
        self,
        fetch: Any,
        db_symbol: str,
        *,
        poll_interval: float,
        wakeup: Optional[threading.Event] = None,
    ):
        self._fetch = fetch
        self._db_symbol = db_symbol
        self._poll_interval = poll_interval
        self._wakeup = wakeup

        self._bar: Optional[Bar] = None
        self._dirty = False
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._updates_received = 0

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"thetadata-bar-{self._db_symbol}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=10)

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

    def _loop(self) -> None:
        failures = 0
        while self._running:
            started = time.monotonic()
            try:
                bar = self._fetch()
                if bar is not None:
                    with self._lock:
                        self._bar = bar
                        self._dirty = True
                        self._updates_received += 1
                    if self._wakeup is not None:
                        self._wakeup.set()
                failures = 0
            except Exception as e:  # noqa: BLE001
                failures += 1
                logger.warning("thetadata bar poll for %s failed: %s", self._db_symbol, e)
            delay = self._poll_interval * min(2**failures, 16)
            elapsed = time.monotonic() - started
            time.sleep(max(0.0, min(delay, 60.0) - elapsed))


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


class ThetaDataProvider(MarketDataProvider):
    """ThetaData, via the v3 gRPC client and a local Theta Terminal.

    ``stage`` records which terminal this instance is pointed at. It is
    documentation and a label, not a switch: the actual selection is the
    port, because ThetaData applies the Market Value adjustment at the
    terminal rather than per-endpoint. Register two instances on different
    ports to compare the two feeds, which is exactly what the comparison
    harness wants.
    """

    name = "thetadata"

    _CAPABILITIES = ProviderCapabilities(
        option_quotes=True,
        option_chain_discovery=True,
        option_open_interest=True,
        underlying_bars=True,
        index_bars=True,
        # ThetaData sells no CME product. Declared false so a deployment
        # that forgets to configure a second provider for ES/NQ fails at
        # startup instead of writing an empty futures_quotes table.
        futures_bars=False,
        # Real-time equities are Nasdaq Basic (the client's venue default
        # is "nqb"), which sees a fraction of consolidated volume. The
        # caller must classify buy/sell from trades rather than trusting a
        # signed split that this feed cannot provide.
        signed_underlying_volume=False,
    )

    #: Right-code spellings seen across option endpoints.
    _CALL_CODES = {"C", "CALL", "c", "call"}

    def __init__(
        self,
        client: Any,
        *,
        stage: str = "realtime",
        poll_interval: float = 5.0,
        oi_poll_interval: float = 900.0,
        strike_range: Optional[int] = None,
    ):
        self._client = client
        self.stage = stage
        self._poll_interval = poll_interval
        self._oi_poll_interval = oi_poll_interval
        self._strike_range = strike_range

    @classmethod
    def from_env(cls, *, stage: Optional[str] = None) -> "ThetaDataProvider":
        """Build from ``THETADATA_*`` environment variables.

        ``stage`` picks the port: ``THETADATA_MDDS_PORT`` for the default
        terminal, ``THETADATA_MV_MDDS_PORT`` for the Market Value one.
        """
        try:
            from thetadata import ThetaClient
        except ImportError as e:  # pragma: no cover - depends on deployment
            raise ImportError(
                "the 'thetadata' package is required for this provider "
                "(pip install thetadata; needs Python 3.12+)"
            ) from e

        resolved_stage = (stage or os.getenv("THETADATA_STAGE", "realtime")).lower()
        port_var = (
            "THETADATA_MV_MDDS_PORT"
            if resolved_stage in ("mv", "market_value", "marketvalue")
            else "THETADATA_MDDS_PORT"
        )
        client = ThetaClient(
            email=os.getenv("THETADATA_EMAIL") or None,
            password=os.getenv("THETADATA_PASSWORD") or None,
            creds_file=os.getenv("THETADATA_CREDS_FILE") or None,
            mdds_host=os.getenv("THETADATA_MDDS_HOST") or None,
            mdds_port=os.getenv(port_var) or None,
            dataframe_type="pandas",
        )
        return cls(
            client,
            stage=resolved_stage,
            poll_interval=float(os.getenv("THETADATA_POLL_SECONDS", "5")),
            oi_poll_interval=float(os.getenv("THETADATA_OI_POLL_SECONDS", "900")),
            strike_range=(
                int(os.getenv("THETADATA_STRIKE_RANGE"))
                if os.getenv("THETADATA_STRIKE_RANGE")
                else None
            ),
        )

    @property
    def capabilities(self) -> ProviderCapabilities:
        return self._CAPABILITIES

    @property
    def client(self) -> Any:
        return self._client

    # -- chain fetch -------------------------------------------------------

    def fetch_chain_state(
        self, option_symbols: Sequence[str], *, include_open_interest: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """Current state for ``option_symbols``, joined across endpoints.

        Groups by (root, expiration) and makes ONE call per group per
        endpoint, because ``strike="*"`` returns a whole expiration. Asking
        for 4,000 contracts one at a time would be four thousand calls for
        data that arrives in a dozen.

        Open interest is fetched on its own slower cadence: it settles once
        daily, so polling it at quote frequency spends calls for nothing.
        """
        groups: Dict[Tuple[str, date], List[str]] = defaultdict(list)
        wanted: Dict[Tuple[str, date, float, str], str] = {}
        for symbol in option_symbols:
            parsed = parse_occ_symbol(symbol)
            if parsed is None:
                logger.debug("unparseable option symbol skipped: %r", symbol)
                continue
            root, expiration, strike, right = parsed
            groups[(root, expiration)].append(symbol)
            wanted[(root, expiration, round(strike, 4), right)] = symbol

        out: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for root, expiration in groups:
            calls = [("quote", self._quote_call), ("ohlc", self._client.option_snapshot_ohlc)]
            if include_open_interest:
                calls.append(("open_interest", self._client.option_snapshot_open_interest))
            for kind, fn in calls:
                try:
                    kwargs: Dict[str, Any] = {
                        "symbol": root,
                        "expiration": expiration,
                        "strike": "*",
                        "right": "both",
                    }
                    if self._strike_range is not None:
                        kwargs["strike_range"] = self._strike_range
                    frame = fn(**kwargs)
                except Exception as e:  # noqa: BLE001 - one endpoint failing
                    # must not lose the others; a chain with quotes but no
                    # OI is degraded, a chain with nothing is an outage.
                    logger.warning(
                        "thetadata %s snapshot failed for %s %s: %s",
                        kind,
                        root,
                        expiration,
                        e,
                    )
                    continue
                self._merge_frame(frame, kind, root, expiration, wanted, out)
        return dict(out)

    def _quote_call(self, **kwargs: Any) -> Any:
        """The quote endpoint.

        Market Value is selected by which terminal this client points at,
        so the same endpoint serves both stages. See the module docstring
        on why the ``*_snapshot_market_value`` endpoints are not used here.
        """
        return self._client.option_snapshot_quote(**kwargs)

    def _merge_frame(
        self,
        frame: Any,
        kind: str,
        root: str,
        expiration: date,
        wanted: Dict[Tuple[str, date, float, str], str],
        out: Dict[str, Dict[str, Any]],
    ) -> None:
        for row in _rows(frame):
            strike = _normalise_strike(_pick(row, "strike"))
            right_raw = _pick(row, "right")
            if strike is None or right_raw is None:
                continue
            right = "C" if str(right_raw).strip() in self._CALL_CODES else "P"
            symbol = wanted.get((root, expiration, round(strike, 4), right))
            if symbol is None:
                # A contract outside the requested set. Expected whenever
                # strike="*" returns the whole expiration; not an error.
                continue
            target = out[symbol]
            if kind == "quote":
                target["bid"] = _as_float(_pick(row, "bid"))
                target["ask"] = _as_float(_pick(row, "ask"))
                target["bid_size"] = _as_int(_pick(row, "bid_size"))
                target["ask_size"] = _as_int(_pick(row, "ask_size"))
                target["timestamp"] = datetime.now(timezone.utc)
            elif kind == "ohlc":
                target["last"] = _as_float(_pick(row, "last"))
                target["volume"] = _as_int(_pick(row, "volume"))
            elif kind == "open_interest":
                target["open_interest"] = _as_int(_pick(row, "open_interest"))

    # -- streams -----------------------------------------------------------

    def stream_option_quotes(
        self,
        option_symbols: Sequence[str],
        *,
        wakeup: Any = None,
        max_symbols_per_connection: Optional[int] = None,
    ) -> OptionQuoteStream:
        self._CAPABILITIES.require("option_quotes")
        # max_symbols_per_connection is a TradeStation artefact: its stream
        # endpoint embeds symbols in the URL and 414s past ~25KB. Here the
        # unit of work is an expiration, so the parameter has nothing to
        # constrain and is accepted only for interface compatibility.
        return _PollingOptionQuoteStream(
            self,
            option_symbols,
            poll_interval=self._poll_interval,
            oi_poll_interval=self._oi_poll_interval,
            wakeup=wakeup,
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
        resolved = db_symbol or symbol
        root = symbol.upper().lstrip("$").split(".")[0]

        def fetch() -> Optional[Bar]:
            frame = self._client.stock_snapshot_ohlc(symbol=root)
            rows = _rows(frame)
            return _bar_from_row(rows[0], resolved) if rows else None

        return _PollingBarStream(fetch, resolved, poll_interval=self._poll_interval, wakeup=wakeup)

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
        resolved = db_symbol or symbol
        root = symbol.upper().lstrip("$").split(".")[0]

        def fetch() -> Optional[Bar]:
            frame = self._client.index_snapshot_ohlc(symbol=root)
            rows = _rows(frame)
            return _bar_from_row(rows[0], resolved) if rows else None

        return _PollingBarStream(fetch, resolved, poll_interval=self._poll_interval)

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
        # Always raises: ThetaData sells no CME product. Pair this provider
        # with one that does (Databento GLBX.MDP3) or source ES/NQ from
        # delayed CME, which carries no exchange fee so long as nothing
        # displays it.
        self._CAPABILITIES.require("futures_bars")
        raise AssertionError("unreachable: futures_bars capability is False")

    # -- discovery and snapshots -------------------------------------------

    def get_option_expirations(
        self, underlying: str, strike_price: Optional[float] = None
    ) -> List[date]:
        self._CAPABILITIES.require("option_chain_discovery")
        root = underlying.upper().lstrip("$").split(".")[0]
        frame = self._client.option_list_expirations(symbol=root)
        out: List[date] = []
        for row in _rows(frame):
            value = _pick(row, "expiration")
            parsed = _coerce_date(value)
            if parsed is not None:
                out.append(parsed)
        return sorted(set(out))

    def get_option_strikes(self, underlying: str, expiration: Optional[str] = None) -> List[float]:
        self._CAPABILITIES.require("option_chain_discovery")
        root = underlying.upper().lstrip("$").split(".")[0]
        parsed = _coerce_date(expiration) if expiration else None
        if parsed is None:
            raise ValueError(
                "thetadata requires an expiration for option_list_strikes; " f"got {expiration!r}"
            )
        frame = self._client.option_list_strikes(symbol=root, expiration=parsed)
        out = []
        for row in _rows(frame):
            strike = _normalise_strike(_pick(row, "strike"))
            if strike is not None:
                out.append(strike)
        return sorted(set(out))

    def snapshot_option_quotes(self, option_symbols: Sequence[str]) -> Dict[str, OptionQuote]:
        self._CAPABILITIES.require("option_open_interest")
        state = self.fetch_chain_state(option_symbols, include_open_interest=True)
        return {s: _to_quote(s, st) for s, st in state.items()}

    def build_option_symbol(
        self, underlying: str, expiration: date, strike: float, option_type: str
    ) -> str:
        root = underlying.upper().lstrip("$").split(".")[0]
        return build_occ_symbol(root, expiration, strike, option_type)


def _bar_from_row(row: Dict[str, Any], db_symbol: str) -> Optional[Bar]:
    close = _as_float(_pick(row, "close"))
    if close is None:
        return None
    return Bar(
        symbol=db_symbol,
        timestamp=datetime.now(timezone.utc),
        open=_as_float(_pick(row, "open")),
        high=_as_float(_pick(row, "high")),
        low=_as_float(_pick(row, "low")),
        close=close,
        volume=_as_int(_pick(row, "volume")),
        # None, not 0: this feed cannot report a signed split at all, which
        # is a different statement from "no signed volume this bar".
        up_volume=None,
        down_volume=None,
    )


def _coerce_date(value: Any) -> Optional[date]:
    """Accept a date, datetime, ISO string, or YYYYMMDD int."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None
