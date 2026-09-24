"""ThetaData implementation of :class:`MarketDataProvider`.

Written against the v3 gRPC client (``thetadata`` on PyPI, 1.0.10) during
the September 2026 evaluation.  Four things about this vendor shape the
implementation, and none of them are obvious from the interface:

**1. There is no streaming API on this Python client.**  Every method is
a snapshot or a history query; the package exposes no ``stream_*`` at all.
So the stream objects below are polling loops.  That is a better fit than
it sounds: the ingestion engine already samples its accumulators every
five seconds and buckets to one minute, and ThetaData's stated limit is
8 concurrent calls with *no cap on total calls*, so a once-per-interval
chain poll sits well inside it.

Note the qualifier.  The Theta Terminal itself *does* serve a streaming
interface — FPSS, on ``ws_port`` (25520 by default), confirmed listening
on a live terminal 2026-09-14.  The Python package simply does not wrap
it.  Polling is the right call for one-minute buckets, but "ThetaData has
no streaming" would be the wrong conclusion to carry into a later
tick-level design.

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

**Market Value: resolved 2026-09-14.**  ThetaData sells a "Market Value"
feed that adjusts each quote's bid and ask by up to a cent, which they
characterise as a derived product carrying no exchange fees.  Two
mechanisms appeared to compete — a terminal "stage", and dedicated
``option_snapshot_market_value`` / ``index_snapshot_market_value``
endpoints.  ThetaData support confirmed both are real, and which one
applies depends on how you read the data:

* **Snapshots and history (what this module uses): the endpoints.**  Call
  ``option_snapshot_market_value`` instead of ``option_snapshot_quote``.
  One terminal serves both, so no second process is needed.
* **Websocket streaming (FPSS): the stage.**  A single terminal cannot
  subscribe to Market Value and realtime simultaneously, so streaming both
  at once needs a second terminal pointed at stage — ``fpss_region``
  changed to ``fpss_stage_hosts``, with distinct ``port`` and ``ws_port``
  when they share a host.

Hence ``market_value_endpoints`` defaults on, and the ``*_MV_MDDS_PORT``
second-terminal wiring is vestigial for a polling deployment — it is kept
only for a future streaming implementation.  Note that ``config.toml``
documents the stage hosts as *"TESTING ONLY! ... This server is not
stable"*, which is a live concern for that streaming path and not for this
one.

They also confirmed the adjustment never *introduces* a crossed quote and
never takes a price to zero, and that every quote is adjusted.  Nothing in
this module attempts to undo the adjustment: recovering the true quote
would reconstruct licensed exchange data and defeat the only reason the
feed is usable without an exchange licence.  Do not add averaging of
repeated polls of a static quote here, however tempting it is as a noise
reduction — that is the same thing by another name.

**Verified against a live terminal, 2026-09-14.**  This was first written
from the wheel's signatures alone.  A probe run against a running terminal
(``make feed-probe PROVIDER=thetadata UNDERLYING=SPY``, 240/240 contracts,
5.5s) confirmed the shape and corrected three things this module had
wrong:

* **Strikes arrive in dollars, not thousandths.**  The original
  normaliser divided anything >= 1000 by a thousand, which left SPY and
  QQQ correct while turning every SPX strike of 6500 into 6.50 and every
  NDX strike of 25000 into 25.00.  Rescaled strikes still parse, so they
  failed at the join and those chains returned empty — reading as a
  missing entitlement rather than a unit bug.  See
  :func:`_normalise_strike`.
* **Rights are spelled ``"CALL"`` / ``"PUT"``**, not ``"C"`` / ``"P"``.
* **Snapshots carry the last quote whether or not the market is open**,
  with the vendor's own timestamp.  A Sunday probe returned quotes stamped
  the previous Friday at 16:14, so :meth:`_merge_frame` takes that
  timestamp rather than stamping ``now()`` — the same rule the
  TradeStation provider follows, for the same reason.  The same is true of
  ``option_snapshot_ohlc``, which serves the last available DAILY bar;
  ThetaData confirmed (2026-09-14) that filtering on the timestamp is how
  a caller is expected to handle it, and that no session-scoped volume
  field exists.

Also confirmed: the client reaches a local terminal with no
``mdds_host``/``mdds_port`` override, and ``option_list_expirations``
answers from the historical reference database (see
:meth:`ThetaDataProvider.get_option_expirations`).

Response column names remain server-supplied rather than declared in the
package, so :data:`_FIELD_CANDIDATES` still maps logical fields to column
spellings — now pruned to the verified ones.  If the vendor renames a
column the chain does not quietly empty: :func:`report_unmapped` logs the
columns that actually arrived.  :meth:`ThetaDataProvider.describe_columns`
re-runs that check against a live terminal at any time.
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from src.ingestion.providers.base import (
    Bar,
    BarStream,
    MarketDataProvider,
    OptionQuote,
    OptionQuoteStream,
    ProviderCapabilities,
    ProviderCapabilityError,
)
from src.symbols import is_cash_index, resolve_underlying_from_option_root
from src.utils import get_logger

logger = get_logger(__name__)

__all__ = ["ThetaDataProvider"]

#: "Market value" is overloaded: in options vernacular it usually means a
#: mark or theoretical valuation, which is why the ambiguity in the module
#: docstring exists at all. Resolve it with ThetaData before reporting any
#: Market Value comparison as a measurement of the penny adjustment.
_MARKET_VALUE_ENDPOINT_NOTE = __doc__

#: Logical field -> the column spellings to accept, first present one wins.
#:
#: Response columns are server-supplied rather than declared anywhere in the
#: package, so these began as guesses. They are now the spellings a live
#: terminal actually returned (``make feed-probe PROVIDER=thetadata``,
#: 2026-09-14), pruned to what was observed:
#:
#:   option_snapshot_quote          ask ask_condition ask_exchange ask_size
#:                                  bid bid_condition bid_exchange bid_size
#:                                  expiration right strike symbol timestamp
#:   option_snapshot_ohlc           close count expiration high low open
#:                                  right strike symbol timestamp volume
#:   option_snapshot_open_interest  expiration open_interest right strike
#:                                  symbol timestamp
#:   stock/index _snapshot_ohlc     close count high low open symbol
#:                                  timestamp volume
#:
#: Kept as tuples because a vendor may still rename a column, but pruned to
#: the verified spelling: an unverified fallback that fires silently is the
#: same hazard as a wrong guess. If one stops matching, the chain does not
#: quietly empty -- :func:`report_unmapped` names the columns that arrived.
_FIELD_CANDIDATES: Dict[str, Tuple[str, ...]] = {
    # The Market Value endpoints answer with their OWN column names --
    # market_bid / market_ask / market_price, and no sizes. Verified against
    # a live terminal 2026-09-15, where accepting only "bid"/"ask" made
    # every Market Value quote read as absent and the comparison report
    # two-sided=0 while looking otherwise healthy.
    "bid": ("bid", "market_bid"),
    "ask": ("ask", "market_ask"),
    #: The vendor's own mark. Preferred over deriving a midpoint, and the
    #: only price a Market Value row carries besides the two sides.
    "mid": ("market_price",),
    "bid_size": ("bid_size",),
    "ask_size": ("ask_size",),
    # The ohlc endpoint has no "last"; its close IS the last trade.
    "last": ("close",),
    "volume": ("volume",),
    "open_interest": ("open_interest",),
    "strike": ("strike",),
    "right": ("right",),
    "expiration": ("expiration",),
    "symbol": ("symbol",),
    "open": ("open",),
    "high": ("high",),
    "low": ("low",),
    # market_price lets a Market Value stock/index row stand in for a bar:
    # the Market Value feed has no OHLC endpoint, so spot comes from the
    # mark. Without this the MV stage has no spot at all and every sample
    # aborts with "no spot price available".
    "close": ("close", "market_price"),
    "timestamp": ("timestamp",),
}

#: Concurrent chain requests. A live terminal reports "Max concurrent
#: requests: 8" for this account, and the bar-stream pollers draw on that
#: same budget, so the chain fetch deliberately leaves headroom rather than
#: claiming all eight: a bar poll that loses the race backs off
#: exponentially, which is a far worse outcome than one slower chain cycle.
#: Override with THETADATA_MAX_CONCURRENCY; 1 restores sequential fetching.
_DEFAULT_MAX_CONCURRENCY = 6

#: Spellings that select the Market Value stage. Defined once because it is
#: read by from_env, by the provider's own name, and by the endpoint router,
#: and a stage that is Market Value for one and not the others produces a
#: comparison that silently measures nothing.
_MV_STAGES = ("mv", "market_value", "marketvalue")


def is_market_value_stage(stage: Optional[str]) -> bool:
    return (stage or "").strip().lower() in _MV_STAGES


#: Index root used by :meth:`ThetaDataProvider.describe_columns` so the
#: diagnostic covers the index endpoints too. VIX because it is one of the
#: symbols this deployment actually needs and is cheap to ask for.
_DIAGNOSTIC_INDEX = "VIX"

_OCC_RE = re.compile(
    r"^(?P<root>[A-Z0-9.]{1,6})\s*"
    r"(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})"
    r"(?P<cp>[CP])(?P<strike>\d{8})$"
)


#: One ThetaClient per terminal connection, shared by every provider that
#: points at it.
#:
#: Each ThetaClient authenticates on construction and the terminal keeps ONE
#: session: a second client invalidates the first, and every subsequent call
#: fails with "Invalid session ID. This can occur if more than one terminal
#: is running." Two providers on one terminal -- exactly what a realtime vs
#: Market Value comparison is -- therefore cannot each hold their own client.
#: Verified against a live terminal 2026-09-15, where the comparison died on
#: its first sample.
#:
#: Sharing is safe because the Market Value selection for snapshots is
#: per-CALL (the *_market_value endpoints), not per-connection, so one client
#: serves both stages.
_CLIENTS: Dict[Tuple[Any, ...], Any] = {}
_CLIENTS_LOCK = threading.Lock()


def shared_client(factory: Any, key: Tuple[Any, ...]) -> Any:
    """The client for ``key``, constructing it once via ``factory``."""
    with _CLIENTS_LOCK:
        client = _CLIENTS.get(key)
        if client is None:
            client = factory()
            _CLIENTS[key] = client
        return client


def reset_shared_clients() -> None:
    """Drop the cache. For tests, and for a deliberate reconnect."""
    with _CLIENTS_LOCK:
        _CLIENTS.clear()


def option_root_for(symbol: str) -> str:
    """ThetaData option root for a ZeroGEX/TradeStation underlying.

    ``$SPXW.X -> SPXW``, ``SPY -> SPY``. ThetaData keys option endpoints on
    the OCC root, which is what survives stripping TradeStation's ``$`` and
    ``.X`` decoration.
    """
    return (symbol or "").upper().lstrip("$").split(".")[0]


def is_index_symbol(symbol: str) -> bool:
    """Whether ``symbol`` names a cash index rather than a tradable security.

    ZeroGEX carries TradeStation's decoration through as the canonical
    spelling, and it decorates cash indices and only cash indices:
    ``$SPXW.X``, ``$NDXP.X``, ``$VIX.X`` against a bare ``SPY`` or ``QQQ``.
    ``IngestionEngine._infer_asset_type`` already reads the same prefix to
    classify a symbol for the ``symbols`` table, so this is the codebase's
    existing rule rather than a new one.

    It matters here because ThetaData serves indices from a different family
    of endpoints than equities, under a different symbol (see
    ``index_symbol_for``). TradeStation serves both through one barchart
    call, which is why nothing above this layer has ever had to care.
    """
    return (symbol or "").startswith("$")


def index_symbol_for(symbol: str) -> str:
    """ThetaData index symbol for a ZeroGEX/TradeStation underlying.

    This is NOT the option root, and conflating the two is why an SPX probe
    failed with ``No data found for: index_snapshot_ohlc(SPXW)``. ``SPXW``
    is the root of SPX's weekly *option* chain; the *index* whose level
    those options settle against is ``SPX``, and ThetaData has no index
    called SPXW. Same for ``NDXP`` -> ``NDX``.

    Resolution order, most authoritative first:

    1. ``resolve_underlying_from_option_root``, which reads the deployment's
       configured ``OPTION_ROOT_ALIASES`` / ``SYMBOL_ALIASES``.
    2. The root as-is, when it is already a known cash index.
    3. Dropping a single trailing settlement marker (the ``W`` of weeklys,
       the ``P`` of PM-settled) -- but ONLY when that yields a recognised
       cash index. So ``SPXW -> SPX`` and ``NDXP -> NDX``, while ``VIX``,
       ``VXN`` and ``RUT`` are returned untouched rather than mangled into
       ``VI``, ``VX`` and ``RU``.

    Step 3 exists because step 1 needs env configuration that a probe or a
    fresh checkout may not have, and silently querying a symbol that does
    not exist is the failure this function was written to end.
    """
    root = option_root_for(symbol)
    if not root:
        return root

    resolved = resolve_underlying_from_option_root(root)
    if resolved and resolved != root and is_cash_index(resolved):
        return resolved
    if is_cash_index(root):
        return root
    if len(root) > 3 and is_cash_index(root[:-1]):
        return root[:-1]
    return root


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


#: Column shapes already reported, so a mismapped endpoint warns once per
#: shape rather than once per poll.
_REPORTED_SHAPES: set = set()
_REPORTED_LOCK = threading.Lock()


def report_unmapped(kind: str, rows: Sequence[Dict[str, Any]], fields: Sequence[str]) -> None:
    """Warn, once per column shape, when rows arrive but nothing maps.

    This exists because the failure is otherwise invisible. If the server
    spells the strike column something :data:`_FIELD_CANDIDATES` does not
    list, every row is skipped and the chain comes back empty — which
    reads exactly like a closed market or an unentitled symbol. Naming the
    columns that actually arrived turns a silent zero into a one-line fix.
    """
    if not rows:
        return
    shape = (kind, frozenset(rows[0].keys()))
    with _REPORTED_LOCK:
        if shape in _REPORTED_SHAPES:
            return
        _REPORTED_SHAPES.add(shape)
    logger.warning(
        "thetadata %s: %d row(s) returned but no %s column matched. "
        "Columns present: %s. Expected one of: %s. "
        "Add the real spelling to _FIELD_CANDIDATES.",
        kind,
        len(rows),
        " / ".join(fields),
        sorted(rows[0].keys()),
        {f: _FIELD_CANDIDATES.get(f, ()) for f in fields},
    )


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
    """Strike in dollars, as the v3 client reports it.

    This used to divide anything >= 1000 by a thousand, on the assumption
    that OPRA-derived feeds send thousandths. The v3 client does not: a
    live SPY chain returns ``strike: 795.0``, already in dollars
    (verified 2026-09-14).

    That heuristic was not merely redundant, it was destructive on exactly
    the symbols this deployment cares most about. An SPX strike of 6500
    became 6.50 and an NDX strike of 25000 became 25.00, so no index
    contract could ever join back to the requested set and those chains
    came back empty -- indistinguishable from a missing entitlement. SPY
    and QQQ, whose strikes sit below 1000, would have looked perfect
    throughout.

    If a feed ever does send thousandths, :func:`report_unmapped` now
    catches it: nothing joins, and the columns and values that arrived get
    logged instead of a silent empty chain.
    """
    return _as_float(value)


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

    # -- restart across a changing symbol set -------------------------------
    #
    # The polling reader rebuilds prices on its own cadence, so only the
    # STICKY fields need carrying -- open interest above all, because a
    # strike recalibration that blanked it would publish an OI-weighted
    # gamma surface with holes in it until the next open-interest poll,
    # which runs on a deliberately slower cadence than quotes do.

    _STICKY_CARRY_FIELDS = ("open_interest", "volume")

    def sticky_state(self) -> Dict[str, Any]:
        with self._lock:
            out: Dict[str, Any] = {}
            for symbol, st in self._state.items():
                kept = {k: st[k] for k in self._STICKY_CARRY_FIELDS if st.get(k)}
                if kept:
                    out[symbol] = kept
            return out

    def carry_sticky_state(self, carried: Dict[str, Any]) -> int:
        """Adopt sticky fields for symbols this stream tracks.

        Only for symbols in THIS stream's set, and only where we do not
        already hold a positive value -- the carry seeds a gap, it never
        overwrites something the poller has since read for itself.
        """
        if not carried:
            return 0
        wanted = set(self._symbols)
        adopted = 0
        with self._lock:
            for symbol, fields in carried.items():
                if symbol not in wanted or not isinstance(fields, dict):
                    continue
                target = self._state.setdefault(symbol, {})
                took = False
                for key in self._STICKY_CARRY_FIELDS:
                    value = fields.get(key)
                    if value and not target.get(key):
                        target[key] = value
                        took = True
                if took:
                    adopted += 1
        return adopted

    def seed_new_symbols(self, known: Set[str]) -> int:
        """Snapshot only the symbols absent from ``known``.

        One call per (root, expiration) either way, so this is cheaper than
        a full seed only when the new arrivals cluster into fewer
        expirations than the whole set spans -- which is the usual shape of
        a strike-band shift.
        """
        fresh = [s for s in self._symbols if s not in known]
        if not fresh:
            return 0
        rows = self._provider.fetch_chain_state(fresh, include_open_interest=True)
        if not rows:
            return 0
        with self._lock:
            for symbol, incoming in rows.items():
                self._state.setdefault(symbol, {}).update(
                    {k: v for k, v in incoming.items() if v is not None}
                )
                self._dirty.add(symbol)
        return len(rows)

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
        # The vendor's mark when it sent one (Market Value rows carry
        # market_price); otherwise None, and effective_mid() derives it.
        mid=state.get("mid"),
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


class _SessionVolumeDelta:
    """Running session-cumulative volume in, per-bar volume out.

    ``stock_snapshot_ohlc`` serves a running DAILY bar, so its ``volume`` is
    everything traded since the session open. ``Bar.volume`` means the volume
    traded during one bar. Differencing successive observations converts the
    first into the second.

    Anchored on the minute the bar belongs to, the same shape as the
    ``_bar_state`` carry-forward on the TradeStation path: the previous
    minute's last reading is this minute's starting point. Within a minute
    the delta grows with each poll, and the upsert is last-write-wins on
    that minute, so the value that survives is the whole minute's volume.

    Three cases are not a subtraction:

    * **The first observation of the process.** Anchors where it stands and
      reports zero rather than booking session-to-date as one bar -- at a
      midday start that would be a ~390x spike into every z-score reading
      this column, and a plausible-looking one. The cost is that the first
      minute after a restart reports only what arrived after the restart.
      One understated bar, self-healing at the next minute boundary.
    * **A session rollover this object watched happen.** The cumulative
      figure restarts at the open, so everything on the clock belongs to the
      new session's first bar: anchor at zero and report it in full. Told
      apart from the case above by having seen a prior session date, and
      from the case below by the date actually changing.
    * **A step backwards inside one session.** Not a volume, and not
      something to guess a value for. Reported as ``None`` -- unknown, the
      way ``Bar``'s own contract spells it -- and logged, because a vendor
      unwinding a cumulative counter is worth seeing.

    The session date is read in the timestamp's OWN timezone, for the reason
    :func:`is_prior_session` gives: the rows arrive localised to
    America/New_York and the UTC date rolls at 20:00 ET, four hours after
    the session it belongs to has closed.
    """

    def __init__(self, db_symbol: str) -> None:
        self._db_symbol = db_symbol
        self._day: Optional[date] = None
        self._bucket: Optional[datetime] = None
        self._anchor: int = 0
        self._last: Optional[int] = None

    def delta(self, cumulative: Optional[int], timestamp: Any) -> Optional[int]:
        """Per-bar volume, or ``None`` when it cannot be known."""
        if cumulative is None or not isinstance(timestamp, datetime):
            return None

        day = timestamp.date()
        bucket = timestamp.replace(second=0, microsecond=0)

        if self._day is None:
            self._anchor = cumulative
        elif day != self._day:
            self._anchor = 0
        elif bucket != self._bucket:
            self._anchor = self._last if self._last is not None else cumulative

        self._day = day
        self._bucket = bucket
        self._last = cumulative

        volume = cumulative - self._anchor
        if volume < 0:
            logger.warning(
                "%s: session volume stepped backwards (%d below the %d anchor); "
                "reporting this bar's volume as unknown",
                self._db_symbol,
                -volume,
                self._anchor,
            )
            return None
        return volume


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
        # Sleep on an Event rather than time.sleep so stop() wakes the
        # thread immediately. Blocking sleep meant every teardown waited out
        # the remaining poll interval -- 5 seconds of dead time per sample,
        # all of it after the bar had already been delivered.
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._updates_received = 0
        #: Last poll failure, so a caller waiting for a first bar can stop
        #: waiting on an error that will never clear (a wrong endpoint, a
        #: symbol with no entitlement) instead of burning its whole
        #: deadline. Cleared by a successful poll.
        self.last_error: Optional[str] = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"thetadata-bar-{self._db_symbol}"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self._wake.set()
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
                    self.last_error = None
                failures = 0
            except Exception as e:  # noqa: BLE001
                failures += 1
                self.last_error = f"{type(e).__name__}: {e}"
                logger.warning("thetadata bar poll for %s failed: %s", self._db_symbol, e)
            delay = self._poll_interval * min(2**failures, 16)
            elapsed = time.monotonic() - started
            self._wake.wait(max(0.0, min(delay, 60.0) - elapsed))


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


def _log_snapshot_failure(kind: str, root: str, expiration: date, error: Exception) -> None:
    logger.warning("thetadata %s snapshot failed for %s %s: %s", kind, root, expiration, error)


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

    #: Right-code spellings seen across option endpoints. A live terminal
    #: returns the long form ("CALL" / "PUT"), verified 2026-09-14; the
    #: single-letter forms are kept because OCC symbols use them and this
    #: set is also fed values parsed from those.
    _CALL_CODES = {"C", "CALL", "c", "call"}

    def __init__(
        self,
        client: Any,
        *,
        stage: str = "realtime",
        poll_interval: float = 5.0,
        bar_poll_interval: Optional[float] = None,
        oi_poll_interval: float = 900.0,
        strike_range: Optional[int] = None,
        market_value_endpoints: bool = False,
        max_concurrency: int = _DEFAULT_MAX_CONCURRENCY,
    ):
        self._client = client
        self.stage = stage
        # Per-INSTANCE, not the class attribute. Both stages reporting
        # "thetadata" made a realtime vs Market Value comparison
        # unreadable and unpersistable: the shadow tables key on
        # (provider, option_symbol, captured_at), so the second feed's rows
        # collided with the first and were dropped by ON CONFLICT DO
        # NOTHING -- the Market Value side vanished without an error -- and
        # feed_comparisons recorded incumbent and candidate under the same
        # name. Observed 2026-09-15.
        self.name = "thetadata_mv" if is_market_value_stage(stage) else "thetadata"
        self._poll_interval = poll_interval
        self._oi_poll_interval = oi_poll_interval
        self._strike_range = strike_range
        self._max_concurrency = max(1, int(max_concurrency))
        # One pool for the whole provider, not one per call. Every option
        # stream shares this instance, and the concurrency limit belongs to
        # the ACCOUNT, not to a call site: four per-underlying streams each
        # opening their own pool of eight would put 32 requests in flight
        # against a budget of 8. Created lazily so constructing a provider
        # (which tests do constantly) starts no threads.
        self._executor: Optional[ThreadPoolExecutor] = None
        self._executor_lock = threading.Lock()
        # Which mechanism selects the Market Value feed. For the snapshot
        # path this module uses, that is the dedicated *_market_value
        # endpoints on a single terminal; the terminal "stage" selects it
        # for websocket streaming instead. See the module docstring.
        self._market_value_endpoints = market_value_endpoints
        #: Bars poll on their own cadence. One call per underlying, so this
        #: can run far tighter than the chain without threatening the
        #: 8-concurrent budget -- and the candles are built from the
        #: sequence of marks, so the rate IS the wick resolution.
        self._bar_poll_interval = poll_interval if bar_poll_interval is None else bar_poll_interval

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
            if is_market_value_stage(resolved_stage) and os.getenv("THETADATA_MV_MDDS_PORT")
            else "THETADATA_MDDS_PORT"
        )
        host = os.getenv("THETADATA_MDDS_HOST") or None
        port = os.getenv(port_var) or None
        # Keyed by the CONNECTION, so the realtime and Market Value stages
        # share one client when they share a terminal -- which they do, and
        # must, because a second authentication invalidates the first.
        client = shared_client(
            lambda: ThetaClient(
                email=os.getenv("THETADATA_EMAIL") or None,
                password=os.getenv("THETADATA_PASSWORD") or None,
                creds_file=os.getenv("THETADATA_CREDS_FILE") or None,
                mdds_host=host,
                mdds_port=port,
                dataframe_type="pandas",
            ),
            key=(host, port),
        )
        is_mv = is_market_value_stage(resolved_stage)
        return cls(
            client,
            stage=resolved_stage,
            # The snapshot path selects Market Value per call, via the
            # *_market_value endpoints on a single terminal (confirmed with
            # ThetaData 2026-09-14). THETADATA_MV_VIA_ENDPOINTS=0 restores
            # port/stage selection, which belongs to the streaming path.
            market_value_endpoints=(
                is_mv and os.getenv("THETADATA_MV_VIA_ENDPOINTS", "1").strip() != "0"
            ),
            max_concurrency=int(
                os.getenv("THETADATA_MAX_CONCURRENCY", str(_DEFAULT_MAX_CONCURRENCY))
            ),
            poll_interval=float(os.getenv("THETADATA_POLL_SECONDS", "5")),
            # Bars and chains cost wildly different amounts and want
            # different cadences. One underlying's bar is ONE call; its
            # chain is expirations x endpoints. Tying both to one knob
            # means either the candles are coarse or the chain polls
            # overlap. Defaults to THETADATA_POLL_SECONDS so an unset
            # deployment behaves exactly as before.
            bar_poll_interval=float(
                os.getenv("THETADATA_BAR_POLL_SECONDS") or os.getenv("THETADATA_POLL_SECONDS", "5")
            ),
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

        endpoints: List[Tuple[str, Callable[..., Any]]] = [
            ("quote", self._quote_call),
            ("ohlc", self._client.option_snapshot_ohlc),
        ]
        if include_open_interest:
            endpoints.append(("open_interest", self._client.option_snapshot_open_interest))

        units: List[Tuple[str, Callable[..., Any], Dict[str, Any], str, date]] = []
        for root, expiration in groups:
            for kind, fn in endpoints:
                kwargs: Dict[str, Any] = {
                    "symbol": root,
                    "expiration": expiration,
                    "strike": "*",
                    "right": "both",
                }
                if self._strike_range is not None:
                    kwargs["strike_range"] = self._strike_range
                units.append((kind, fn, kwargs, root, expiration))

        out: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for frame, kind, root, expiration in self._gather(units):
            # Merged on THIS thread, not in the workers. The parallelism
            # that pays is the network wait; merging concurrently would buy
            # microseconds and cost every shared-state guarantee in
            # _merge_frame. Endpoints write disjoint keys, so order does not
            # matter.
            self._merge_frame(frame, kind, root, expiration, wanted, out)
        return dict(out)

    def _executor_for(self) -> ThreadPoolExecutor:
        """The shared pool, created on first use."""
        with self._executor_lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._max_concurrency,
                    thread_name_prefix="thetadata-chain",
                )
            return self._executor

    def _gather(
        self,
        units: Sequence[Tuple[str, Callable[..., Any], Dict[str, Any], str, date]],
    ) -> List[Tuple[Any, str, str, date]]:
        """Run the snapshot calls, up to ``max_concurrency`` at a time.

        A cycle is one call per (root, expiration, endpoint) and each spends
        about half a second waiting on the terminal, so running them one
        after another made the real cadence a multiple of the configured
        poll interval: four underlyings x three expirations x two endpoints
        is ~16s against a THETADATA_POLL_SECONDS of 5, and cycles simply
        overlapped.

        A failing endpoint is logged and dropped, never raised: a chain with
        quotes but no open interest is degraded, a chain with nothing is an
        outage, and the caller can tell those apart only if the degraded
        case still returns.
        """
        if not units:
            return []

        results: List[Tuple[Any, str, str, date]] = []

        if self._max_concurrency <= 1 or len(units) == 1:
            # Sequential. Also what tests exercise, so the merge logic is
            # verified without a scheduler in the way.
            for kind, fn, kwargs, root, expiration in units:
                try:
                    results.append((fn(**kwargs), kind, root, expiration))
                except Exception as e:  # noqa: BLE001 - see docstring
                    _log_snapshot_failure(kind, root, expiration, e)
            return results

        pool = self._executor_for()
        futures = {
            pool.submit(fn, **kwargs): (kind, root, expiration)
            for kind, fn, kwargs, root, expiration in units
        }
        for future in as_completed(futures):
            kind, root, expiration = futures[future]
            try:
                results.append((future.result(), kind, root, expiration))
            except Exception as e:  # noqa: BLE001 - see docstring
                _log_snapshot_failure(kind, root, expiration, e)
        return results

    def close(self) -> None:
        """Shut this provider's pool down. Safe to call more than once.

        Deliberately does NOT close the client: it is shared with any other
        provider on the same terminal, and closing it out from under a
        still-running comparison would fail the other side.
        """
        with self._executor_lock:
            executor, self._executor = self._executor, None
        if executor is not None:
            executor.shutdown(wait=True)

    def _endpoint(self, mv_name: str, realtime_name: str) -> Callable[..., Any]:
        """The Market Value endpoint on an MV stage, else the ordinary one.

        Raises rather than falling back. A Market Value deployment that
        quietly served realtime data for one feed would bill exchange fees
        on exactly the thing it switched to Market Value to avoid, and
        nothing downstream would show it -- the numbers would look right.
        ThetaData confirmed (2026-09-14) that all three products (stock,
        options, indices) have a Market Value feed and that the Market Value
        product is exchange-fee exempt, so a missing endpoint here means
        this client wraps it under some other name, not that the feed does
        not exist. Run `make feed-probe` to see what the installed client
        actually exposes.
        """
        if not self._market_value_endpoints:
            return getattr(self._client, realtime_name)
        fn = getattr(self._client, mv_name, None)
        if fn is None:
            raise ProviderCapabilityError(
                f"stage {self.stage!r} is Market Value, but this thetadata "
                f"client has no {mv_name!r}. Serving {realtime_name!r} "
                "instead would silently put a realtime, exchange-fee-bearing "
                "feed inside a Market Value deployment. Check the client "
                "version for the endpoint's real name, or set "
                "THETADATA_MV_VIA_ENDPOINTS=0 to select Market Value by "
                "terminal stage instead."
            )
        return fn

    def _quote_call(self, **kwargs: Any) -> Any:
        """The quote endpoint, honouring whichever Market Value mechanism
        is configured.

        With ``market_value_endpoints=True`` this calls
        ``option_snapshot_market_value`` instead of
        ``option_snapshot_quote``. The two share a signature, so the swap
        is total: nothing downstream needs to know which one answered.
        """
        return self._endpoint("option_snapshot_market_value", "option_snapshot_quote")(**kwargs)

    def _merge_frame(
        self,
        frame: Any,
        kind: str,
        root: str,
        expiration: date,
        wanted: Dict[Tuple[str, date, float, str], str],
        out: Dict[str, Dict[str, Any]],
    ) -> None:
        rows = _rows(frame)
        matched = 0
        for row in rows:
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
            # Counted here rather than above, so the "nothing mapped"
            # warning fires on a unit or key mismatch too -- not just on a
            # column that could not be read at all. A strike parsed into
            # the wrong units parses fine and joins to nothing.
            matched += 1
            target = out[symbol]
            if kind == "quote":
                target["bid"] = _as_float(_pick(row, "bid"))
                target["ask"] = _as_float(_pick(row, "ask"))
                target["bid_size"] = _as_int(_pick(row, "bid_size"))
                target["ask_size"] = _as_int(_pick(row, "ask_size"))
                # Present only on Market Value rows; None elsewhere, where
                # OptionQuote.effective_mid() derives it from bid/ask.
                target["mid"] = _as_float(_pick(row, "mid"))
                # The vendor's quote time, NOT now(). These snapshots carry
                # the last quote whether or not the market is open: a probe
                # run on Sunday the 13th returned quotes stamped Friday the
                # 11th at 16:14. Stamping now() would have recorded a
                # two-day-old quote as current and defeated every staleness
                # check downstream. Matches the TradeStation provider,
                # which takes the vendor timestamp for the same reason.
                target["timestamp"] = _coerce_datetime(_pick(row, "timestamp"))
            elif kind == "ohlc":
                # `last` is the last trade at whatever price it happened,
                # which stays meaningful however old it is.
                target["last"] = _as_float(_pick(row, "last"))
                # `volume` does NOT. This endpoint returns the most recent
                # DAILY bar, so a contract that has not traded today comes
                # back carrying the volume of the last day it did -- a live
                # SPX probe returned Friday's single contract on a Monday.
                # CONFIRMED by ThetaData support 2026-09-14: "the snapshot
                # displays the last available result and for some contracts
                # there isn't enough liquidity for every day", and filtering
                # on the timestamp field is how they expect a caller to
                # handle it. There is no session-scoped volume field to use
                # instead, so this is the supported approach rather than a
                # workaround for one.
                #
                # OptionQuote.volume is cumulative volume for THIS session,
                # and the engine differences successive snapshots to get
                # flow. Passing a prior day's figure through would book that
                # volume as today's trades, classify it Lee-Ready into
                # ask/bid flow, and -- when the stale figure exceeds today's
                # first real print -- trip the engine's vendor-reset branch
                # and count the whole stale total a second time.
                #
                # A contract that has not traded this session has traded
                # zero this session, which is a fact, not a gap: 0, not None.
                if is_prior_session(_pick(row, "timestamp")):
                    target["volume"] = 0
                else:
                    target["volume"] = _as_int(_pick(row, "volume"))
            elif kind == "open_interest":
                target["open_interest"] = _as_int(_pick(row, "open_interest"))
        if matched == 0:
            report_unmapped(kind, rows, ("strike", "right"))

    def describe_columns(self, underlying: str = "SPY") -> Dict[str, Dict[str, Any]]:
        """One call per endpoint, reporting the columns the server actually sends.

        :data:`_FIELD_CANDIDATES` was written from the wheel's signatures
        rather than from a live terminal, and the response columns are
        server-supplied. This is how you replace those guesses with facts:
        run it once against a running terminal, then prune each candidate
        tuple down to the spelling that appears here.

        Diagnostic only. It makes live calls but writes nothing, touches no
        accumulator, and is never on the ingestion path.
        """
        out: Dict[str, Dict[str, Any]] = {}

        # Which Market Value endpoints this client actually wraps. ThetaData
        # says all three products have a Market Value feed and that the
        # product is exchange-fee exempt; whether this Python package
        # exposes an endpoint per family is a separate question, and a
        # Market Value deployment missing one would silently want the
        # realtime endpoint in its place. Settle it by looking.
        out["market_value_endpoints"] = {
            name: ("present" if hasattr(self._client, name) else "ABSENT")
            for name in (
                "option_snapshot_market_value",
                "index_snapshot_market_value",
                "stock_snapshot_market_value",
            )
        }

        expirations = self.get_option_expirations(underlying)
        if not expirations:
            # Keep the endpoint inventory: a chain that cannot be discovered
            # is the moment you most want to know what this client exposes.
            out["error"] = {"detail": f"no expirations returned for {underlying}"}
            return out
        expiration = expirations[0]

        # The endpoints speak ThetaData's vocabulary, not TradeStation's.
        # Passing "$SPXW.X" straight through made every probe line read
        # "No data found", which says nothing about entitlement or columns.
        option_root = option_root_for(underlying)
        index_root = index_symbol_for(underlying)
        equity_root = option_root

        probes = [
            (
                "option_quote",
                self._quote_call,
                {
                    "symbol": option_root,
                    "expiration": expiration,
                    "strike": "*",
                    "right": "both",
                },
            ),
            (
                "option_ohlc",
                self._client.option_snapshot_ohlc,
                {
                    "symbol": option_root,
                    "expiration": expiration,
                    "strike": "*",
                    "right": "both",
                },
            ),
            (
                "option_open_interest",
                self._client.option_snapshot_open_interest,
                {
                    "symbol": option_root,
                    "expiration": expiration,
                    "strike": "*",
                    "right": "both",
                },
            ),
            # Routed through _endpoint so an MV stage reports the columns it
            # will really read. Calling the client directly showed realtime
            # column names for a Market Value provider, which is how the
            # market_bid/market_ask mismatch stayed hidden.
            (
                "stock_quote",
                self._endpoint("stock_snapshot_market_value", "stock_snapshot_quote"),
                {"symbol": equity_root},
            ),
            ("stock_ohlc", self._client.stock_snapshot_ohlc, {"symbol": equity_root}),
            # The index endpoints serve SPX / NDX / VIX / VXN spot, which is
            # a different licence and may well be a different row shape.
            # Probed with the symbol's OWN index when it has one, so an
            # index underlying reports on the feed it will actually use.
            (
                "index_ohlc",
                self._endpoint("index_snapshot_market_value", "index_snapshot_ohlc"),
                {"symbol": index_root if index_root != equity_root else _DIAGNOSTIC_INDEX},
            ),
        ]

        for name, fn, kwargs in probes:
            try:
                rows = _rows(fn(**kwargs))
            except Exception as e:  # noqa: BLE001 - a diagnostic reports
                out[name] = {"error": f"{type(e).__name__}: {e}"}  # failures,
                continue  # it does not raise
            if not rows:
                out[name] = {"rows": 0, "columns": [], "sample": {}}
                continue
            out[name] = {
                "rows": len(rows),
                "columns": sorted(rows[0].keys()),
                "sample": {k: rows[0][k] for k in sorted(rows[0].keys())},
            }
        return out

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
        # An index underlying ($SPXW.X, $NDXP.X) is not a stock. Its level
        # comes from the index endpoints, under the INDEX symbol rather than
        # the option root -- ThetaData has no security called SPXW, so the
        # stock call returns "No data found" and initialize() fails.
        # TradeStation answers both from one barchart call, so StreamManager
        # has only ever called stream_underlying_bars and must keep working
        # unchanged against either feed. Routing here, not at the call site,
        # is what keeps that true.
        index = is_index_symbol(symbol)
        root = index_symbol_for(symbol) if index else option_root_for(symbol)

        # Built unconditionally; the index path returns before using it.
        volume_delta = _SessionVolumeDelta(resolved)

        def fetch() -> Optional[Bar]:
            if index:
                call = self._endpoint("index_snapshot_market_value", "index_snapshot_ohlc")
            else:
                call = self._endpoint("stock_snapshot_market_value", "stock_snapshot_ohlc")
            rows = _rows(call(symbol=root))
            if not rows:
                return None
            bar = _bar_from_row(rows[0], resolved)
            if bar is None:
                return None
            if index:
                # volume stays None: a cash index has no share volume of its
                # own. Same reasoning as stream_index_bars.
                return bar

            # Volume comes from stock_snapshot_ohlc either way. On the
            # Market Value path the primary call above is
            # stock_snapshot_market_value, which answers market_bid /
            # market_ask / market_price and nothing else -- so the volume
            # takes a second call. That call is realtime data rather than
            # Market Value, the same standing as option_snapshot_ohlc and
            # option_snapshot_open_interest, which this provider already
            # makes unconditionally: see F4 in
            # docs/compliance/market-data-feed-comparison-findings-2026-09.md.
            #
            # Worth knowing what it is and is not. The client defaults
            # stock_snapshot_quote to venue="nqb" -- Nasdaq Basic, a
            # fraction of consolidated tape volume, not the whole market.
            # Every view that reads this column is scale-invariant
            # (a volume-weighted mean, a z-score against a rolling mean and
            # standard deviation), so a consistent fraction cancels and the
            # published figures stay correct. An absolute share count
            # displayed as market volume would NOT be correct, and nothing
            # should start doing that on the strength of this column.
            source = rows[0]
            if _pick(source, "volume") is None:
                ohlc = _rows(self._client.stock_snapshot_ohlc(symbol=root))
                source = ohlc[0] if ohlc else {}

            return replace(
                bar,
                volume=volume_delta.delta(
                    _as_int(_pick(source, "volume")),
                    # The vendor's own localised timestamp, not bar.timestamp:
                    # _coerce_datetime has already converted that one to UTC,
                    # where the date rolls at 20:00 ET and would call the
                    # afternoon a new session every evening.
                    _pick(source, "timestamp"),
                ),
            )

        return _PollingBarStream(
            fetch, resolved, poll_interval=self._bar_poll_interval, wakeup=wakeup
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
        resolved = db_symbol or symbol
        # The INDEX, not the option root: SPX's weekly chain is rooted
        # "SPXW" but there is no index by that name. See index_symbol_for.
        root = index_symbol_for(symbol)

        def fetch() -> Optional[Bar]:
            # Honours the Market Value stage like the option chain does.
            # Index values are licensed separately from OPRA (Cboe CGIF for
            # SPX and VIX, Nasdaq GIDS for NDX), so an MV deployment that
            # kept calling the realtime index endpoint would carry exchange
            # fees on half this deployment's underlyings.
            call = self._endpoint("index_snapshot_market_value", "index_snapshot_ohlc")
            rows = _rows(call(symbol=root))
            if not rows:
                return None
            # volume stays None, and _bar_from_row already leaves it there.
            # A cash index has no share volume of its own -- SPX is a
            # calculation over its constituents, not something that trades --
            # so None here is the fact, not a gap. Index VWAP is computed
            # from an ETF proxy's volume instead; see the proxy_volume CTE in
            # unified_signal_engine.
            return _bar_from_row(rows[0], resolved)

        return _PollingBarStream(fetch, resolved, poll_interval=self._bar_poll_interval)

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
        root = option_root_for(underlying)
        frame = self._client.option_list_expirations(symbol=root)
        # ThetaData answers this from its historical reference database:
        # SPY comes back with ~2,100 expirations starting in 2012. Callers
        # slice the front of this list to build a live chain, so handing
        # back history would hand them contracts that expired years ago and
        # quote empty. Confirmed against a live terminal, 2026-09-14.
        today = datetime.now(timezone.utc).date()
        out: List[date] = []
        for row in _rows(frame):
            value = _pick(row, "expiration")
            parsed = _coerce_date(value)
            if parsed is not None and parsed >= today:
                out.append(parsed)
        return sorted(set(out))

    def get_option_strikes(self, underlying: str, expiration: Optional[str] = None) -> List[float]:
        self._CAPABILITIES.require("option_chain_discovery")
        root = option_root_for(underlying)
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

    def snapshot_underlying_bar(self, symbol: str) -> Optional[Bar]:
        """One current bar, no stream and no stream state touched.

        Same endpoint choice as ``stream_underlying_bars`` -- index symbols
        come from the index family under the index symbol -- but
        deliberately NOT the same volume handling. That path converts the
        vendor's cumulative session volume into a per-bar delta by
        remembering the previous reading; calling it here would consume the
        difference and the next streamed bar would report the remainder.
        The only caller reads ``close``, so ``volume`` is left as
        ``_bar_from_row`` sets it.
        """
        self._CAPABILITIES.require("underlying_bars")
        if is_index_symbol(symbol):
            call = self._endpoint("index_snapshot_market_value", "index_snapshot_ohlc")
            root = index_symbol_for(symbol)
        else:
            call = self._endpoint("stock_snapshot_market_value", "stock_snapshot_ohlc")
            root = option_root_for(symbol)
        rows = _rows(call(symbol=root))
        if not rows:
            logger.debug("No bar returned for %s (%s)", symbol, root)
            return None
        return _bar_from_row(rows[0], symbol)

    def snapshot_option_quotes(self, option_symbols: Sequence[str]) -> Dict[str, OptionQuote]:
        self._CAPABILITIES.require("option_open_interest")
        state = self.fetch_chain_state(option_symbols, include_open_interest=True)
        return {s: _to_quote(s, st) for s, st in state.items()}

    def build_option_symbol(
        self, underlying: str, expiration: date, strike: float, option_type: str
    ) -> str:
        root = option_root_for(underlying)
        return build_occ_symbol(root, expiration, strike, option_type)


def _first_not_none(value: Optional[float], fallback: float) -> float:
    """``value`` unless it is absent.

    Written as an explicit None test rather than ``or`` so the intent is
    local. ``_as_float`` already maps a price of exactly 0 to None on
    purpose -- a zero quote means "no quote" -- so today the two spellings
    agree; this one keeps agreeing if that policy ever changes.
    """
    return fallback if value is None else value


def _bar_from_row(row: Dict[str, Any], db_symbol: str) -> Optional[Bar]:
    close = _as_float(_pick(row, "close"))
    if close is None:
        return None
    return Bar(
        symbol=db_symbol,
        # The vendor's time, not now(), for the same reason the option
        # quotes use it: these snapshots serve the last available value
        # whether or not the market is open, and now() would present a
        # stale bar as current. Falls back to now() only when the feed
        # sent no timestamp at all.
        timestamp=_coerce_datetime(_pick(row, "timestamp")) or datetime.now(timezone.utc),
        # A Market Value row is a MARK, not a bar: verified 2026-09-22, the
        # stock endpoint answers market_bid / market_ask / market_price and
        # the index endpoint answers market_price alone. Neither carries
        # open, high or low.
        #
        # Passed through as None those reach underlying_quotes as NULL, and
        # the candles the site draws -- 1-minute rows aggregated to 5-minute
        # OHLC -- lose their bodies and wicks entirely. So present the mark
        # as the degenerate bar it is, open = high = low = close, and let
        # IngestionEngine._upsert_underlying_quote build the real candle out
        # of the sequence: first-seen open, GREATEST high, LEAST low, last
        # close. At a one-second poll that is sixty observations a minute.
        #
        # Only when the row carries none of its own. The realtime
        # stock_snapshot_ohlc endpoint does supply all three -- as a running
        # DAILY bar, which is its own problem and not this function's to
        # solve.
        open=_first_not_none(_as_float(_pick(row, "open")), close),
        high=_first_not_none(_as_float(_pick(row, "high")), close),
        low=_first_not_none(_as_float(_pick(row, "low")), close),
        close=close,
        # NOT the row's own `volume`. ``Bar.volume`` means volume traded
        # during THIS bar -- what TradeStation's TotalVolume carries and
        # what every consumer of underlying_quotes.volume assumes: the VWAP
        # view sums it, the opening-range and spike views take its mean and
        # standard deviation. What stock_snapshot_ohlc reports is a running
        # DAILY bar, so its `volume` is cumulative since the session open.
        # Passing that through would be a unit mismatch across providers and
        # a silent one -- cumulative volume climbs monotonically all day, so
        # a VWAP built on it weights the close ~390x the open and still looks
        # plausible. _PollingBarStream differences it instead, via
        # _SessionVolumeDelta, and sets the per-bar figure here.
        volume=None,
        # None, not 0: this feed cannot report a signed split at all, which
        # is a different statement from "no signed volume this bar".
        up_volume=None,
        down_volume=None,
    )


def is_prior_session(value: Any) -> bool:
    """True when this row's timestamp falls on an earlier calendar day.

    Compared in the feed's OWN timezone rather than UTC. The rows arrive
    localised to America/New_York, and the UTC date rolls over at 20:00 ET
    (19:00 in winter) -- while the session closed at 16:00. So from 20:00 ET
    until midnight, a UTC comparison calls that afternoon's bars a prior
    session and zeroes their volume, every evening. Evaluating in the
    timestamp's own zone sidesteps the question without a tz database.

    ``False`` when the timestamp is missing or naive: this gates whether to
    discard a value, and guessing wrong in that direction loses real data.
    """
    if not isinstance(value, datetime) or value.tzinfo is None:
        return False
    return value.date() < datetime.now(value.tzinfo).date()


def _coerce_datetime(value: Any) -> Optional[datetime]:
    """Timezone-aware UTC datetime from whatever the feed sent.

    The v3 client hands back ``pandas.Timestamp`` values localised to
    America/New_York. ``pandas.Timestamp`` subclasses ``datetime``, so
    this converts them without importing pandas into the provider.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # A naive timestamp from an exchange feed is Eastern, but
            # guessing would silently shift every quote by four hours.
            # Left as-is and marked UTC only when the feed says so.
            return None
        return value.astimezone(timezone.utc)
    return None


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
