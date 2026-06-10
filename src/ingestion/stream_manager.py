"""
Stream Manager - Streams real-time data and yields to IngestionEngine

Uses TradeStation Stream Bars API for underlying quotes (provides
UpVolume/DownVolume tracking) and a persistent streaming connection
for option chain quotes.

Option quotes are accumulated in a background thread that continuously
reads from TradeStation's streaming quotes endpoint.  The main polling
loop periodically snapshots the accumulated state.  A single REST
snapshot at startup (and on strike recalibration) seeds fields like
open interest and IV that stream deltas may omit.

This manager ONLY fetches data from TradeStation API.
Storage is handled by IngestionEngine.
"""

import json
import os
import threading
import time
from datetime import datetime, date
from typing import Generator, List, Dict, Any, Optional, Set
import pytz
import requests as _requests

from src.ingestion.tradestation_client import TradeStationClient
from src.utils import get_logger
from src.validation import (
    safe_float,
    safe_int,
    safe_datetime,
    validate_bar_data,
    get_market_session,
    underlying_feed_expected,
)
from src.symbols import resolve_option_root
from src.config import (
    OPTION_BATCH_SIZE,
    DELAY_BETWEEN_BATCHES,
    MARKET_HOURS_POLL_INTERVAL,
    EXTENDED_HOURS_POLL_INTERVAL,
    CLOSED_HOURS_POLL_INTERVAL,
    STRIKE_RECALC_INTERVAL,
    STRIKE_CLEANUP_INTERVAL,
    SESSION_TEMPLATE,
    API_REQUEST_TIMEOUT,
    STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION,
    UNDERLYING_STREAM_STALE_WARN_SECONDS,
    UNDERLYING_STREAM_STALE_RESTART_SECONDS,
    UNDERLYING_STREAM_STALE_WARN_SECONDS_EXTENDED,
    UNDERLYING_STREAM_STALE_RESTART_SECONDS_EXTENDED,
    UNDERLYING_STREAM_RESTART_COOLDOWN_SECONDS,
    UNDERLYING_STREAM_MAX_RESTART_ATTEMPTS,
    UNDERLYING_STREAM_BACKOFF_RETRY_INTERVAL_SECONDS,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")

# Stream read timeout — how long the background reader waits for the next
# event before the socket times out (triggers a reconnect).
_STREAM_READ_TIMEOUT = int(os.getenv("TS_STREAM_READ_TIMEOUT", "300"))

# JSON decode error budgeting.  Streams sometimes emit partial/garbled
# lines, and swallowing them silently masks real outages (expired session,
# proxy truncation, etc).  We log a WARNING every N failures and raise
# once the per-minute rate exceeds the threshold so the outer reader loop
# tears down the connection and reconnects with a fresh token.
_DECODE_WARN_EVERY = int(os.getenv("TS_STREAM_DECODE_WARN_EVERY", "100"))
_DECODE_MAX_PER_MINUTE = int(os.getenv("TS_STREAM_DECODE_MAX_PER_MINUTE", "50"))

# A stream that connects (200 OK) and then ends within this many seconds
# almost certainly hit the TradeStation per-account concurrent-stream cap
# (~10): the gateway accepts the connection but the upstream throttler
# closes it shortly after. There is no 414 / 429 to grep for — the only
# fingerprint is the short lifetime. Reader loops emit a dedicated
# cap-exhaustion WARNING when a connection ends inside this window so
# the symptom is named instead of looking like a generic disconnect.
_STREAM_CAP_SUSPECT_SECONDS = int(os.getenv("TS_STREAM_CAP_SUSPECT_SECONDS", "30"))

# When a stream connects (200 OK) and ends without delivering ANY data,
# TradeStation's upstream gateway is the likely culprit rather than the
# per-account cap (which closes connections that were producing data).
# Reader loops only sleep on raised exceptions, so a clean iter_lines()
# return immediately retries — back off this many seconds in the
# finally block to keep an upstream blip from hot-looping at 10 Hz.
_STREAM_DEGRADED_UPSTREAM_BACKOFF_SECONDS = int(
    os.getenv("TS_STREAM_DEGRADED_UPSTREAM_BACKOFF_SECONDS", "2")
)

# Possible field names for implied volatility across TradeStation payload variants
_IV_FIELD_NAMES = ("ImpliedVolatility", "IV", "Volatility", "IVol")


class _DecodeErrorTracker:
    """Counts stream JSON-decode failures and triggers a reconnect when sustained.

    Scoped to a single stream connection: the counters reset each time
    ``_read_stream`` opens a new response.
    """

    def __init__(self, name: str):
        self._name = name
        self._total = 0
        self._window_start = time.monotonic()
        self._in_window = 0

    def record(self, snippet: str) -> None:
        self._total += 1
        self._in_window += 1
        now = time.monotonic()
        elapsed = now - self._window_start
        if elapsed >= 60.0:
            # Reset the rolling window.
            self._window_start = now
            self._in_window = 1
            elapsed = 0.0
        if self._total % _DECODE_WARN_EVERY == 0:
            logger.warning(
                "%s: %d JSON decode failures so far; last snippet: %s",
                self._name,
                self._total,
                (snippet or "")[:200],
            )
        if self._in_window >= _DECODE_MAX_PER_MINUTE:
            raise RuntimeError(
                f"{self._name}: JSON decode error rate exceeded "
                f"({self._in_window} in {elapsed:.0f}s) — forcing reconnect"
            )


def _is_auth_error_payload(payload: Dict[str, Any]) -> bool:
    """Best-effort detection of auth-expiry messages inside stream payloads."""
    fields = (
        str(payload.get("Error", "")),
        str(payload.get("Message", "")),
        str(payload.get("Description", "")),
        str(payload.get("Code", "")),
    )
    text = " ".join(fields).lower()
    return any(token in text for token in ("unauthorized", "401", "token", "forbidden"))


def _stale_thresholds_for_session(session: str) -> tuple[int, int]:
    """Return ``(warn_seconds, restart_seconds)`` for a market session.

    The regular cash session has dense ~60s 1-minute bar cadence; in
    pre/after-hours an equity/ETF trades thinly and bars are legitimately
    minutes apart (cash indices don't print extended hours at all and are
    excluded upstream by ``underlying_feed_expected``). Extended hours
    therefore use much wider thresholds so normal sparse cadence is not
    misread as a dead feed and force-restarted.
    """
    if session in ("pre-market", "after-hours"):
        return (
            UNDERLYING_STREAM_STALE_WARN_SECONDS_EXTENDED,
            UNDERLYING_STREAM_STALE_RESTART_SECONDS_EXTENDED,
        )
    return (
        UNDERLYING_STREAM_STALE_WARN_SECONDS,
        UNDERLYING_STREAM_STALE_RESTART_SECONDS,
    )


def _bar_timestamp_advanced(bar_ts: Any, last_fresh_ts: Optional[datetime]) -> bool:
    """True when *bar_ts* is a genuinely newer bar than *last_fresh_ts*.

    A reconnect opens the bar stream with ``barsback=1``, so every forced
    restart immediately replays one historical bar carrying the same (or
    older) timestamp. Counting that replay as liveness resets the staleness
    clock and restart escalation, so a starved feed loops
    "restart -> replay -> reset -> starve -> restart" forever and never
    reaches the backed-off upstream-outage state. Only a strictly advancing
    timestamp counts as fresh.

    Fails safe to ``True`` (non-datetime payload, or a naive/aware
    comparison mismatch) so a genuine stall is still detected rather than
    silently suppressed by a wedged comparison.
    """
    if not isinstance(bar_ts, datetime):
        return True
    if last_fresh_ts is None:
        return True
    try:
        return bar_ts > last_fresh_ts
    except TypeError:
        return True


# ---------------------------------------------------------------------------
# OptionStreamAccumulator — background thread for persistent quote streaming
# ---------------------------------------------------------------------------


class OptionStreamAccumulator:
    """
    Persistent background reader for TradeStation streaming option quotes.

    Opens a streaming HTTP connection in a daemon thread and continuously
    merges quote updates into per-contract state.  The main thread can
    call :meth:`snapshot` at any cadence to read the latest accumulated
    values for every contract.

    Key behaviours:
    * Seeded from a REST snapshot on :meth:`start` so that OI, IV, and
      prices are fully populated before the first poll iteration.
    * OI is only overwritten when a new **positive** value arrives
      (OI updates once daily at settlement; stream deltas often send 0).
    * IV is only overwritten when a new **positive** value arrives.
    * All other fields (price, volume, timestamp) overwrite on every
      update so the snapshot always reflects the latest tick.
    """

    def __init__(
        self,
        client: TradeStationClient,
        symbols: List[str],
        wakeup: Optional[threading.Event] = None,
        max_symbols_per_connection: Optional[int] = None,
    ):
        self._client = client
        self._symbols = list(symbols)
        self._state: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._running = False
        # The streaming quotes endpoint embeds the symbol list in the URL
        # path; a single ~1000+ symbol URL exceeds ~25KB and returns 414.
        # Split into chunks so each request stays well under typical HTTP
        # server URL limits (~8KB) and run one daemon reader per chunk.
        chunk_size = max_symbols_per_connection or STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION
        if chunk_size <= 0:
            chunk_size = len(self._symbols) or 1
        self._chunk_size = chunk_size
        self._chunks: List[List[str]] = [
            self._symbols[i : i + chunk_size] for i in range(0, len(self._symbols), chunk_size)
        ] or [[]]
        self._threads: List[threading.Thread] = []
        self._current_responses: List[Optional[Any]] = []
        self._response_lock = threading.Lock()
        self._updates_received: int = 0
        self._connected = threading.Event()
        # Symbols that have received at least one update since last drain().
        self._dirty: Set[str] = set()
        # Shared event the main loop blocks on; set whenever new data arrives.
        self._wakeup = wakeup

    # -- lifecycle ---------------------------------------------------------

    def start(self, seed_from_rest: bool = True):
        """Seed state from REST, then begin background stream reading."""
        if seed_from_rest:
            self._seed_from_rest()
        self._running = True
        # One reader thread per chunk, each with its own current-response slot.
        self._current_responses = [None] * len(self._chunks)
        self._threads = []
        if len(self._chunks) > 1:
            logger.info(
                "Starting %d option stream connections (%d symbols, %d per connection)",
                len(self._chunks),
                len(self._symbols),
                self._chunk_size,
            )
        for idx, chunk in enumerate(self._chunks):
            t = threading.Thread(
                target=self._reader_loop,
                args=(idx, chunk),
                daemon=True,
                name=f"option-stream-{idx}" if len(self._chunks) > 1 else "option-stream",
            )
            t.start()
            self._threads.append(t)
        # Give at least one stream a moment to connect before returning.
        self._connected.wait(timeout=10)

    def stop(self):
        """Stop the background readers and close all stream connections."""
        self._running = False
        # Interrupt any blocking iter_lines() calls.
        with self._response_lock:
            for idx, resp in enumerate(self._current_responses):
                if resp is not None:
                    try:
                        resp.close()
                    except Exception:
                        pass
                self._current_responses[idx] = None
        for t in self._threads:
            t.join(timeout=10)
            if t.is_alive():
                logger.warning(
                    "Option stream reader thread %s did not exit within 10s after stop(); "
                    "abandoning reference (thread will continue consuming memory until it "
                    "unblocks)",
                    t.name,
                )
        self._threads = []

    @property
    def is_alive(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    @property
    def updates_received(self) -> int:
        return self._updates_received

    # -- public API --------------------------------------------------------

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Return a copy of the current accumulated state keyed by symbol."""
        with self._lock:
            return {k: dict(v) for k, v in self._state.items()}

    def drain(self) -> Dict[str, Dict[str, Any]]:
        """Return state for contracts updated since last drain and clear the dirty set.

        Unlike :meth:`snapshot`, this only returns contracts that received at
        least one stream update (or were REST-seeded) since the previous drain.
        This eliminates the need for external change-detection.
        """
        with self._lock:
            if not self._dirty:
                return {}
            result = {sym: dict(self._state[sym]) for sym in self._dirty if sym in self._state}
            self._dirty.clear()
            return result

    # -- internal ----------------------------------------------------------

    def _seed_from_rest(self):
        """Fetch one full REST snapshot to populate OI, IV, and prices."""
        logger.info(f"Seeding option state from REST ({len(self._symbols)} symbols)...")
        seeded = 0
        for i in range(0, len(self._symbols), OPTION_BATCH_SIZE):
            batch = self._symbols[i : i + OPTION_BATCH_SIZE]
            try:
                data = self._client.get_option_quotes(batch)
                for q in data.get("Quotes", []):
                    self._merge_single_quote(q)
                    seeded += 1
            except Exception as e:
                logger.warning(f"REST seed batch failed: {e}")
            if DELAY_BETWEEN_BATCHES > 0:
                time.sleep(DELAY_BETWEEN_BATCHES)
        logger.info(f"REST seed complete: {seeded} quotes loaded")

    def _reader_loop(self, chunk_idx: int, chunk_symbols: List[str]):
        """Continuously read stream events for one chunk; auto-reconnect on failure."""
        label = (
            f"Option stream chunk {chunk_idx + 1}/{len(self._chunks)}"
            if len(self._chunks) > 1
            else "Option stream"
        )
        while self._running:
            try:
                self._read_stream(chunk_idx, chunk_symbols, label)
            except Exception as e:
                if self._running:
                    logger.warning(f"{label} disconnected ({e}), reconnecting in 2s...")
                    time.sleep(2)

    def _read_stream(self, chunk_idx: int, chunk_symbols: List[str], label: str):
        """Open one stream connection for *chunk_symbols* and read events until it ends."""
        symbols_str = ",".join(chunk_symbols)
        url = f"{self._client.base_url}/marketdata/stream/quotes/{symbols_str}"
        headers = self._client.auth.get_headers()

        response = _requests.get(
            url,
            headers=headers,
            stream=True,
            timeout=(API_REQUEST_TIMEOUT, _STREAM_READ_TIMEOUT),
        )

        try:
            if response.status_code == 401:
                response.close()
                self._client.auth.force_refresh_access_token()
                return  # will retry on next loop iteration

            if response.status_code == 414:
                # URL too large despite chunking — operator pushed the per-chunk
                # cap above what TradeStation's gateway accepts. Log loudly so
                # the cause is obvious instead of being buried as a generic
                # disconnect, then raise to trigger the standard reconnect path.
                logger.error(
                    "%s received 414 Request-URI Too Large for %d symbols. "
                    "Reduce STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION (currently %d).",
                    label,
                    len(chunk_symbols),
                    self._chunk_size,
                )

            response.raise_for_status()
        except Exception:
            response.close()
            raise

        # Track how long the stream stays open. TradeStation enforces a
        # per-account concurrent-stream cap (~10) by silently closing
        # accepted-then-terminated connections — no 414, no 429, just an
        # iter_lines() that ends seconds after it began. A short lifetime
        # is the strongest fingerprint we have of cap exhaustion; warn
        # loudly when we see it so it isn't lost in the generic
        # "disconnected (Response ended prematurely)" noise.
        connection_open_mono = time.monotonic()

        with self._response_lock:
            self._current_responses[chunk_idx] = response

        self._connected.set()

        decode_tracker = _DecodeErrorTracker(label)
        # Quote-payload counter for the cap-vs-degraded-upstream
        # classifier in ``finally``. A connection that flowed data and
        # was cut points at the per-account cap; one that accepted 200
        # OK and immediately closed without yielding any quotes points
        # at an upstream gateway hiccup (the 06:33 burst in the
        # 2026-06-09 journal was ten 0.0s-elapsed closes capped by an
        # explicit 502 — pure-200/no-data is the early fingerprint).
        payloads_received = 0

        try:
            for raw_line in response.iter_lines(decode_unicode=True):
                if not self._running:
                    break
                if not raw_line:
                    continue
                line = (
                    raw_line.strip()
                    if isinstance(raw_line, str)
                    else raw_line.decode("utf-8", errors="ignore").strip()
                )
                if not line or line in ("[DONE]", "heartbeat"):
                    continue
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line:
                    continue

                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    decode_tracker.record(line)
                    continue

                if isinstance(payload, dict) and _is_auth_error_payload(payload):
                    logger.warning(
                        "%s reported auth error payload; refreshing token and reconnecting",
                        label,
                    )
                    self._client.auth.force_refresh_access_token()
                    break

                # Handle both {"Quotes": [...]} wrappers and bare objects.
                if isinstance(payload, dict) and "Quotes" in payload:
                    for q in payload["Quotes"]:
                        if isinstance(q, dict):
                            self._merge_single_quote(q)
                            payloads_received += 1
                elif isinstance(payload, dict) and "Symbol" in payload:
                    self._merge_single_quote(payload)
                    payloads_received += 1
        finally:
            with self._response_lock:
                self._current_responses[chunk_idx] = None
            response.close()
            elapsed = time.monotonic() - connection_open_mono
            # Only warn while we're still meant to be running: a quick exit
            # caused by stop() / shutdown is not cap exhaustion.
            if self._running and elapsed < _STREAM_CAP_SUSPECT_SECONDS:
                if payloads_received == 0:
                    # 200 OK with an empty stream — upstream degradation,
                    # not the per-account cap. The reader-loop only sleeps
                    # on raised exceptions, so a normal-return + immediate
                    # retry hot-loops the upstream (10 attempts in ~1s in
                    # the prod burst); back off here to space the retries.
                    logger.warning(
                        "%s ended after %.1fs without yielding any quotes "
                        "(under %ds). Likely cause: TradeStation upstream "
                        "gateway degradation (accepted-then-closed with no "
                        "data), NOT the per-account concurrent-stream cap. "
                        "Backing off %ds before reconnect.",
                        label,
                        elapsed,
                        _STREAM_CAP_SUSPECT_SECONDS,
                        _STREAM_DEGRADED_UPSTREAM_BACKOFF_SECONDS,
                    )
                    time.sleep(_STREAM_DEGRADED_UPSTREAM_BACKOFF_SECONDS)
                else:
                    logger.warning(
                        "%s ended after only %.1fs (under %ds) after %d "
                        "quote payloads. Likely cause: TradeStation "
                        "per-account concurrent-stream cap (~10) exhausted. "
                        "Total streams in this process: 1 underlying + %d "
                        "option chunks. With N ingestion processes the "
                        "account total is N × (1 + chunks). Reduce chunk "
                        "count by raising STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION.",
                        label,
                        elapsed,
                        _STREAM_CAP_SUSPECT_SECONDS,
                        payloads_received,
                        len(self._chunks),
                    )

    def _merge_single_quote(self, q: dict):
        """Merge one raw quote into accumulated state.

        Price/volume fields always overwrite.  OI and IV only overwrite
        when the incoming value is positive — these fields update
        infrequently and stream deltas often send 0 or omit them.
        """
        symbol = q.get("Symbol", "")
        if not symbol:
            return

        with self._lock:
            prior = self._state.get(symbol, {})
            merged = dict(prior)
            merged["Symbol"] = symbol

            # Always-overwrite fields
            for key in (
                "Last",
                "Bid",
                "Ask",
                "Mid",
                "TimeStamp",
                "High",
                "Low",
                "Open",
                "Close",
                "NetChange",
                "NetChangePct",
                "BidSize",
                "AskSize",
            ):
                val = q.get(key)
                if val is not None:
                    merged[key] = val

            # Volume: only overwrite when > 0 — streaming deltas frequently
            # send Volume=0 between trades, which would erase the accumulated
            # cumulative daily volume (same pattern as OI/IV below).
            vol_val = q.get("Volume")
            if vol_val is not None:
                try:
                    if int(vol_val) > 0:
                        merged["Volume"] = vol_val
                except (ValueError, TypeError):
                    pass

            # OI: only overwrite when new value > 0
            for oi_key in ("DailyOpenInterest", "OpenInterest"):
                val = q.get(oi_key)
                if val is not None:
                    try:
                        if int(val) > 0:
                            merged[oi_key] = val
                    except (ValueError, TypeError):
                        pass

            # IV: only overwrite when new value > 0
            for iv_key in _IV_FIELD_NAMES:
                val = q.get(iv_key)
                if val is not None:
                    try:
                        if float(val) > 0:
                            merged[iv_key] = val
                    except (ValueError, TypeError):
                        pass

            self._state[symbol] = merged
            self._dirty.add(symbol)
            self._updates_received += 1

        # Signal the main loop outside the lock to avoid holding it
        # while the main thread wakes.
        if self._wakeup is not None:
            self._wakeup.set()


# ---------------------------------------------------------------------------
# UnderlyingBarAccumulator — persistent stream for underlying OHLCV bars
# ---------------------------------------------------------------------------


class UnderlyingBarAccumulator:
    """
    Persistent background reader for TradeStation streaming bar data.

    Mirrors :class:`OptionStreamAccumulator` but for 1-minute OHLCV bars
    of a single underlying symbol.  Partial bar payloads are merged
    with carry-forward semantics for volume fields that may be omitted.
    """

    def __init__(
        self,
        client: TradeStationClient,
        symbol: str,
        db_symbol: str,
        session_template: str = "Default",
        wakeup: Optional[threading.Event] = None,
    ):
        self._client = client
        self._symbol = symbol
        self._db_symbol = db_symbol
        self._session_template = session_template
        self._wakeup = wakeup

        self._bar: Optional[Dict[str, Any]] = None
        self._dirty = False
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._current_response = None
        self._response_lock = threading.Lock()
        self._connected = threading.Event()
        self._updates_received: int = 0
        # Carry-forward state for partial bar payloads, keyed by minute bucket.
        self._bar_state: Dict[datetime, Dict[str, Any]] = {}

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        """Begin background bar stream reading."""
        self._running = True
        self._thread = threading.Thread(
            target=self._reader_loop,
            daemon=True,
            name="underlying-stream",
        )
        self._thread.start()
        self._connected.wait(timeout=10)

    def stop(self):
        """Stop the background reader and close the stream connection."""
        self._running = False
        with self._response_lock:
            if self._current_response is not None:
                try:
                    self._current_response.close()
                except Exception:
                    pass
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                logger.warning(
                    "Underlying bar stream reader thread did not exit within 10s after stop(); "
                    "abandoning reference (thread will continue consuming memory until it unblocks)"
                )
            self._thread = None

    @property
    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def updates_received(self) -> int:
        return self._updates_received

    # -- public API --------------------------------------------------------

    def drain(self) -> Optional[Dict[str, Any]]:
        """Return latest bar if updated since last drain, else None."""
        with self._lock:
            if not self._dirty:
                return None
            self._dirty = False
            return dict(self._bar) if self._bar else None

    # -- internal ----------------------------------------------------------

    def _reader_loop(self):
        """Continuously read bar stream events; auto-reconnect on failure."""
        while self._running:
            try:
                self._read_stream()
            except Exception as e:
                if self._running:
                    logger.warning(
                        f"Underlying bar stream disconnected ({e}), reconnecting in 2s..."
                    )
                    time.sleep(2)

    def _read_stream(self):
        """Open one stream connection and read bar events until it ends."""
        url = f"{self._client.base_url}/marketdata/stream/barcharts/{self._symbol}"
        headers = self._client.auth.get_headers()
        params = {
            "interval": "1",
            "unit": "Minute",
            "barsback": "1",
            "sessiontemplate": self._session_template,
        }

        response = _requests.get(
            url,
            headers=headers,
            params=params,
            stream=True,
            timeout=(API_REQUEST_TIMEOUT, _STREAM_READ_TIMEOUT),
        )

        try:
            if response.status_code == 401:
                response.close()
                logger.warning(
                    "Underlying bar stream: 401 auth failure, " "forcing token refresh and retrying"
                )
                self._client.auth.force_refresh_access_token()
                return

            response.raise_for_status()
        except Exception:
            response.close()
            raise

        logger.debug("Underlying bar stream: connected (HTTP %s)", response.status_code)

        # See _STREAM_CAP_SUSPECT_SECONDS — a connection that ends well
        # inside its read timeout is almost certainly being closed by
        # TradeStation's per-account concurrent-stream cap rather than a
        # network hiccup. Track open time so the finally block can emit a
        # dedicated cap-exhaustion warning that names the symptom.
        connection_open_mono = time.monotonic()

        with self._response_lock:
            self._current_response = response

        self._connected.set()

        _heartbeat_count = 0
        _line_count = 0
        decode_tracker = _DecodeErrorTracker("Underlying bar stream")

        try:
            for raw_line in response.iter_lines(decode_unicode=True):
                if not self._running:
                    break
                if not raw_line:
                    continue
                line = (
                    raw_line.strip()
                    if isinstance(raw_line, str)
                    else raw_line.decode("utf-8", errors="ignore").strip()
                )
                if not line:
                    continue
                if line in ("[DONE]", "heartbeat"):
                    _heartbeat_count += 1
                    if _heartbeat_count % 50 == 0:
                        logger.debug(
                            "Underlying bar stream: %d heartbeats, " "%d data lines so far",
                            _heartbeat_count,
                            _line_count,
                        )
                    continue
                _line_count += 1
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line:
                    continue

                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    decode_tracker.record(line)
                    continue

                if isinstance(payload, dict) and _is_auth_error_payload(payload):
                    logger.warning(
                        "Underlying bar stream reported auth error payload; "
                        "refreshing token and reconnecting"
                    )
                    self._client.auth.force_refresh_access_token()
                    break

                # Handle various bar payload shapes.
                bars: list = []
                if isinstance(payload, dict) and "Bars" in payload:
                    bars = payload["Bars"]
                elif (
                    isinstance(payload, dict)
                    and "Bar" in payload
                    and isinstance(payload["Bar"], dict)
                ):
                    bars = [payload["Bar"]]
                elif isinstance(payload, dict) and "TimeStamp" in payload:
                    bars = [payload]

                if not bars:
                    logger.debug(
                        "Underlying bar stream: received payload with " "no bar data: keys=%s",
                        (
                            list(payload.keys())
                            if isinstance(payload, dict)
                            else type(payload).__name__
                        ),
                    )

                for bar in bars:
                    self._merge_bar(bar)
        finally:
            with self._response_lock:
                self._current_response = None
            response.close()
            elapsed = time.monotonic() - connection_open_mono
            # Only warn while we're still meant to be running: a quick exit
            # caused by stop() / shutdown is not cap exhaustion.
            if self._running and elapsed < _STREAM_CAP_SUSPECT_SECONDS:
                if _line_count == 0 and _heartbeat_count == 0:
                    # See _read_stream's mirror branch: 200 OK with no
                    # traffic (not even a heartbeat) is the upstream-
                    # gateway-degraded fingerprint, not the per-account
                    # cap. Back off so the reader loop doesn't spin.
                    logger.warning(
                        "Underlying bar stream for %s ended after %.1fs "
                        "without yielding any traffic (under %ds). Likely "
                        "cause: TradeStation upstream gateway degradation "
                        "(accepted-then-closed with no data), NOT the "
                        "per-account concurrent-stream cap. Backing off "
                        "%ds before reconnect.",
                        self._symbol,
                        elapsed,
                        _STREAM_CAP_SUSPECT_SECONDS,
                        _STREAM_DEGRADED_UPSTREAM_BACKOFF_SECONDS,
                    )
                    time.sleep(_STREAM_DEGRADED_UPSTREAM_BACKOFF_SECONDS)
                else:
                    logger.warning(
                        "Underlying bar stream for %s ended after only %.1fs "
                        "(under %ds) after %d data lines and %d heartbeats. "
                        "Likely cause: TradeStation per-account concurrent-"
                        "stream cap (~10) exhausted by option chunks. The "
                        "heatmap query is anchored on MAX(underlying_quotes."
                        "timestamp), so an evicted underlying feed freezes "
                        "the chart even while options are still flowing. "
                        "Reduce option chunk count by raising "
                        "STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION.",
                        self._symbol,
                        elapsed,
                        _STREAM_CAP_SUSPECT_SECONDS,
                        _line_count,
                        _heartbeat_count,
                    )

    def _merge_bar(self, bar: dict):
        """Merge one raw bar into accumulated state with carry-forward."""
        if not validate_bar_data(bar):
            return

        timestamp_str = bar.get("TimeStamp", "")
        timestamp = safe_datetime(timestamp_str, field_name="TimeStamp")
        if not timestamp:
            # A bar with an unparseable timestamp has no reliable place in
            # the time series. Stamping it with wall-clock now() buckets it
            # into the *current* minute and the unconditional OHLC upsert
            # then overwrites that minute's real bar with a misdated one --
            # corrupting the underlying price Greeks are computed against.
            # Dropping the bar is strictly safer than fabricating a time.
            logger.warning("Dropping underlying bar with unparseable TimeStamp=%r", timestamp_str)
            return

        minute_bucket = timestamp.replace(second=0, microsecond=0)
        prior = self._bar_state.get(minute_bucket, {})

        raw_up = bar.get("UpVolume")
        raw_down = bar.get("DownVolume")
        raw_total = bar.get("TotalVolume")

        up_volume = safe_int(raw_up, field_name="UpVolume")
        down_volume = safe_int(raw_down, field_name="DownVolume")
        total_volume = safe_int(raw_total, field_name="TotalVolume")

        # Carry forward omitted volume fields from the same minute bucket.
        if raw_up in (None, "", "N/A"):
            up_volume = prior.get("up_volume", up_volume)
        if raw_down in (None, "", "N/A"):
            down_volume = prior.get("down_volume", down_volume)
        if raw_total in (None, "", "N/A"):
            total_volume = prior.get("volume", total_volume)

        bar_data = {
            "symbol": self._db_symbol,
            "timestamp": timestamp,
            "open": safe_float(bar.get("Open"), field_name="Open"),
            "high": safe_float(bar.get("High"), field_name="High"),
            "low": safe_float(bar.get("Low"), field_name="Low"),
            "close": safe_float(bar.get("Close"), field_name="Close"),
            "up_volume": up_volume,
            "down_volume": down_volume,
            "volume": total_volume,
        }

        # Update carry-forward state for this minute.
        self._bar_state[minute_bucket] = {
            "up_volume": up_volume,
            "down_volume": down_volume,
            "volume": total_volume,
        }
        # Evict stale minute buckets.
        stale = [k for k in self._bar_state if k < minute_bucket]
        for k in stale:
            del self._bar_state[k]

        with self._lock:
            self._bar = bar_data
            self._dirty = True
            self._updates_received += 1

        if self._wakeup is not None:
            self._wakeup.set()


class StreamManager:
    """Manages streaming of real-time underlying and options data"""

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        db_underlying: str = None,  # type: ignore[assignment]
        num_expirations: int = 3,
        strike_count_max: int = 40,
        strike_pct_range: float = 3.0,
    ):
        """Initialize stream manager"""
        self.client = client
        self.underlying = (
            underlying.upper()
        )  # TradeStation API symbol for underlying (e.g. "$SPXW.X")
        self.db_underlying = (
            db_underlying or underlying
        ).upper()  # canonical alias for DB (e.g. "SPX")
        self.option_root = resolve_option_root(
            self.underlying
        )  # option root for quotes (e.g. "SPXW")
        self.num_expirations = num_expirations
        # Strike selection: take every strike within ±strike_pct_range % of
        # spot, then trim from the furthest-from-spot strikes inward if the
        # result exceeds strike_count_max (hard ceiling, total per expiration).
        self.strike_count_max = strike_count_max
        self.strike_pct_range = strike_pct_range

        # Track state
        self.current_price: Optional[float] = None
        self.target_expirations: List[date] = []
        self.tracked_strikes: Set[float] = set()
        self.tracked_option_symbols: List[str] = []
        # Pre-parsed metadata (strike, expiration, option_type) per option symbol
        # so we don't re-parse the symbol string every poll cycle.
        self._symbol_metadata: Dict[str, Dict[str, Any]] = {}

        # Shared wakeup event — either accumulator sets this when new data arrives
        # so the main loop can react immediately instead of sleeping a fixed interval.
        self._wakeup = threading.Event()
        # Shutdown latch. ``stream()`` checks this around its idle wait so a
        # SIGTERM-triggered ``request_stop()`` exits in milliseconds, not after
        # the full extended-hours poll interval (was up to 30s, causing systemd
        # to SIGKILL the worker past TimeoutStopSec).
        self._stop_event = threading.Event()
        # Background accumulators for persistent streaming connections.
        self._accumulator: Optional[OptionStreamAccumulator] = None
        self._underlying_accumulator: Optional[UnderlyingBarAccumulator] = None

        # Track expired strikes for cleanup
        self.all_tracked_strikes: Dict[date, Set[float]] = {}

        # Track last expiration refresh time
        self.last_expiration_refresh: Optional[datetime] = None
        self.option_oi_coverage_alert_threshold = float(
            os.getenv("OPTION_OI_COVERAGE_ALERT_THRESHOLD", "0.35")
        )
        self.option_volume_coverage_alert_threshold = float(
            os.getenv("OPTION_VOLUME_COVERAGE_ALERT_THRESHOLD", "0.35")
        )
        # Volume is cumulative for the day; the first ~30 min after open
        # are a natural ramp where most contracts haven't traded yet, so
        # gate the warning on a warmup window past 09:30 ET.
        self.option_volume_warmup_minutes = int(os.getenv("OPTION_VOLUME_WARMUP_MINUTES", "30"))
        # OI is sticky in the accumulator (only overwritten by positive
        # values), but the REST seed and stream may not carry yesterday's
        # settled OI before the regular open — so contracts that tick in
        # pre-market often still show OI=0. Gate the alarm on a short
        # warmup past 09:30 ET so the warning only fires when a genuine
        # gap persists into the regular session.
        self.option_oi_warmup_minutes = int(os.getenv("OPTION_OI_WARMUP_MINUTES", "5"))
        self.seed_rest_on_recalc = (
            os.getenv("OPTION_REST_SEED_ON_RECALC", "false").lower() == "true"
        )
        # Session-cumulative set of option symbols seen with Volume>0 since the
        # ET day rollover. Kept on the StreamManager (NOT the per-cycle
        # accumulator, which is torn down and rebuilt without a REST re-seed
        # every STRIKE_RECALC_INTERVAL ~60s) so volume coverage reflects the
        # whole session rather than just trades since the last recalc reset.
        self._session_volume_symbols: Set[str] = set()
        self._session_volume_date: Optional[date] = None

        logger.info(f"Initialized StreamManager for {underlying}")
        logger.info(
            f"Config: {num_expirations} expirations, "
            f"±{strike_pct_range}% strike band (max {strike_count_max} strikes/exp)"
        )
        logger.info(
            "Option REST seed on strike recalibration: %s",
            "enabled" if self.seed_rest_on_recalc else "disabled",
        )

    def _fetch_underlying_bar(self) -> Optional[Dict[str, Any]]:
        """
        Fetch a single underlying bar via REST (used only for initialization
        and strike recalibration, NOT for the hot streaming path).
        """
        try:
            bars_data = self.client.get_stream_bars(
                symbol=self.underlying,
                interval=1,
                unit="Minute",
                barsback=1,
                sessiontemplate=SESSION_TEMPLATE,
                warn_if_closed=False,
            )

            if "Bars" not in bars_data or len(bars_data["Bars"]) == 0:
                logger.debug(f"No bar data returned for {self.underlying}")
                return None

            bar = bars_data["Bars"][0]
            if not validate_bar_data(bar):
                logger.warning("Invalid bar data, skipping")
                return None

            timestamp_str = bar.get("TimeStamp", "")
            timestamp = safe_datetime(timestamp_str, field_name="TimeStamp")
            if not timestamp:
                timestamp = datetime.now(ET)

            underlying_data = {
                "symbol": self.db_underlying,
                "timestamp": timestamp,
                "open": safe_float(bar.get("Open"), field_name="Open"),
                "high": safe_float(bar.get("High"), field_name="High"),
                "low": safe_float(bar.get("Low"), field_name="Low"),
                "close": safe_float(bar.get("Close"), field_name="Close"),
                "up_volume": safe_int(bar.get("UpVolume"), field_name="UpVolume"),
                "down_volume": safe_int(bar.get("DownVolume"), field_name="DownVolume"),
                "volume": safe_int(bar.get("TotalVolume"), field_name="TotalVolume"),
            }

            logger.debug(
                f"Bar (REST): {self.underlying} @ {timestamp} " f"C=${underlying_data['close']:.2f}"
            )
            return underlying_data

        except Exception as e:
            logger.error(f"Error fetching underlying bar: {e}", exc_info=True)
            return None

    def _get_underlying_price(self) -> Optional[float]:
        """
        Fetch current underlying price

        Used only for initialization and strike recalculation
        For streaming data, use _fetch_underlying_bar()
        """
        try:
            # Fetch latest bar and extract close price
            bar_data = self._fetch_underlying_bar()

            if bar_data:
                price = bar_data["close"]
                logger.debug(f"Current {self.underlying} price: ${price:.2f}")
                return price  # type: ignore[no-any-return]

            return None

        except Exception as e:
            logger.error(f"Error fetching underlying price: {e}", exc_info=True)
            return None

    def _should_refresh_expirations(self) -> bool:
        """
        Check if expirations need to be refreshed

        Refresh conditions:
        1. First time (never refreshed)
        2. Market close occurred since last refresh (4:00 PM ET)
        3. Any tracked expiration has expired (is in the past)

        Returns:
            True if expirations should be refreshed
        """
        now_et = datetime.now(ET)
        today = now_et.date()

        # First time - always refresh
        if self.last_expiration_refresh is None:
            logger.info("First expiration refresh needed")
            return True

        # Check if any tracked expiration has expired
        if self.target_expirations:
            earliest_exp = min(self.target_expirations)
            if earliest_exp < today:
                logger.info(f"Expiration {earliest_exp} has passed, refresh needed")
                return True

        # Check if we've crossed 4:00 PM ET since last refresh
        last_refresh_et = self.last_expiration_refresh.astimezone(ET)
        market_close_time = datetime.strptime("16:00:00", "%H:%M:%S").time()

        # If last refresh was before today's 4:00 PM and now is after 4:00 PM
        if last_refresh_et.date() < now_et.date() or (
            last_refresh_et.date() == now_et.date()
            and last_refresh_et.time() < market_close_time
            and now_et.time() >= market_close_time
        ):
            logger.info("Market close occurred since last refresh, expirations may need update")
            return True

        return False

    def _refresh_expirations(self) -> bool:
        """
        Refresh target expirations and rebuild option symbols

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Refreshing target expirations...")

            # Get fresh expirations
            new_expirations = self._get_target_expirations()

            if not new_expirations:
                logger.error("Failed to get new expirations")
                return False

            # Check if expirations actually changed
            if new_expirations == self.target_expirations:
                logger.info("Expirations unchanged, skipping rebuild")
                self.last_expiration_refresh = datetime.now(ET)
                return True

            # Log the change
            logger.info("Expirations changed:")
            logger.info(f"  Old: {[str(exp) for exp in self.target_expirations]}")
            logger.info(f"  New: {[str(exp) for exp in new_expirations]}")

            # Update expirations
            self.target_expirations = new_expirations

            # Rebuild option symbols with new expirations
            if self.current_price:
                self.tracked_option_symbols = self._build_option_symbols()
                logger.info(
                    f"Rebuilt {len(self.tracked_option_symbols)} option symbols "
                    "with new expirations"
                )

            # Update refresh timestamp
            self.last_expiration_refresh = datetime.now(ET)

            return True

        except Exception as e:
            logger.error(f"Error refreshing expirations: {e}", exc_info=True)
            return False

    def _get_target_expirations(self) -> List[date]:
        """Get target expiration dates.

        Always queries get_option_expirations(self.underlying) — e.g. "$SPXW.X" — since
        that is the symbol TradeStation uses for expiration/strike structure lookups.
        self.option_root (e.g. "SPXW") is only used later inside build_option_symbol()
        when constructing the actual option chain symbols for get_option_quotes().

        For SPX weekly options: use SYMBOL_ALIASES=SPX=$SPXW.X (not $SPX.X) so that
        expirations and strikes are fetched under $SPXW.X, then set
        OPTION_ROOT_ALIASES=$SPXW.X=SPXW so quotes are built as "SPXW 260320C6630".
        """
        try:
            all_expirations = self.client.get_option_expirations(self.underlying)

            if not all_expirations:
                logger.warning(f"No expirations found for {self.underlying}")
                return []

            # Filter to future expirations
            today = date.today()
            future_expirations = [exp for exp in all_expirations if exp >= today]

            if not future_expirations:
                logger.warning("No future expirations available")
                return []

            # Take first N
            target_exps = future_expirations[: self.num_expirations]

            logger.info(f"Target expirations: {[str(exp) for exp in target_exps]}")
            return target_exps

        except Exception as e:
            logger.error(f"Error fetching expirations: {e}", exc_info=True)
            return []

    def _get_strikes_near_price(self, expiration: date, current_price: float) -> List[float]:
        """Get strikes within ±strike_pct_range% of spot, capped at strike_count_max.

        Selection is two-step: first filter to the percentage band around
        spot, then if the band still holds more strikes than
        ``strike_count_max`` trim from the furthest-from-spot strikes inward
        until the count fits.
        """
        try:
            exp_str = expiration.strftime("%m-%d-%Y")
            all_strikes = self.client.get_option_strikes(self.underlying, expiration=exp_str)

            if not all_strikes:
                logger.warning(f"No strikes found for exp {exp_str}")
                return []

            pct = self.strike_pct_range / 100.0
            low = current_price * (1.0 - pct)
            high = current_price * (1.0 + pct)
            in_band = [s for s in all_strikes if low <= s <= high]

            trimmed_count = 0
            if len(in_band) > self.strike_count_max:
                trimmed_count = len(in_band) - self.strike_count_max
                in_band.sort(key=lambda s: abs(s - current_price))
                in_band = in_band[: self.strike_count_max]

            nearby_strikes = sorted(in_band)
            below = sum(1 for s in nearby_strikes if s <= current_price)
            above = len(nearby_strikes) - below

            log_msg = (
                f"Exp {exp_str}: {len(nearby_strikes)} strikes "
                f"({below} below, {above} above ${current_price:.2f}) "
                f"within ±{self.strike_pct_range}% [{low:.2f}, {high:.2f}]"
            )
            if trimmed_count:
                log_msg += f"; trimmed {trimmed_count} furthest at cap {self.strike_count_max}"
            logger.debug(log_msg)

            return nearby_strikes

        except Exception as e:
            logger.error(f"Error fetching strikes for {expiration}: {e}", exc_info=True)
            return []

    def _build_option_symbols(self) -> List[str]:
        """Build list of option symbols to track and pre-parse metadata."""
        if not self.current_price:
            logger.warning("No current price, cannot build option symbols")
            return []

        option_symbols = []
        self.tracked_strikes = set()
        self.all_tracked_strikes = {}
        self._symbol_metadata = {}

        for expiration in self.target_expirations:
            strikes = self._get_strikes_near_price(expiration, self.current_price)
            self.all_tracked_strikes[expiration] = set(strikes)

            for strike in strikes:
                for opt_type in ("C", "P"):
                    symbol = self.client.build_option_symbol(
                        self.underlying, expiration, opt_type, strike
                    )
                    option_symbols.append(symbol)
                    self.tracked_strikes.add(strike)
                    self._symbol_metadata[symbol] = {
                        "strike": strike,
                        "expiration": expiration,
                        "option_type": opt_type,
                    }

        logger.info(f"Built {len(option_symbols)} option symbols to track")
        return option_symbols

    def _update_session_volume_coverage(
        self,
        full_state: Dict[str, Dict[str, Any]],
        tracked_total: int,
        now_et: Optional[datetime] = None,
    ) -> float:
        """Session-cumulative fraction of the tracked universe seen trading.

        The per-cycle option accumulator is torn down and rebuilt WITHOUT a
        REST re-seed every ``STRIKE_RECALC_INTERVAL`` (~60s at the default 5s
        poll), zeroing its in-memory ``Volume``. Counting ``Volume>0`` straight
        off the accumulator therefore only ever reflects ~1 minute of trades
        and reads far below the true session figure (~80% measured) — the
        chronic false positive behind the "Low option volume coverage" alert.

        Instead, accumulate the set of symbols seen with ``Volume>0`` on the
        StreamManager (it survives the accumulator swaps), reset it at the ET
        day rollover, and report the distinct count over the current universe
        size, capped at 1.0 (the band drifts with spot, so the day's union can
        exceed the current snapshot). Stays low only when volume genuinely
        isn't arriving, which is what the alert is for; a volume STOP after
        trades were already seen is left to stream_updates / flow-staleness.
        """
        et_date = (now_et or datetime.now(ET)).date()
        if self._session_volume_date != et_date:
            self._session_volume_symbols = set()
            self._session_volume_date = et_date
        self._session_volume_symbols.update(
            sym for sym, raw in full_state.items() if int(raw.get("Volume") or 0) > 0
        )
        if tracked_total <= 0:
            return 0.0
        return min(1.0, len(self._session_volume_symbols) / tracked_total)

    def _cleanup_expired_strikes(self):
        """Remove strikes for expired expirations to prevent memory leak"""
        today = date.today()
        expired = [exp for exp in self.all_tracked_strikes.keys() if exp < today]

        for exp in expired:
            del self.all_tracked_strikes[exp]
            logger.debug(f"Cleaned up strikes for expired expiration: {exp}")

    def _validate_option_quote_symbol(self) -> bool:
        """Validate at least one built option symbol returns a quote without API symbol errors."""
        if not self.tracked_option_symbols:
            logger.error("No option symbols available for validation")
            return False

        test_symbol = self.tracked_option_symbols[0]
        logger.info(f"Validating option quote symbol: {test_symbol}")

        try:
            result = self.client.get_option_quotes([test_symbol])

            errors = result.get("Errors", []) if isinstance(result, dict) else []
            if errors:
                logger.error(f"Option quote validation failed for {test_symbol}: {errors[0]}")
                logger.error(
                    "This usually means the option symbol format is not accepted "
                    "by TradeStation quotes endpoint for this underlying."
                )
                return False

            quotes = result.get("Quotes", []) if isinstance(result, dict) else []
            if not quotes:
                logger.warning(
                    f"Option quote validation returned no quotes for {test_symbol}; continuing "
                    "because endpoint did not report INVALID SYMBOL"
                )
                return True

            logger.info(f"✅ Option quote validation passed for {test_symbol}")
            return True

        except Exception as e:
            logger.error(f"Error validating option quote symbol {test_symbol}: {e}", exc_info=True)
            return False

    def initialize(self) -> bool:
        """Initialize stream"""
        logger.info(f"Initializing stream for {self.underlying}...")

        # Get current price
        self.current_price = self._get_underlying_price()
        if not self.current_price:
            logger.error("Failed to get underlying price")
            return False

        # Get target expirations
        self.target_expirations = self._get_target_expirations()
        if not self.target_expirations:
            logger.error("Failed to get target expirations")
            return False

        # Build option symbols
        self.tracked_option_symbols = self._build_option_symbols()
        if not self.tracked_option_symbols:
            logger.error("Failed to build option symbols")
            return False

        # Validate at least one option quote symbol actually resolves on quote endpoint
        if not self._validate_option_quote_symbol():
            logger.error("Failed option quote validation during initialization")
            return False

        # Set initial refresh timestamp (NEW)
        self.last_expiration_refresh = datetime.now(ET)

        logger.info("✅ Initialization complete:")
        logger.info(f"   Price: ${self.current_price:.2f}")
        logger.info(f"   Tracking {len(self.target_expirations)} expirations")
        logger.info(f"   Tracking {len(self.tracked_option_symbols)} option contracts")

        return True

    def _start_accumulators(
        self,
        seed_option_rest: bool = True,
        restart_underlying: bool = True,
    ):
        """Start (or restart) background stream readers for options and underlying.

        ``restart_underlying=False`` leaves the live underlying bar stream
        untouched and only cycles the option accumulator. Callers that swap
        the option symbol set (expiration refresh, strike recalibration)
        should pass False — the underlying symbol is fixed for this engine's
        lifetime, the bar stream is independent of the option chunks, and
        needlessly tearing it down forces it to re-race the option streams
        for a TradeStation stream slot every minute. Under cap pressure
        that race is exactly how the underlying feed loses its slot and
        the heatmap freezes.
        """
        # Stop existing option accumulator (always — its symbol set may have
        # changed) and the underlying only when explicitly asked to.
        if self._accumulator is not None:
            self._accumulator.stop()
        if restart_underlying and self._underlying_accumulator is not None:
            self._underlying_accumulator.stop()

        self._wakeup.clear()

        # Open the underlying bar stream FIRST so it claims its TradeStation
        # stream slot before the option chunks consume the remaining budget.
        # The bar stream is the data source the GEX heatmap is anchored on
        # (latest underlying_quotes.timestamp drives the query window), so
        # losing its slot freezes every downstream chart even when options
        # are still flowing. Option chunks degrading gracefully under cap
        # pressure is far less harmful than the underlying feed going dark.
        if restart_underlying or self._underlying_accumulator is None:
            self._underlying_accumulator = UnderlyingBarAccumulator(
                client=self.client,
                symbol=self.underlying,
                db_symbol=self.db_underlying,
                session_template=SESSION_TEMPLATE,
                wakeup=self._wakeup,
            )
            self._underlying_accumulator.start()

        self._accumulator = OptionStreamAccumulator(
            client=self.client,
            symbols=self.tracked_option_symbols,
            wakeup=self._wakeup,
        )
        self._accumulator.start(seed_from_rest=seed_option_rest)

    def request_stop(self):
        """Ask :meth:`stream` to exit at its next checkpoint.

        Safe to call from a signal handler: only sets two threading.Event
        flags, no buffer touching, no logging. The set on ``_wakeup`` is the
        critical bit — without it the loop sits on its idle wait for up to
        the full extended-hours poll interval (30s) and gets SIGKILLed by
        systemd's TimeoutStopSec before it can drain.
        """
        self._stop_event.set()
        # Interrupt any in-flight ``_wakeup.wait(timeout=...)``. The wait is
        # the dominant blocker — accumulator HTTP reads happen on daemon
        # threads, and ``run_streaming``'s DB writes happen between yields,
        # not inside this generator.
        self._wakeup.set()

    def _restart_underlying_accumulator(self, reason: str):
        """Tear down and recreate ONLY the underlying bar stream.

        Used to recover from a stalled underlying feed (dead thread, or
        socket-alive-but-data-starved) without disturbing the healthy
        options stream — restarting that would force an expensive REST
        re-seed and gap option ingestion for no reason. Cheap: the
        accumulator is just a reader thread, no REST seed.
        """
        logger.error("Restarting underlying bar stream: %s", reason)
        if self._underlying_accumulator is not None:
            self._underlying_accumulator.stop()
        self._underlying_accumulator = UnderlyingBarAccumulator(
            client=self.client,
            symbol=self.underlying,
            db_symbol=self.db_underlying,
            session_template=SESSION_TEMPLATE,
            wakeup=self._wakeup,
        )
        self._underlying_accumulator.start()

    def _yield_option_snapshot(self, state: Dict[str, Dict[str, Any]]):
        """
        Convert raw accumulator state into yielded option data dicts.

        *state* should come from :meth:`OptionStreamAccumulator.drain` so
        it only contains contracts that have received new data since the
        last drain — no external change-detection needed.

        Returns a list (not a generator) so callers can count results.
        """
        results = []
        for option_symbol, raw in state.items():
            meta = self._symbol_metadata.get(option_symbol)
            if not meta:
                continue

            raw_ts = raw.get("TimeStamp", "")
            timestamp = safe_datetime(raw_ts, field_name="TimeStamp")
            if not timestamp:
                timestamp = datetime.now(ET)

            last = safe_float(raw.get("Last"), default=None, field_name="Last")
            bid = safe_float(raw.get("Bid"), default=None, field_name="Bid")
            ask = safe_float(raw.get("Ask"), default=None, field_name="Ask")
            mid = safe_float(raw.get("Mid"), default=None, field_name="Mid")

            if mid is None and bid is not None and ask is not None:
                mid = (bid + ask) / 2.0

            volume = safe_int(raw.get("Volume"), default=None, field_name="Volume")

            open_interest = safe_int(
                raw.get("DailyOpenInterest"),
                default=None,  # type: ignore[arg-type]
                field_name="DailyOpenInterest",
            )
            if open_interest is None:
                open_interest = safe_int(
                    raw.get("OpenInterest"),
                    default=None,
                    field_name="OpenInterest",
                )

            implied_volatility = None
            for iv_field in _IV_FIELD_NAMES:
                iv_val = safe_float(raw.get(iv_field), field_name=iv_field)
                if iv_val and iv_val > 0:
                    implied_volatility = iv_val
                    break

            results.append(
                {
                    "option_symbol": option_symbol,
                    "timestamp": timestamp,
                    "underlying": self.db_underlying,
                    "strike": meta["strike"],
                    "expiration": meta["expiration"],
                    "option_type": meta["option_type"],
                    "last": last,
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "volume": volume,
                    "open_interest": open_interest,
                    "implied_volatility": implied_volatility,
                }
            )

        return results

    def stream(self, max_iterations: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Stream real-time data and yield to caller.

        Underlying bars are fetched via Stream Bars snapshot each cycle.
        Option quotes are accumulated by a background thread; this method
        drains only the contracts that received new data since the last
        cycle and yields them as a single batch for efficient DB writes.

        Yields dictionaries with:
            {
                'type': 'underlying',
                'data': {...}
            }
        or:
            {
                'type': 'option_batch',
                'data': [list of option dicts]
            }
        """
        if not self.tracked_option_symbols:
            logger.error("Not initialized. Call initialize() first.")
            return

        logger.info("Starting stream loop (event-driven)...")
        logger.info("Press Ctrl+C to stop")

        # Start persistent background streams for underlying and options.
        self._start_accumulators()

        iteration = 0
        # Observability counters — logged every _METRICS_LOG_INTERVAL cycles.
        _METRICS_LOG_INTERVAL = 20
        _total_option_batches = 0
        _total_options_yielded = 0
        _total_underlying_yields = 0
        _total_empty_cycles = 0
        _last_metrics_time = time.monotonic()
        # Underlying-stream staleness watchdog. Gauged in wall-clock
        # seconds since the last bar — NEVER the empty-drain count: the
        # loop wakes sub-second on every option tick (shared wakeup
        # event), so the drain count races ~1/sec while a 1-minute bar
        # feed is silent ~60s between bars by design. Gating the warning
        # on the count fires it ~50s before the feed is actually late.
        # `_last_underlying_bar_mono` is None until the first bar arrives
        # so a slow feed open isn't mistaken for a stall. The empty-drain
        # count is kept only as diagnostic context in the log line.
        _consecutive_empty_underlying = 0
        _last_bar_updates = 0  # track updates_received delta
        _last_underlying_bar_mono: Optional[float] = None
        _last_forced_restart_mono = 0.0
        _underlying_restart_attempts = 0
        _underlying_restart_backed_off = False
        _stale_warned = False
        _last_stale_warn_mono = 0.0
        # Timestamp of the last *genuinely new* bar. A reconnect opens the
        # bar stream with barsback=1, so every forced restart immediately
        # replays one historical bar. Resetting the staleness/escalation
        # state on a replayed (same-or-older) bar makes a starved feed loop
        # "restart -> replay -> reset -> starve -> restart" forever and
        # never reach the backed-off upstream-outage state. Gate the resets
        # on the bar timestamp actually advancing.
        _last_fresh_bar_ts: Optional[datetime] = None

        try:
            while not self._stop_event.is_set():
                iteration += 1

                # Get current market session for dynamic polling
                session = get_market_session()

                # Determine max wait: used as the *timeout* on the wakeup
                # event, not as a fixed sleep.  When data arrives, the loop
                # wakes immediately.
                if session == "regular":
                    max_wait = MARKET_HOURS_POLL_INTERVAL
                elif session in ["pre-market", "after-hours"]:
                    max_wait = EXTENDED_HOURS_POLL_INTERVAL
                else:  # closed
                    max_wait = CLOSED_HOURS_POLL_INTERVAL

                # Staleness thresholds are cadence-sensitive (dense regular
                # session vs. sparse extended hours) — see
                # _stale_thresholds_for_session.
                stale_warn_secs, stale_restart_secs = _stale_thresholds_for_session(session)

                # --- block until data arrives or timeout for housekeeping ---
                # Clear before waiting so signals arriving during processing
                # are not lost (set-before-clear race).
                self._wakeup.clear()
                # Stop arriving between clear() and wait() would otherwise be
                # erased by the clear; re-check before parking. request_stop()
                # sets both flags, so a stop here means wakeup was also set
                # and the subsequent wait would return immediately anyway —
                # this check just spares one extra loop trip.
                if self._stop_event.is_set():
                    break
                self._wakeup.wait(timeout=max_wait)
                # Stop arriving during the wait: request_stop() sets _wakeup,
                # so the wait returns in milliseconds rather than sitting on
                # the full max_wait (up to 30s in extended hours).
                if self._stop_event.is_set():
                    break

                cycle_start = time.monotonic()

                if iteration % 10 == 1:
                    logger.info(
                        f"Iteration {iteration} - "
                        f"{datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S ET')} "
                        f"[{session}]"
                    )

                # Check if expirations need refresh
                if self._should_refresh_expirations():
                    logger.info("Refreshing expirations...")
                    if self._refresh_expirations():
                        logger.info("✅ Expirations refreshed successfully")
                        # C3: drain the consumer's pending option buckets
                        # before swapping accumulators — symbols dropped by
                        # the refreshed expiration set never tick again.
                        yield {"type": "flush_options", "reason": "expiration_refresh"}
                        # Underlying symbol is unchanged; leave its bar
                        # stream untouched so it doesn't have to re-race
                        # the option chunks for a TradeStation stream slot.
                        self._start_accumulators(restart_underlying=False)
                    else:
                        logger.warning(
                            "⚠️  Expiration refresh failed, continuing " "with current expirations"
                        )

                try:
                    # --- underlying stream health checks ---
                    # Only treat underlying silence as a fault inside the
                    # window the feed actually delivers bars: the
                    # SESSION_TEMPLATE window, clamped to the regular cash
                    # session for cash indices (SPX has no pre/after-hours
                    # print). Outside it, silence is expected — don't warn,
                    # reconnect, or restart a (cleanly-ended) reader.
                    feed_expected = underlying_feed_expected(
                        datetime.now(ET), SESSION_TEMPLATE, self.db_underlying
                    )

                    assert self._underlying_accumulator is not None
                    # Dead reader thread during the live window: recover
                    # the underlying stream only. The options stream is
                    # independent and may be healthy; restarting it would
                    # force an expensive REST re-seed and gap option
                    # ingestion for no reason.
                    if feed_expected and not self._underlying_accumulator.is_alive:
                        self._restart_underlying_accumulator("reader thread is DEAD")

                    # Drain underlying bar from persistent stream.
                    underlying_data = self._underlying_accumulator.drain()
                    bar_advanced = False
                    if underlying_data:
                        self.current_price = underlying_data["close"]
                        # Yield unconditionally — the downstream OHLC upsert
                        # is keyed by minute bucket and idempotent, so a
                        # replayed bar is harmless to ingest. It is NOT
                        # harmless to the watchdog, though: only a bar whose
                        # timestamp advances past the last fresh one counts
                        # as real liveness. A barsback=1 replay (same/older
                        # timestamp on every reconnect) must NOT reset the
                        # staleness clock or restart escalation.
                        yield {"type": "underlying", "data": underlying_data}
                        _total_underlying_yields += 1

                        bar_ts = underlying_data.get("timestamp")
                        if _bar_timestamp_advanced(bar_ts, _last_fresh_bar_ts):
                            bar_advanced = True
                            if _stale_warned:
                                logger.info(
                                    "Underlying bar stream RECOVERED after "
                                    "%.0fs / %d empty drains",
                                    (
                                        time.monotonic() - _last_underlying_bar_mono
                                        if _last_underlying_bar_mono is not None
                                        else 0.0
                                    ),
                                    _consecutive_empty_underlying,
                                )
                            if isinstance(bar_ts, datetime):
                                _last_fresh_bar_ts = bar_ts
                            _consecutive_empty_underlying = 0
                            _last_underlying_bar_mono = time.monotonic()
                            _underlying_restart_attempts = 0
                            _underlying_restart_backed_off = False
                            _stale_warned = False

                    if not bar_advanced:
                        if feed_expected:
                            # No FRESH bar this cycle while the feed should
                            # be live. Treats "drain returned nothing" and
                            # "drain returned a non-advancing bar" the same
                            # — otherwise an upstream that keeps re-emitting
                            # the last known bar (server-side stuck on
                            # yesterday's close timestamp) silently bypasses
                            # the staleness ladder: branch (A) above absorbs
                            # every iteration, the Greeks engine rejects
                            # until 16:00, and no reconnect ever fires.
                            # Staleness is judged in wall-clock seconds only:
                            # the empty-drain count climbs ~1/sec (options
                            # wake the loop sub-second) while a 1-minute bar
                            # feed is legitimately silent ~60s between bars,
                            # so gating on the count fires a false STALE
                            # ~50s early.
                            _consecutive_empty_underlying += 1
                            if _last_underlying_bar_mono is None:
                                # Feed hasn't produced its first bar yet
                                # (slow open). Don't count warm-up as a
                                # stall — arm the clock from the first
                                # expectant cycle.
                                _last_underlying_bar_mono = time.monotonic()
                            now_mono = time.monotonic()
                            stale_seconds = now_mono - _last_underlying_bar_mono

                            # Warn once silence exceeds the threshold (which
                            # sits above the bar cadence), then re-warn at
                            # that same interval while it persists — never
                            # every cycle.
                            if stale_seconds >= stale_warn_secs and (
                                not _stale_warned
                                or now_mono - _last_stale_warn_mono >= stale_warn_secs
                            ):
                                cur_updates = self._underlying_accumulator.updates_received
                                logger.warning(
                                    "Underlying bar stream appears STALE: "
                                    "%.0fs without a fresh bar, "
                                    "%d empty/replay drains, "
                                    "bar_stream_updates=%d (delta=%d), "
                                    "thread_alive=%s",
                                    stale_seconds,
                                    _consecutive_empty_underlying,
                                    cur_updates,
                                    cur_updates - _last_bar_updates,
                                    self._underlying_accumulator.is_alive,
                                )
                                _stale_warned = True
                                _last_stale_warn_mono = now_mono

                            # Active recovery. A socket-alive-but-data-
                            # starved feed never trips the socket read
                            # timeout or the dead-thread check, so force a
                            # reconnect once it has been stale long enough —
                            # rate-limited by a cooldown, then escalated to
                            # a backed-off upstream-outage state rather than
                            # tight-looping.
                            if (
                                stale_seconds >= stale_restart_secs
                                and not _underlying_restart_backed_off
                                and now_mono - _last_forced_restart_mono
                                >= UNDERLYING_STREAM_RESTART_COOLDOWN_SECONDS
                            ):
                                _underlying_restart_attempts += 1
                                if (
                                    _underlying_restart_attempts
                                    > UNDERLYING_STREAM_MAX_RESTART_ATTEMPTS
                                ):
                                    _underlying_restart_backed_off = True
                                    logger.error(
                                        "Underlying bar stream still dead after %d "
                                        "forced reconnects (%.0fs stale) — treating "
                                        "as an upstream TradeStation outage. Will "
                                        "slow-retry every %ds until a bar arrives. "
                                        "Options ingestion is unaffected.",
                                        _underlying_restart_attempts - 1,
                                        stale_seconds,
                                        UNDERLYING_STREAM_BACKOFF_RETRY_INTERVAL_SECONDS,
                                    )
                                else:
                                    self._restart_underlying_accumulator(
                                        f"data-starved {stale_seconds:.0f}s during "
                                        f"{session} (attempt "
                                        f"{_underlying_restart_attempts}/"
                                        f"{UNDERLYING_STREAM_MAX_RESTART_ATTEMPTS})"
                                    )
                                    _last_forced_restart_mono = now_mono
                            elif (
                                stale_seconds >= stale_restart_secs
                                and _underlying_restart_backed_off
                                and now_mono - _last_forced_restart_mono
                                >= UNDERLYING_STREAM_BACKOFF_RETRY_INTERVAL_SECONDS
                            ):
                                # Slow re-attempt after the fast-retry budget
                                # was consumed. Without this, the supervisor
                                # would stay dead until the process restarts —
                                # the bug behind the 2026-06 17h outage with
                                # 1.1M Greeks rejects. Reset the fast-retry
                                # counter so a subsequent transient gap gets
                                # the full budget again.
                                logger.warning(
                                    "Underlying bar stream still stale "
                                    "(%.0fs) — slow re-attempt after %ds "
                                    "backoff interval.",
                                    stale_seconds,
                                    UNDERLYING_STREAM_BACKOFF_RETRY_INTERVAL_SECONDS,
                                )
                                _underlying_restart_attempts = 1
                                _underlying_restart_backed_off = False
                                self._restart_underlying_accumulator(
                                    f"backoff-retry {stale_seconds:.0f}s during "
                                    f"{session}"
                                )
                                _last_forced_restart_mono = now_mono
                        else:
                            # Feed legitimately not expected (overnight /
                            # weekend / holiday for a cash index). Reset the
                            # staleness clock so the NEXT expected session
                            # measures from its own open — otherwise the
                            # first expectant cycle compares against last
                            # session's final bar and fires a spurious ~10h
                            # "STALE" (and one needless reconnect) at 09:30
                            # before the feed has had a chance to deliver
                            # the day's first bar.
                            _consecutive_empty_underlying = 0
                            _last_underlying_bar_mono = None
                            _stale_warned = False
                            _underlying_restart_attempts = 0
                            _underlying_restart_backed_off = False

                    # Drain only option contracts that changed since last cycle.
                    assert self._accumulator is not None
                    changed = self._accumulator.drain()
                    if changed:
                        option_results = self._yield_option_snapshot(changed)

                        if option_results:
                            option_count = len(option_results)
                            _total_option_batches += 1
                            _total_options_yielded += option_count

                            option_with_oi = sum(
                                1 for o in option_results if (o.get("open_interest") or 0) > 0
                            )

                            tracked_total = len(self.tracked_option_symbols)
                            oi_coverage = option_with_oi / option_count

                            # Volume coverage = session-cumulative fraction of the
                            # tracked universe that has traded. Tracked on the
                            # StreamManager rather than counted off the live
                            # accumulator: the accumulator is rebuilt WITHOUT a REST
                            # re-seed every STRIKE_RECALC_INTERVAL (~60s), so a direct
                            # count collapses to ~1 minute of trades each recalc and
                            # chronically false-trips the alert (~0.4-16% seen vs ~80%
                            # actual). See _update_session_volume_coverage.
                            full_state = self._accumulator.snapshot()
                            volume_coverage = self._update_session_volume_coverage(
                                full_state, tracked_total
                            )

                            logger.info(
                                f"Option batch: {option_count} updated, "
                                f"{tracked_total - option_count} unchanged, "
                                f"oi_coverage={oi_coverage:.1%}, "
                                f"volume_coverage={volume_coverage:.1%}, "
                                f"stream_updates={self._accumulator.updates_received}"
                            )

                            if session == "regular":
                                now_et = datetime.now(ET)
                                minutes_since_open = (now_et.hour - 9) * 60 + (now_et.minute - 30)
                                if (
                                    minutes_since_open >= self.option_oi_warmup_minutes
                                    and oi_coverage < self.option_oi_coverage_alert_threshold
                                ):
                                    logger.warning(
                                        f"⚠️ Low option OI coverage: "
                                        f"{oi_coverage:.1%} "
                                        f"(threshold "
                                        f"{self.option_oi_coverage_alert_threshold:.1%}, "
                                        f"{minutes_since_open}min into session)"
                                    )
                                if (
                                    minutes_since_open >= self.option_volume_warmup_minutes
                                    and volume_coverage
                                    < self.option_volume_coverage_alert_threshold
                                ):
                                    logger.warning(
                                        f"⚠️ Low option volume coverage: "
                                        f"{volume_coverage:.1%} "
                                        f"(threshold "
                                        f"{self.option_volume_coverage_alert_threshold:.1%}, "
                                        f"{minutes_since_open}min into session)"
                                    )

                            yield {"type": "option_batch", "data": option_results}
                    else:
                        _total_empty_cycles += 1

                    # --- observability: periodic metrics summary ---
                    if iteration % _METRICS_LOG_INTERVAL == 0:
                        elapsed = time.monotonic() - _last_metrics_time
                        cycle_ms = (time.monotonic() - cycle_start) * 1000
                        assert self._accumulator is not None
                        assert self._underlying_accumulator is not None
                        logger.info(
                            f"[METRICS] last {_METRICS_LOG_INTERVAL} cycles in {elapsed:.1f}s | "
                            f"option_batches={_total_option_batches} "
                            f"options_yielded={_total_options_yielded} "
                            f"underlying_yields={_total_underlying_yields} "
                            f"empty_cycles={_total_empty_cycles} "
                            f"opt_stream_updates={self._accumulator.updates_received} "
                            f"bar_stream_updates={self._underlying_accumulator.updates_received} "
                            f"cycle_ms={cycle_ms:.1f}"
                        )
                        _last_bar_updates = self._underlying_accumulator.updates_received
                        _total_option_batches = 0
                        _total_options_yielded = 0
                        _total_underlying_yields = 0
                        _total_empty_cycles = 0
                        _last_metrics_time = time.monotonic()

                    # Recalibrate strike range periodically.
                    if iteration % STRIKE_RECALC_INTERVAL == 0 and iteration > 0:
                        if self.current_price:
                            new_price = self._get_underlying_price()
                            if new_price:
                                self.current_price = new_price
                                self.tracked_option_symbols = self._build_option_symbols()
                                # C3: flush the consumer's pending option
                                # buckets BEFORE swapping accumulators —
                                # contracts dropped from the recalibrated
                                # tracked set never tick again, so their
                                # last partial bucket's classified flow is
                                # otherwise lost.
                                yield {"type": "flush_options", "reason": "strike_recalc"}
                                # Underlying symbol is unchanged; leave its
                                # bar stream untouched on every recalc so
                                # it doesn't have to re-race the option
                                # chunks for a TradeStation stream slot.
                                self._start_accumulators(
                                    seed_option_rest=self.seed_rest_on_recalc,
                                    restart_underlying=False,
                                )
                                logger.info(
                                    f"Recalibrated strikes around "
                                    f"${self.current_price:.2f} "
                                    f"(±{self.strike_pct_range}% band, "
                                    f"max {self.strike_count_max} strikes/exp)"
                                )

                    # Cleanup expired strikes periodically
                    if iteration % STRIKE_CLEANUP_INTERVAL == 0:
                        self._cleanup_expired_strikes()

                    # Check max iterations
                    if max_iterations and iteration >= max_iterations:
                        logger.info(f"Reached max iterations ({max_iterations})")
                        break

                except Exception as e:
                    logger.error(f"Stream iteration error: {e}", exc_info=True)
                    time.sleep(max_wait)
        finally:
            # Always clean up the background stream threads.
            if self._accumulator is not None:
                self._accumulator.stop()
                self._accumulator = None
            if self._underlying_accumulator is not None:
                self._underlying_accumulator.stop()
                self._underlying_accumulator = None

        logger.info("Stream stopped")


def main():
    """Standalone streaming for testing"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="Stream real-time options data")
    parser.add_argument(
        "--underlying",
        default=os.getenv("INGEST_UNDERLYING", "SPY"),
        help="Underlying symbol or alias (default: SPY)",
    )
    parser.add_argument(
        "--expirations",
        type=int,
        default=int(os.getenv("INGEST_EXPIRATIONS", "3")),
        help="Number of expirations to track (default: 3)",
    )
    parser.add_argument(
        "--strike-count-max",
        type=int,
        default=int(os.getenv("INGEST_STRIKE_COUNT_MAX", "40")),
        help="Hard cap on strikes per expiration after the pct-range filter (default: 40)",
    )
    parser.add_argument(
        "--strike-pct-range",
        type=float,
        default=float(os.getenv("INGEST_STRIKE_PCT_RANGE", "3.0")),
        help="Strike-selection band as percent of spot, e.g. 3.0 -> ±3%% (default: 3.0)",
    )
    parser.add_argument(
        "--max-iterations", type=int, help="Maximum iterations (default: unlimited)"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        from src.utils import set_log_level

        set_log_level("DEBUG")

    print("\n" + "=" * 80)
    print("STREAM MANAGER - STANDALONE TEST")
    print("=" * 80)
    print(f"Underlying: {args.underlying}")
    print(f"Expirations: {args.expirations}")
    print(f"Strike Pct Range: ±{args.strike_pct_range}%")
    print(f"Strike Count Max: {args.strike_count_max} per expiration")
    if args.max_iterations:
        print(f"Max Iterations: {args.max_iterations}")
    else:
        print("Max Iterations: Unlimited (press Ctrl+C to stop)")
    print("=" * 80 + "\n")

    # Initialize client
    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true",
    )

    # Initialize stream manager
    manager = StreamManager(
        client=client,
        underlying=args.underlying,
        num_expirations=args.expirations,
        strike_count_max=args.strike_count_max,
        strike_pct_range=args.strike_pct_range,
    )

    # Initialize
    if not manager.initialize():
        print("❌ Failed to initialize stream manager")
        import sys

        sys.exit(1)

    print()

    # Track counts
    underlying_count = 0
    option_count = 0

    try:
        # Run stream and count yielded items
        for item in manager.stream(max_iterations=args.max_iterations):
            if item["type"] == "underlying":
                underlying_count += 1
                if underlying_count % 10 == 0:
                    data = item["data"]
                    print(
                        f"Underlying bars: {underlying_count} - Latest: "
                        f"${data['close']:.2f} "
                        f"(Up: {data['up_volume']:,}, Down: {data['down_volume']:,})"
                    )
            elif item["type"] == "option_batch":
                option_count += len(item["data"])
                if option_count % 100 < len(item["data"]):
                    print(f"Option quotes: {option_count}")

        print("\n" + "=" * 80)
        print("STREAM COMPLETE")
        print("=" * 80)
        print(f"✅ Underlying bars yielded: {underlying_count}")
        print(f"✅ Option quotes yielded: {option_count}")
        print("=" * 80 + "\n")
        print("NOTE: This standalone test only YIELDS data, it does NOT store it.")
        print("Use 'python run.py ingest' to stream AND store data in database.")
        print()

    except KeyboardInterrupt:
        print("\n\n⚠️  Stream interrupted by user")
        print(f"Results: {underlying_count} underlying, {option_count} options yielded")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Stream failed: {e}", exc_info=True)
        import sys

        sys.exit(1)


if __name__ == "__main__":
    main()
