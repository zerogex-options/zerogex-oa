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
    safe_float, safe_int, safe_datetime,
    validate_bar_data, get_market_session
)
from src.symbols import resolve_option_root, get_weekly_option_roots
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
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")

# Stream read timeout — how long the background reader waits for the next
# event before the socket times out (triggers a reconnect).
_STREAM_READ_TIMEOUT = int(os.getenv("TS_STREAM_READ_TIMEOUT", "300"))

# Possible field names for implied volatility across TradeStation payload variants
_IV_FIELD_NAMES = ("ImpliedVolatility", "IV", "Volatility", "IVol")


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
    ):
        self._client = client
        self._symbols = list(symbols)
        self._state: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._current_response = None
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
        self._thread = threading.Thread(
            target=self._reader_loop, daemon=True, name="option-stream",
        )
        self._thread.start()
        # Give the stream a moment to connect before returning.
        self._connected.wait(timeout=10)

    def stop(self):
        """Stop the background reader and close the stream connection."""
        self._running = False
        # Interrupt any blocking iter_lines() call.
        with self._response_lock:
            if self._current_response is not None:
                try:
                    self._current_response.close()
                except Exception:
                    pass
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

    @property
    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

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
        logger.info(
            f"Seeding option state from REST ({len(self._symbols)} symbols)..."
        )
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

    def _reader_loop(self):
        """Continuously read stream events; auto-reconnect on failure."""
        while self._running:
            try:
                self._read_stream()
            except Exception as e:
                if self._running:
                    logger.warning(
                        f"Option stream disconnected ({e}), reconnecting in 2s..."
                    )
                    time.sleep(2)

    def _read_stream(self):
        """Open one stream connection and read events until it ends."""
        symbols_str = ",".join(self._symbols)
        url = (
            f"{self._client.base_url}/marketdata/stream/quotes/{symbols_str}"
        )
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

            response.raise_for_status()
        except Exception:
            response.close()
            raise

        with self._response_lock:
            self._current_response = response

        self._connected.set()

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
                    continue

                if isinstance(payload, dict) and _is_auth_error_payload(payload):
                    logger.warning(
                        "Option stream reported auth error payload; refreshing token and reconnecting"
                    )
                    self._client.auth.force_refresh_access_token()
                    break

                # Handle both {"Quotes": [...]} wrappers and bare objects.
                if isinstance(payload, dict) and "Quotes" in payload:
                    for q in payload["Quotes"]:
                        if isinstance(q, dict):
                            self._merge_single_quote(q)
                elif isinstance(payload, dict) and "Symbol" in payload:
                    self._merge_single_quote(payload)
        finally:
            with self._response_lock:
                self._current_response = None
            response.close()

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
                "Last", "Bid", "Ask", "Mid", "Volume", "TimeStamp",
                "High", "Low", "Open", "Close", "NetChange",
                "NetChangePct", "BidSize", "AskSize",
            ):
                val = q.get(key)
                if val is not None:
                    merged[key] = val

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
            target=self._reader_loop, daemon=True, name="underlying-stream",
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
        url = (
            f"{self._client.base_url}/marketdata/stream/barcharts/{self._symbol}"
        )
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
                    "Underlying bar stream: 401 auth failure, "
                    "forcing token refresh and retrying"
                )
                self._client.auth.force_refresh_access_token()
                return

            response.raise_for_status()
        except Exception:
            response.close()
            raise

        logger.debug(
            "Underlying bar stream: connected (HTTP %s)", response.status_code
        )

        with self._response_lock:
            self._current_response = response

        self._connected.set()

        _heartbeat_count = 0
        _line_count = 0

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
                            "Underlying bar stream: %d heartbeats, "
                            "%d data lines so far",
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
                    logger.warning(
                        "Underlying bar stream: JSON decode failed, "
                        "skipping line: %s",
                        line[:200],
                    )
                    continue

                if isinstance(payload, dict) and _is_auth_error_payload(payload):
                    logger.warning(
                        "Underlying bar stream reported auth error payload; refreshing token and reconnecting"
                    )
                    self._client.auth.force_refresh_access_token()
                    break

                # Handle various bar payload shapes.
                bars: list = []
                if isinstance(payload, dict) and "Bars" in payload:
                    bars = payload["Bars"]
                elif isinstance(payload, dict) and "Bar" in payload and isinstance(payload["Bar"], dict):
                    bars = [payload["Bar"]]
                elif isinstance(payload, dict) and "TimeStamp" in payload:
                    bars = [payload]

                if not bars:
                    logger.debug(
                        "Underlying bar stream: received payload with "
                        "no bar data: keys=%s",
                        list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
                    )

                for bar in bars:
                    self._merge_bar(bar)
        finally:
            with self._response_lock:
                self._current_response = None
            response.close()

    def _merge_bar(self, bar: dict):
        """Merge one raw bar into accumulated state with carry-forward."""
        if not validate_bar_data(bar):
            return

        timestamp_str = bar.get("TimeStamp", "")
        timestamp = safe_datetime(timestamp_str, field_name="TimeStamp")
        if not timestamp:
            timestamp = datetime.now(ET)

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
        db_underlying: str = None,
        num_expirations: int = 3,
        num_strikes: int = 10,
    ):
        """Initialize stream manager"""
        self.client = client
        self.underlying = underlying.upper()           # TradeStation API symbol for underlying (e.g. "$SPX.X")
        self.db_underlying = (db_underlying or underlying).upper()  # canonical alias for DB (e.g. "SPX")
        self.option_root = resolve_option_root(self.underlying)  # option root for expirations/chains (e.g. "SPXW")
        self.num_expirations = num_expirations
        self.num_strikes = num_strikes  # number of strikes to track on each side of current price

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
        self.seed_rest_on_recalc = (
            os.getenv("OPTION_REST_SEED_ON_RECALC", "false").lower() == "true"
        )

        logger.info(f"Initialized StreamManager for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, {num_strikes} strikes each side")
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
                f"Bar (REST): {self.underlying} @ {timestamp} "
                f"C=${underlying_data['close']:.2f}"
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
                return price

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
        if (last_refresh_et.date() < now_et.date() or 
            (last_refresh_et.date() == now_et.date() and 
             last_refresh_et.time() < market_close_time and 
             now_et.time() >= market_close_time)):
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
            logger.info(f"Expirations changed:")
            logger.info(f"  Old: {[str(exp) for exp in self.target_expirations]}")
            logger.info(f"  New: {[str(exp) for exp in new_expirations]}")

            # Update expirations
            self.target_expirations = new_expirations

            # Rebuild option symbols with new expirations
            if self.current_price:
                self.tracked_option_symbols = self._build_option_symbols()
                logger.info(f"Rebuilt {len(self.tracked_option_symbols)} option symbols with new expirations")

            # Update refresh timestamp
            self.last_expiration_refresh = datetime.now(ET)

            return True

        except Exception as e:
            logger.error(f"Error refreshing expirations: {e}", exc_info=True)
            return False

    def _get_target_expirations(self) -> List[date]:
        """Get target expiration dates.

        Always queries get_option_expirations(self.underlying) — e.g. "$SPX.X" — since
        that is the symbol TradeStation uses for expiration/strike structure lookups.
        self.option_root (e.g. "SPXW") is only used later inside build_option_symbol()
        when constructing the actual option chain symbols for get_option_quotes().

        If self.option_root is listed in OPTION_WEEKLY_ROOTS, the returned dates are
        filtered to Mon/Wed/Fri only, because building a "SPXW ..." symbol for a
        non-weekly expiration would be rejected by the TradeStation API.
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

            # If the option root is weekly-only (e.g. SPXW), keep only Mon/Wed/Fri dates
            if self.option_root in get_weekly_option_roots():
                future_expirations = [exp for exp in future_expirations if exp.weekday() in (0, 2, 4)]
                logger.info(f"Filtered to weekly expirations for {self.option_root} (Mon/Wed/Fri)")

            # Take first N
            target_exps = future_expirations[:self.num_expirations]

            logger.info(f"Target expirations: {[str(exp) for exp in target_exps]}")
            return target_exps

        except Exception as e:
            logger.error(f"Error fetching expirations: {e}", exc_info=True)
            return []

    def _get_strikes_near_price(self, expiration: date, current_price: float) -> List[float]:
        """Get the N nearest strikes on each side of the current price."""
        try:
            exp_str = expiration.strftime("%m-%d-%Y")
            all_strikes = self.client.get_option_strikes(self.underlying, expiration=exp_str)

            if not all_strikes:
                logger.warning(f"No strikes found for exp {exp_str}")
                return []

            below = sorted([s for s in all_strikes if s <= current_price], reverse=True)[:self.num_strikes]
            above = sorted([s for s in all_strikes if s > current_price])[:self.num_strikes]
            nearby_strikes = sorted(below + above)

            logger.debug(f"Exp {exp_str}: {len(nearby_strikes)} strikes "
                        f"({len(below)} below, {len(above)} above ${current_price:.2f})")

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
                    "This usually means the option symbol format is not accepted by TradeStation quotes endpoint "
                    "for this underlying."
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

        logger.info(f"✅ Initialization complete:")
        logger.info(f"   Price: ${self.current_price:.2f}")
        logger.info(f"   Tracking {len(self.target_expirations)} expirations")
        logger.info(f"   Tracking {len(self.tracked_option_symbols)} option contracts")

        return True

    def _start_accumulators(self, seed_option_rest: bool = True):
        """Start (or restart) background stream readers for options and underlying."""
        # Stop existing accumulators if any.
        if self._accumulator is not None:
            self._accumulator.stop()
        if self._underlying_accumulator is not None:
            self._underlying_accumulator.stop()

        self._wakeup.clear()

        self._accumulator = OptionStreamAccumulator(
            client=self.client,
            symbols=self.tracked_option_symbols,
            wakeup=self._wakeup,
        )
        self._underlying_accumulator = UnderlyingBarAccumulator(
            client=self.client,
            symbol=self.underlying,
            db_symbol=self.db_underlying,
            session_template=SESSION_TEMPLATE,
            wakeup=self._wakeup,
        )
        self._accumulator.start(seed_from_rest=seed_option_rest)
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

            volume = safe_int(
                raw.get("Volume"), default=None, field_name="Volume"
            )

            open_interest = safe_int(
                raw.get("DailyOpenInterest"),
                default=None,
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

            results.append({
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
            })

        return results

    def stream(
        self,
        max_iterations: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
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
        # Stale stream detection: consecutive cycles with no underlying data.
        _consecutive_empty_underlying = 0
        _STALE_UNDERLYING_THRESHOLD = 10  # warn after this many empty drains
        _last_bar_updates = 0  # track updates_received delta

        try:
            while True:
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

                # --- block until data arrives or timeout for housekeeping ---
                # Clear before waiting so signals arriving during processing
                # are not lost (set-before-clear race).
                self._wakeup.clear()
                self._wakeup.wait(timeout=max_wait)

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
                        self._start_accumulators()
                    else:
                        logger.warning(
                            "⚠️  Expiration refresh failed, continuing "
                            "with current expirations"
                        )

                try:
                    # --- underlying stream health checks ---
                    if not self._underlying_accumulator.is_alive:
                        logger.error(
                            "Underlying bar stream thread is DEAD — "
                            "restarting accumulators"
                        )
                        self._start_accumulators()

                    # Drain underlying bar from persistent stream.
                    underlying_data = self._underlying_accumulator.drain()
                    if underlying_data:
                        self.current_price = underlying_data["close"]
                        yield {"type": "underlying", "data": underlying_data}
                        _total_underlying_yields += 1
                        _consecutive_empty_underlying = 0
                    else:
                        _consecutive_empty_underlying += 1
                        if (
                            _consecutive_empty_underlying
                            == _STALE_UNDERLYING_THRESHOLD
                        ):
                            cur_updates = (
                                self._underlying_accumulator.updates_received
                            )
                            logger.warning(
                                "Underlying bar stream appears STALE: "
                                "%d consecutive empty drains, "
                                "bar_stream_updates=%d (delta=%d), "
                                "thread_alive=%s",
                                _consecutive_empty_underlying,
                                cur_updates,
                                cur_updates - _last_bar_updates,
                                self._underlying_accumulator.is_alive,
                            )
                        elif (
                            _consecutive_empty_underlying
                            > _STALE_UNDERLYING_THRESHOLD
                            and _consecutive_empty_underlying % 50 == 0
                        ):
                            logger.warning(
                                "Underlying bar stream still stale: "
                                "%d consecutive empty drains",
                                _consecutive_empty_underlying,
                            )

                    # Drain only option contracts that changed since last cycle.
                    changed = self._accumulator.drain()
                    if changed:
                        option_results = self._yield_option_snapshot(changed)

                        if option_results:
                            option_count = len(option_results)
                            _total_option_batches += 1
                            _total_options_yielded += option_count

                            option_with_oi = sum(
                                1 for o in option_results
                                if (o.get("open_interest") or 0) > 0
                            )
                            option_with_volume = sum(
                                1 for o in option_results
                                if (o.get("volume") or 0) > 0
                            )

                            tracked_total = len(self.tracked_option_symbols)
                            oi_coverage = option_with_oi / option_count
                            volume_coverage = option_with_volume / option_count

                            logger.info(
                                f"Option batch: {option_count} updated, "
                                f"{tracked_total - option_count} unchanged, "
                                f"oi_coverage={oi_coverage:.1%}, "
                                f"volume_coverage={volume_coverage:.1%}, "
                                f"stream_updates={self._accumulator.updates_received}"
                            )

                            if oi_coverage < self.option_oi_coverage_alert_threshold:
                                logger.warning(
                                    f"⚠️ Low option OI coverage: {oi_coverage:.1%} "
                                    f"(threshold "
                                    f"{self.option_oi_coverage_alert_threshold:.1%})"
                                )
                            if volume_coverage < self.option_volume_coverage_alert_threshold:
                                logger.warning(
                                    f"⚠️ Low option volume coverage: "
                                    f"{volume_coverage:.1%} "
                                    f"(threshold "
                                    f"{self.option_volume_coverage_alert_threshold:.1%})"
                                )

                            yield {"type": "option_batch", "data": option_results}
                    else:
                        _total_empty_cycles += 1

                    # --- observability: periodic metrics summary ---
                    if iteration % _METRICS_LOG_INTERVAL == 0:
                        elapsed = time.monotonic() - _last_metrics_time
                        cycle_ms = (time.monotonic() - cycle_start) * 1000
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
                        _last_bar_updates = (
                            self._underlying_accumulator.updates_received
                        )
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
                                self.tracked_option_symbols = (
                                    self._build_option_symbols()
                                )
                                self._start_accumulators(
                                    seed_option_rest=self.seed_rest_on_recalc
                                )
                                logger.info(
                                    f"Recalibrated strikes around "
                                    f"${self.current_price:.2f} "
                                    f"(±{self.num_strikes} strikes each side)"
                                )

                    # Cleanup expired strikes periodically
                    if iteration % STRIKE_CLEANUP_INTERVAL == 0:
                        self._cleanup_expired_strikes()

                    # Check max iterations
                    if max_iterations and iteration >= max_iterations:
                        logger.info(
                            f"Reached max iterations ({max_iterations})"
                        )
                        break

                except Exception as e:
                    logger.error(
                        f"Stream iteration error: {e}", exc_info=True
                    )
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
    parser.add_argument("--underlying", default=os.getenv("INGEST_UNDERLYING", "SPY"),
                       help="Underlying symbol or alias (default: SPY)")
    parser.add_argument("--expirations", type=int,
                       default=int(os.getenv("INGEST_EXPIRATIONS", "3")),
                       help="Number of expirations to track (default: 3)")
    parser.add_argument("--num-strikes", type=int,
                       default=int(os.getenv("INGEST_STRIKE_COUNT", "10")),
                       help="Number of strikes to track on each side of current price (default: 10)")
    parser.add_argument("--max-iterations", type=int,
                       help="Maximum iterations (default: unlimited)")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        from src.utils import set_log_level
        set_log_level("DEBUG")

    print("\n" + "="*80)
    print("STREAM MANAGER - STANDALONE TEST")
    print("="*80)
    print(f"Underlying: {args.underlying}")
    print(f"Expirations: {args.expirations}")
    print(f"Strikes Each Side: {args.num_strikes}")
    if args.max_iterations:
        print(f"Max Iterations: {args.max_iterations}")
    else:
        print("Max Iterations: Unlimited (press Ctrl+C to stop)")
    print("="*80 + "\n")

    # Initialize client
    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"
    )

    # Initialize stream manager
    manager = StreamManager(
        client=client,
        underlying=args.underlying,
        num_expirations=args.expirations,
        num_strikes=args.num_strikes,
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
                    print(f"Underlying bars: {underlying_count} - Latest: "
                          f"${data['close']:.2f} "
                          f"(Up: {data['up_volume']:,}, Down: {data['down_volume']:,})")
            elif item["type"] == "option_batch":
                option_count += len(item["data"])
                if option_count % 100 < len(item["data"]):
                    print(f"Option quotes: {option_count}")

        print("\n" + "="*80)
        print("STREAM COMPLETE")
        print("="*80)
        print(f"✅ Underlying bars yielded: {underlying_count}")
        print(f"✅ Option quotes yielded: {option_count}")
        print("="*80 + "\n")
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
