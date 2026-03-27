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

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        """Seed state from REST, then begin background stream reading."""
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

        if response.status_code == 401:
            response.close()
            self._client.auth.force_refresh_access_token()
            return  # will retry on next loop iteration

        response.raise_for_status()

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
            for iv_key in ("ImpliedVolatility", "IV", "Volatility", "IVol"):
                val = q.get(iv_key)
                if val is not None:
                    try:
                        if float(val) > 0:
                            merged[iv_key] = val
                    except (ValueError, TypeError):
                        pass

            self._state[symbol] = merged
            self._updates_received += 1


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
        # Last seen underlying bar snapshot keyed by minute bucket, used to merge
        # partial stream bar payloads that may omit one side of volume.
        self._underlying_bar_state: Dict[datetime, Dict[str, Any]] = {}
        # Pre-parsed metadata (strike, expiration, option_type) per option symbol
        # so we don't re-parse the symbol string every poll cycle.
        self._symbol_metadata: Dict[str, Dict[str, Any]] = {}
        # Background accumulator for persistent option quote streaming.
        self._accumulator: Optional[OptionStreamAccumulator] = None
        # Last-yielded raw TimeStamp string per contract so we only write to
        # the DB when the stream has actually delivered a newer update.
        self._last_yielded_ts: Dict[str, str] = {}

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

        logger.info(f"Initialized StreamManager for {underlying}")
        logger.info(f"Config: {num_expirations} expirations, {num_strikes} strikes each side")

    def _fetch_underlying_bar(self) -> Optional[Dict[str, Any]]:
        """
        Fetch latest underlying bar with volume breakdown using Stream Bars API.

        Returns bar data with OHLC + UpVolume/DownVolume
        """
        try:
            # Use stream bars API with barsback=1 to fetch the latest bar payload
            bars_data = self.client.get_stream_bars(
                symbol=self.underlying,
                interval=1,
                unit="Minute",
                barsback=1,
                sessiontemplate=SESSION_TEMPLATE,
                warn_if_closed=False
            )

            if "Bars" not in bars_data or len(bars_data["Bars"]) == 0:
                logger.debug(f"No bar data returned for {self.underlying} - likely between bars or market just opened")
                return None

            bar = bars_data["Bars"][0]

            # Validate bar data
            if not validate_bar_data(bar):
                logger.warning("Invalid bar data, skipping")
                return None

            # Parse bar timestamp
            timestamp_str = bar.get("TimeStamp", "")
            timestamp = safe_datetime(timestamp_str, field_name="TimeStamp")

            if not timestamp:
                timestamp = datetime.now(ET)

            minute_bucket = timestamp.replace(second=0, microsecond=0)
            prior_bar = self._underlying_bar_state.get(minute_bucket, {})

            raw_up_volume = bar.get("UpVolume")
            raw_down_volume = bar.get("DownVolume")
            raw_total_volume = bar.get("TotalVolume")

            up_volume = safe_int(raw_up_volume, field_name="UpVolume")
            down_volume = safe_int(raw_down_volume, field_name="DownVolume")
            total_volume = safe_int(raw_total_volume, field_name="TotalVolume")

            # Stream bar payloads can be partial. If a field is omitted, carry
            # forward the last seen value for this minute bucket.
            if raw_up_volume in (None, "", "N/A"):
                up_volume = prior_bar.get("up_volume", up_volume)
            if raw_down_volume in (None, "", "N/A"):
                down_volume = prior_bar.get("down_volume", down_volume)
            if raw_total_volume in (None, "", "N/A"):
                total_volume = prior_bar.get("volume", total_volume)

            # Parse OHLCV with volume breakdown
            underlying_data = {
                "symbol": self.db_underlying,
                "timestamp": timestamp,
                "open": safe_float(bar.get("Open"), field_name="Open"),
                "high": safe_float(bar.get("High"), field_name="High"),
                "low": safe_float(bar.get("Low"), field_name="Low"),
                "close": safe_float(bar.get("Close"), field_name="Close"),
                "up_volume": up_volume,
                "down_volume": down_volume,
                "volume": total_volume,
            }

            self._underlying_bar_state[minute_bucket] = {
                "up_volume": underlying_data["up_volume"],
                "down_volume": underlying_data["down_volume"],
                "volume": underlying_data["volume"],
            }

            # Evict stale minute buckets to prevent unbounded memory growth.
            stale_keys = [k for k in self._underlying_bar_state if k < minute_bucket]
            for k in stale_keys:
                del self._underlying_bar_state[k]

            logger.debug(f"Bar: {self.underlying} @ {timestamp} "
                        f"C=${underlying_data['close']:.2f} "
                        f"UpVol={underlying_data['up_volume']:,} "
                        f"DownVol={underlying_data['down_volume']:,}")

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

    def _start_accumulator(self):
        """Start (or restart) the background option quote stream."""
        if self._accumulator is not None:
            self._accumulator.stop()
        self._accumulator = OptionStreamAccumulator(
            client=self.client,
            symbols=self.tracked_option_symbols,
        )
        self._accumulator.start()
        # Reset change-tracking so the fresh REST seed gets yielded.
        self._last_yielded_ts.clear()

    def _yield_option_snapshot(self, state: Dict[str, Dict[str, Any]]):
        """
        Convert raw accumulator state into yielded option data dicts.

        Only yields a contract when the stream has delivered a newer
        update (compared by raw TimeStamp string) since the last time
        we yielded it.  This prevents writing duplicate rows to the DB
        when a contract has no new activity.

        Returns a list (not a generator) so callers can count results.
        """
        results = []
        for option_symbol in self.tracked_option_symbols:
            raw = state.get(option_symbol)
            if not raw:
                continue  # no data received yet for this contract

            meta = self._symbol_metadata.get(option_symbol)
            if not meta:
                continue

            # Skip if the stream hasn't delivered a newer update.
            raw_ts = raw.get("TimeStamp", "")
            prev_ts = self._last_yielded_ts.get(option_symbol)
            if prev_ts is not None and raw_ts == prev_ts:
                continue

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
            for iv_field in ("ImpliedVolatility", "IV", "Volatility", "IVol"):
                iv_val = safe_float(raw.get(iv_field), field_name=iv_field)
                if iv_val and iv_val > 0:
                    implied_volatility = iv_val
                    break

            self._last_yielded_ts[option_symbol] = raw_ts

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
        Option quotes are accumulated by a background thread reading from
        a persistent streaming connection; this method snapshots that state
        on each poll interval and yields all contracts.

        Yields dictionaries with:
            {
                'type': 'underlying' | 'option',
                'data': {...}
            }
        """
        if not self.tracked_option_symbols:
            logger.error("Not initialized. Call initialize() first.")
            return

        logger.info("Starting stream loop...")
        logger.info("Press Ctrl+C to stop")

        # Start persistent background stream for option quotes.
        self._start_accumulator()

        iteration = 0

        try:
            while True:
                iteration += 1

                # Get current market session for dynamic polling
                session = get_market_session()

                # Determine poll interval based on session
                if session == "regular":
                    poll_interval = MARKET_HOURS_POLL_INTERVAL
                elif session in ["pre-market", "after-hours"]:
                    poll_interval = EXTENDED_HOURS_POLL_INTERVAL
                else:  # closed
                    poll_interval = CLOSED_HOURS_POLL_INTERVAL

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
                        # Symbols may have changed — restart accumulator.
                        self._start_accumulator()
                    else:
                        logger.warning(
                            "⚠️  Expiration refresh failed, continuing "
                            "with current expirations"
                        )

                try:
                    # Fetch underlying bar using Stream Bars API
                    # (single symbol — stream snapshot works well here)
                    underlying_data = self._fetch_underlying_bar()

                    if underlying_data:
                        self.current_price = underlying_data["close"]
                        yield {"type": "underlying", "data": underlying_data}

                    # Snapshot accumulated option state from background stream.
                    state = self._accumulator.snapshot()
                    option_results = self._yield_option_snapshot(state)

                    option_count = len(option_results)
                    option_with_oi = 0
                    option_with_volume = 0

                    for option_data in option_results:
                        yield {"type": "option", "data": option_data}
                        if (option_data.get("open_interest") or 0) > 0:
                            option_with_oi += 1
                        if (option_data.get("volume") or 0) > 0:
                            option_with_volume += 1

                    tracked_total = len(self.tracked_option_symbols)
                    skipped = tracked_total - option_count

                    if option_count > 0:
                        oi_coverage = option_with_oi / option_count
                        volume_coverage = option_with_volume / option_count
                        logger.info(
                            f"Option snapshot: yielded={option_count}, "
                            f"unchanged={skipped}, "
                            f"oi_coverage={oi_coverage:.1%}, "
                            f"volume_coverage={volume_coverage:.1%}, "
                            f"stream_updates={self._accumulator.updates_received}"
                        )
                    elif skipped > 0:
                        logger.debug(
                            f"Option snapshot: no new updates "
                            f"({skipped} contracts unchanged)"
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

                    # Recalibrate strike range periodically.
                    if iteration % STRIKE_RECALC_INTERVAL == 0 and iteration > 0:
                        if self.current_price:
                            new_price = self._get_underlying_price()
                            if new_price:
                                self.current_price = new_price
                                self.tracked_option_symbols = (
                                    self._build_option_symbols()
                                )
                                # Restart accumulator with new symbol set.
                                self._start_accumulator()
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

                    # Sleep with dynamic interval
                    logger.debug(f"Sleeping for {poll_interval}s...")
                    time.sleep(poll_interval)

                except Exception as e:
                    logger.error(
                        f"Stream iteration error: {e}", exc_info=True
                    )
                    time.sleep(poll_interval)
        finally:
            # Always clean up the background stream thread.
            if self._accumulator is not None:
                self._accumulator.stop()
                self._accumulator = None

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
            elif item["type"] == "option":
                option_count += 1
                if option_count % 100 == 0:
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
