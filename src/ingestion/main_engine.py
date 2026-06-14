"""
ZeroGEX Main Ingestion Engine

This engine:
1. Streams real-time data using StreamManager
2. Handles 1-minute aggregation
3. Calculates Greeks for options (if enabled)
4. Stores data in PostgreSQL/TimescaleDB
5. Monitors data quality and pipeline health
"""

import os
import random
import signal
import sys
import hashlib
import json
import threading
import time
import time as _time
from dataclasses import dataclass
from multiprocessing import Process
from datetime import datetime, date as _date, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict
import pytz
from psycopg2.extras import execute_values

from src.ingestion.tradestation_client import TradeStationClient
from src.ingestion.stream_manager import StreamManager
from src.ingestion.greeks_calculator import GreeksCalculator
from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.validation import (
    bucket_timestamp,
    cash_session_date,
    cash_session_start_utc,
    get_market_session,
    is_engine_run_window,
    seconds_until_engine_run_window,
    underlying_feed_expected,
)
from src.symbols import parse_underlyings, get_canonical_symbol
from src.config import (
    AGGREGATION_BUCKET_SECONDS,
    MAX_BUFFER_SIZE,
    BUFFER_FLUSH_INTERVAL,
    GREEKS_ENABLED,
    INGEST_PARITY_GUARD_ENABLED,
    OPTION_BUCKET_WRITE_MIN_SECONDS,
    FLOW_CLASSIFY_MID_BAND_PCT,
    FLOW_CLASSIFY_SKIP_OPEN_AUCTION,
    FLOW_CLASSIFY_PRIOR_TICK_MAX_AGE_SECONDS,
    SESSION_TEMPLATE,
    resolve_dividend_yield,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


def _greeks_max_age_for_session(session: str, base: float, extended: float) -> float:
    """Max tolerated underlying-price age (seconds) for Greeks in *session*.

    The regular cash session has dense ~60s underlying bars; in
    pre/after-hours an equity/ETF trades thinly and its 1-minute bars are
    legitimately minutes apart (cash indices don't print extended hours and
    are excluded upstream by ``underlying_feed_expected``), so the tight
    regular-session gate would refuse Greeks for the entire extended
    session. Use the wider gate outside the regular session.
    """
    if session in ("pre-market", "after-hours"):
        return extended
    return base


@dataclass
class _FlowAccumulator:
    """Per-contract session-cumulative classified flow state.

    Single source of truth for an option's running totals within one ET
    session: TS-reported cumulative volume, Lee-Ready-classified
    ask/mid/bid cumulative flow, and the most recent NBBO used as the
    prior tick for the next classification.

    The downstream ``flow_contract_facts`` derivation (api/database.py)
    recovers per-bucket flow via ``LAG()`` deltas of these cumulative
    columns, matching what it already does for the cumulative ``volume``
    column.  Writing per-bucket additive values (the prior design) leaked
    signal when consecutive buckets had similar magnitudes (the LAG of
    ``[10, 10]`` is 0 even though 10 trades occurred in the second
    bucket).  Storing cumulative makes the writer and the consumer agree.

    Instances are keyed by ``(option_symbol, session_date_ET)``; a new
    session creates a fresh instance hydrated from the latest persisted
    row for that contract in the new session (or zeros if none exists).
    """

    session_date: _date
    last_volume_cum: int
    ask_cum: int
    mid_cum: int
    bid_cum: int
    last_bid: Optional[float] = None
    last_ask: Optional[float] = None
    last_mid: Optional[float] = None
    # Timestamp of the snapshot whose NBBO is currently stored in
    # last_bid/last_ask/last_mid.  Used to age-check the prior tick before
    # trusting it as the pre-trade quote (see _select_classify_quote).
    last_quote_ts: Optional[datetime] = None


def _compute_db_backoff_seconds(consecutive_failures: int) -> float:
    """Exponential backoff in seconds with 0–10% jitter.

    Lives at module scope so both upsert call sites share a single
    backoff policy and so the policy is unit-testable without booting
    the engine. Base is `2^N` capped at 60s; jitter is uniform on
    [0, base * 0.1) so concurrent workers hitting the same DB blip
    don't retry in lockstep.
    """
    base = min(2**consecutive_failures, 60)
    return base + random.uniform(0, base * 0.1)  # type: ignore[no-any-return]


# Minimum gap between rolled-up "[CIRCUIT-BREAKER] Skipping write" WARNINGs
# while in backoff. The first skip after a fresh backoff opens still logs
# immediately so the cause is visible; subsequent skips are coalesced into
# a single rolled-up line per interval. Without this, a 2s backoff during
# a brief DB outage produces ~1000 identical warnings (~1044 observed in a
# 2-3s burst during the 2026-06 RDS restart) because every batch attempt
# from the live option stream hits the early-return + log path.
_CB_SKIP_LOG_INTERVAL_SECONDS = float(os.getenv("CIRCUIT_BREAKER_SKIP_LOG_INTERVAL_SECONDS", "5.0"))


def _to_db_float(value: Any) -> Optional[float]:
    """Convert numeric-like values (including numpy scalars) to plain float for DB writes."""
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:  # NaN check
        return None
    return parsed


class IngestionEngine:
    """
    Main ingestion engine - forward-only streaming with storage

    StreamManager fetches data, IngestionEngine stores it.
    """

    def __init__(
        self,
        client: TradeStationClient,
        underlying: str = "SPY",
        num_expirations: int = 3,
        strike_count_max: int = 40,
        strike_pct_range: float = 3.0,
    ):
        """Initialize main ingestion engine"""
        self.client = client
        self.underlying = underlying.upper()  # TradeStation API symbol (e.g. "$SPX.X")
        self.db_symbol = get_canonical_symbol(
            self.underlying
        )  # canonical alias for DB (e.g. "SPX")
        self.num_expirations = num_expirations
        self.strike_count_max = strike_count_max
        self.strike_pct_range = strike_pct_range

        self.running = False

        # Buffering for options only (underlying writes every update)
        self.underlying_buffer: List[Dict[str, Any]] = []
        self.options_buffer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        # Per-contract session-cumulative classified-flow accumulators.
        # Replaces the prior baseline-cache + SEED_FLAG + last-quote-cache
        # stack: each ``_FlowAccumulator`` holds the running cumulative
        # volume / ask / mid / bid totals for one contract in one ET
        # session, plus the most recent NBBO used as the next snapshot's
        # prior tick.  Hydrated from the DB on first observation of a
        # (contract, session) pair; the in-memory state is then the
        # source of truth for the rest of the session.
        self._option_flow: Dict[str, _FlowAccumulator] = {}
        self._option_flow_lock = threading.Lock()

        # Track latest underlying price for Greeks calculation.  Pair it
        # with the timestamp of the underlying bar so we can refuse to
        # compute Greeks against a price that's drifted: delta and gamma
        # are highly sensitive to S near strike, and a 5-minute-stale
        # price across a 10bp move would produce nonsense Greeks that
        # then get persisted to option_chains and propagate through every
        # downstream calculation.
        self.latest_underlying_price: Optional[float] = None
        self.latest_underlying_timestamp: Optional[datetime] = None
        self.greeks_max_underlying_age_seconds = float(
            os.getenv("GREEKS_MAX_UNDERLYING_AGE_SECONDS", "90")
        )
        # Pre/after-hours an equity/ETF underlying trades thinly and its
        # 1-minute bars are legitimately minutes apart, so the tight
        # regular-session gate above would refuse Greeks for the whole
        # extended session. Defaults to the stream watchdog's extended
        # STALE-warn threshold so the two mechanisms stay coherent: Greeks
        # are refused only once the feed itself is considered stale.
        self.greeks_max_underlying_age_seconds_extended = float(
            os.getenv("GREEKS_MAX_UNDERLYING_AGE_SECONDS_EXTENDED", "300")
        )
        # Counter so operators can see how often staleness rejects fire.
        # ``_total`` advances on every reject (in- and out-of-session);
        # ``_in_session`` advances only when the underlying feed is
        # actually expected to be live. The WARN message reports both so
        # SPX's overnight DEBUG-path noise (cash index doesn't print
        # pre/after-hours, options do — millions of expected rejects
        # accumulate by morning) doesn't make the in-session count look
        # like a 100k+ data-quality incident at the open.
        self.greeks_stale_underlying_rejects = 0
        self.greeks_stale_underlying_rejects_in_session = 0
        # Time-based throttle for the in-session staleness WARNING. A dense
        # option stream produces thousands of rejects/min against a single
        # stale underlying, so the previous per-100-reject gate collapsed to
        # a multi-Hz journal flood at the open. Warn at most once per
        # interval instead, mirroring the stream watchdog's time-based
        # cadence so the log reflects how long the feed has been stale, not
        # how fast options tick. ``_greeks_stale_last_warn_mono`` starts at
        # 0.0 so the first reject of a run warns immediately.
        self.greeks_stale_warn_interval_seconds = float(
            os.getenv("GREEKS_STALE_WARN_INTERVAL_SECONDS", "60")
        )
        self._greeks_stale_last_warn_mono = 0.0
        # Watchdog: when the underlying price stays stale in-session past
        # this many seconds, the in-process restart hooks have failed and
        # we exit nonzero so systemd (Restart=always) recycles the process.
        # Sized well above the stream watchdog's full retry budget
        # (UNDERLYING_STREAM_MAX_RESTART_ATTEMPTS × cooldown + a backoff
        # retry interval) so a healthy in-process recovery never trips
        # this. Set to 0 to disable. The 2026-06 prod incident sat in a
        # broken state for 17h with 1.1M rejects because no escalation
        # path existed; this is the last line of defense.
        self.greeks_stale_fatal_seconds = float(os.getenv("GREEKS_STALE_FATAL_SECONDS", "1800"))
        # Wall-clock monotonic timestamp of the current stale episode's
        # start, or None when Greeks are flowing. Reset to None on the
        # first successful Greek calc after a stale window.
        self._greeks_stale_episode_started_mono: Optional[float] = None
        # Counter for crossed/missing-quote fallbacks in _classify_volume_chunk.
        self._classify_fallback_count: int = 0

        # Greeks calculator (initialize if enabled). Resolve this symbol's
        # dividend yield q once here (each worker is single-symbol), so the
        # per-symbol DIVIDEND_YIELD_BY_SYMBOL override applies; falls back to
        # the scalar DIVIDEND_YIELD for symbols not in the map.
        self.greeks_calculator = None
        if GREEKS_ENABLED:
            self.greeks_calculator = GreeksCalculator(
                dividend_yield=resolve_dividend_yield(self.db_symbol)
            )
            logger.info("✅ Greeks calculation ENABLED")
            logger.info("   Note: Will use mid-price for IV calculation if API doesn't provide IV")
        else:
            logger.info("⚠️  Greeks calculation DISABLED (set GREEKS_ENABLED=true to enable)")

        # Metrics
        self.underlying_bars_stored = 0
        self.option_quotes_stored = 0
        self.greeks_calculated = 0
        self.last_flush_time = datetime.now(ET)
        self.errors_count = 0

        # Observability: write-path performance counters (reset on log).
        self._obs_batches_written = 0
        self._obs_rows_written = 0
        self._obs_write_time_ms = 0.0
        self._obs_last_log = _time.monotonic()

        # Circuit breaker: stop hammering a dead database.
        self._db_consecutive_failures = 0
        self._db_backoff_until = 0.0  # monotonic timestamp
        # Throttle the per-batch "skipping write" warning. A wide universe
        # (e.g. 3×~2000 symbols) generates hundreds of write attempts per
        # second; emitting one WARNING per skipped batch produced ~1044
        # warnings in 2-3s during the 33s RDS restart observed in production.
        # Track the first skip in the current backoff window + roll-up
        # counters and emit at most one line per
        # ``_CB_SKIP_LOG_INTERVAL_SECONDS``.
        self._cb_skip_window_first_logged = False
        self._cb_skip_count_since_last_log = 0
        self._cb_skip_rows_since_last_log = 0
        self._cb_skip_last_log_mono = 0.0
        # Computed-but-unpersisted option aggregates are retained here and
        # re-submitted on the next write attempt. Without this, a DB write
        # failure (or a circuit-breaker skip) at/after a bucket rollover
        # permanently loses that bucket's classified flow: _prepare_option_agg
        # has already cleared the buffer and the accumulator has already
        # advanced past those snapshots.  Re-submission is unconditionally
        # safe: the upsert now uses ``GREATEST`` on every monotonic field
        # (ask/mid/bid_volume, volume, open_interest), so a row that was
        # actually committed by a previous attempt becomes a no-op via the
        # WHERE-clause guard the second time it's sent.  Bounded so a
        # prolonged outage can't grow unbounded.
        self._pending_failed_option_rows: List[Dict[str, Any]] = []
        self._pending_failed_option_rows_max = int(
            os.getenv("OPTION_FAILED_ROWS_RETAIN_MAX", "20000")
        )
        self._last_underlying_signature: Optional[str] = None
        self._option_bucket_last_write: Dict[tuple[str, datetime], float] = {}

        # Active StreamManager during run_streaming(). Held so _signal_handler
        # can wake its idle wait — otherwise the loop sits on its wakeup for
        # up to the full extended-hours poll interval (30s) before noticing
        # self.running flipped, blowing past systemd's TimeoutStopSec.
        self._active_stream_manager: Optional[StreamManager] = None

        logger.info(f"Initialized IngestionEngine for {underlying}")
        logger.info(
            f"Config: {num_expirations} expirations, "
            f"±{strike_pct_range}% strike band (max {strike_count_max} strikes/exp)"
        )

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Initialize database
        self._initialize_database()
        self._ensure_symbol_exists()

    def _infer_asset_type(self, symbol: str) -> str:
        """Infer a sensible asset type for symbols table bootstrap."""
        if symbol.startswith("$"):
            return "INDEX"
        if symbol in {"SPY", "QQQ", "IWM", "DIA"}:
            return "ETF"
        return "EQUITY"

    def _ensure_symbol_exists(self):
        """Ensure underlying exists in symbols table (required by FK on underlying_quotes)."""
        try:
            symbol_payload = {
                "symbol": self.db_symbol,
                "name": self.db_symbol,
                "asset_type": self._infer_asset_type(self.underlying),
                "is_active": True,
            }
            self._log_parity_signature("symbols", symbol_payload)

            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO symbols (symbol, name, asset_type, is_active)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (symbol) DO UPDATE SET
                        is_active = TRUE,
                        updated_at = NOW()
                    """,
                    (
                        symbol_payload["symbol"],
                        symbol_payload["name"],
                        symbol_payload["asset_type"],  # ts_symbol has $ prefix for indexes
                    ),
                )
                conn.commit()
            logger.info(f"✅ Ensured symbols row exists for {self.db_symbol}")
        except Exception as e:
            logger.error(f"Error ensuring symbols row for {self.db_symbol}: {e}", exc_info=True)

    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully.

        Signal handlers run on the main thread between bytecodes, so we must
        not touch the ingestion buffers or DB pool here — the main loop may be
        mid-append/mid-iterate, which would corrupt state or raise
        ``RuntimeError: dictionary changed size during iteration``.

        Flip ``running`` so the main loop exits, and poke the active
        StreamManager's stop event so its idle ``_wakeup.wait`` returns
        immediately instead of blocking up to the full extended-hours poll
        interval (30s — long enough for systemd to SIGKILL the worker past
        TimeoutStopSec). The ``finally`` blocks downstream still handle the
        flush and pool close.
        """
        logger.info(f"\n⚠️  Received signal {signum}, shutting down gracefully...")
        self.running = False
        sm = self._active_stream_manager
        if sm is not None:
            try:
                sm.request_stop()
            except Exception:
                # A signal handler must never raise — losing the wake-up is
                # bad, but propagating an exception out of the handler is
                # worse (kills the interpreter before any flush runs).
                pass

    def _initialize_database(self):
        """Initialize database tables if needed"""
        try:
            with db_connection() as conn:
                cursor = conn.cursor()

                # Check if tables exist
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name IN ('underlying_quotes', 'option_chains')
                """)

                existing_tables = [row[0] for row in cursor.fetchall()]

                if len(existing_tables) < 2:
                    logger.warning("Database tables not found. Please run sql/schema.sql")
                    logger.warning("Attempting to continue, but storage will fail...")
                else:
                    logger.info(f"✅ Database initialized: {existing_tables}")

        except Exception as e:
            logger.error(f"Error checking database: {e}", exc_info=True)

    def _store_underlying(self, data: Dict[str, Any]):
        """Store latest 1-minute underlying bar snapshot with upsert semantics."""
        # The stream delivers the current 1-minute bar continuously.
        # Persist each update immediately and overwrite the in-progress minute.
        timestamp = data["timestamp"]
        # TradeStation close-stamps each 1-minute bar at its END boundary, so
        # the raw bar timestamp is one interval ahead of the minute the bar
        # actually covers (the 09:30 bar arrives stamped 09:31:00). Bucket the
        # instant just before the close so the stored row lands on the bar's
        # own minute — matching the option-quote convention (floor of the
        # quote's print time) and wall-clock now(). Without this the
        # underlying bucket sat one minute ahead of the contemporaneous option
        # bucket, which (a) wrote underlying_quotes rows ~1 min into the future
        # and (b) broke the analytics ``underlying.timestamp <= option_ts``
        # pairing at the cash open — there it fell back across the session
        # boundary to the prior 16:00 close and refused the first minute of GEX
        # cycles. A bar stamped mid-interval floors to its own minute unchanged.
        bucket = bucket_timestamp(timestamp - timedelta(seconds=1), AGGREGATION_BUCKET_SECONDS)

        payload = {
            "symbol": self.db_symbol,
            "timestamp": bucket,
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"],
            "up_volume": data.get("up_volume", 0),
            "down_volume": data.get("down_volume", 0),
        }

        payload_sig = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        if payload_sig == self._last_underlying_signature:
            # Stream can emit many duplicate updates for the same minute bucket.
            # Skip redundant upserts to reduce DB load.
            return

        self._log_parity_signature("underlying_quotes", payload)

        self._upsert_underlying_quote(payload)
        self._last_underlying_signature = payload_sig

        # Track latest underlying price for Greeks calculation.  Paired
        # with the bar timestamp so _enrich_with_greeks can refuse stale
        # prices — delta/gamma are highly sensitive to S near strike and
        # silently using a 10-minute-old price would corrupt the
        # persisted Greeks for any option that quotes faster than the
        # underlying bar feed updates.
        old_price = self.latest_underlying_price
        if "close" in data and data["close"] > 0:
            self.latest_underlying_price = data["close"]
            # Intentionally the RAW (close-boundary) bar timestamp, NOT the
            # period-start ``bucket`` written to the DB above. The Greeks gate
            # compares this against the option quote time; the close-stamp's
            # one-interval lead gives ~1 bar of slack that the 90s threshold
            # was tuned around. Normalizing it here would effectively tighten
            # the gate by a bucket and refuse Greeks whenever the bar feed
            # streams a touch late within its minute.
            self.latest_underlying_timestamp = data.get("timestamp") or datetime.now(ET)

            # Log when we first get underlying price (important for Greeks)
            if old_price is None:
                logger.info(
                    f"🎯 First underlying price received: ${self.latest_underlying_price:.2f}"
                )
                logger.info("   Greeks calculation can now proceed for options")
            elif self.underlying_bars_stored % 10 == 0:  # Log every 10 bars
                logger.debug(f"Underlying price updated: ${self.latest_underlying_price:.2f}")

    def _upsert_underlying_quote(self, quote: Dict[str, Any]):
        """Upsert one underlying quote row for the current minute bucket."""
        # Share circuit breaker with option writes — if DB is down, skip.
        # The stream re-sends the in-progress minute bar continuously, so a
        # skipped write self-heals once the breaker closes; emit a DEBUG line
        # so the skip is at least observable (the option path logs its skips
        # via the throttled circuit-breaker summary; this one was silent).
        if _time.monotonic() < self._db_backoff_until:
            logger.debug(
                "[CIRCUIT-BREAKER] Skipping underlying upsert during backoff " "(%.1fs remaining)",
                self._db_backoff_until - _time.monotonic(),
            )
            return
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                # The stream re-sends the in-progress minute bar repeatedly;
                # on a reconnect or out-of-order delivery a later partial can
                # carry a High below / Low above what an earlier partial of
                # the same minute already reported. _merge_bar only carries
                # volume forward, not running H/L, so an unconditional
                # overwrite would regress the stored extremes. Take the
                # period-correct aggregate in the conflict clause: first-seen
                # open, max high, min low; close stays last-tick-wins.
                cursor.execute(
                    """
                    INSERT INTO underlying_quotes
                    (symbol, timestamp, open, high, low, close, up_volume, down_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                        open = COALESCE(underlying_quotes.open, EXCLUDED.open),
                        high = GREATEST(underlying_quotes.high, EXCLUDED.high),
                        low = LEAST(underlying_quotes.low, EXCLUDED.low),
                        close = EXCLUDED.close,
                        up_volume = EXCLUDED.up_volume,
                        down_volume = EXCLUDED.down_volume,
                        updated_at = NOW()
                """,
                    (
                        quote["symbol"],
                        quote["timestamp"],
                        quote["open"],
                        quote["high"],
                        quote["low"],
                        quote["close"],
                        quote["up_volume"],
                        quote["down_volume"],
                    ),
                )
                conn.commit()
                # Reset breaker on success (underlying writes confirm DB is alive).
                self._db_consecutive_failures = 0
                self._db_backoff_until = 0.0
                self._flush_circuit_breaker_skip_summary()

            self.underlying_bars_stored += 1
            self.last_flush_time = datetime.now(ET)

        except Exception as e:
            self._db_consecutive_failures += 1
            self.errors_count += 1
            backoff = _compute_db_backoff_seconds(self._db_consecutive_failures)
            self._db_backoff_until = _time.monotonic() + backoff
            logger.error(
                f"[CIRCUIT-BREAKER] Underlying upsert failed "
                f"(attempt #{self._db_consecutive_failures}, backoff {backoff:.2f}s): {e}",
                exc_info=True,
            )

    def _enrich_with_greeks(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply Greeks calculation to option data, returning enriched copy."""
        if data is None:
            return None

        if self.greeks_calculator and self.latest_underlying_price:
            # Refuse to compute Greeks against a stale underlying price.
            # The age gate is session-aware: a tight regular-session
            # threshold (dense ~60s bars) plus a wider extended-hours one,
            # since pre/after-hours an equity/ETF underlying trades thinly
            # and its 1-minute bars are legitimately minutes apart — the
            # regular gate would otherwise refuse Greeks for the whole
            # extended session. Both still reject outright stale prices
            # (halts, feed gaps).
            if self.latest_underlying_timestamp is not None:
                option_ts = data.get("timestamp") or datetime.now(ET)
                try:
                    age = (option_ts - self.latest_underlying_timestamp).total_seconds()
                except TypeError:
                    # Mismatched naive/aware datetimes — treat as fresh
                    # rather than rejecting; the underlying writer just
                    # set the timestamp, this can only happen mid-test.
                    age = 0.0
                session = get_market_session(option_ts)
                max_age = _greeks_max_age_for_session(
                    session,
                    self.greeks_max_underlying_age_seconds,
                    self.greeks_max_underlying_age_seconds_extended,
                )
                if age > max_age:
                    self.greeks_stale_underlying_rejects += 1
                    # Staleness is only a real problem while the feed
                    # should be delivering bars — its SESSION_TEMPLATE
                    # window, clamped to the regular cash session for cash
                    # indices (SPX has no pre/after-hours print even under
                    # a 24h template, though its options trade then). In
                    # that window it's a WARNING; outside it the feed
                    # legitimately stops, so refusing Greeks is expected —
                    # log at DEBUG and far less often so off-window runs
                    # don't flood the journal with a known-benign state.
                    if underlying_feed_expected(option_ts, SESSION_TEMPLATE, self.db_symbol):
                        self.greeks_stale_underlying_rejects_in_session += 1
                        # Throttle by wall-clock, not reject count: under a
                        # dense option stream a per-100-reject gate fires
                        # many times per second. One warning per interval
                        # keeps the staleness visible without flooding.
                        now_mono = _time.monotonic()
                        if (
                            now_mono - self._greeks_stale_last_warn_mono
                            >= self.greeks_stale_warn_interval_seconds
                        ):
                            self._greeks_stale_last_warn_mono = now_mono
                            logger.warning(
                                "Refusing Greeks: underlying price is %.0fs stale "
                                "(threshold %.0fs) while the feed should be live. "
                                "Rejects this run: %d in-session / %d total.",
                                age,
                                max_age,
                                self.greeks_stale_underlying_rejects_in_session,
                                self.greeks_stale_underlying_rejects,
                            )
                        # Watchdog escalation: if the stream watchdog and
                        # its slow-retry path have both failed to recover
                        # for too long, exit so systemd recycles us. Only
                        # arms in-session — out-of-session staleness is
                        # legitimate and would false-trip.
                        if self._greeks_stale_episode_started_mono is None:
                            self._greeks_stale_episode_started_mono = now_mono
                        stuck_seconds = now_mono - self._greeks_stale_episode_started_mono
                        if (
                            self.greeks_stale_fatal_seconds > 0
                            and stuck_seconds >= self.greeks_stale_fatal_seconds
                        ):
                            logger.error(
                                "Underlying price stuck stale for %.0fs in-session — "
                                "stream-watchdog recovery did not converge. "
                                "Exiting nonzero so systemd recycles the process. "
                                "Rejects this run: %d in-session / %d total.",
                                stuck_seconds,
                                self.greeks_stale_underlying_rejects_in_session,
                                self.greeks_stale_underlying_rejects,
                            )
                            sys.exit(1)
                    elif self.greeks_stale_underlying_rejects % 5000 == 1:
                        logger.debug(
                            "Refusing Greeks: underlying price is %.0fs stale "
                            "(threshold %.0fs); outside the feed's session "
                            "window, this is expected. "
                            "Total rejects this run: %d",
                            age,
                            max_age,
                            self.greeks_stale_underlying_rejects,
                        )
                    data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None
                    data["implied_volatility"] = data.get("implied_volatility")
                    return data
            # Fresh underlying — the staleness episode (if any) has ended.
            # Clear the watchdog timer so the next gap starts measuring
            # from its own onset rather than carrying forward.
            self._greeks_stale_episode_started_mono = None
            try:
                if self.greeks_calculated == 0:
                    logger.info(
                        f"Starting Greeks calculation with underlying price: ${self.latest_underlying_price:.2f}"
                    )
                    logger.debug(f"Sample option data before Greeks: {data}")

                enriched_data = self.greeks_calculator.enrich_option_data(
                    data, self.latest_underlying_price
                )

                if enriched_data is None:
                    logger.error(
                        f"Greeks calculator returned None for {data.get('option_symbol', 'unknown')}, using original data"
                    )
                    data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None
                else:
                    data = enriched_data
                    self.greeks_calculated += 1
                    if self.greeks_calculated % 100 == 0:
                        logger.info(f"Calculated Greeks for {self.greeks_calculated} options")
                    if self.greeks_calculated == 1:
                        logger.info(
                            f"✅ First Greek calculated successfully: delta={data.get('delta')}, gamma={data.get('gamma')}"
                        )

            except Exception as e:
                logger.error(
                    f"Error calculating Greeks for {data.get('option_symbol', 'unknown')}: {e}",
                    exc_info=True,
                )
                data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None
        elif self.greeks_calculator and not self.latest_underlying_price:
            if self.greeks_calculated == 0:
                logger.warning(
                    "⚠️  Skipping Greeks calculation - no underlying price available yet"
                )
            data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None
        else:
            data["delta"] = data["gamma"] = data["theta"] = data["vega"] = None

        return data

    def _store_option(self, data: Dict[str, Any]):
        """Store a single option quote (delegates to batch method)."""
        self._store_option_batch([data])

    def _store_option_batch(self, batch: List[Dict[str, Any]]):
        """
        Process a batch of option quotes with batched DB writes.

        Each quote is enriched with Greeks, classified into the
        per-contract running session-cumulative flow accumulator, and
        buffered into per-symbol 1-minute buckets.  All pending
        aggregations are then flushed to the database in a single
        transaction — one commit for the entire batch rather than one
        commit per contract.

        Volume classification happens here at snapshot arrival (not
        later in ``_prepare_option_agg``) so the accumulator's
        ``last_volume_cum`` advances exactly once per snapshot.  That
        makes the per-snapshot delta computation idempotent under
        replay and removes the need for a separate ``_SEED_FLAG``
        marker on carried-over snapshots: by the time a snapshot is
        re-observed (because it was retained as the buffer's last
        element for the next bucket), its cumulative volume is
        already ≤ the accumulator's watermark and contributes a
        zero delta.
        """
        if not batch:
            return

        rows_to_write: List[Dict[str, Any]] = []

        for data in batch:
            if data is None:
                continue

            pre_symbol = data.get("option_symbol", "unknown")
            data = self._enrich_with_greeks(data)  # type: ignore[assignment]
            if data is None:
                logger.warning(
                    "Dropping option quote after Greeks enrichment returned None: %s",
                    pre_symbol,
                )
                continue

            timestamp = data.get("timestamp")
            if timestamp is None:
                logger.error(
                    f"Option data missing timestamp: {data.get('option_symbol', 'unknown')}"
                )
                continue

            bucket = bucket_timestamp(timestamp, AGGREGATION_BUCKET_SECONDS)

            option_symbol = data.get("option_symbol")
            if option_symbol is None:
                logger.error("Option data missing option_symbol")
                continue

            # If this symbol crossed into a new time bucket, FINALIZE the
            # previous bucket BEFORE this snapshot advances the cumulative
            # accumulator.  _prepare_option_agg stamps the row with the live
            # accumulator cumulatives (volume / ask / mid / bid), so
            # ingesting the new-bucket snapshot first folded its increment
            # into the previous bucket's stored cumulative — the downstream
            # LAG-delta reader (api/database.py) then attributed the first
            # tick of every minute to the PRIOR minute.  Closing the prior
            # bucket against the pre-ingest watermark keeps each bucket's
            # cumulative pinned to its own boundary.
            existing = self.options_buffer.get(option_symbol)
            if existing:
                prev_timestamp = existing[-1].get("timestamp")
                if prev_timestamp is not None:
                    prev_bucket = bucket_timestamp(prev_timestamp, AGGREGATION_BUCKET_SECONDS)
                    if prev_bucket != bucket:
                        agg = self._prepare_option_agg(
                            option_symbol, prev_bucket, keep_last_snapshot=False
                        )
                        if agg:
                            rows_to_write.append(agg)
                        # Seed the new bucket with the previous snapshot so
                        # the bucket carries a defined quote/Greek baseline
                        # for the first throttled write.  It was already
                        # classified on its own arrival, so it is not
                        # re-ingested below.
                        self.options_buffer[option_symbol] = [existing[-1]]

            # Classify this snapshot into the running cumulative accumulator.
            # Done AFTER the prev-bucket finalize above so the previous
            # bucket's stored cumulative excludes this (new-bucket) snapshot.
            # Still exactly once per snapshot regardless of how many times the
            # same snapshot ends up in the buffer (rollover seed, throttled
            # re-flush): the watermark makes a replay contribute a zero delta.
            acc = self._get_flow_accumulator(option_symbol, bucket)
            self._ingest_snapshot_into_accumulator(acc, data, bucket)

            self.options_buffer[option_symbol].append(data)

            # Prepare aggregation for the current bucket, but throttle
            # in-minute writes to reduce UPDATE churn/dead tuples.
            if self._should_write_option_bucket(option_symbol, bucket):
                agg = self._prepare_option_agg(option_symbol, bucket, keep_last_snapshot=True)
                if agg:
                    rows_to_write.append(agg)

        # Write all aggregated rows in a single DB transaction.
        if rows_to_write:
            self._write_option_rows(rows_to_write)

        # Safety valve: flush everything if total buffer exceeds limit.
        # Use each symbol's latest buffered timestamp so data lands in the
        # correct time bucket (not forced into "now").
        total_buffered = sum(len(v) for v in self.options_buffer.values())
        if total_buffered >= MAX_BUFFER_SIZE:
            logger.debug(
                f"Option buffer limit reached ({total_buffered} items), flushing all option buffers"
            )
            overflow_rows = []
            for sym in list(self.options_buffer.keys()):
                buf = self.options_buffer.get(sym)
                if buf:
                    last_ts = buf[-1].get("timestamp")
                    sym_bucket = bucket_timestamp(
                        last_ts if last_ts else datetime.now(ET),
                        AGGREGATION_BUCKET_SECONDS,
                    )
                    agg = self._prepare_option_agg(sym, sym_bucket)
                    if agg:
                        overflow_rows.append(agg)
            if overflow_rows:
                self._write_option_rows(overflow_rows)

    def _log_parity_signature(self, stream_name: str, payload: Dict[str, Any]):
        """
        Emit a stable payload signature for runtime parity checks.

        This is feature-flagged and does not alter DB writes.
        """
        if not INGEST_PARITY_GUARD_ENABLED:
            return

        try:
            canonical = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
            digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
            logger.info(f"[PARITY] {stream_name} sig={digest} payload={canonical}")
        except Exception as e:
            logger.warning(f"Failed to emit parity signature for {stream_name}: {e}")

    def _should_write_option_bucket(
        self,
        option_symbol: str,
        bucket: datetime,
        *,
        force: bool = False,
    ) -> bool:
        """Rate-limit writes for the same option contract and time bucket."""
        key = (option_symbol, bucket)
        now_mono = _time.monotonic()

        if force or OPTION_BUCKET_WRITE_MIN_SECONDS <= 0:
            self._option_bucket_last_write[key] = now_mono
            return True

        last_write = self._option_bucket_last_write.get(key)
        if last_write is not None and (now_mono - last_write) < OPTION_BUCKET_WRITE_MIN_SECONDS:
            return False

        self._option_bucket_last_write[key] = now_mono
        return True

    def _select_classify_quote(self, acc: "_FlowAccumulator", snap: Dict[str, Any]) -> tuple:
        """Pick the NBBO to classify ``snap``'s trade against.

        Prefer the **prior tick** (``acc.last_*`` — the quote prevailing
        before this trade): it keeps a marketable order from being scored
        against the post-trade NBBO it just moved, which is the failure
        mode the prior-tick rule exists to avoid.

        But the prior tick is only a valid pre-trade proxy when it is
        *recent*.  On a contract that has been quiet and then the price
        moves, the last NBBO we recorded can be many seconds old; scoring
        a fresh print against that stale quote turns the Lee-Ready
        quote-test into a bar-over-bar tick-test and **inverts** the side
        (e.g. a bid-hitting sell during a fast up-move reads as a lift ->
        ``ask_volume`` -> "buy").  When the prior tick is older than
        ``FLOW_CLASSIFY_PRIOR_TICK_MAX_AGE_SECONDS`` relative to this
        trade, fall back to the snapshot's own (contemporaneous) NBBO,
        which is sampled together with the trade.

        Cold start (no prior tick yet) uses the snapshot's own NBBO, the
        same degraded path the prior design used.  Returns
        ``(bid, ask, mid)``.
        """
        snap_bid = snap.get("bid")
        snap_ask = snap.get("ask")
        snap_mid = snap.get("mid")

        # No usable prior tick (cold start / hydrate miss): contemporaneous.
        if acc.last_bid is None or acc.last_ask is None:
            return snap_bid, snap_ask, snap_mid

        max_age = FLOW_CLASSIFY_PRIOR_TICK_MAX_AGE_SECONDS
        prior_is_fresh = True
        if max_age > 0:
            snap_ts = snap.get("timestamp")
            if snap_ts is not None and acc.last_quote_ts is not None:
                try:
                    age = (snap_ts - acc.last_quote_ts).total_seconds()
                except (TypeError, ValueError):
                    age = None
                if age is not None and age > max_age:
                    prior_is_fresh = False

        if prior_is_fresh:
            return acc.last_bid, acc.last_ask, acc.last_mid

        # Stale prior tick -> contemporaneous quote, falling back to the
        # prior value per-side only if the snapshot omits that side.
        return (
            snap_bid if snap_bid is not None else acc.last_bid,
            snap_ask if snap_ask is not None else acc.last_ask,
            snap_mid if snap_mid is not None else acc.last_mid,
        )

    def _classify_volume_chunk(
        self,
        volume_delta: int,
        last: Optional[float],
        bid: Optional[float],
        ask: Optional[float],
        mid: Optional[float],
        band_pct: float = FLOW_CLASSIFY_MID_BAND_PCT,
    ) -> tuple:
        """
        Classify a volume chunk into ask_volume, mid_volume, or bid_volume
        using the Lee-Ready convention: trade price near ask =>
        buyer-initiated (ask_volume), near bid => seller-initiated
        (bid_volume), otherwise mid_volume.

        Callers should pass the *prior-tick* bid/ask/mid (the quote that
        was prevailing before the trade), not the post-trade quote.

        ``band_pct`` is the fraction of each half-spread that counts as
        mid_volume:
          * ``0.0`` = pure Lee-Ready: anything above mid is ask, below mid is bid
          * ``0.5`` ≈ nearest-neighbor: matches the legacy classification
          * ``1.0`` = everything between bid and ask is mid (only at-or-beyond
            quotes count as ask/bid)
        Default ``0.70`` gives a wider mid zone than nearest-neighbor so
        borderline fills (like a 5.57 print between mid 5.555 and ask 5.58)
        land in mid rather than getting full ask credit.

        Returns (ask_vol, mid_vol, bid_vol) tuple where exactly one is non-zero.
        """
        if volume_delta <= 0:
            return (0, 0, 0)

        if last is None or last <= 0:
            return (0, volume_delta, 0)

        effective_mid = mid
        if effective_mid is None:
            if bid is not None and ask is not None:
                effective_mid = (bid + ask) / 2.0
            else:
                return (0, volume_delta, 0)

        # Without both quote sides we can't define the band; fall back to
        # nearest-neighbor against whatever sides we do have.  Increment
        # a counter and log periodically so persistent bad quotes
        # (data-feed glitches, halted contracts, malformed snapshots) are
        # visible to operators -- the previous silent fallback let
        # entire contracts route their flow through nearest-neighbor
        # classification with no telemetry.
        #
        # A LOCKED market (``bid == ask``) is NOT a degraded quote: it is
        # a legitimate state for tight ATM and illiquid contracts whose
        # NBBO momentarily coincides at one tick.  Strict ``ask < bid``
        # routes only TRULY crossed quotes into the fallback; the locked
        # case falls through to the band logic below where the
        # half-spread collapses to zero and the ask/bid thresholds
        # collapse to the locked price -- ``last > price`` is then
        # cleanly ask_volume (lift), ``last < price`` is bid_volume
        # (hit), and ``last == price`` is mid_volume.  Before this gate
        # was tightened, every locked print routed through
        # nearest-neighbor, where the three distances are identical and
        # everything degenerated to mid_volume regardless of trade
        # direction -- and each first-of-process locked print fired the
        # WARN above, drowning operators in false data-quality alerts.
        if bid is None or ask is None or ask < bid:
            # Use getattr/setattr so test fixtures that build the engine
            # via ``IngestionEngine.__new__(...)`` (skipping __init__)
            # don't AttributeError on the counter access.
            count = getattr(self, "_classify_fallback_count", 0) + 1
            self._classify_fallback_count = count
            if (count % 1000) == 1:
                logger.warning(
                    "_classify_volume_chunk fallback fired (cumulative=%d) "
                    "bid=%s ask=%s last=%s -- nearest-neighbor classification used",
                    count,
                    bid,
                    ask,
                    last,
                )
            dist_to_ask = abs(last - ask) if ask is not None else float("inf")
            dist_to_mid = abs(last - effective_mid)
            dist_to_bid = abs(last - bid) if bid is not None else float("inf")
            min_dist = min(dist_to_ask, dist_to_mid, dist_to_bid)
            if dist_to_ask == min_dist:
                return (volume_delta, 0, 0)
            if dist_to_bid == min_dist:
                return (0, 0, volume_delta)
            return (0, volume_delta, 0)

        half_spread = (ask - bid) / 2.0
        # Clamp band to [0, 1] so misconfiguration can't invert the zones.
        band = max(0.0, min(1.0, band_pct))
        ask_threshold = effective_mid + band * half_spread
        bid_threshold = effective_mid - band * half_spread

        if last > ask_threshold:
            return (volume_delta, 0, 0)
        if last < bid_threshold:
            return (0, 0, volume_delta)
        return (0, volume_delta, 0)

    @staticmethod
    def _is_opening_auction_bucket(bucket: datetime) -> bool:
        """True when ``bucket`` is the 09:30 ET cash-equity opening bucket.

        The opening cross is a single auction print whose price is set by
        the auction itself; running Lee-Ready against the post-open NBBO
        misclassifies it. We carve this bucket out and route its volume
        to mid_volume instead.
        """
        if bucket is None:
            return False
        try:
            local = bucket.astimezone(ET) if bucket.tzinfo else ET.localize(bucket)
        except Exception:
            return False
        return local.hour == 9 and local.minute == 30

    @staticmethod
    def _bucket_session_date(bucket: datetime) -> _date:
        """ET session date for ``bucket`` (tz-naive treated as UTC).

        TradeStation resets option cumulative volume to 0 at session open,
        so per-contract flow accumulators are scoped per ET session date.

        Returns the cash-session date (pre-09:30 belongs to the PRIOR
        session), aligning the in-memory rollover with the vendor's exact
        reset boundary.
        """
        return cash_session_date(bucket)

    def _hydrate_flow_accumulator(
        self, option_symbol: str, session_date: _date
    ) -> _FlowAccumulator:
        """Build a fresh accumulator for ``(option_symbol, session_date)``.

        Loads the latest persisted row for this contract in this ET
        session (if any) so the in-memory cumulative resumes exactly
        where the DB left off — same recovery semantics as the prior
        baseline cache, but for all four cumulative columns at once
        (volume + ask + mid + bid).  Also picks up the row's NBBO so
        the next snapshot's Lee-Ready classification has a real prior
        tick from the start.  Zeros on DB failure or empty result.
        """
        # ``session_date`` is the cash-session date; the session began at
        # 09:30 ET on that date.  Hydration must pull only rows in that
        # cash-session window so we don't carry forward late-night
        # extended-hours rows that belong to the PRIOR session.
        session_start_utc = cash_session_start_utc(session_date)
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT volume, ask_volume, mid_volume, bid_volume,
                           bid, ask, mid, timestamp
                    FROM option_chains
                    WHERE option_symbol = %s
                      AND timestamp >= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (option_symbol, session_start_utc),
                )
                row = cursor.fetchone()
                if row is not None:
                    return _FlowAccumulator(
                        session_date=session_date,
                        last_volume_cum=int(row[0] or 0),
                        ask_cum=int(row[1] or 0),
                        mid_cum=int(row[2] or 0),
                        bid_cum=int(row[3] or 0),
                        last_bid=_to_db_float(row[4]),
                        last_ask=_to_db_float(row[5]),
                        last_mid=_to_db_float(row[6]),
                        last_quote_ts=row[7],
                    )
        except Exception as e:
            logger.warning(
                "Failed hydrating flow accumulator for %s: %s — starting from zero",
                option_symbol,
                e,
            )
        return _FlowAccumulator(
            session_date=session_date,
            last_volume_cum=0,
            ask_cum=0,
            mid_cum=0,
            bid_cum=0,
        )

    def _get_flow_accumulator(self, option_symbol: str, bucket: datetime) -> _FlowAccumulator:
        """Return the live accumulator for this contract in the bucket's ET session.

        Triggers a hydrate on first observation and on session rollover
        (the existing accumulator's ``session_date`` no longer matches
        the bucket's).  The rollover branch is what makes the
        TradeStation 09:30 ET reset safe: a stale prior-session value
        cannot survive into a new session.
        """
        session_date = self._bucket_session_date(bucket)
        with self._option_flow_lock:
            acc = self._option_flow.get(option_symbol)
            if acc is None or acc.session_date != session_date:
                acc = self._hydrate_flow_accumulator(option_symbol, session_date)
                self._option_flow[option_symbol] = acc
            return acc

    def _ingest_snapshot_into_accumulator(
        self,
        acc: _FlowAccumulator,
        snap: Dict[str, Any],
        bucket: datetime,
    ) -> None:
        """Advance ``acc`` by the classified delta this snapshot represents.

        Idempotent: ``acc.last_volume_cum`` is the watermark, so a
        snapshot replayed at the same cumulative volume produces
        ``vol_delta == 0`` and contributes nothing the second time.
        Classification uses the accumulator's stored prior-tick NBBO
        (``last_bid`` / ``last_ask`` / ``last_mid``), preserving the
        Lee-Ready prior-tick rule across snapshots and across bucket
        boundaries within the same session — but only while that prior
        tick is recent.  ``_select_classify_quote`` falls back to the
        snapshot's own contemporaneous NBBO when the prior tick has gone
        stale (a quiet contract that then moves), which otherwise inverts
        the side by degrading the quote-test into a tick-test.

        Reset detection: TradeStation's cumulative volume resets to 0
        at the 09:30 ET cash open.  ``_bucket_session_date`` anchors
        per-contract accumulators to the ET *calendar* day (midnight
        ET), which means the first snapshot of the day at 00:00 ET
        observes the prior session's residual cumulative and seeds
        ``last_volume_cum`` with that residual.  When the vendor then
        resets at 09:30 ET and the post-reset cumulative is below the
        watermark, ``max(curr_vol - watermark, 0)`` would silently swallow
        every cash-session trade for the rest of the day, and the
        ``GREATEST`` upsert would preserve the bogus watermark in
        storage.  Re-anchor the watermark when we observe a *positive*
        cumulative below the watermark — a real reset hands us a
        positive post-reset value, while ``curr_vol == 0`` is the
        signature of a missing Volume field (e.g. a stream reconnect
        whose first message after the merger's _state was cleared
        omits Volume) and must NOT be treated as a vendor reset, or it
        zeros the stored volume in the middle of pre-RTH.  Classified
        ``ask_cum`` / ``mid_cum`` / ``bid_cum`` stay monotonic across
        the boundary so the reader's LAG continues to recover correct
        per-bar deltas in the cash session.
        """
        curr_vol = int(snap.get("volume") or 0)
        # Re-anchor on a legitimate vendor cumulative reset only.  A real
        # reset (TS at the 09:30 ET cash open) hands us a *positive*
        # post-reset cumulative; the merge already filters explicit
        # Volume=0 quote messages (stream_manager._merge_single_quote).
        # The path that still produces ``curr_vol == 0`` here is a
        # missing Volume field -- which happens on a stream reconnect
        # when the merger's _state was cleared and the first message
        # back lacks Volume.  Treating that as a vendor reset zeros the
        # watermark for the wrong reason and creates a phantom mid-day
        # cliff (the reported 06:40 ET reset on SPY 260605P752: a
        # missing-Volume snapshot dropped the stored volume to 0 while
        # bid_volume kept its 2422 baseline by design).  Require
        # ``curr_vol > 0`` so only a real post-reset cumulative trips it.
        if curr_vol > 0 and curr_vol < acc.last_volume_cum:
            acc.last_volume_cum = 0
        vol_delta = max(curr_vol - acc.last_volume_cum, 0)
        if vol_delta > 0:
            skip = FLOW_CLASSIFY_SKIP_OPEN_AUCTION and self._is_opening_auction_bucket(bucket)
            if skip:
                acc.mid_cum += vol_delta
            else:
                # Quote to classify against: the prior tick when it is
                # recent enough to be a valid pre-trade proxy, else the
                # snapshot's own (contemporaneous) NBBO.  See
                # _select_classify_quote for the staleness rationale.
                q_bid, q_ask, q_mid = self._select_classify_quote(acc, snap)
                av, mv, bv = self._classify_volume_chunk(
                    vol_delta,
                    snap.get("last"),
                    q_bid,
                    q_ask,
                    q_mid,
                )
                acc.ask_cum += av
                acc.mid_cum += mv
                acc.bid_cum += bv
        if curr_vol > acc.last_volume_cum:
            acc.last_volume_cum = curr_vol
        # Update the prior-tick NBBO for the next classification, and
        # stamp it with this snapshot's time so the next trade can
        # age-check it (a quiet contract leaves this timestamp old, which
        # is what trips the staleness fallback in _select_classify_quote).
        quote_seen = False
        if snap.get("bid") is not None:
            acc.last_bid = _to_db_float(snap.get("bid"))
            quote_seen = True
        if snap.get("ask") is not None:
            acc.last_ask = _to_db_float(snap.get("ask"))
            quote_seen = True
        if snap.get("mid") is not None:
            acc.last_mid = _to_db_float(snap.get("mid"))
            quote_seen = True
        elif acc.last_bid is not None and acc.last_ask is not None:
            acc.last_mid = (acc.last_bid + acc.last_ask) / 2.0
        if quote_seen and snap.get("timestamp") is not None:
            acc.last_quote_ts = snap.get("timestamp")

    def _prepare_option_agg(
        self, option_symbol: str, bucket: datetime, keep_last_snapshot: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Emit a write-ready row dict for ``(option_symbol, bucket)``.

        Volume classification already happened at snapshot arrival
        (``_ingest_snapshot_into_accumulator``).  This method just
        reads the accumulator's running session-cumulative totals
        and pairs them with the best-available quote/Greek fields
        from the buffered snapshots for this bucket.

        Returns ``None`` if the buffer is empty.
        """
        buffer = self.options_buffer.get(option_symbol, [])
        if not buffer:
            return None

        try:
            last = buffer[-1]
            acc = self._get_flow_accumulator(option_symbol, bucket)

            # Use the best available bid/ask/last from any snapshot in
            # the buffer — fall back through the buffer so a single delta
            # that omits price fields doesn't wipe previously-seen values.
            best_last = next(
                (b["last"] for b in reversed(buffer) if b.get("last") is not None), None
            )
            best_bid = next((b["bid"] for b in reversed(buffer) if b.get("bid") is not None), None)
            best_ask = next((b["ask"] for b in reversed(buffer) if b.get("ask") is not None), None)
            best_mid = next((b["mid"] for b in reversed(buffer) if b.get("mid") is not None), None)
            if best_mid is None and best_bid is not None and best_ask is not None:
                best_mid = (best_bid + best_ask) / 2.0

            agg = {
                "option_symbol": last["option_symbol"],
                "timestamp": bucket,
                "underlying": last["underlying"],
                "strike": last["strike"],
                "expiration": last["expiration"],
                "option_type": last["option_type"],
                "last": best_last,
                "bid": best_bid,
                "ask": best_ask,
                "mid": best_mid,
                "volume": acc.last_volume_cum,
                "open_interest": max((b.get("open_interest") or 0) for b in buffer),
                "implied_volatility": _to_db_float(last.get("implied_volatility")),
                # SESSION-CUMULATIVE classified flow (resets at 09:30 ET).
                # Downstream LAG-delta consumers (flow_contract_facts) and
                # the volume column share the same semantics now.
                "ask_volume": acc.ask_cum,
                "mid_volume": acc.mid_cum,
                "bid_volume": acc.bid_cum,
                "delta": _to_db_float(last.get("delta")),
                "gamma": _to_db_float(last.get("gamma")),
                "theta": _to_db_float(last.get("theta")),
                "vega": _to_db_float(last.get("vega")),
            }

            self._log_parity_signature("option_chains", agg)

            # Trim buffer.  When keeping the last snapshot, no special
            # marker is needed: the accumulator already counted its
            # volume on arrival, so the buffer scan in this method
            # treating it as the only element again would produce
            # zero new flow (vol_delta against an equal watermark).
            if keep_last_snapshot and buffer:
                self.options_buffer[option_symbol] = [buffer[-1]]
            else:
                self.options_buffer[option_symbol] = []
                stale_keys = [
                    key
                    for key in self._option_bucket_last_write
                    if key[0] == option_symbol and key[1] <= bucket
                ]
                for key in stale_keys:
                    self._option_bucket_last_write.pop(key, None)

            return agg

        except Exception as e:
            logger.error(f"Error preparing option agg for {option_symbol}: {e}", exc_info=True)
            self.errors_count += 1
            return None

    # Dual-write companion: maintains the "latest quote per option_symbol"
    # cache that the analytics snapshot reads when
    # ``ANALYTICS_USE_LATEST_CACHE=true``.  One row per option_symbol;
    # the ``WHERE EXCLUDED.timestamp >= option_chains_latest.timestamp``
    # guard prevents an out-of-order / retry write from clobbering a
    # newer row.
    #
    # Greeks and IV use ``COALESCE(EXCLUDED.x, option_chains_latest.x)``
    # so a NULL-Greek write does not clobber a previously-good value.
    # Required for the off-session freeze semantic: once the underlying
    # feed stops, ingestion writes NULL Greeks (the in-engine staleness
    # gate). Because the cache is keyed by option_symbol alone — one row
    # per contract — a direct ``= EXCLUDED.gamma`` assignment would wipe
    # the last-good gamma on every post-close quote, leaving the
    # analytics snapshot's ``WHERE gamma IS NOT NULL`` filter to drop the
    # affected contracts. The history table (option_chains) is keyed by
    # (option_symbol, timestamp) so each new write is a new row and the
    # DISTINCT ON snapshot query can fall back to an earlier
    # gamma-bearing row; the cache has no such fallback, so it must
    # preserve the last good Greek itself.
    _OPTION_LATEST_UPSERT_SQL = """
        INSERT INTO option_chains_latest
        (option_symbol, timestamp, underlying, strike, expiration, option_type,
         last, bid, ask, mid, volume, open_interest, implied_volatility,
         ask_volume, mid_volume, bid_volume,
         delta, gamma, theta, vega)
        VALUES %s
        ON CONFLICT (option_symbol) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            underlying = EXCLUDED.underlying,
            strike = EXCLUDED.strike,
            expiration = EXCLUDED.expiration,
            option_type = EXCLUDED.option_type,
            last = EXCLUDED.last,
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            mid = EXCLUDED.mid,
            volume = GREATEST(option_chains_latest.volume, EXCLUDED.volume),
            open_interest = GREATEST(option_chains_latest.open_interest, EXCLUDED.open_interest),
            implied_volatility = COALESCE(EXCLUDED.implied_volatility, option_chains_latest.implied_volatility),
            ask_volume = GREATEST(option_chains_latest.ask_volume, EXCLUDED.ask_volume),
            mid_volume = GREATEST(option_chains_latest.mid_volume, EXCLUDED.mid_volume),
            bid_volume = GREATEST(option_chains_latest.bid_volume, EXCLUDED.bid_volume),
            delta = COALESCE(EXCLUDED.delta, option_chains_latest.delta),
            gamma = COALESCE(EXCLUDED.gamma, option_chains_latest.gamma),
            theta = COALESCE(EXCLUDED.theta, option_chains_latest.theta),
            vega = COALESCE(EXCLUDED.vega, option_chains_latest.vega),
            updated_at = NOW()
        WHERE EXCLUDED.timestamp >= option_chains_latest.timestamp
    """

    # SQL template shared by single and batch writes.
    #
    # ALL monotonic numeric columns (volume, open_interest, ask_volume,
    # mid_volume, bid_volume) use ``GREATEST`` so any UPSERT is
    # idempotent: re-sending a row that already committed is a no-op
    # because the WHERE clause's ``IS DISTINCT FROM`` guard rejects
    # the update.  This is what makes the unified pre-commit /
    # commit-phase retry path safe — there is no scenario where a
    # double-applied retry inflates the stored value.
    _OPTION_UPSERT_SQL = """
        INSERT INTO option_chains
        (option_symbol, timestamp, underlying, strike, expiration, option_type,
         last, bid, ask, mid, volume, open_interest, implied_volatility,
         ask_volume, mid_volume, bid_volume,
         delta, gamma, theta, vega)
        VALUES %s
        ON CONFLICT (option_symbol, timestamp) DO UPDATE SET
            last = COALESCE(EXCLUDED.last, option_chains.last),
            bid = COALESCE(EXCLUDED.bid, option_chains.bid),
            ask = COALESCE(EXCLUDED.ask, option_chains.ask),
            mid = COALESCE(EXCLUDED.mid, option_chains.mid),
            volume = GREATEST(option_chains.volume, EXCLUDED.volume),
            open_interest = GREATEST(option_chains.open_interest, EXCLUDED.open_interest),
            implied_volatility = COALESCE(EXCLUDED.implied_volatility, option_chains.implied_volatility),
            ask_volume = GREATEST(option_chains.ask_volume, EXCLUDED.ask_volume),
            mid_volume = GREATEST(option_chains.mid_volume, EXCLUDED.mid_volume),
            bid_volume = GREATEST(option_chains.bid_volume, EXCLUDED.bid_volume),
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega,
            updated_at = NOW()
        WHERE
            COALESCE(EXCLUDED.last, option_chains.last) IS DISTINCT FROM option_chains.last
            OR COALESCE(EXCLUDED.bid, option_chains.bid) IS DISTINCT FROM option_chains.bid
            OR COALESCE(EXCLUDED.ask, option_chains.ask) IS DISTINCT FROM option_chains.ask
            OR COALESCE(EXCLUDED.mid, option_chains.mid) IS DISTINCT FROM option_chains.mid
            OR GREATEST(option_chains.volume, EXCLUDED.volume) IS DISTINCT FROM option_chains.volume
            OR GREATEST(option_chains.open_interest, EXCLUDED.open_interest) IS DISTINCT FROM option_chains.open_interest
            OR COALESCE(EXCLUDED.implied_volatility, option_chains.implied_volatility) IS DISTINCT FROM option_chains.implied_volatility
            OR GREATEST(option_chains.ask_volume, EXCLUDED.ask_volume) IS DISTINCT FROM option_chains.ask_volume
            OR GREATEST(option_chains.mid_volume, EXCLUDED.mid_volume) IS DISTINCT FROM option_chains.mid_volume
            OR GREATEST(option_chains.bid_volume, EXCLUDED.bid_volume) IS DISTINCT FROM option_chains.bid_volume
            OR EXCLUDED.delta IS DISTINCT FROM option_chains.delta
            OR EXCLUDED.gamma IS DISTINCT FROM option_chains.gamma
            OR EXCLUDED.theta IS DISTINCT FROM option_chains.theta
            OR EXCLUDED.vega IS DISTINCT FROM option_chains.vega
    """

    def _coalesce_option_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Collapse duplicate (option_symbol, timestamp) rows before DB writes.

        All cumulative monotonic numeric fields (volume, open_interest,
        ask/mid/bid_volume) merge with ``max``; quote and Greek fields
        take the latest non-null value.  No additive merging — the
        per-contract flow accumulator already holds the running
        session-cumulative totals, so two rows for the same
        ``(option_symbol, timestamp)`` produced in the same batch
        contain the same cumulative snapshot (possibly differing only
        in which had the freshest quote), not two disjoint deltas.
        """
        coalesced: Dict[tuple, Dict[str, Any]] = {}

        for row in rows:
            key = (row["option_symbol"], row["timestamp"])
            existing = coalesced.get(key)
            if existing is None:
                coalesced[key] = dict(row)
                continue

            # Preserve latest non-null quote / Greek fields.
            for field in (
                "last",
                "bid",
                "ask",
                "mid",
                "implied_volatility",
                "delta",
                "gamma",
                "theta",
                "vega",
            ):
                if row.get(field) is not None:
                    existing[field] = row[field]

            # All cumulative monotonic fields use max-wins.
            for field in (
                "volume",
                "open_interest",
                "ask_volume",
                "mid_volume",
                "bid_volume",
            ):
                existing[field] = max(existing.get(field) or 0, row.get(field) or 0)

        return list(coalesced.values())

    def _retain_failed_option_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Hold computed aggregates that did not persist, for re-submission.

        Re-submission is unconditionally safe under the current upsert
        contract: every monotonic numeric column uses ``GREATEST`` and
        the WHERE clause gates the UPDATE on ``IS DISTINCT FROM``, so a
        retry of a row that actually committed on the prior attempt is
        a no-op.  This collapses the prior pre-commit / commit-phase
        fork: there is no longer a scenario where a double-applied
        retry would inflate ``ask_volume`` / ``mid_volume`` / ``bid_volume``.

        Bounded — under a prolonged outage we drop the OLDEST rows and log
        an error rather than grow without limit or (as before) lose every
        row silently. ``getattr``/``setattr`` so ``__new__``-built test
        stubs don't need the attribute.
        """
        if not rows:
            return
        pending = getattr(self, "_pending_failed_option_rows", None)
        if pending is None:
            pending = []
        pending.extend(rows)
        cap = getattr(self, "_pending_failed_option_rows_max", 20000)
        if cap > 0 and len(pending) > cap:
            dropped = len(pending) - cap
            pending = pending[dropped:]
            logger.error(
                "[CIRCUIT-BREAKER] Pending failed-write buffer exceeded %d rows; "
                "dropped %d oldest aggregates (classified flow for those "
                "buckets is lost). DB outage longer than the retain budget.",
                cap,
                dropped,
            )
        self._pending_failed_option_rows = pending

    def _log_circuit_breaker_skip(self, row_count: int, now_mono: float) -> None:
        """Throttled WARNING for option writes skipped during DB backoff.

        Logs the first skip in a backoff window immediately (so the cause
        appears at the head of the journal), then coalesces subsequent skips
        into a single rolled-up line per ``_CB_SKIP_LOG_INTERVAL_SECONDS``.
        Counters are flushed by ``_flush_circuit_breaker_skip_summary`` on
        recovery so the journal records the total skipped during the outage.

        ``getattr`` so test stubs that build the engine via ``__new__`` (see
        tests/test_ingestion_failed_write_retention.py) don't have to seed
        these counters — mirrors ``_retain_failed_option_rows`` above.
        """
        backoff_remaining = self._db_backoff_until - now_mono
        first_logged = getattr(self, "_cb_skip_window_first_logged", False)
        if not first_logged:
            logger.warning(
                "[CIRCUIT-BREAKER] Skipping write of %d rows — "
                "DB backoff for %.1fs more (further skips rolled up "
                "every %.1fs)",
                row_count,
                backoff_remaining,
                _CB_SKIP_LOG_INTERVAL_SECONDS,
            )
            self._cb_skip_window_first_logged = True
            self._cb_skip_count_since_last_log = 0
            self._cb_skip_rows_since_last_log = 0
            self._cb_skip_last_log_mono = now_mono
            return

        self._cb_skip_count_since_last_log = getattr(self, "_cb_skip_count_since_last_log", 0) + 1
        self._cb_skip_rows_since_last_log = (
            getattr(self, "_cb_skip_rows_since_last_log", 0) + row_count
        )
        last_log_mono = getattr(self, "_cb_skip_last_log_mono", now_mono)
        if now_mono - last_log_mono >= _CB_SKIP_LOG_INTERVAL_SECONDS:
            logger.warning(
                "[CIRCUIT-BREAKER] Still in backoff (%.1fs more) — "
                "skipped %d batches / %d rows in last %.1fs",
                max(0.0, backoff_remaining),
                self._cb_skip_count_since_last_log,
                self._cb_skip_rows_since_last_log,
                now_mono - last_log_mono,
            )
            self._cb_skip_count_since_last_log = 0
            self._cb_skip_rows_since_last_log = 0
            self._cb_skip_last_log_mono = now_mono

    def _flush_circuit_breaker_skip_summary(self) -> None:
        """Emit a final rolled-up line when the breaker closes, then reset.

        Tolerant of test stubs built via ``__new__`` that never opened a
        backoff window (no attributes seeded).
        """
        first_logged = getattr(self, "_cb_skip_window_first_logged", False)
        residual = getattr(self, "_cb_skip_count_since_last_log", 0)
        if first_logged and residual > 0:
            logger.info(
                "[CIRCUIT-BREAKER] Final rollup: %d additional batches / "
                "%d rows were skipped before recovery",
                residual,
                getattr(self, "_cb_skip_rows_since_last_log", 0),
            )
        self._cb_skip_window_first_logged = False
        self._cb_skip_count_since_last_log = 0
        self._cb_skip_rows_since_last_log = 0
        self._cb_skip_last_log_mono = 0.0

    def _write_option_rows(self, rows: List[Dict[str, Any]]):
        """Write multiple aggregated option rows in a single DB transaction.

        Includes a circuit breaker: after consecutive failures the engine
        backs off exponentially (2s, 4s, 8s … capped at 60s) so we don't
        hammer a dead database.  On recovery the breaker resets immediately.

        Failure handling is unified across pre-commit and commit-phase
        errors: the upsert is idempotent (``GREATEST`` on every monotonic
        column, ``IS DISTINCT FROM`` WHERE guard), so retaining and
        re-submitting any failed batch is safe regardless of whether
        the prior attempt actually applied server-side.
        """
        # Re-submit aggregates a prior attempt failed to persist (or
        # skipped during backoff). Prepend so they coalesce with any
        # new same-(option_symbol, timestamp) rows under the max-wins
        # rule in _coalesce_option_rows.
        pending = getattr(self, "_pending_failed_option_rows", None)
        if pending:
            rows = pending + list(rows)
            self._pending_failed_option_rows = []

        if not rows:
            return

        # Many stream iterations can generate repeated updates for the same
        # option/timestamp key. Coalesce them before touching the DB.
        rows = self._coalesce_option_rows(rows)

        # Circuit breaker: skip write if still in backoff window.
        now_mono = _time.monotonic()
        if now_mono < self._db_backoff_until:
            self._log_circuit_breaker_skip(len(rows), now_mono)
            self._retain_failed_option_rows(rows)
            return

        t0 = _time.monotonic()
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                values = [
                    (
                        agg["option_symbol"],
                        agg["timestamp"],
                        agg["underlying"],
                        agg["strike"],
                        agg["expiration"],
                        agg["option_type"],
                        agg["last"],
                        agg["bid"],
                        agg["ask"],
                        agg["mid"],
                        agg["volume"],
                        agg["open_interest"],
                        agg["implied_volatility"],
                        agg["ask_volume"],
                        agg["mid_volume"],
                        agg["bid_volume"],
                        agg["delta"],
                        agg["gamma"],
                        agg["theta"],
                        agg["vega"],
                    )
                    for agg in rows
                ]
                execute_values(
                    cursor,
                    self._OPTION_UPSERT_SQL,
                    values,
                    page_size=500,
                )
                # Dual-write to option_chains_latest in the SAME transaction
                # so the cache cannot drift from history under partial
                # failure -- either both upserts commit or both roll back.
                #
                # The cache UPSERT is keyed by option_symbol alone (one row
                # per contract), while the history UPSERT is keyed by
                # (option_symbol, timestamp).  A single batch can legitimately
                # contain multiple timestamps for the same contract (e.g.
                # several 1-minute bars flushed together), and PostgreSQL
                # refuses to UPDATE the same target row twice in one
                # `INSERT ... ON CONFLICT` statement ("ON CONFLICT DO UPDATE
                # command cannot affect row a second time"), regardless of
                # any WHERE clause on the UPDATE.  So pre-dedupe by
                # option_symbol, keeping only the row with the latest
                # timestamp -- the cache is meant to hold latest-per-contract
                # anyway, and the older rows in the batch will be persisted
                # in the history table by the previous UPSERT.
                values_latest = list(
                    {
                        # Tuple index 0 = option_symbol, 1 = timestamp.
                        # Iterate in order so the last-write-wins selection
                        # picks the row with the highest timestamp for each
                        # option_symbol.
                        v[0]: v
                        for v in sorted(values, key=lambda v: v[1])
                    }.values()
                )
                execute_values(
                    cursor,
                    self._OPTION_LATEST_UPSERT_SQL,
                    values_latest,
                    page_size=500,
                )
                conn.commit()

            elapsed_ms = (_time.monotonic() - t0) * 1000
            self.option_quotes_stored += len(rows)
            self.last_flush_time = datetime.now(ET)

            # Reset circuit breaker on success.
            if self._db_consecutive_failures > 0:
                logger.info(
                    f"[CIRCUIT-BREAKER] DB recovered after "
                    f"{self._db_consecutive_failures} consecutive failures"
                )
            self._db_consecutive_failures = 0
            self._db_backoff_until = 0.0
            self._flush_circuit_breaker_skip_summary()

            # Observability accumulators.
            self._obs_batches_written += 1
            self._obs_rows_written += len(rows)
            self._obs_write_time_ms += elapsed_ms

            # Log first few with Greeks.
            if self.option_quotes_stored <= len(rows) + 3:
                for agg in rows[:3]:
                    d = agg.get("delta")
                    if d is not None:
                        logger.info(
                            f"✅ Stored option with Greeks: {agg['option_symbol']} "
                            f"delta={d:.4f} gamma={agg.get('gamma', 0):.6f}"
                        )

            logger.debug(
                f"Wrote {len(rows)} option rows in single transaction " f"({elapsed_ms:.1f}ms)"
            )

            # Periodic observability summary (every 60s).
            now = _time.monotonic()
            if now - self._obs_last_log >= 60:
                avg_ms = (
                    self._obs_write_time_ms / self._obs_batches_written
                    if self._obs_batches_written
                    else 0
                )
                logger.info(
                    f"[DB-METRICS] last 60s: "
                    f"batches={self._obs_batches_written} "
                    f"rows={self._obs_rows_written} "
                    f"avg_write_ms={avg_ms:.1f} "
                    f"total_stored={self.option_quotes_stored} "
                    f"errors={self.errors_count}"
                )
                self._obs_batches_written = 0
                self._obs_rows_written = 0
                self._obs_write_time_ms = 0.0
                self._obs_last_log = now

                # Persist any completed API-call window even during a quiet
                # period: the client only flushes window counts on the next
                # request or on close_all_streams, so a lull would otherwise
                # drop the last window's count. Best-effort.
                try:
                    self.client.flush_api_call_window()
                except Exception as flush_err:
                    logger.debug("API-call window flush skipped: %s", flush_err)

        except Exception as e:
            self._db_consecutive_failures += 1
            self.errors_count += 1
            backoff = _compute_db_backoff_seconds(self._db_consecutive_failures)
            self._db_backoff_until = _time.monotonic() + backoff

            # Single safe retry path: re-submission is idempotent under
            # the GREATEST / IS DISTINCT FROM upsert contract, so
            # whether this failure was pre-commit (rolled back) or
            # commit-phase (may have applied), retaining the rows for
            # the next attempt is correct.
            self._retain_failed_option_rows(rows)

            # Include affected-symbol counts, unique underlyings, and the full
            # timestamp range. Without this, root cause analysis is impossible
            # when a single bad row triggers a whole batch rollback — a
            # 5-symbol sample buries the outlier that caused the failure.
            unique_symbols = {r["option_symbol"] for r in rows}
            unique_underlyings = sorted({r.get("underlying") for r in rows if r.get("underlying")})  # type: ignore[type-var]
            timestamps = [r.get("timestamp") for r in rows if r.get("timestamp") is not None]
            ts_min = min(timestamps) if timestamps else None  # type: ignore[type-var]
            ts_max = max(timestamps) if timestamps else None  # type: ignore[type-var]
            logger.error(
                "[CIRCUIT-BREAKER] DB write failed (%d rows, %d unique symbols, "
                "underlyings=%s, attempt #%d, backoff %.2fs): %s\n"
                "  first_symbol=%s last_symbol=%s\n"
                "  timestamp range: %s .. %s",
                len(rows),
                len(unique_symbols),
                unique_underlyings,
                self._db_consecutive_failures,
                backoff,
                e,
                rows[0].get("option_symbol") if rows else None,
                rows[-1].get("option_symbol") if rows else None,
                ts_min,
                ts_max,
                exc_info=True,
            )

    def _flush_option_bucket(
        self, option_symbol: str, bucket: datetime, keep_last_snapshot: bool = False
    ):
        """Flush a single option bucket (used by _flush_all_buffers)."""
        agg = self._prepare_option_agg(option_symbol, bucket, keep_last_snapshot)
        if agg:
            self._write_option_rows([agg])

    def _flush_all_buffers(self):
        """Flush all pending buffers"""
        logger.info(
            f"Flushing all buffers... (Underlying: {len(self.underlying_buffer)}, Options: {sum(len(v) for v in self.options_buffer.values())} across {len(self.options_buffer)} symbols)"
        )

        # Flush all options. Derive each symbol's bucket from its latest
        # buffered tick timestamp (mirroring the buffer-overflow path) so a
        # timeout/shutdown flush lands volume in the minute it actually
        # traded — not whatever wall-clock minute the flush happens to fire
        # in, which would mis-bucket any ticks that haven't crossed a minute
        # boundary yet.
        current_time = datetime.now(ET)

        options_flushed = 0
        for option_symbol in list(self.options_buffer.keys()):
            buf = self.options_buffer.get(option_symbol)
            if not buf:  # Only flush if buffer has data
                continue
            last_ts = buf[-1].get("timestamp")
            sym_bucket = bucket_timestamp(
                last_ts if last_ts else current_time,
                AGGREGATION_BUCKET_SECONDS,
            )
            self._flush_option_bucket(option_symbol, sym_bucket)
            options_flushed += 1

        logger.info(f"✅ Flushed buffers: {options_flushed} option symbols")
        self.last_flush_time = current_time

    def _check_buffer_flush_timeout(self):
        """Check if buffers should be flushed due to timeout"""
        now = datetime.now(ET)

        if (now - self.last_flush_time).total_seconds() > BUFFER_FLUSH_INTERVAL:
            logger.debug("Buffer flush timeout reached, flushing all buffers...")
            self._flush_all_buffers()

    def run_streaming(self):
        """Run streaming phase"""
        if not is_engine_run_window():
            logger.info("Skipping stream start outside configured run window")
            return True
        logger.info("=" * 80)
        logger.info("STREAMING PHASE")
        logger.info("=" * 80)

        stream_manager = StreamManager(
            client=self.client,
            underlying=self.underlying,
            db_underlying=self.db_symbol,
            num_expirations=self.num_expirations,
            strike_count_max=self.strike_count_max,
            strike_pct_range=self.strike_pct_range,
        )

        # Publish the active manager before initialize()/stream() so a
        # SIGTERM arriving during either path reaches request_stop().
        self._active_stream_manager = stream_manager

        window_closed = False
        try:
            if not stream_manager.initialize():
                logger.error("Failed to initialize streaming")
                return

            logger.info("✅ Streaming initialized")
            logger.info("Press Ctrl+C to stop\n")

            self.running = True

            for item in stream_manager.stream(max_iterations=None):
                if not self.running:
                    break
                if not is_engine_run_window():
                    logger.info("Run window closed; stopping active streams until next run window")
                    window_closed = True
                    self.running = False
                    break

                if item["type"] == "underlying":
                    self._store_underlying(item["data"])
                elif item["type"] == "option_batch":
                    self._store_option_batch(item["data"])
                elif item["type"] == "flush_options":
                    # C3: the stream is about to swap the tracked option
                    # symbol set (strike recalc / expiration refresh).
                    # Contracts dropped from the new set never tick again,
                    # so flush their pending partial buckets NOW or their
                    # last bucket's classified flow is lost (the periodic
                    # flush-timeout backstop runs at ~= recalc cadence and
                    # may never fire for a just-dropped symbol).
                    self._flush_all_buffers()

                # Check for flush timeout
                self._check_buffer_flush_timeout()

        except KeyboardInterrupt:
            logger.info("\n⚠️  Stream interrupted by user")
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
        finally:
            # Drop the reference BEFORE the slow flush path so a second
            # SIGTERM during shutdown doesn't try to poke a half-torn-down
            # stream manager.
            self._active_stream_manager = None
            self._flush_all_buffers()
            try:
                self.client.close_all_streams()
            except Exception as e:
                logger.warning(f"Error closing TradeStation streams: {e}")
            logger.info("Streaming stopped")
        return window_closed

    def run(self):
        """Run forward-only ingestion pipeline"""
        logger.info("\n" + "=" * 80)
        logger.info("ZEROGEX MAIN INGESTION ENGINE - FORWARD ONLY")
        logger.info("=" * 80)
        logger.info(f"Underlying: {self.underlying}")
        logger.info(f"Expirations: {self.num_expirations}")
        logger.info(f"Strike Band: ±{self.strike_pct_range}% of spot")
        logger.info(f"Strike Count Max: {self.strike_count_max} per expiration")
        logger.info(f"Greeks: {'ENABLED' if GREEKS_ENABLED else 'DISABLED'}")
        logger.info("")
        logger.info("NOTE: This engine streams forward-looking data.")
        logger.info("=" * 80 + "\n")

        self.running = True
        try:
            while self.running:
                if not is_engine_run_window():
                    sleep_for = seconds_until_engine_run_window()
                    logger.info(
                        "IngestionEngine [%s] paused outside run window (24x5: weekdays, non-holidays); sleeping %ss",
                        self.underlying,
                        sleep_for,
                    )
                    time.sleep(max(1, sleep_for))
                    continue

                window_closed = self.run_streaming()
                if not self.running:
                    if window_closed:
                        # run_streaming intentionally sets running=False when window closes;
                        # restore loop sentinel so scheduler can sleep and resume next window.
                        self.running = True
                    else:
                        break

        except Exception as e:
            logger.error(f"Fatal error in main engine: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # Print final stats
            logger.info("\n" + "=" * 80)
            logger.info("SESSION SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Underlying bars stored: {self.underlying_bars_stored}")
            logger.info(f"Option quotes stored: {self.option_quotes_stored}")
            if GREEKS_ENABLED:
                logger.info(f"Greeks calculated: {self.greeks_calculated}")
            logger.info(f"Errors encountered: {self.errors_count}")
            logger.info("=" * 80 + "\n")

            close_connection_pool()


def main():
    """Main entry point"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="ZeroGEX Main Ingestion Engine")
    parser.add_argument(
        "--underlying", default=None, help="Single underlying symbol (backward compatible)"
    )
    parser.add_argument(
        "--underlyings",
        default=os.getenv("INGEST_UNDERLYINGS", os.getenv("INGEST_UNDERLYING", "SPY")),
        help="Comma-separated underlying symbols or aliases (default: SPY)",
    )
    parser.add_argument(
        "--expirations",
        type=int,
        default=int(os.getenv("INGEST_EXPIRATIONS", "3")),
        help="Number of expirations (default: 3)",
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
        "--session-template",
        default=os.getenv("SESSION_TEMPLATE", "Default"),
        choices=["Default", "USEQPre", "USEQ24Hour"],
        help="Session template (default: Default)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        from src.utils import set_log_level

        set_log_level("DEBUG")

    raw_underlyings = args.underlying if args.underlying else args.underlyings
    symbols = parse_underlyings(raw_underlyings)

    if not symbols:
        logger.error("No valid underlyings provided")
        sys.exit(1)

    def run_for_symbol(symbol: str):
        from src.ingestion.api_call_tracker import attach_db_writer

        client = TradeStationClient(
            os.getenv("TRADESTATION_CLIENT_ID"),  # type: ignore[arg-type]
            os.getenv("TRADESTATION_CLIENT_SECRET"),  # type: ignore[arg-type]
            os.getenv("TRADESTATION_REFRESH_TOKEN"),  # type: ignore[arg-type]
            sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true",
        )
        attach_db_writer(client)
        engine = IngestionEngine(
            client=client,
            underlying=symbol,
            num_expirations=args.expirations,
            strike_count_max=args.strike_count_max,
            strike_pct_range=args.strike_pct_range,
        )
        engine.run()

    def run_vix_ingester():
        from src.ingestion.vix_ingester import main as vix_main

        vix_main()

    # Always run the VIX ingester alongside the per-symbol engines so that
    # /api/market/vix can read from `vix_bars` without hitting TradeStation.
    vix_enabled = os.getenv("INGEST_VIX_ENABLED", "true").lower() != "false"

    if len(symbols) == 1 and not vix_enabled:
        run_for_symbol(symbols[0])
        return

    logger.info(f"Starting ingestion engines for symbols: {', '.join(symbols)}")
    if vix_enabled:
        logger.info("Starting VIX ingester alongside symbol engines")
    processes: List[Process] = []

    for symbol in symbols:
        process = Process(target=run_for_symbol, args=(symbol,), name=f"ingest-{symbol}")
        process.start()
        processes.append(process)

    if vix_enabled:
        vix_process = Process(target=run_vix_ingester, name="ingest-vix")
        vix_process.start()
        processes.append(vix_process)

    def shutdown_children(signum, frame):
        logger.info(f"Received signal {signum}, terminating ingestion workers...")
        for proc in processes:
            if proc.is_alive():
                proc.terminate()

    signal.signal(signal.SIGINT, shutdown_children)
    signal.signal(signal.SIGTERM, shutdown_children)

    exit_code = 0
    for proc in processes:
        proc.join()
        if proc.exitcode not in (0, None):
            exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
