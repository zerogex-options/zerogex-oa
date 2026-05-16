"""
ZeroGEX Analytics Engine - Independent GEX & Max Pain Calculations

This engine runs independently from ingestion and calculates:
1. Gamma Exposure (GEX) by strike
2. GEX summary metrics (max gamma, flip point, max pain)
3. Second-order Greeks (Vanna, Charm)
4. Put/Call ratios and open interest analysis

Runs on a configured interval and writes to gex_summary and gex_by_strike tables.
"""

import os
import signal
import sys
import time
import time as _time
from multiprocessing import Process
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
import pytz
import numpy as np
from scipy import stats
from psycopg2.extras import execute_values

from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.config import RISK_FREE_RATE, ANALYTICS_FLOW_CACHE_REFRESH_ENABLED
from src.symbols import parse_underlyings, get_canonical_symbol
from src.analytics.walls import compute_call_put_walls
from src.flow_series_sql import SNAPSHOT_UPSERT_PSYCOPG2
from src.market_calendar import (
    calculate_time_to_expiration,
    expiration_close_time_et,
    is_engine_run_window,
    is_spx_am_settled_expiration,
    seconds_until_engine_run_window,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class AnalyticsEngine:
    """
    Independent analytics engine for GEX and second-order Greeks calculations

    Decoupled from ingestion - runs on its own schedule against database data.
    """

    def __init__(
        self,
        underlying: str = "SPY",
        calculation_interval: int = 60,
        risk_free_rate: float = RISK_FREE_RATE,
    ):
        """
        Initialize analytics engine

        Args:
            underlying: Underlying symbol to analyze
            calculation_interval: Seconds between calculations
            risk_free_rate: Risk-free rate for Greeks
        """
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(
            self.underlying
        )  # canonical alias for DB queries (e.g. "SPX")
        self.calculation_interval = calculation_interval
        self.risk_free_rate = risk_free_rate
        self.running = False
        self.snapshot_lookback_hours = max(
            1, int(os.getenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2"))
        )
        self.snapshot_cold_start_lookback_hours = max(
            self.snapshot_lookback_hours,
            int(os.getenv("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", "96")),
        )
        # The wide cold-start scan can legitimately run longer than the
        # pool-wide DB_STATEMENT_TIMEOUT_MS (default 90s) when the buffer
        # pool is cold.  Give just that one query a higher per-statement
        # ceiling via SET LOCAL so a cold first cycle isn't killed at 90s.
        self.snapshot_cold_start_statement_timeout_ms = max(
            0, int(os.getenv("ANALYTICS_SNAPSHOT_COLD_START_STATEMENT_TIMEOUT_MS", "180000"))
        )
        self._snapshot_cold_start_consumed = False
        self.min_oi_coverage_pct_alert = float(
            os.getenv("ANALYTICS_MIN_OI_COVERAGE_PCT_ALERT", "0.35")
        )

        # Off-hours mode: keep cycling on weekends / NYSE holidays instead
        # of sleeping until the next run window.  The snapshot is anchored
        # to the latest option_chains row (not wall-clock NOW()), so an
        # off-hours cycle recomputes against the most recent available data
        # (e.g. Friday's close on a Saturday) rather than reporting nothing.
        # A longer interval is used off-hours since the underlying data is
        # static until the next session.
        self.off_hours_enabled = os.getenv(
            "ANALYTICS_OFF_HOURS_ENABLED", "true"
        ).strip().lower() in ("1", "true", "yes", "on")
        self.off_hours_interval = max(
            self.calculation_interval,
            int(os.getenv("ANALYTICS_OFF_HOURS_INTERVAL_SECONDS", "300")),
        )

        # Metrics
        self.calculations_completed = 0
        self.errors_count = 0
        self.last_calculation_time: Optional[datetime] = None

        # Timestamp of the last SUCCESSFULLY processed snapshot.  Used to
        # skip a full recompute when _get_snapshot returns the same
        # option_chains timestamp as the last good cycle (off-hours, the
        # snapshot is frozen on the latest row until the next session, so
        # every interval would otherwise recompute identical input ->
        # identical output -> already a no-op upsert).  Only a truly
        # unchanged timestamp skips; an RTH bar advances the timestamp
        # every minute so legitimate intraday recompute is unaffected.
        self._last_processed_snapshot_ts: Optional[datetime] = None
        self._last_flow_cache_ts: Optional[datetime] = None
        self._last_flow_cache_refresh_mono: float = 0.0
        self._flow_cache_refresh_min_seconds: float = float(
            os.getenv("FLOW_CACHE_REFRESH_MIN_SECONDS", "15")
        )
        self._analytics_flow_cache_refresh_enabled: bool = ANALYTICS_FLOW_CACHE_REFRESH_ENABLED

        logger.info(f"Initialized AnalyticsEngine for {underlying}")
        logger.info(f"Calculation interval: {calculation_interval}s")
        logger.info(f"Risk-free rate: {risk_free_rate:.4f}")
        if not self._analytics_flow_cache_refresh_enabled:
            logger.info(
                "Analytics legacy flow cache refresh is DISABLED "
                "(ANALYTICS_FLOW_CACHE_REFRESH_ENABLED=false)"
            )

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"\n⚠️  Received signal {signum}, shutting down...")
        self.running = False

    # SQL for the latest-per-contract snapshot (query #3 in _get_snapshot).
    # Kept as a class attribute so the cold-start and steady-state /
    # fallback paths execute byte-identical SQL (same plan shape).
    _SNAPSHOT_QUERY = """
        SELECT DISTINCT ON (oc.option_symbol)
            oc.option_symbol,
            oc.strike,
            oc.expiration,
            oc.option_type,
            oc.last,
            oc.bid,
            oc.ask,
            oc.volume,
            oc.open_interest,
            oc.delta,
            oc.gamma,
            oc.theta,
            oc.vega,
            oc.implied_volatility,
            oc.timestamp
        FROM option_chains oc
        WHERE oc.underlying = %s
          AND oc.timestamp <= %s
          AND oc.timestamp >= %s
          AND oc.expiration > %s
          AND oc.gamma IS NOT NULL
        ORDER BY oc.option_symbol, oc.timestamp DESC
        LIMIT %s
        """

    def _run_snapshot_query(
        self,
        cursor,
        timestamp: datetime,
        lookback_hours: int,
        min_expiration,
        row_cap: int,
        statement_timeout_ms: int = 0,
    ) -> list:
        """Execute the latest-per-contract snapshot query and return rows.

        When ``statement_timeout_ms`` > 0 a ``SET LOCAL statement_timeout``
        is issued first so this single query gets a higher per-statement
        ceiling than the pool-wide default (used for the wide cold-start
        scan, which can outlast the 90s pool timeout on a cold buffer
        pool).  ``SET LOCAL`` is scoped to the current transaction and
        reverts on commit/rollback, so it never leaks to other queries.

        Caveat: ``SET LOCAL`` is a silent no-op outside a transaction.
        Today the pool is non-autocommit and query #1 in ``_get_snapshot``
        has already opened the enclosing transaction, so this is correct.
        But if the pool is ever switched to autocommit, ``SET LOCAL``
        would silently drop the cold-start ceiling and reintroduce the
        May-13-style snapshot wedge with zero signal.  Guard against that
        by emitting a WARNING (not a hard failure) when the connection is
        in autocommit mode so the regression is at least observable.
        """
        if statement_timeout_ms and statement_timeout_ms > 0:
            # psycopg2 connections expose a real bool ``autocommit``.  Use
            # ``is True`` so a MagicMock cursor in unit tests (whose
            # auto-attributed ``.connection.autocommit`` is a truthy mock)
            # doesn't spuriously trip the warning.
            autocommit = getattr(getattr(cursor, "connection", None), "autocommit", False)
            if autocommit is True:
                logger.warning(
                    "Cold-start statement_timeout (%dms) requested but the "
                    "connection is in AUTOCOMMIT mode; SET LOCAL is a no-op "
                    "outside a transaction, so the cold-start timeout is being "
                    "SKIPPED. A wide cold-start snapshot scan can now run "
                    "unbounded by the pool-wide ceiling (May-13-style wedge "
                    "risk). Investigate the connection-pool autocommit setting.",
                    statement_timeout_ms,
                )
            cursor.execute("SET LOCAL statement_timeout = %s", (str(int(statement_timeout_ms)),))
        lookback_start = timestamp - timedelta(hours=lookback_hours)
        cursor.execute(
            self._SNAPSHOT_QUERY,
            (self.db_symbol, timestamp, lookback_start, min_expiration, row_cap),
        )
        return cursor.fetchall()

    def _get_snapshot(self) -> Optional[Dict[str, Any]]:
        """Fetch latest timestamp, underlying price, and option data.

        Issued as three separate queries rather than a single CTE-heavy
        query.  The previous combined form referenced ``latest_ts.ts``
        inside a timestamp-range predicate on option_chains, which the
        planner cannot push into a timestamp-keyed index because it
        treats the CTE value as unknown at plan time -- forcing a Seq
        Scan.  Splitting into three round-trips with literal timestamps
        lets the planner see the range as constants and choose a
        timestamp-range index plan.

        Plan choice for the latest-per-contract step (query #3) depends
        on the lookback width and is left to the optimizer:

          * Narrow lookback (2h steady-state): the planner picks a
            single Index Scan on ``idx_option_chains_timestamp_expiration``,
            does an in-memory quicksort of the few thousand candidates,
            and dedupes to ~700-1000 contracts in ~70 ms warm.

          * Wide lookback (96h cold-start): the planner picks a Parallel
            Bitmap Heap Scan that BitmapAnd's ``underlying_timestamp``
            against ``expiration``, sorts ~500 k candidates via external
            merge, then dedupes.  Runs in ~40 sec warm; can blow past
            the pool-level 90 s statement_timeout when the buffer pool
            is cold (e.g. just after VACUUM evicts).

        The DISTINCT ON walk's cost is dominated by the lookback width
        since PostgreSQL has no skip-scan.  Steady-state cycles therefore
        use ``ANALYTICS_SNAPSHOT_LOOKBACK_HOURS`` (default 2), which is
        operationally safe during/around RTH because every active
        contract is requoted within minutes -- the latest quote per
        contract is virtually always present in the last hour.

        The first cycle after process start MAY use
        ``ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS`` (default 96) so
        a worker booting on Monday morning still reaches the prior
        Friday's closing quotes for every contract.  This is gated:
        the wide window is used only when the newest option_chains row
        is older than the steady-state lookback (data is stale).  A
        mid-session restart with live ingestion sees a fresh newest
        row, so it skips straight to the cheap steady-state window even
        on cycle 1 -- the slow first cycle simply doesn't happen there.

        When the wide window IS used it runs under
        ``ANALYTICS_SNAPSHOT_COLD_START_STATEMENT_TIMEOUT_MS`` (default
        180000) applied via ``SET LOCAL`` so it isn't killed by the
        lower pool-wide ceiling on a cold buffer pool.  If it still
        fails (timeout or otherwise) the engine rolls back and retries
        the SAME cycle with the cheap steady-state window, so the first
        cycle yields a narrower-but-non-empty result instead of a hard
        error + a stalled interval.  The "consumed" flag flips
        regardless of outcome so a slow/failed cold-start can never
        loop or wedge the cycle loop.

        Background -- the May 13, 2026 incident wedged production with
        a 23-minute snapshot wallclock at the historical 96-hour default
        lookback when concurrent autovacuum IO + a saturated buffer pool
        pushed the bitmap-heap-scan past every retry boundary.  The
        partial covering index keyed on (underlying, option_symbol,
        timestamp DESC) was built hoping to convert the query into an
        Index Only Scan, but EXPLAIN ANALYZE on production data showed
        the planner picks bitmap-heap-scan regardless (verified both
        with the index present and with it dropped inside a rolled-back
        transaction -- identical plans).  The actual remedies were the
        narrower steady-state lookback above, the cold-start gating +
        in-cycle steady-state fallback, and the dedicated (higher)
        cold-start statement_timeout backstop.  The covering index
        remains in place because it serves other queries (notably the
        LATERAL flow-cache backfill at
        src/api/database.py:_do_refresh_flow_cache).

        Returns dict with keys 'timestamp', 'underlying_price',
        'options' or None if no data is available.
        """
        try:
            with db_connection() as conn:
                cursor = conn.cursor()

                # 1. Latest option-chain timestamp for this underlying.
                cursor.execute(
                    """
                    SELECT timestamp
                    FROM option_chains
                    WHERE underlying = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol,),
                )
                ts_row = cursor.fetchone()
                if not ts_row or ts_row[0] is None:
                    conn.commit()
                    return None
                timestamp = ts_row[0]

                # 2. Underlying close as of that timestamp.
                cursor.execute(
                    """
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s
                      AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, timestamp),
                )
                uq_row = cursor.fetchone()
                underlying_price = float(uq_row[0]) if uq_row and uq_row[0] is not None else None
                if underlying_price is None:
                    conn.commit()
                    logger.warning("No underlying price found for snapshot")
                    return None

                # 3. Latest per-contract option quote, scoped to the
                # lookback window plus the contract-expiration roll-off.
                # Returns one row per option_symbol — the most recent
                # quote at or before ``timestamp`` whose contract has not
                # yet cleared its 16:15 ET settlement cutoff and whose
                # gamma has been populated by ingestion.  Feeds the GEX
                # and max-pain calculations in run_calculation().
                #
                # Plan choice is left to the optimizer and varies with
                # the lookback width (see the function docstring for the
                # full breakdown).  At the 2h steady-state width the
                # planner picks a single Index Scan + in-memory sort
                # (~70 ms warm); at the 96h cold-start width it picks a
                # Parallel Bitmap Heap Scan + external merge sort
                # (~40 sec warm, bounded by the cold-start statement
                # timeout when the buffer pool is cold).
                #
                # Cold-start gating (first cycle after process start):
                # the wide window only earns its cost when the newest
                # option_chains row is itself stale -- e.g. a Monday boot
                # whose latest data is the prior Friday's close, where a
                # 2h window off that timestamp would miss most of the
                # session's contracts.  When ingestion is live and the
                # newest row is within the steady-state window, the
                # narrow lookback already covers the active universe, so
                # the expensive wide scan is skipped even on cycle 1
                # (this is what keeps a mid-session restart fast).  The
                # "consumed" flag flips regardless of outcome so a
                # slow/failed cold-start can never loop.
                ts_et = timestamp.astimezone(ET)
                if ts_et.time() < dt_time(16, 15):
                    min_expiration = ts_et.date() - timedelta(days=1)
                else:
                    min_expiration = ts_et.date()
                # Hard cap on rows returned.  The previous value of 2000 was
                # below the contract count for SPX (~7k–14k unique option
                # symbols during an active session) and was silently
                # truncating the lexicographic tail of the DISTINCT ON
                # walk -- producing a deterministic bias in GEX/max-pain
                # against contracts whose option_symbol sorts last.
                # The new cap is well above any realistic chain size; if
                # we hit it we log a warning rather than silently dropping.
                snapshot_row_cap = int(os.getenv("ANALYTICS_SNAPSHOT_MAX_ROWS", "50000"))

                data_age = datetime.now(timezone.utc) - timestamp
                want_cold_start = not self._snapshot_cold_start_consumed and data_age > timedelta(
                    hours=self.snapshot_lookback_hours
                )
                self._snapshot_cold_start_consumed = True

                if want_cold_start:
                    logger.info(
                        "Cold-start snapshot: latest data is %.1fh old; using "
                        "%dh lookback (steady-state %dh) with %dms statement_timeout",
                        data_age.total_seconds() / 3600.0,
                        self.snapshot_cold_start_lookback_hours,
                        self.snapshot_lookback_hours,
                        self.snapshot_cold_start_statement_timeout_ms,
                    )
                    try:
                        rows = self._run_snapshot_query(
                            cursor,
                            timestamp,
                            self.snapshot_cold_start_lookback_hours,
                            min_expiration,
                            snapshot_row_cap,
                            statement_timeout_ms=self.snapshot_cold_start_statement_timeout_ms,
                        )
                        conn.commit()
                    except Exception as cold_err:
                        # Most commonly a statement-timeout QueryCanceled
                        # on a cold buffer pool.  Roll back the aborted
                        # transaction and immediately retry this SAME
                        # cycle with the cheap steady-state window so the
                        # first cycle still produces a (narrower but
                        # non-empty) result instead of stalling a whole
                        # interval and emitting a hard error.
                        try:
                            conn.rollback()
                        except Exception:
                            logger.warning(
                                "Rollback after cold-start failure also failed",
                                exc_info=True,
                            )
                        logger.warning(
                            "Cold-start snapshot failed (%s); retrying this "
                            "cycle with the %dh steady-state lookback",
                            cold_err.__class__.__name__,
                            self.snapshot_lookback_hours,
                            exc_info=True,
                        )
                        rows = self._run_snapshot_query(
                            cursor,
                            timestamp,
                            self.snapshot_lookback_hours,
                            min_expiration,
                            snapshot_row_cap,
                        )
                        conn.commit()
                else:
                    rows = self._run_snapshot_query(
                        cursor,
                        timestamp,
                        self.snapshot_lookback_hours,
                        min_expiration,
                        snapshot_row_cap,
                    )
                    conn.commit()

                if len(rows) >= snapshot_row_cap:
                    logger.warning(
                        "Analytics snapshot hit row cap (%d). GEX/max-pain "
                        "may be incomplete; raise ANALYTICS_SNAPSHOT_MAX_ROWS.",
                        snapshot_row_cap,
                    )

                options = [
                    {
                        "option_symbol": row[0],
                        "strike": float(row[1]),
                        "expiration": row[2],
                        "option_type": row[3],
                        "last": float(row[4]) if row[4] else 0.0,
                        "bid": float(row[5]) if row[5] else 0.0,
                        "ask": float(row[6]) if row[6] else 0.0,
                        "volume": int(row[7]) if row[7] else 0,
                        "open_interest": int(row[8]) if row[8] else 0,
                        "delta": float(row[9]) if row[9] else 0.0,
                        "gamma": float(row[10]) if row[10] else 0.0,
                        "theta": float(row[11]) if row[11] else 0.0,
                        "vega": float(row[12]) if row[12] else 0.0,
                        "implied_volatility": float(row[13]) if row[13] else 0.2,
                    }
                    for row in rows
                ]

                # Drop AM-settled SPX expirations whose 09:30 ET SOQ has
                # already happened.  Their option_chains rows can linger
                # for hours after settlement, but Greeks against an
                # unsettled-but-actually-expired strike are nonsense.
                # SPXW (weekly, PM-settled) shares the $SPX.X underlying
                # and should NOT be filtered, so we branch on the option
                # symbol prefix when available.
                today_et = ts_et.date()
                am_dropped = 0
                if ts_et.time() >= dt_time(9, 30):
                    filtered: List[Dict[str, Any]] = []
                    for opt in options:
                        is_spxw = (opt["option_symbol"] or "").upper().startswith("SPXW")
                        if (
                            opt["expiration"] == today_et
                            and not is_spxw
                            and is_spx_am_settled_expiration(self.db_symbol, opt["expiration"])
                        ):
                            am_dropped += 1
                            continue
                        filtered.append(opt)
                    options = filtered

                logger.info(
                    "Fetched %d options with Greeks "
                    "(latest-per-contract; lookback=%dh, min_expiration>%s%s)",
                    len(options),
                    self.snapshot_lookback_hours,
                    min_expiration,
                    f"; dropped {am_dropped} AM-settled" if am_dropped else "",
                )

                # Count how many have OI > 0 for informational purposes
                options_with_oi = sum(1 for opt in options if opt["open_interest"] > 0)
                oi_coverage = (options_with_oi / len(options)) if options else 0.0
                if options_with_oi > 0:
                    logger.info(
                        f"  {options_with_oi} options have open interest > 0 "
                        f"({oi_coverage:.1%} coverage)"
                    )
                else:
                    logger.info(f"  Note: All options have OI=0 (normal for real-time data)")
                    logger.info(f"  GEX will be calculated but will be 0 until OI updates")
                if options and oi_coverage < self.min_oi_coverage_pct_alert:
                    logger.warning(
                        f"⚠️ Low OI coverage in analytics snapshot: {oi_coverage:.1%} "
                        f"(threshold {self.min_oi_coverage_pct_alert:.1%})"
                    )

                return {
                    "timestamp": timestamp,
                    "underlying_price": underlying_price,
                    "options": options,
                }

        except Exception as e:
            logger.error(f"Error fetching analytics snapshot: {e}", exc_info=True)
            return None

    # Time-to-expiration math lives in src.market_calendar — see the
    # canonical ``calculate_time_to_expiration`` function imported at
    # the top of this module.  Kept as a method-style accessor so
    # existing ``self._calculate_time_to_expiration(...)`` call sites
    # keep working without touching dozens of lines of calc code.
    # Anchors at the per-symbol settlement time (09:30 ET for SPX
    # AM-settled monthlies, 16:00 ET for everything else) so the
    # morning of an SPX 3rd-Friday doesn't carry ~6.5 hours of phantom
    # time value into Greeks downstream.
    def _calculate_time_to_expiration(self, current_date: datetime, expiration_date) -> float:
        close_t = expiration_close_time_et(self.db_symbol, expiration_date)
        return calculate_time_to_expiration(
            current_date, expiration_date, market_close_time=close_t
        )

    def _calculate_vanna(self, S: float, K: float, T: float, r: float, sigma: float) -> float:
        """
        Calculate Vanna (∂²V/∂S∂σ)

        Vanna measures how delta changes with volatility.
        """
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return 0.0

        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        vanna = -stats.norm.pdf(d1) * d2 / sigma

        return vanna

    def _calculate_charm(self, S: float, K: float, T: float, r: float, sigma: float) -> float:
        """
        Calculate Charm (∂²V/∂S∂T)

        Charm measures how delta changes with time (delta decay).

        No ``option_type`` parameter: with q=0 (the dividend-free model used
        everywhere else in this codebase) put charm equals call charm by
        put-call parity, so charm is option-type independent.  The F1 fix
        pass kept an unused ``option_type`` arg for caller compatibility;
        it's now removed since this is the only call site.
        """
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return 0.0

        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        # Call charm: -N'(d1) * [2rT - d2*sigma*sqrt(T)] / [2T*sigma*sqrt(T)]
        # With q=0 (no dividend yield, the model used everywhere else in
        # this codebase), put charm equals call charm:
        #   Δ_put = Δ_call − 1 (put-call parity), so ∂Δ_put/∂t = ∂Δ_call/∂t.
        # The previous version added r·e^(−rT) for puts; that's a theta
        # adjustment, not a charm one, and is incorrect at q=0.
        charm = (
            -stats.norm.pdf(d1)
            * (2 * r * T - d2 * sigma * np.sqrt(T))
            / (2 * T * sigma * np.sqrt(T))
        )

        # Convert to per day
        charm_per_day = charm / 365.0

        return charm_per_day

    def _calculate_gex_by_strike(
        self, options: List[Dict[str, Any]], underlying_price: float, timestamp: datetime
    ) -> List[Dict[str, Any]]:
        """
        Calculate gamma exposure by strike.

        GEX = Gamma × Open Interest × 100 × Underlying Price² × 0.01

        This is the industry-standard "dollar gamma per 1% move" convention
        used by Cheddar Flow, SpotGamma, and SqueezeMetrics.  The trailing
        ``S × 0.01`` factor converts share-equivalent dealer exposure into
        the notional dollar value of the delta change for a 1% move in spot.

        For dealers (who are typically short options):
        - Call GEX is POSITIVE (dealers are short gamma on calls)
        - Put GEX is NEGATIVE (dealers are long gamma on puts)

        Net GEX = Call GEX - Put GEX
        """
        # Cache time-to-expiration per expiration date to avoid redundant
        # datetime arithmetic and scipy calls inside the inner loop.
        _tte_cache: Dict = {}

        # Group by strike and expiration
        strike_data = defaultdict(lambda: {"calls": [], "puts": []})

        for opt in options:
            key = (opt["strike"], opt["expiration"])
            if opt["option_type"] == "C":
                strike_data[key]["calls"].append(opt)
            else:
                strike_data[key]["puts"].append(opt)

        # Calculate GEX for each strike
        gex_results = []

        for (strike, expiration), data in strike_data.items():
            # Aggregate gamma by contract with OI weighting.
            # Note: there is typically one call/put contract per strike+expiration,
            # but we still compute this as a true weighted sum so the math remains
            # correct if upstream snapshots ever include multiple rows.
            call_gamma = sum(opt["gamma"] * opt["open_interest"] for opt in data["calls"])
            call_oi = sum(opt["open_interest"] for opt in data["calls"])
            call_volume = sum(opt["volume"] for opt in data["calls"])
            # Industry-standard dollar GEX per 1% move: γ × OI × 100 × S² × 0.01.
            call_gex = call_gamma * 100 * underlying_price * underlying_price * 0.01

            # Calculate put GEX (negative for dealers)
            put_gamma = sum(opt["gamma"] * opt["open_interest"] for opt in data["puts"])
            put_oi = sum(opt["open_interest"] for opt in data["puts"])
            put_volume = sum(opt["volume"] for opt in data["puts"])
            put_gex = -1 * put_gamma * 100 * underlying_price * underlying_price * 0.01

            # Total gamma (absolute)
            total_gamma = call_gamma + put_gamma

            # Net GEX (call - put, from dealer perspective)
            net_gex = call_gex + put_gex  # put_gex is already negative

            # Calculate Vanna and Charm exposure — split by option type so
            # downstream signals can apply the correct dealer-sign convention.
            call_vanna_exposure = 0.0
            put_vanna_exposure = 0.0
            call_charm_exposure = 0.0
            put_charm_exposure = 0.0

            # T is the same for all options at this (strike, expiration),
            # so cache it to avoid redundant datetime math.
            T = _tte_cache.get(expiration)
            if T is None:
                T = self._calculate_time_to_expiration(timestamp, expiration)
                _tte_cache[expiration] = T

            for opt in data["calls"] + data["puts"]:
                vanna = self._calculate_vanna(
                    underlying_price, strike, T, self.risk_free_rate, opt["implied_volatility"]
                )

                charm = self._calculate_charm(
                    underlying_price,
                    strike,
                    T,
                    self.risk_free_rate,
                    opt["implied_volatility"],
                )

                share_notional = opt["open_interest"] * 100 * underlying_price
                # Put vanna/charm on dimensionally-honest dollar bases,
                # each the per-unit-perturbation analog of GEX's
                # "$ per 1% spot move" (γ·OI·100·S²·0.01):
                #   vanna_$  = ∂Δ/∂σ · share_notional · 0.01
                #              -> $ change in the dealer delta-hedge
                #                 notional per ONE volatility point (Δσ=1%).
                #              (one S only: the vol perturbation is an
                #               absolute 0.01, not proportional to S, so
                #               there is no second S like gamma's.)
                #   charm_$  = ∂Δ/∂t(per day) · share_notional
                #              -> $ delta-hedge notional drift per DAY
                #                 (_calculate_charm already returns /day).
                # The two are different axes (vol vs time) BY NATURE;
                # downstream consumers must normalize each independently
                # (see vanna_charm_flow) rather than summing raw dollars.
                vanna_dollars = vanna * share_notional * 0.01
                charm_dollars = charm * share_notional
                if opt["option_type"] == "C":
                    call_vanna_exposure += vanna_dollars
                    call_charm_exposure += charm_dollars
                else:
                    put_vanna_exposure += vanna_dollars
                    put_charm_exposure += charm_dollars

            # Market-level aggregate (legacy columns, keeps schema stable).
            vanna_exposure = call_vanna_exposure + put_vanna_exposure
            charm_exposure = call_charm_exposure + put_charm_exposure

            # Dealer-sign convention: dealers are net short the retail book,
            # so dealer delta-hedging flow is the NEGATIVE of market-aggregate
            # charm/vanna.  Positive dealer_charm_exposure at a strike => as
            # time passes dealers must BUY the underlying at that strike
            # (bullish EOD pressure).  Bug fix (C3): previously signals read
            # the un-flipped ``charm_exposure`` and had inverted direction
            # near ATM.  Prefer ``dealer_charm_exposure`` downstream.
            dealer_vanna_exposure = -vanna_exposure
            dealer_charm_exposure = -charm_exposure

            # Bucket expirations so EOD pressure can weight 0DTE charm
            # separately from weeklies/monthlies (S2).
            try:
                dte_days = max(0, (expiration - timestamp.date()).days)
            except Exception:
                dte_days = 0
            if dte_days == 0:
                expiration_bucket = "0dte"
            elif dte_days <= 7:
                expiration_bucket = "weekly"
            elif dte_days <= 45:
                expiration_bucket = "monthly"
            else:
                expiration_bucket = "leaps"

            gex_results.append(
                {
                    "underlying": self.db_symbol,
                    "timestamp": timestamp,
                    "strike": strike,
                    "expiration": expiration,
                    "total_gamma": total_gamma,
                    "call_gamma": call_gamma,
                    "put_gamma": put_gamma,
                    "net_gex": net_gex,
                    "call_volume": call_volume,
                    "put_volume": put_volume,
                    "call_oi": call_oi,
                    "put_oi": put_oi,
                    "vanna_exposure": vanna_exposure,
                    "charm_exposure": charm_exposure,
                    "call_vanna_exposure": call_vanna_exposure,
                    "put_vanna_exposure": put_vanna_exposure,
                    "call_charm_exposure": call_charm_exposure,
                    "put_charm_exposure": put_charm_exposure,
                    "dealer_vanna_exposure": dealer_vanna_exposure,
                    "dealer_charm_exposure": dealer_charm_exposure,
                    "expiration_bucket": expiration_bucket,
                }
            )

        return gex_results

    def _calculate_max_pain(
        self, options: List[Dict[str, Any]], strike_range: Optional[Tuple[float, float]] = None
    ) -> Optional[float]:
        """
        Calculate Max Pain for a SINGLE-expiration option set.

        Callers must filter ``options`` to one expiration; the function does
        not partition internally.  Pooling across expirations conflates
        contracts that settle at different times and produces a synthetic
        value that doesn't correspond to any real settlement event.  Use
        ``_calculate_max_pain_by_expiration`` for multi-expiration data.

        Convention:
        - We compute intrinsic payout to option holders at each candidate strike.
        - "Max pain" is the strike where this aggregate payout is lowest
          (i.e., minimum liability for option writers).

        Returns:
            Max pain strike price, or ``None`` when there's no usable data.
            (Returning ``0.0`` was a footgun — downstream consumers like
            ``EODPressureSignal._pin_target`` would treat zero as a valid
            pin anchor and saturate the pin-gravity score.)
        """
        # Get unique strikes
        strikes = sorted(set(opt["strike"] for opt in options))

        if strike_range:
            strikes = [s for s in strikes if strike_range[0] <= s <= strike_range[1]]

        if not strikes:
            return None

        # Calculate total intrinsic payout at each candidate settlement strike.
        strike_payouts = {}

        for test_strike in strikes:
            total_payout = 0.0

            for opt in options:
                if opt["open_interest"] == 0:
                    continue

                strike = opt["strike"]
                oi = opt["open_interest"]

                if opt["option_type"] == "C":
                    # Call intrinsic payoff at settlement: max(0, S - K)
                    if test_strike > strike:
                        total_payout += (test_strike - strike) * oi * 100
                else:  # Put
                    # Put intrinsic payoff at settlement: max(0, K - S)
                    if test_strike < strike:
                        total_payout += (strike - test_strike) * oi * 100

            strike_payouts[test_strike] = total_payout

        # Max pain is where aggregate payout to holders is minimized
        if not strike_payouts:
            return None
        max_pain_strike = min(strike_payouts.items(), key=lambda x: x[1])[0]

        return max_pain_strike

    def _calculate_max_pain_by_expiration(self, options: List[Dict[str, Any]]) -> Dict[Any, float]:
        """Return ``{expiration: max_pain_strike}`` for every expiration.

        Each expiration's max pain is computed independently — that's the
        actual definition of max pain (the settlement strike that
        minimizes writer liability at *that* expiration's settlement).
        """
        by_exp: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for opt in options:
            by_exp[opt["expiration"]].append(opt)

        result: Dict[Any, float] = {}
        for exp, exp_options in by_exp.items():
            mp = self._calculate_max_pain(exp_options)
            if mp is not None:
                result[exp] = mp
        return result

    def _calculate_gamma_flip_point(
        self, gex_by_strike: List[Dict[str, Any]], underlying_price: float
    ) -> Optional[float]:
        """
        Calculate the gamma flip / "zero gamma" level.

        Industry convention (SpotGamma / SqueezeMetrics): the spot level
        at which *cumulative* dealer gamma exposure crosses zero — i.e.
        run the strikes low→high, accumulate net GEX, and find where the
        running total changes sign.  Above the level dealers are net long
        gamma (stabilizing); below, net short (destabilizing).

        Previously this used the *per-strike* net-GEX adjacent sign
        change, which finds where calls start out-weighting puts
        strike-by-strike — a different, non-standard level that can sit a
        long way from the cumulative zero-gamma level on lumpy OI.
        """
        if not gex_by_strike:
            return None

        # Aggregate net_gex by strike across all expirations.
        # The raw gex_by_strike has one entry per (strike, expiration),
        # so we must sum before accumulating.
        agg: Dict[float, float] = defaultdict(float)
        for entry in gex_by_strike:
            agg[entry["strike"]] += entry["net_gex"]

        strikes_sorted = sorted(agg.items())  # ascending by strike
        if len(strikes_sorted) < 2:
            return None

        # Running cumulative GEX from the lowest strike upward.
        cumulative: List[Tuple[float, float]] = []
        running = 0.0
        for strike, net_gex in strikes_sorted:
            running += net_gex
            cumulative.append((strike, running))

        # Zero crossing(s) of the cumulative curve. There can be more
        # than one on lumpy books; keep the one nearest spot (the
        # established tie-break in this codebase / the actionable level).
        best_flip = None
        best_dist = float("inf")

        def _consider(candidate: float) -> None:
            nonlocal best_flip, best_dist
            dist = abs(candidate - underlying_price)
            if dist < best_dist:
                best_dist = dist
                best_flip = candidate

        for i in range(len(cumulative) - 1):
            s1, c1 = cumulative[i]
            s2, c2 = cumulative[i + 1]
            if c1 == 0.0:
                _consider(s1)
            elif c1 * c2 < 0.0:
                _consider(s1 + (s2 - s1) * (-c1) / (c2 - c1))
        # Whole book nets flat by the top strike => flip at that strike.
        last_strike, last_cum = cumulative[-1]
        if last_cum == 0.0:
            _consider(last_strike)

        if best_flip is not None:
            logger.info(
                "Gamma flip point (cumulative zero-gamma): $%.2f " "(nearest to spot $%.2f)",
                best_flip,
                underlying_price,
            )

        return best_flip

    def _calculate_gex_summary(
        self,
        gex_by_strike: List[Dict[str, Any]],
        options: List[Dict[str, Any]],
        underlying_price: float,
        timestamp: datetime,
    ) -> Dict[str, Any]:
        """Calculate summary GEX metrics"""

        if not gex_by_strike:
            logger.warning("No GEX data to summarize")
            return None

        # Find max gamma strike.  Each gex_by_strike row is one
        # (strike, expiration) pair; aggregate net_gex by strike across
        # expirations first so a strike that's moderate at each
        # expiration but dominant in aggregate isn't passed over in
        # favor of a single-expiration outlier.  Matches the industry
        # convention used by SpotGamma / SqueezeMetrics.
        _agg_by_strike: Dict[float, float] = defaultdict(float)
        for _row in gex_by_strike:
            _agg_by_strike[_row["strike"]] += _row["net_gex"]
        _mgs_strike, _mgs_value = max(_agg_by_strike.items(), key=lambda kv: abs(kv[1]))
        max_gamma_strike = {"strike": _mgs_strike, "net_gex": _mgs_value}

        # Calculate gamma flip point
        gamma_flip_point = self._calculate_gamma_flip_point(gex_by_strike, underlying_price)

        # Calculate max pain per expiration, then pick the front month
        # (nearest non-expired settlement) for the headline scalar.  The
        # full per-expiration dict is persisted in gex_summary.max_pain_by_expiration
        # for callers that want the breakdown.  Pooling all expirations
        # into a single max-pain (the previous behavior) produced a
        # synthetic blended number that didn't correspond to any actual
        # settlement event.
        max_pain_by_exp = self._calculate_max_pain_by_expiration(options)
        if max_pain_by_exp:
            today = timestamp.astimezone(ET).date()
            future = sorted(e for e in max_pain_by_exp.keys() if e >= today)
            front_exp = future[0] if future else min(max_pain_by_exp.keys())
            max_pain = max_pain_by_exp[front_exp]
        else:
            max_pain = None

        # Total volumes and OI
        total_call_volume = sum(opt["volume"] for opt in options if opt["option_type"] == "C")
        total_put_volume = sum(opt["volume"] for opt in options if opt["option_type"] == "P")
        total_call_oi = sum(opt["open_interest"] for opt in options if opt["option_type"] == "C")
        total_put_oi = sum(opt["open_interest"] for opt in options if opt["option_type"] == "P")

        # Put/call ratio
        put_call_ratio = total_put_volume / total_call_volume if total_call_volume > 0 else 0

        # Total net GEX
        total_net_gex = sum(strike["net_gex"] for strike in gex_by_strike)

        # Distance from spot to flip (normalized by spot).
        # Close-to-zero means price is sitting near a regime boundary where
        # dealer hedging behavior can change abruptly.
        flip_distance = None
        distance_to_flip = None
        if gamma_flip_point is not None and underlying_price > 0:
            flip_distance = (underlying_price - gamma_flip_point) / underlying_price
            distance_to_flip = abs(flip_distance)

        # Local gamma density around spot (±1%). This uses absolute exposure so
        # dense nearby gamma does not cancel out due to opposing signs.
        local_band = underlying_price * 0.01
        local_gex = sum(
            abs(row["net_gex"])
            for row in gex_by_strike
            if abs(row["strike"] - underlying_price) <= local_band
        )

        # Convexity risk proxy:
        # large GEX imbalance while sitting near the flip implies higher
        # acceleration risk if the regime boundary breaks.
        convexity_risk = None
        if distance_to_flip is not None:
            convexity_risk = abs(total_net_gex) / max(distance_to_flip, 1e-6)

        # Canonical Call/Put Wall strikes — single source of truth for every
        # downstream consumer (REST endpoints, unified signal engine, playbook
        # patterns).  Defined in src/analytics/walls.py.
        call_wall, put_wall = compute_call_put_walls(gex_by_strike, underlying_price)

        summary = {
            "underlying": self.db_symbol,
            "timestamp": timestamp,
            "underlying_price": underlying_price,
            "max_gamma_strike": max_gamma_strike["strike"],
            "max_gamma_value": max_gamma_strike["net_gex"],
            "gamma_flip_point": gamma_flip_point,
            "flip_distance": flip_distance,
            "local_gex": local_gex,
            "convexity_risk": convexity_risk,
            "put_call_ratio": put_call_ratio,
            "max_pain": max_pain,
            "total_call_volume": total_call_volume,
            "total_put_volume": total_put_volume,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "total_net_gex": total_net_gex,
            "call_wall": call_wall,
            "put_wall": put_wall,
            "max_pain_by_expiration": max_pain_by_exp,
        }

        return summary

    def _store_gex_by_strike(self, gex_data: List[Dict[str, Any]], cursor) -> None:
        """Write GEX-by-strike rows on ``cursor``.

        Pure unit of work: it issues the bulk upsert and nothing else.
        The transaction boundary (open connection, commit-on-success,
        rollback-on-error) is owned by the caller's ``db_connection()``
        scope -- see ``_store_calculation_results``.  Keeping commit/
        rollback out of here is what lets the by-strike and summary
        writes share one atomic transaction.
        """
        rows = [
            (
                data["underlying"],
                data["timestamp"],
                float(data["strike"]),
                data["expiration"],
                float(data["total_gamma"]),
                float(data["call_gamma"]),
                float(data["put_gamma"]),
                float(data["net_gex"]),
                int(data["call_volume"]),
                int(data["put_volume"]),
                int(data["call_oi"]),
                int(data["put_oi"]),
                float(data["vanna_exposure"]),
                float(data["charm_exposure"]),
                float(data.get("call_vanna_exposure", 0.0)),
                float(data.get("put_vanna_exposure", 0.0)),
                float(data.get("call_charm_exposure", 0.0)),
                float(data.get("put_charm_exposure", 0.0)),
                float(data.get("dealer_vanna_exposure", -float(data["vanna_exposure"]))),
                float(data.get("dealer_charm_exposure", -float(data["charm_exposure"]))),
                data.get("expiration_bucket"),
            )
            for data in gex_data
        ]

        execute_values(
            cursor,
            """
            INSERT INTO gex_by_strike
            (underlying, timestamp, strike, expiration, total_gamma,
             call_gamma, put_gamma, net_gex, call_volume, put_volume,
             call_oi, put_oi, vanna_exposure, charm_exposure,
             call_vanna_exposure, put_vanna_exposure,
             call_charm_exposure, put_charm_exposure,
             dealer_vanna_exposure, dealer_charm_exposure,
             expiration_bucket)
            VALUES %s
            ON CONFLICT (underlying, timestamp, strike, expiration) DO UPDATE SET
                total_gamma = EXCLUDED.total_gamma,
                call_gamma = EXCLUDED.call_gamma,
                put_gamma = EXCLUDED.put_gamma,
                net_gex = EXCLUDED.net_gex,
                call_volume = EXCLUDED.call_volume,
                put_volume = EXCLUDED.put_volume,
                call_oi = EXCLUDED.call_oi,
                put_oi = EXCLUDED.put_oi,
                vanna_exposure = EXCLUDED.vanna_exposure,
                charm_exposure = EXCLUDED.charm_exposure,
                call_vanna_exposure = EXCLUDED.call_vanna_exposure,
                put_vanna_exposure = EXCLUDED.put_vanna_exposure,
                call_charm_exposure = EXCLUDED.call_charm_exposure,
                put_charm_exposure = EXCLUDED.put_charm_exposure,
                dealer_vanna_exposure = EXCLUDED.dealer_vanna_exposure,
                dealer_charm_exposure = EXCLUDED.dealer_charm_exposure,
                expiration_bucket = EXCLUDED.expiration_bucket
            WHERE
                EXCLUDED.total_gamma IS DISTINCT FROM gex_by_strike.total_gamma
                OR EXCLUDED.call_gamma IS DISTINCT FROM gex_by_strike.call_gamma
                OR EXCLUDED.put_gamma IS DISTINCT FROM gex_by_strike.put_gamma
                OR EXCLUDED.net_gex IS DISTINCT FROM gex_by_strike.net_gex
                OR EXCLUDED.call_volume IS DISTINCT FROM gex_by_strike.call_volume
                OR EXCLUDED.put_volume IS DISTINCT FROM gex_by_strike.put_volume
                OR EXCLUDED.call_oi IS DISTINCT FROM gex_by_strike.call_oi
                OR EXCLUDED.put_oi IS DISTINCT FROM gex_by_strike.put_oi
                OR EXCLUDED.vanna_exposure IS DISTINCT FROM gex_by_strike.vanna_exposure
                OR EXCLUDED.charm_exposure IS DISTINCT FROM gex_by_strike.charm_exposure
                OR EXCLUDED.dealer_charm_exposure IS DISTINCT FROM gex_by_strike.dealer_charm_exposure
                OR EXCLUDED.dealer_vanna_exposure IS DISTINCT FROM gex_by_strike.dealer_vanna_exposure
            """,
            rows,
        )

        logger.info(f"✅ Stored {len(gex_data)} GEX by strike records")

    def _store_gex_summary(self, summary: Dict[str, Any], cursor) -> None:
        """Write the GEX summary row on ``cursor``.

        Pure unit of work (gamma-flip carry-forward SELECT + the summary
        upsert).  Like ``_store_gex_by_strike`` it owns no transaction
        boundary; the caller's ``db_connection()`` scope commits/rolls
        back so this write stays in the same atomic transaction as the
        by-strike write.
        """
        gamma_flip_point = summary.get("gamma_flip_point")
        if gamma_flip_point is None:
            cursor.execute(
                """
                SELECT gamma_flip_point
                FROM gex_summary
                WHERE underlying = %s
                  AND gamma_flip_point IS NOT NULL
                  AND timestamp < %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (summary["underlying"], summary["timestamp"]),
            )
            prev_row = cursor.fetchone()
            if prev_row and prev_row[0] is not None:
                gamma_flip_point = float(prev_row[0])
                logger.info(
                    "Gamma flip carry-forward applied: using prior value %.4f at %s",
                    gamma_flip_point,
                    summary["timestamp"],
                )

        flip_distance = summary.get("flip_distance")
        convexity_risk = summary.get("convexity_risk")
        spot_price = float(summary.get("underlying_price") or 0.0)
        total_net_gex = float(summary.get("total_net_gex") or 0.0)
        if flip_distance is None and gamma_flip_point is not None and spot_price > 0:
            flip_distance = (spot_price - gamma_flip_point) / spot_price
        if convexity_risk is None and flip_distance is not None:
            convexity_risk = abs(total_net_gex) / max(abs(flip_distance), 1e-6)

        call_wall_val = summary.get("call_wall")
        put_wall_val = summary.get("put_wall")
        mp_by_exp_raw = summary.get("max_pain_by_expiration") or {}
        # Serialize {date -> strike} into a JSON-shaped dict with
        # iso-date keys.  psycopg2 will adapt the dict to JSONB.
        import json as _json

        mp_by_exp_json = (
            _json.dumps(
                {
                    (exp.isoformat() if hasattr(exp, "isoformat") else str(exp)): float(v)
                    for exp, v in mp_by_exp_raw.items()
                }
            )
            if mp_by_exp_raw
            else None
        )
        cursor.execute(
            """
            INSERT INTO gex_summary
            (underlying, timestamp, max_gamma_strike, max_gamma_value,
             gamma_flip_point, put_call_ratio, max_pain, total_call_volume,
             total_put_volume, total_call_oi, total_put_oi, total_net_gex,
             flip_distance, local_gex, convexity_risk, call_wall, put_wall,
             max_pain_by_expiration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (underlying, timestamp) DO UPDATE SET
                max_gamma_strike = EXCLUDED.max_gamma_strike,
                max_gamma_value = EXCLUDED.max_gamma_value,
                gamma_flip_point = EXCLUDED.gamma_flip_point,
                put_call_ratio = EXCLUDED.put_call_ratio,
                max_pain = EXCLUDED.max_pain,
                total_call_volume = EXCLUDED.total_call_volume,
                total_put_volume = EXCLUDED.total_put_volume,
                total_call_oi = EXCLUDED.total_call_oi,
                total_put_oi = EXCLUDED.total_put_oi,
                total_net_gex = EXCLUDED.total_net_gex,
                flip_distance = EXCLUDED.flip_distance,
                local_gex = EXCLUDED.local_gex,
                convexity_risk = EXCLUDED.convexity_risk,
                call_wall = EXCLUDED.call_wall,
                put_wall = EXCLUDED.put_wall,
                max_pain_by_expiration = EXCLUDED.max_pain_by_expiration
            WHERE
                EXCLUDED.max_gamma_strike IS DISTINCT FROM gex_summary.max_gamma_strike
                OR EXCLUDED.max_gamma_value IS DISTINCT FROM gex_summary.max_gamma_value
                OR EXCLUDED.gamma_flip_point IS DISTINCT FROM gex_summary.gamma_flip_point
                OR EXCLUDED.put_call_ratio IS DISTINCT FROM gex_summary.put_call_ratio
                OR EXCLUDED.max_pain IS DISTINCT FROM gex_summary.max_pain
                OR EXCLUDED.total_call_volume IS DISTINCT FROM gex_summary.total_call_volume
                OR EXCLUDED.total_put_volume IS DISTINCT FROM gex_summary.total_put_volume
                OR EXCLUDED.total_call_oi IS DISTINCT FROM gex_summary.total_call_oi
                OR EXCLUDED.total_put_oi IS DISTINCT FROM gex_summary.total_put_oi
                OR EXCLUDED.total_net_gex IS DISTINCT FROM gex_summary.total_net_gex
                OR EXCLUDED.flip_distance IS DISTINCT FROM gex_summary.flip_distance
                OR EXCLUDED.local_gex IS DISTINCT FROM gex_summary.local_gex
                OR EXCLUDED.convexity_risk IS DISTINCT FROM gex_summary.convexity_risk
                OR EXCLUDED.call_wall IS DISTINCT FROM gex_summary.call_wall
                OR EXCLUDED.put_wall IS DISTINCT FROM gex_summary.put_wall
                OR EXCLUDED.max_pain_by_expiration IS DISTINCT FROM gex_summary.max_pain_by_expiration
        """,
            (
                summary["underlying"],
                summary["timestamp"],
                float(summary["max_gamma_strike"]),
                float(summary["max_gamma_value"]),
                gamma_flip_point,
                float(summary["put_call_ratio"]),
                (float(summary["max_pain"]) if summary.get("max_pain") is not None else None),
                int(summary["total_call_volume"]),
                int(summary["total_put_volume"]),
                int(summary["total_call_oi"]),
                int(summary["total_put_oi"]),
                float(summary["total_net_gex"]),
                float(flip_distance) if flip_distance is not None else None,
                float(summary.get("local_gex", 0.0)),
                float(convexity_risk) if convexity_risk is not None else None,
                float(call_wall_val) if call_wall_val is not None else None,
                float(put_wall_val) if put_wall_val is not None else None,
                mp_by_exp_json,
            ),
        )
        logger.info("✅ Stored GEX summary")

    def _store_calculation_results(
        self,
        gex_data: List[Dict[str, Any]],
        summary: Dict[str, Any],
    ) -> None:
        """Persist by-strike + summary in ONE transaction (all rows land or none).

        Both writes run on the same connection inside a single
        ``db_connection()`` scope.  That context manager commits exactly
        once on a clean exit and rolls back on ANY exception, so a failure
        in the summary write discards the by-strike rows written earlier
        in the same transaction.  This atomicity ("both stores commit
        together") is the invariant downstream consumers rely on, so the
        grouping must not be split into two independent transactions.
        """
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                self._store_gex_by_strike(gex_data, cursor)
                self._store_gex_summary(summary, cursor)
                # db_connection() commits on a clean __exit__; the explicit
                # commit makes the single-transaction boundary unambiguous
                # and is a harmless no-op when the CM commits again.
                conn.commit()
        except Exception as e:
            logger.error("Error storing calculation results: %s", e, exc_info=True)
            self.errors_count += 1
            raise

    def _validate_gex_calculations(
        self,
        gex_by_strike: List[Dict[str, Any]],
        summary: Dict[str, Any],
        underlying_price: float,
    ):
        """Run consistency checks and log any numerical drift or sign anomalies."""
        mismatches = 0
        sign_anomalies = 0
        for row in gex_by_strike:
            # Recompute with the same convention as ``_calculate_gex_by_strike``
            # (γ × OI × 100 × S² × 0.01).  Mismatches here mean by-strike rows
            # were derived with a different formula than ``net_gex``.
            call_gex = row["call_gamma"] * 100 * underlying_price * underlying_price * 0.01
            put_gex = -1 * row["put_gamma"] * 100 * underlying_price * underlying_price * 0.01
            if abs((call_gex + put_gex) - row["net_gex"]) > 1e-6:
                mismatches += 1
            if row["call_gamma"] < 0 or row["put_gamma"] < 0:
                sign_anomalies += 1

        summary_total = sum(strike["net_gex"] for strike in gex_by_strike)
        if abs(summary_total - summary["total_net_gex"]) > 1e-6:
            mismatches += 1

        if mismatches:
            logger.warning(
                "GEX validation: detected %d by-strike arithmetic mismatches", mismatches
            )
        if sign_anomalies:
            logger.warning(
                "GEX validation: detected %d sign anomalies (negative aggregated gamma)",
                sign_anomalies,
            )
        if not mismatches and not sign_anomalies:
            logger.info("GEX validation: all by-strike calculations passed")

    def _symbol_tuned_float(self, base: str, default: float) -> float:
        """Resolve a per-symbol env-tunable float.

        Precedence (matches put_call_ratio_state's convention):
          1. ``<BASE>_<SYMBOL>``  (e.g. SMART_MONEY_IV_INCL_SPX)
          2. ``<BASE>_DEFAULT``
          3. the hardcoded ``default``
        Non-positive / unparseable overrides are ignored.
        """
        sym = (self.db_symbol or "").upper()
        for key in (f"{base}_{sym}" if sym else None, f"{base}_DEFAULT"):
            if not key:
                continue
            raw = os.getenv(key)
            if raw:
                try:
                    v = float(raw)
                    if v > 0:
                        return v
                except ValueError:
                    pass
        return default

    def _smart_money_calibration(
        self,
        vol_p95: Optional[float],
        prem_p95: Optional[float],
        underlying_price: Optional[float],
    ) -> Tuple[Tuple[int, int, int, int], Tuple[float, float, float, float], str]:
        """Resolve smart-money score tier thresholds (D6 follow-up).

        Distribution-based when a positive per-symbol rolling p95 of
        volume_delta / premium is available in component_normalizer_cache
        (the defensible "unusual = upper percentile of recent flow"
        definition): tier breakpoints are env-tunable multiples of p95,
        with tier 2 sitting AT p95.  Falls back PER FIELD to the existing
        env-tunable tiers on cold start (missing/non-positive p95) -- the
        volume tiers stay raw contract counts, the premium tiers stay
        ``N x notional_per_contract``.  Returns
        ``(vol_tiers, prem_tiers, mode)`` where ``mode`` is logged.
        """
        notional_per_contract = max(float(underlying_price or 0.0) * 100.0, 1.0)

        vol_mult = (
            float(os.getenv("SMART_MONEY_VOL_DIST_T1_P95_X", "0.5")),
            float(os.getenv("SMART_MONEY_VOL_DIST_T2_P95_X", "1.0")),
            float(os.getenv("SMART_MONEY_VOL_DIST_T3_P95_X", "2.0")),
            float(os.getenv("SMART_MONEY_VOL_DIST_T4_P95_X", "4.0")),
        )
        prem_mult = (
            float(os.getenv("SMART_MONEY_PREM_DIST_T1_P95_X", "0.5")),
            float(os.getenv("SMART_MONEY_PREM_DIST_T2_P95_X", "1.0")),
            float(os.getenv("SMART_MONEY_PREM_DIST_T3_P95_X", "2.0")),
            float(os.getenv("SMART_MONEY_PREM_DIST_T4_P95_X", "4.0")),
        )

        if vol_p95 is not None and vol_p95 > 0:
            # max(1, ...) so a tiny p95 can't yield a 0 threshold (which
            # would make the inclusion floor admit every contract).
            vol_tiers = tuple(max(1, int(round(m * vol_p95))) for m in vol_mult)
            vol_mode = "dist"
        else:
            vol_tiers = (
                int(os.getenv("SMART_MONEY_VOL_T1", "50")),
                int(os.getenv("SMART_MONEY_VOL_T2", "100")),
                int(os.getenv("SMART_MONEY_VOL_T3", "200")),
                int(os.getenv("SMART_MONEY_VOL_T4", "500")),
            )
            vol_mode = "tier"

        if prem_p95 is not None and prem_p95 > 0:
            prem_tiers = tuple(m * prem_p95 for m in prem_mult)
            prem_mode = "dist"
        else:
            prem_tiers = (
                float(os.getenv("SMART_MONEY_PREM_T1_NOTIONAL_X", "1.0")) * notional_per_contract,
                float(os.getenv("SMART_MONEY_PREM_T2_NOTIONAL_X", "2.0")) * notional_per_contract,
                float(os.getenv("SMART_MONEY_PREM_T3_NOTIONAL_X", "5.0")) * notional_per_contract,
                float(os.getenv("SMART_MONEY_PREM_T4_NOTIONAL_X", "10.0")) * notional_per_contract,
            )
            prem_mode = "tier"

        return vol_tiers, prem_tiers, f"vol={vol_mode},prem={prem_mode}"

    def _fetch_smart_money_p95(self, cursor) -> Tuple[Optional[float], Optional[float]]:
        """Read rolling p95(volume_delta) / p95(premium) for this symbol
        from component_normalizer_cache.  Returns (None, None) on a cold
        cache or any read error so the caller falls back to static tiers."""
        try:
            cursor.execute(
                """
                SELECT field_name, p95
                FROM component_normalizer_cache
                WHERE underlying = %s
                  AND field_name IN ('smart_money_volume_delta', 'smart_money_premium')
                """,
                (self.db_symbol,),
            )
            vol_p95: Optional[float] = None
            prem_p95: Optional[float] = None
            for field_name, p95 in cursor.fetchall():
                if p95 is None:
                    continue
                if field_name == "smart_money_volume_delta":
                    vol_p95 = float(p95)
                elif field_name == "smart_money_premium":
                    prem_p95 = float(p95)
            return vol_p95, prem_p95
        except Exception:
            logger.warning(
                "smart-money p95 lookup failed; falling back to static tiers",
                exc_info=True,
            )
            return None, None

    def _refresh_flow_caches(self, timestamp: datetime, underlying_price: Optional[float] = None):
        """
        Refresh flow cache tables for the given timestamp.

        underlying_price should be passed in from run_calculation() where it is
        already fetched, avoiding a redundant query.

        Uses LAG() window functions instead of LATERAL joins for O(n) performance.
        """
        if not self._analytics_flow_cache_refresh_enabled:
            return

        if self._last_flow_cache_ts == timestamp:
            logger.debug("Skipping flow cache refresh (timestamp unchanged)")
            return

        now_mono = _time.monotonic()
        if (now_mono - self._last_flow_cache_refresh_mono) < self._flow_cache_refresh_min_seconds:
            logger.debug("Skipping flow cache refresh (min-seconds throttle)")
            return

        try:
            with db_connection() as conn:
                cursor = conn.cursor()

                # Refresh flow_by_contract: unified 5-min-bucketed rollup
                # keyed by (timestamp, symbol, option_type, strike, expiration).
                # Each row stores DAY-TO-DATE cumulative values for one
                # contract as of the end of its bucket. The session resets
                # at 09:30 ET (TradeStation RTH open), so cumulative counters
                # zero at open. We upsert both the current bucket and the
                # previous one on every refresh: refreshing the previous
                # bucket after rollover captures any trailing facts that
                # landed between its last refresh and its bucket boundary.
                bucket_epoch = int(timestamp.timestamp() // 300) * 300
                curr_bucket_start = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
                curr_bucket_end = curr_bucket_start + timedelta(minutes=5)
                prev_bucket_start = curr_bucket_start - timedelta(minutes=5)

                # Session open: 09:30 ET of the day containing `timestamp`.
                ts_et = timestamp.astimezone(ET)
                session_open_et = ET.localize(datetime(ts_et.year, ts_et.month, ts_et.day, 9, 30))
                session_open = session_open_et.astimezone(timezone.utc)

                logger.info(
                    "Refreshing flow_by_contract for %s buckets=[%s, %s]",
                    self.db_symbol,
                    prev_bucket_start.isoformat(),
                    curr_bucket_start.isoformat(),
                )
                # Single statement covers both bucket rows via a values table.
                # Each bucket aggregates facts from [session_open, bucket_end),
                # giving cumulative values keyed at bucket_start.
                cursor.execute(
                    """
                    WITH bucket_targets AS (
                        SELECT * FROM (VALUES
                            (%s::timestamptz, %s::timestamptz),
                            (%s::timestamptz, %s::timestamptz)
                        ) AS t(bucket_start, bucket_end)
                        WHERE bucket_start >= %s::timestamptz
                    )
                    INSERT INTO flow_by_contract (
                        timestamp,
                        symbol,
                        option_type,
                        strike,
                        expiration,
                        raw_volume,
                        raw_premium,
                        net_volume,
                        net_premium,
                        underlying_price
                    )
                    SELECT
                        bt.bucket_start                              AS timestamp,
                        f.symbol,
                        f.option_type,
                        f.strike,
                        f.expiration,
                        SUM(f.volume_delta)::bigint                  AS raw_volume,
                        SUM(f.premium_delta)::numeric                AS raw_premium,
                        SUM(f.buy_volume - f.sell_volume)::bigint    AS net_volume,
                        SUM(f.buy_premium - f.sell_premium)::numeric AS net_premium,
                        COALESCE(MAX(f.underlying_price), %s)::numeric AS underlying_price
                    FROM flow_contract_facts f
                    CROSS JOIN bucket_targets bt
                    WHERE f.symbol = %s
                      AND f.timestamp >= %s::timestamptz
                      AND f.timestamp <  bt.bucket_end
                    GROUP BY bt.bucket_start, f.symbol, f.option_type, f.strike, f.expiration
                    HAVING SUM(f.volume_delta) > 0
                    ON CONFLICT (timestamp, symbol, option_type, strike, expiration)
                    DO UPDATE SET
                        raw_volume = EXCLUDED.raw_volume,
                        raw_premium = EXCLUDED.raw_premium,
                        net_volume = EXCLUDED.net_volume,
                        net_premium = EXCLUDED.net_premium,
                        underlying_price = EXCLUDED.underlying_price,
                        updated_at = NOW()
                """,
                    (
                        prev_bucket_start,
                        curr_bucket_start,  # row 1: (prev_start, prev_end)
                        curr_bucket_start,
                        curr_bucket_end,  # row 2: (curr_start, curr_end)
                        session_open,  # bucket_start >= session_open
                        underlying_price,  # underlying_price fallback
                        self.db_symbol,  # f.symbol filter
                        session_open,  # f.timestamp >= session_open
                    ),
                )
                logger.info(
                    "flow_by_contract refresh upserted %d rows for %s (buckets [%s, %s])",
                    cursor.rowcount,
                    self.db_symbol,
                    prev_bucket_start.isoformat(),
                    curr_bucket_start.isoformat(),
                )

                # Refresh flow_smart_money.
                #
                # Scoring is distribution-based when the per-symbol rolling
                # p95 of volume_delta / premium is in
                # component_normalizer_cache ("unusual" = upper percentile
                # of recent per-contract flow); it falls back per field to
                # the env-tunable static tiers on a cold cache (volume =
                # raw contract counts, premium = N x notional_per_contract
                # so SPX ~$550k/contract and SPY ~$45k/contract stay
                # comparable).  The IV / deep-OTM inclusion thresholds are
                # per-symbol env-tunable.  Calibration discussion lives in
                # docs/runbooks/smart_money_calibration.md.
                vol_p95, prem_p95 = self._fetch_smart_money_p95(cursor)
                (
                    (vol_t1, vol_t2, vol_t3, vol_t4),
                    (
                        prem_t1,
                        prem_t2,
                        prem_t3,
                        prem_t4,
                    ),
                    calib_mode,
                ) = self._smart_money_calibration(vol_p95, prem_p95, underlying_price)
                # IV / deep-OTM inclusion thresholds (D6 follow-up:
                # previously hardcoded 0.4 IV / 0.15 |delta|).
                iv_incl = self._symbol_tuned_float("SMART_MONEY_IV_INCL", 0.4)
                deep_otm_delta = self._symbol_tuned_float("SMART_MONEY_DEEP_OTM_DELTA", 0.15)
                logger.debug(
                    "Refreshing flow_smart_money (%s, vol_p95=%s, prem_p95=%s, "
                    "iv_incl=%.3f, deep_otm_delta=%.3f)...",
                    calib_mode,
                    vol_p95,
                    prem_p95,
                    iv_incl,
                    deep_otm_delta,
                )
                cursor.execute(
                    """
                    WITH with_prev AS (
                        SELECT
                            oc.timestamp,
                            oc.option_symbol,
                            oc.option_type,
                            oc.strike,
                            oc.expiration,
                            oc.last,
                            oc.implied_volatility,
                            oc.delta,
                            CASE
                                WHEN LAG(oc.volume) OVER w IS NULL THEN COALESCE(oc.volume, 0)
                                WHEN (LAG(oc.timestamp) OVER w AT TIME ZONE 'America/New_York')::date
                                    = (oc.timestamp AT TIME ZONE 'America/New_York')::date
                                    THEN GREATEST(COALESCE(oc.volume, 0) - COALESCE(LAG(oc.volume) OVER w, 0), 0)
                                ELSE COALESCE(oc.volume, 0)
                            END::bigint AS volume_delta
                        FROM option_chains oc
                        WHERE oc.underlying = %s
                          AND oc.timestamp >= %s - INTERVAL '2 minutes'
                          AND oc.timestamp <= %s
                        WINDOW w AS (PARTITION BY oc.option_symbol ORDER BY oc.timestamp)
                    )
                    INSERT INTO flow_smart_money (
                        timestamp,
                        symbol,
                        option_symbol,
                        strike,
                        expiration,
                        option_type,
                        total_volume,
                        total_premium,
                        avg_iv,
                        avg_delta,
                        unusual_activity_score,
                        underlying_price
                    )
                    SELECT
                        timestamp,
                        %s::varchar,
                        option_symbol,
                        strike,
                        expiration,
                        option_type,
                        volume_delta::bigint,
                        (volume_delta * COALESCE(last, 0) * 100)::numeric,
                        implied_volatility::numeric,
                        delta::numeric,
                        LEAST(10, GREATEST(0,
                            CASE WHEN volume_delta >= %s THEN 4 WHEN volume_delta >= %s THEN 3 WHEN volume_delta >= %s THEN 2 WHEN volume_delta >= %s THEN 1 ELSE 0 END +
                            CASE WHEN volume_delta * COALESCE(last, 0) * 100 >= %s THEN 4 WHEN volume_delta * COALESCE(last, 0) * 100 >= %s THEN 3 WHEN volume_delta * COALESCE(last, 0) * 100 >= %s THEN 2 WHEN volume_delta * COALESCE(last, 0) * 100 >= %s THEN 1 ELSE 0 END +
                            CASE WHEN implied_volatility > 1.0 THEN 2 WHEN implied_volatility > 0.6 THEN 1 ELSE 0 END
                        ))::numeric,
                        %s::numeric
                    FROM with_prev
                    WHERE timestamp = %s
                      AND volume_delta > 0
                      AND (
                        volume_delta >= %s
                        OR volume_delta * COALESCE(last, 0) * 100 >= %s
                        OR (implied_volatility > %s AND volume_delta >= 20)
                        OR (ABS(delta) < %s AND volume_delta >= 20)
                      )
                    ON CONFLICT (timestamp, symbol, option_symbol)
                    DO UPDATE SET
                        strike = EXCLUDED.strike,
                        expiration = EXCLUDED.expiration,
                        option_type = EXCLUDED.option_type,
                        total_volume = EXCLUDED.total_volume,
                        total_premium = EXCLUDED.total_premium,
                        avg_iv = EXCLUDED.avg_iv,
                        avg_delta = EXCLUDED.avg_delta,
                        unusual_activity_score = EXCLUDED.unusual_activity_score,
                        underlying_price = EXCLUDED.underlying_price,
                        updated_at = NOW()
                """,
                    (
                        self.db_symbol,
                        timestamp,
                        timestamp,
                        self.db_symbol,
                        # Volume score tiers (descending so the highest matches first)
                        vol_t4,
                        vol_t3,
                        vol_t2,
                        vol_t1,
                        # Premium score tiers (descending)
                        prem_t4,
                        prem_t3,
                        prem_t2,
                        prem_t1,
                        underlying_price,
                        timestamp,
                        # Inclusion filter: floor matches t1, then the
                        # per-symbol IV / deep-OTM thresholds.
                        vol_t1,
                        prem_t1,
                        iv_incl,
                        deep_otm_delta,
                    ),
                )

                # Retention policy: keep only recent smart-money cache rows
                cursor.execute("""
                    DELETE FROM flow_smart_money
                    WHERE timestamp < NOW() - INTERVAL '7 days'
                """)

                conn.commit()
                self._last_flow_cache_ts = timestamp
                self._last_flow_cache_refresh_mono = now_mono
                logger.info("✅ Flow cache tables refreshed successfully")

        except Exception as e:
            logger.error(f"Error refreshing flow caches: {e}", exc_info=True)

    def _refresh_flow_series_snapshot(self, timestamp: datetime):
        """Materialise flow_series_5min for the current session.

        Runs the *exact* /api/flow/series CTE (unfiltered) for this symbol
        over the resolved current-session window and UPSERTs every row.
        The stored rows therefore equal what the API CTE would compute by
        construction — parity is not re-implemented. The outer window is
        ROWS UNBOUNDED PRECEDING ORDER BY bar_start, so a closed bar's
        cumulative values are window-invariant: once its 5-min boundary
        passes, the row is final and the IS DISTINCT FROM guard turns
        subsequent cycles into no-ops. Only the open bar (and any quiet
        carry-forward tail) churns cycle-to-cycle, which is also how the
        prior bucket gets "finalised" — the next cycle recomputes it to
        its closed value and writes it once.

        Best-effort and gated by the same flag as the flow-cache refresh:
        the snapshot is downstream of flow_by_contract, and a failure here
        must never break the analytics cycle or the GEX path. Mirrors
        _refresh_flow_caches' error handling (log, do not raise).
        """
        if not self._analytics_flow_cache_refresh_enabled:
            return

        try:
            # Resolve the current-session window exactly as the API's
            # _resolve_flow_series_session does for session='current', so
            # engine-written rows match the window the API will read.
            ts_et = timestamp.astimezone(ET)
            session_open_et = ET.localize(datetime(ts_et.year, ts_et.month, ts_et.day, 9, 30))
            session_start = session_open_et.astimezone(timezone.utc)
            session_close = session_start + timedelta(hours=6, minutes=45)
            now_utc = datetime.now(timezone.utc)
            now_floor_epoch = int(now_utc.timestamp() // 300) * 300
            now_floored = datetime.fromtimestamp(now_floor_epoch, tz=timezone.utc)
            session_end = min(now_floored, session_close)
            if session_end < session_start:
                session_end = session_start

            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    SNAPSHOT_UPSERT_PSYCOPG2,
                    {
                        "symbol": self.db_symbol,
                        "session_start": session_start,
                        "session_end": session_end,
                        "strikes": None,
                        "expirations": None,
                    },
                )
                conn.commit()
                logger.info(
                    "flow_series_5min snapshot upserted %d rows for %s " "(window [%s, %s])",
                    cursor.rowcount,
                    self.db_symbol,
                    session_start.isoformat(),
                    session_end.isoformat(),
                )

        except Exception as e:
            logger.error(f"Error refreshing flow_series_5min snapshot: {e}", exc_info=True)

    def run_calculation(self) -> bool:
        """
        Run one complete analytics calculation cycle

        Returns:
            True if successful, False otherwise
        """
        stage_timings: Dict[str, float] = {}

        try:
            # Single DB call: get timestamp, underlying price, and option data
            t0 = _time.monotonic()
            snapshot = self._get_snapshot()
            stage_timings["snapshot"] = _time.monotonic() - t0

            if not snapshot:
                logger.warning("No option data available in database")
                return False

            latest_timestamp = snapshot["timestamp"]
            underlying_price = snapshot["underlying_price"]
            options = snapshot["options"]

            # Skip the recompute when the snapshot timestamp is unchanged
            # since the last successful cycle.  Off-hours the latest
            # option_chains row is frozen until the next session, so every
            # off_hours_interval would otherwise recompute the full
            # GEX-by-strike / vanna-charm / per-expiration max-pain / walls
            # pipeline for the SAME (underlying, timestamp) -- identical
            # input -> identical output -> an already no-op
            # `IS DISTINCT FROM`-guarded upsert.  We still sleep the
            # interval in run(); we just don't burn CPU recomputing.
            #
            # Scoped to an EXACT timestamp match so it never suppresses
            # legitimate intraday recompute: during RTH a new bar advances
            # the timestamp every minute, so latest_timestamp moves and the
            # guard falls through.  Only set on SUCCESS (see end of method)
            # so a failed/partial cycle re-attempts the same timestamp.
            if (
                self._last_processed_snapshot_ts is not None
                and latest_timestamp == self._last_processed_snapshot_ts
            ):
                logger.info(
                    "Snapshot timestamp %s unchanged since last successful "
                    "cycle; skipping recompute (identical input -> identical "
                    "output -> no-op upsert). Sleeping the interval.",
                    latest_timestamp,
                )
                return True

            logger.info(f"Running calculation for timestamp: {latest_timestamp}")
            logger.info(f"Underlying price: ${underlying_price:.2f}")

            if not options:
                logger.warning("No options with Greeks available for calculation")
                return False

            # Calculate GEX by strike
            logger.info("Calculating GEX by strike...")
            t0 = _time.monotonic()
            gex_by_strike = self._calculate_gex_by_strike(
                options, underlying_price, latest_timestamp
            )
            stage_timings["gex_by_strike"] = _time.monotonic() - t0

            if not gex_by_strike:
                logger.warning("No GEX data calculated")
                return False

            logger.info(f"Calculated GEX for {len(gex_by_strike)} strikes")

            # Calculate GEX summary
            logger.info("Calculating GEX summary metrics...")
            t0 = _time.monotonic()
            gex_summary = self._calculate_gex_summary(
                gex_by_strike, options, underlying_price, latest_timestamp
            )
            stage_timings["gex_summary"] = _time.monotonic() - t0

            if not gex_summary:
                logger.warning("Failed to calculate GEX summary")
                return False

            # Validate internal arithmetic consistency before persisting.
            self._validate_gex_calculations(gex_by_strike, gex_summary, underlying_price)

            # Store results
            logger.info("Storing results to database...")
            t0 = _time.monotonic()
            self._store_calculation_results(gex_by_strike, gex_summary)
            stage_timings["store_results"] = _time.monotonic() - t0

            # Refresh flow cache tables
            logger.info("Refreshing flow cache tables...")
            t0 = _time.monotonic()
            self._refresh_flow_caches(latest_timestamp, underlying_price)
            stage_timings["refresh_flow_caches"] = _time.monotonic() - t0

            # Materialise the flow_series_5min snapshot off the same
            # timestamp (downstream of the flow_by_contract refresh above).
            logger.info("Refreshing flow series snapshot...")
            t0 = _time.monotonic()
            self._refresh_flow_series_snapshot(latest_timestamp)
            stage_timings["flow_series_snapshot"] = _time.monotonic() - t0

            # Log summary
            logger.info("")
            logger.info("=" * 80)
            logger.info("GEX SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Max Gamma Strike: ${gex_summary['max_gamma_strike']:.2f}")
            logger.info(f"Max Gamma Value: {gex_summary['max_gamma_value']:,.0f}")
            logger.info(
                f"Gamma Flip Point: ${gex_summary['gamma_flip_point']:.2f}"
                if gex_summary["gamma_flip_point"]
                else "Gamma Flip Point: N/A"
            )
            logger.info(
                f"Flip Distance: {gex_summary['flip_distance']:.4f}"
                if gex_summary.get("flip_distance") is not None
                else "Flip Distance: N/A"
            )
            logger.info(f"Local GEX (±1%): {gex_summary.get('local_gex', 0.0):,.0f}")
            logger.info(
                f"Convexity Risk: {gex_summary['convexity_risk']:,.0f}"
                if gex_summary.get("convexity_risk") is not None
                else "Convexity Risk: N/A"
            )
            logger.info(
                f"Max Pain: ${gex_summary['max_pain']:.2f}"
                if gex_summary.get("max_pain") is not None
                else "Max Pain: N/A"
            )
            logger.info(f"Put/Call Ratio: {gex_summary['put_call_ratio']:.2f}")
            logger.info(f"Total Net GEX: {gex_summary['total_net_gex']:,.0f}")
            logger.info("=" * 80)
            logger.info("")

            self.calculations_completed += 1
            self.last_calculation_time = datetime.now(ET)
            # Record only after a fully successful cycle so a transient
            # mid-cycle failure re-attempts the same timestamp next round.
            self._last_processed_snapshot_ts = latest_timestamp

            # Emit per-stage timings so cycle-overrun warnings can be
            # diagnosed without guessing which step is slow.
            self._last_stage_timings = stage_timings
            total_stage_time = sum(stage_timings.values())
            timings_str = ", ".join(f"{label}={secs:.2f}s" for label, secs in stage_timings.items())
            logger.info(
                "Stage timings (total %.2fs): %s",
                total_stage_time,
                timings_str,
            )

            return True

        except Exception as e:
            logger.error(f"Error in calculation cycle: {e}", exc_info=True)
            self.errors_count += 1
            self._last_stage_timings = stage_timings
            return False

    def run(self):
        """Run analytics engine continuously"""
        logger.info("\n" + "=" * 80)
        logger.info("ZEROGEX ANALYTICS ENGINE")
        logger.info("=" * 80)
        logger.info(f"Underlying: {self.underlying}")
        logger.info(f"Calculation Interval: {self.calculation_interval}s")
        logger.info(f"Risk-free Rate: {self.risk_free_rate:.4f}")
        if self.off_hours_enabled:
            logger.info(
                "Off-hours mode: ENABLED (weekends/holidays keep cycling at "
                "%ss against the latest available data)",
                self.off_hours_interval,
            )
        else:
            logger.info("Off-hours mode: DISABLED (paused outside the 24x5 run window)")
        logger.info("=" * 80 + "\n")

        self.running = True

        logger.info("Starting analytics loop...")
        logger.info("Press Ctrl+C to stop\n")

        try:
            while self.running:
                in_run_window = is_engine_run_window()

                # Outside the 24x5 window with off-hours mode off: sleep
                # until the next session opens (legacy behavior).
                if not in_run_window and not self.off_hours_enabled:
                    sleep_for = seconds_until_engine_run_window()
                    logger.info(
                        "AnalyticsEngine [%s] paused outside run window (24x5: weekdays, non-holidays); sleeping %ss",
                        self.underlying,
                        sleep_for,
                    )
                    time.sleep(max(1, sleep_for))
                    continue

                # Off-hours: keep cycling, but at the slower off-hours
                # interval since the underlying data is static until the
                # next session.  The snapshot is anchored to the latest
                # option_chains row (not NOW()), so the cycle recomputes
                # against the most recent available data (e.g. Friday's
                # close on a Saturday) rather than reporting nothing.
                effective_interval = (
                    self.calculation_interval if in_run_window else self.off_hours_interval
                )
                if not in_run_window:
                    logger.info(
                        "AnalyticsEngine [%s] off-hours: recomputing against the "
                        "latest available data (interval=%ss)",
                        self.underlying,
                        effective_interval,
                    )

                cycle_start = time.time()

                # Run calculation
                success = self.run_calculation()

                if success:
                    logger.info(f"✅ Calculation cycle {self.calculations_completed} complete")
                else:
                    logger.warning(f"⚠️  Calculation cycle had issues")

                # Calculate sleep time
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, effective_interval - cycle_duration)

                if sleep_time > 0:
                    logger.info(f"Sleeping for {sleep_time:.1f}s until next calculation...\n")
                    time.sleep(sleep_time)
                else:
                    stage_breakdown = getattr(self, "_last_stage_timings", None) or {}
                    breakdown_str = (
                        ", ".join(
                            f"{label}={secs:.1f}s"
                            for label, secs in sorted(
                                stage_breakdown.items(),
                                key=lambda kv: kv[1],
                                reverse=True,
                            )
                        )
                        if stage_breakdown
                        else "n/a"
                    )
                    logger.warning(
                        "Calculation took %.1fs, longer than interval (%ds). "
                        "Stage timings: %s\n",
                        cycle_duration,
                        effective_interval,
                        breakdown_str,
                    )

        except KeyboardInterrupt:
            logger.info("\n⚠️  Interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            sys.exit(1)
        finally:
            logger.info("\n" + "=" * 80)
            logger.info("ANALYTICS ENGINE SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Calculations completed: {self.calculations_completed}")
            logger.info(f"Errors encountered: {self.errors_count}")
            if self.last_calculation_time:
                logger.info(
                    f"Last calculation: {self.last_calculation_time.strftime('%Y-%m-%d %H:%M:%S ET')}"
                )
            logger.info("=" * 80 + "\n")

            close_connection_pool()


def main():
    """Main entry point"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="ZeroGEX Analytics Engine")
    parser.add_argument(
        "--underlying", default=None, help="Single underlying symbol (backward compatible)"
    )
    parser.add_argument(
        "--underlyings",
        default=os.getenv("ANALYTICS_UNDERLYINGS", os.getenv("ANALYTICS_UNDERLYING", "SPY")),
        help="Comma-separated underlying symbols or aliases (default: SPY)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("ANALYTICS_INTERVAL", "60")),
        help="Calculation interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--risk-free-rate",
        type=float,
        default=float(os.getenv("RISK_FREE_RATE", "0.05")),
        help="Risk-free rate (default: 0.05)",
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit (for testing)")
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
        engine = AnalyticsEngine(
            underlying=symbol,
            calculation_interval=args.interval,
            risk_free_rate=args.risk_free_rate,
        )

        if args.once:
            logger.info(f"Running single calculation cycle for {symbol}...")
            success = engine.run_calculation()
            sys.exit(0 if success else 1)
        else:
            engine.run()

    if len(symbols) == 1:
        run_for_symbol(symbols[0])
        return

    logger.info(f"Starting analytics engines for symbols: {', '.join(symbols)}")
    processes: List[Process] = []

    for symbol in symbols:
        process = Process(target=run_for_symbol, args=(symbol,), name=f"analytics-{symbol}")
        process.start()
        processes.append(process)

    def shutdown_children(signum, frame):
        logger.info(f"Received signal {signum}, terminating analytics workers...")
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
