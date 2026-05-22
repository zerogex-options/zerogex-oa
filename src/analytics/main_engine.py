"""
ZeroGEX Analytics Engine - Independent GEX & Max Pain Calculations

This engine runs independently from ingestion and calculates:
1. Gamma Exposure (GEX) by strike
2. GEX summary metrics (max gamma, flip point, max pain)
3. Second-order Greeks (Vanna, Charm)
4. Put/Call ratios and open interest analysis

Runs on a configured interval and writes to gex_summary and gex_by_strike tables.
"""

import bisect
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
from src.config import (
    _getenv_bool,
    _getenv_float,
    _getenv_int,
    RISK_FREE_RATE,
    ANALYTICS_FLOW_CACHE_REFRESH_ENABLED,
    GAMMA_PROFILE_SPAN_PCT,
    GAMMA_PROFILE_SPAN_LADDER,
    GAMMA_PROFILE_INTERIOR_MARGIN,
    GAMMA_PROFILE_STRUCTURAL_MIN_FRAC,
    GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT,
    GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE,
    GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT,
    GAMMA_PROFILE_STRUCTURAL_ACTIVE_DISTANCE_PCT,
    GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT,
    GAMMA_PROFILE_STEP_PCT,
    GAMMA_PROFILE_DTE_WEIGHTING,
    GAMMA_PROFILE_DTE_REF_DAYS,
)
from src.symbols import parse_underlyings, get_canonical_symbol
from src.analytics.walls import compute_call_put_walls
from src.flow_series_sql import SNAPSHOT_UPSERT_PSYCOPG2, SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2
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
        # Accept fractional hours (e.g. 0.5 = 30 min, 0.25 = 15 min) so an
        # operator on a cold-storage buffer pool can dial the snapshot
        # working set down without a code change.  Floor at 5 minutes
        # (1/12 h) -- below that you risk losing recently-quoted contracts
        # that haven't been requoted in the narrowed window, which
        # silently distorts GEX/max-pain instead of failing loudly.
        #
        # All four env vars in this block go through _getenv_int /
        # _getenv_float, which tolerate inline shell-style ``# comment``
        # tails in the .env file (python-dotenv preserves everything
        # after ``=`` literally, so a stray ``KEY=1 # foo`` would
        # otherwise crash the analytics workers at startup).
        self.snapshot_lookback_hours = _getenv_float(
            "ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", 2.0, min=1.0 / 12.0
        )
        # Cold-start lookback floors at the steady-state value so a
        # misconfigured cold-start < steady-state silently uses the
        # steady-state width (never narrower).
        self.snapshot_cold_start_lookback_hours = max(
            self.snapshot_lookback_hours,
            _getenv_float("ANALYTICS_SNAPSHOT_COLD_START_LOOKBACK_HOURS", 96.0),
        )
        # The wide cold-start scan can legitimately run longer than the
        # pool-wide DB_STATEMENT_TIMEOUT_MS (default 90s) when the buffer
        # pool is cold.  Give just that one query a higher per-statement
        # ceiling via SET LOCAL so a cold first cycle isn't killed at 90s.
        self.snapshot_cold_start_statement_timeout_ms = _getenv_int(
            "ANALYTICS_SNAPSHOT_COLD_START_STATEMENT_TIMEOUT_MS", 180000, min=0
        )
        # Dedicated per-statement timeout for the STEADY-STATE snapshot
        # query.  The pool-wide DB_STATEMENT_TIMEOUT_MS (default 90s) is
        # sized for sub-second API queries; the steady-state 2h snapshot
        # walk usually finishes in seconds but can spike past 90s under
        # autovacuum + concurrent ingestion bursts -- in which case the
        # pool ceiling kills it, the cycle aborts at the "if not snapshot"
        # guard, and the next cycle starts immediately into another 90s
        # kill (no progress, downstream flow_series_5min snapshot never
        # refreshes -> API shortfall alarms).  Default 0 = use the pool
        # ceiling (no behavior change).  Operators seeing the snapshot
        # wedge can raise this (e.g. 150000) without raising the
        # pool-wide ceiling, since SET LOCAL is scoped to this single
        # query and reverts on commit.
        self.snapshot_statement_timeout_ms = _getenv_int(
            "ANALYTICS_SNAPSHOT_STATEMENT_TIMEOUT_MS", 0, min=0
        )
        self._snapshot_cold_start_consumed = False
        self.min_oi_coverage_pct_alert = _getenv_float("ANALYTICS_MIN_OI_COVERAGE_PCT_ALERT", 0.35)

        # Off-hours mode: keep cycling on weekends / NYSE holidays instead
        # of sleeping until the next run window.  The snapshot is anchored
        # to the latest option_chains row (not wall-clock NOW()), so an
        # off-hours cycle recomputes against the most recent available data
        # (e.g. Friday's close on a Saturday) rather than reporting nothing.
        # A longer interval is used off-hours since the underlying data is
        # static until the next session.
        self.off_hours_enabled = _getenv_bool("ANALYTICS_OFF_HOURS_ENABLED", True)
        self.off_hours_interval = max(
            self.calculation_interval,
            _getenv_int("ANALYTICS_OFF_HOURS_INTERVAL_SECONDS", 300),
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
        # Latch for the "snapshot has no Greek-bearing options" state.
        # A weekday night is inside the 24x5 run window, so the engine
        # keeps cycling after the close; once the underlying feed stops
        # the snapshot legitimately has no gamma-populated options.  This
        # latches True on the first such cycle so the benign state is
        # logged once (INFO) per closed period instead of a WARNING every
        # interval, and is cleared the moment Greek-bearing data resumes.
        self._empty_snapshot_state: bool = False
        # Latch for the "gamma flip unresolved" state.  The resolver
        # correctly persists NULL (no clamp, no carry-forward) when no
        # ladder rung yields a structural-interior crossing inside the
        # actionable-distance gate -- but when that condition persists
        # for an entire morning (e.g. SPX positioning placing the flip
        # well beyond ±MAX_FLIP_DISTANCE_PCT, the May 22, 2026 SPX
        # holiday-weekend regime), the verbose diagnostic warning used
        # to fire once per minute, drowning operators in identical
        # multi-line spam.  Now: emit the full diagnostic on the
        # resolved→unresolved transition, and again every
        # ``_gamma_flip_unresolved_warn_throttle_seconds`` while the
        # condition persists, so operators still see a refresh of the
        # underlying chain stats (IV, OI by DTE, peak/floor) instead of
        # the log going silent for hours.  An info line is emitted on
        # the unresolved→resolved transition so the recovery is visible.
        self._gamma_flip_unresolved_state: bool = False
        self._gamma_flip_unresolved_last_warn_mono: float = 0.0
        self._gamma_flip_unresolved_warn_throttle_seconds: float = _getenv_float(
            "GAMMA_FLIP_UNRESOLVED_WARN_THROTTLE_SECONDS", 900.0, min=0.0
        )
        # Distinct frozen snapshot timestamp for which the unchanged-
        # snapshot skip has already been logged at INFO.  Off-hours the
        # timestamp is frozen for hours, so the skip guard would otherwise
        # emit one INFO per worker per interval all weekend/overnight.
        # Log the skip once per distinct frozen timestamp; demote the
        # identical repeats to DEBUG.  Self-resets: a new timestamp
        # advances past this value, and the next freeze logs once again.
        self._last_skip_logged_ts: Optional[datetime] = None
        self._last_flow_cache_ts: Optional[datetime] = None
        self._last_flow_cache_refresh_mono: float = 0.0
        self._flow_cache_refresh_min_seconds: float = _getenv_float(
            "FLOW_CACHE_REFRESH_MIN_SECONDS", 15.0
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
        return cursor.fetchall()  # type: ignore[no-any-return]

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
                snapshot_row_cap = _getenv_int("ANALYTICS_SNAPSHOT_MAX_ROWS", 50000, min=1)

                data_age = datetime.now(timezone.utc) - timestamp
                want_cold_start = not self._snapshot_cold_start_consumed and data_age > timedelta(
                    hours=self.snapshot_lookback_hours
                )
                # Buffer-pool warmup is a separate concern from the
                # data-staleness gate.  A mid-session restart with live
                # ingestion sees fresh data (data_age < snapshot_lookback_hours)
                # and skips the wide cold-start scan, but the buffer pool is
                # still cold from the restart -- the 2h steady-state walk can
                # then drag past the steady-state statement_timeout on that
                # FIRST cycle and the whole process loops at 90-150s
                # snapshot=timeout / no progress until something warms the
                # pool.  Grant the SAME cold-start budget to cycle 1 on the
                # steady-state path; from cycle 2 the pool is warm enough
                # that the configured steady-state budget suffices.
                is_first_cycle = not self._snapshot_cold_start_consumed
                self._snapshot_cold_start_consumed = True
                # Effective per-query ceiling used by the steady-state
                # branch and the cold-start fallback retry.  Cycle 1 gets
                # the cold-start budget because the pool is cold; later
                # cycles get the configured steady-state value.
                steady_state_timeout_ms = (
                    self.snapshot_cold_start_statement_timeout_ms
                    if is_first_cycle
                    else self.snapshot_statement_timeout_ms
                )

                if want_cold_start:
                    logger.info(
                        "Cold-start snapshot: latest data is %.1fh old; using "
                        "%.2fh lookback (steady-state %.2fh) with %dms statement_timeout",
                        data_age.total_seconds() / 3600.0,
                        self.snapshot_cold_start_lookback_hours,
                        self.snapshot_lookback_hours,
                        self.snapshot_cold_start_statement_timeout_ms,
                    )
                    try:
                        rows = self._run_snapshot_query(
                            cursor,
                            timestamp,
                            self.snapshot_cold_start_lookback_hours,  # type: ignore[arg-type]
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
                            self.snapshot_lookback_hours,  # type: ignore[arg-type]
                            min_expiration,
                            snapshot_row_cap,
                            statement_timeout_ms=steady_state_timeout_ms,
                        )
                        conn.commit()
                else:
                    if is_first_cycle and steady_state_timeout_ms > 0:
                        # Surface the warmup-budget upgrade so an operator
                        # diagnosing a slow restart can see why cycle 1's
                        # statement_timeout differs from the configured
                        # steady-state value.
                        logger.info(
                            "First-cycle steady-state snapshot: applying "
                            "%dms cold-start statement_timeout to absorb "
                            "buffer-pool warmup (subsequent cycles use the "
                            "%dms steady-state budget; 0 = pool default)",
                            steady_state_timeout_ms,
                            self.snapshot_statement_timeout_ms,
                        )
                    rows = self._run_snapshot_query(
                        cursor,
                        timestamp,
                        self.snapshot_lookback_hours,  # type: ignore[arg-type]
                        min_expiration,
                        snapshot_row_cap,
                        statement_timeout_ms=steady_state_timeout_ms,
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
                        "implied_volatility": float(row[13]) if row[13] else None,
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
                    logger.info("  Note: All options have OI=0 (normal for real-time data)")
                    logger.info("  GEX will be calculated but will be 0 until OI updates")
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

        return vanna  # type: ignore[no-any-return]

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

        return charm_per_day  # type: ignore[no-any-return]

    def _calculate_bs_gamma(self, S, K: float, T: float, r: float, sigma: float):
        """Black-Scholes gamma (q=0; identical for calls and puts).

        ``γ = N'(d1) / (S·σ·√T)`` using the same dividend-free model and
        ``d1`` form as :meth:`_calculate_vanna` / :meth:`_calculate_charm`.

        Accepts ``S`` as a scalar or a NumPy array of underlying prices —
        the array form so the spot-shift gamma profile can re-price a
        whole price grid in one vectorised call.  Snapshot gamma is gamma
        at the *current* spot only; the zero-gamma level is where exposure
        flips as spot moves, so it can only be found by re-pricing gamma
        at each hypothetical spot (not by cumulating the fixed snapshot
        value).  Returns 0 where inputs are degenerate.
        """
        is_array = isinstance(S, np.ndarray)
        if K <= 0 or T <= 0 or sigma <= 0:
            return np.zeros_like(S, dtype=float) if is_array else 0.0
        S_arr = np.asarray(S, dtype=float)
        sqrt_T = np.sqrt(T)
        with np.errstate(divide="ignore", invalid="ignore"):
            d1 = (np.log(S_arr / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
            gamma = stats.norm.pdf(d1) / (S_arr * sigma * sqrt_T)
        gamma = np.where(S_arr > 0.0, gamma, 0.0)
        return gamma if is_array else float(gamma)

    def _dte_profile_weight(self, T: float) -> float:
        """DTE weight for a contract's spot-shift gamma-profile contribution.

        ``min(1, DTE / GAMMA_PROFILE_DTE_REF_DAYS)`` with ``DTE = T·365``
        (``T`` in calendar years, the same unit
        :meth:`_calculate_time_to_expiration` returns).

        The gamma flip is a *multi-day* regime level, so each expiry is
        weighted by the fraction of the reference horizon over which the
        contract still exists: a same-day 0DTE is gone by today's close
        and is ~irrelevant to where the gamma regime sits over the next
        several days, whereas anything living at least the full reference
        horizon counts in full (weight saturates at 1.0 — longer-dated
        regime structure, and all behavior away from near-dated, is
        unchanged).  This horizon-occupancy ramp also incidentally tames
        Black-Scholes' ``1/√T`` near-expiry gamma spike (the net 0DTE
        contribution scales like ``√DTE → 0``), so a same-day wall can no
        longer pin the regime flip to an irrelevant same-day strike.

        Returns 1.0 unconditionally when DTE weighting is disabled, so
        the profile is byte-for-byte the prior behavior.
        """
        if not GAMMA_PROFILE_DTE_WEIGHTING:
            return 1.0
        if T <= 0.0 or GAMMA_PROFILE_DTE_REF_DAYS <= 0.0:
            return 0.0
        return min(1.0, (T * 365.0) / GAMMA_PROFILE_DTE_REF_DAYS)

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
        strike_data = defaultdict(lambda: {"calls": [], "puts": []})  # type: ignore[var-annotated]

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
                # Skip contracts with no reliable IV. Previously the read
                # path at line 502 substituted the IMPLIED_VOLATILITY_DEFAULT
                # sentinel (0.20) for NULL IVs, which silently fabricated
                # vanna/charm exposure for contracts where the solver had
                # failed (typically deep ITM strikes pre-market). Honestly
                # excluding them matches what _build_gamma_profile already
                # does for the gamma side at line 1005.
                iv = opt.get("implied_volatility")
                if iv is None or iv <= 0:
                    continue

                vanna = self._calculate_vanna(underlying_price, strike, T, self.risk_free_rate, iv)

                charm = self._calculate_charm(
                    underlying_price,
                    strike,
                    T,
                    self.risk_free_rate,
                    iv,
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

        return max_pain_strike  # type: ignore[no-any-return]

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

    def _gamma_exposure_profile(
        self,
        options: List[Dict[str, Any]],
        spot: float,
        timestamp: datetime,
        *,
        span_pct: Optional[float] = None,
    ) -> List[Tuple[float, float]]:
        """SpotGamma / SqueezeMetrics dealer gamma-exposure profile.

        The single shared primitive behind BOTH the gamma flip (its zero
        crossing) and net-GEX-at-spot (its value at the current price), so
        the two can never disagree about which side of the flip spot sits
        on.

        This is the actual industry construction, not the retail
        cumulative-net-GEX-by-strike approximation it replaces: walk a
        wide grid of hypothetical underlying prices, RE-PRICE every
        option's gamma at each hypothetical price via Black-Scholes
        (gamma is a function of spot, so the fixed snapshot gamma cannot
        be cumulated to locate where exposure flips), sum dealer dollar
        gamma with the same convention as :meth:`_calculate_gex_by_strike`
        (calls +, puts −; ``γ·OI·100·S²·0.01``), and return the
        ascending ``[(S, dealer_gex), ...]`` curve.  Each contract's
        implied vol is held at its snapshot value across the shift
        (sticky-strike — the standard simplification; a full
        vol-surface re-shift is out of scope).

        ``span_pct`` (optional) lets the adaptive flip resolver
        (:meth:`_resolve_gamma_flip`) re-build the profile at a wider
        span when the initial scan yields no structural interior
        crossing.  When omitted, defaults to ``GAMMA_PROFILE_SPAN_PCT``
        (the first ladder rung) — preserves the single-span API for
        direct callers (tests, ad-hoc inspection).  Grid step stays
        ``GAMMA_PROFILE_STEP_PCT`` regardless of span, so resolution
        per dollar is constant across rungs.

        Returns ``[]`` when no profile can be built (no spot / no
        usable contracts).

        Each contract's contribution is additionally scaled by
        :meth:`_dte_profile_weight` (``min(1, DTE / DTE_REF)`` when
        ``GAMMA_PROFILE_DTE_WEIGHTING`` is on): a linear horizon-occupancy
        ramp that weights each expiry by the fraction of the multi-day
        reference horizon over which the contract still exists, so an
        OPEX-day 0DTE wall (gone by today's close, and carrying a colossal
        re-greeked ``1/√T`` gamma spike) can no longer pin the
        regime-defining flip to a same-day strike, while anything living
        at least the full reference horizon is unweighted (1.0).  Because
        the weight is applied here, in the one shared profile, the flip
        and net-GEX-at-spot stay sign-consistent.
        """
        if spot <= 0 or not options:
            return []

        effective_span_pct = GAMMA_PROFILE_SPAN_PCT if span_pct is None else span_pct
        span = spot * effective_span_pct
        step = max(spot * GAMMA_PROFILE_STEP_PCT, 1e-6)
        grid = np.arange(spot - span, spot + span + step, step)
        grid = grid[grid > 0.0]
        if grid.size < 2:
            return []

        r = self.risk_free_rate
        tte_cache: Dict = {}
        total = np.zeros_like(grid, dtype=float)
        used = False
        for opt in options:
            sigma = opt.get("implied_volatility") or 0.0
            oi = opt.get("open_interest") or 0
            K = opt.get("strike") or 0.0
            if sigma <= 0 or oi <= 0 or K <= 0:
                continue
            expiration = opt["expiration"]
            T = tte_cache.get(expiration)
            if T is None:
                T = self._calculate_time_to_expiration(timestamp, expiration)
                tte_cache[expiration] = T
            if T <= 0:
                continue
            gamma = self._calculate_bs_gamma(grid, K, T, r, sigma)
            # Industry-standard dollar GEX per 1% move at the hypothetical
            # spot: γ(S) × OI × 100 × S² × 0.01. Dealer sign: short calls
            # (+), long puts (−) — matches _calculate_gex_by_strike.
            dollar_gamma = gamma * oi * 100.0 * grid * grid * 0.01
            sign = 1.0 if opt["option_type"] == "C" else -1.0
            # Horizon-occupancy ramp min(1, DTE/ref): down-weights
            # near-dated so a same-day 0DTE wall (and its 1/√T gamma
            # spike) can't pin the multi-day regime flip (1.0 for
            # DTE≥ref / weighting off).
            dte_w = self._dte_profile_weight(T)
            total += sign * dte_w * dollar_gamma
            used = True

        if not used:
            return []
        return list(zip(grid.tolist(), total.tolist()))

    def _calculate_gamma_flip_point(
        self, profile: List[Tuple[float, float]], underlying_price: float
    ) -> Optional[float]:
        """
        Gamma flip / "zero gamma" level: the hypothetical spot at which
        the dealer gamma-exposure profile crosses zero.

        Industry convention (SpotGamma / SqueezeMetrics): build the
        spot-shift dealer gamma profile (see :meth:`_gamma_exposure_profile`
        — gammas re-priced across a wide hypothetical-price grid) and take
        the price where total dealer gamma changes sign.  Above the level
        dealers are net long gamma (stabilizing); below, net short
        (destabilizing).

        ``profile`` is that curve as an ascending ``[(S, dealer_gex), …]``.
        With multiple crossings on a lumpy book, keep the one nearest spot
        (the actionable level / established tie-break here).

        Returns ``None`` when the profile is one-signed across the entire
        ±span grid (no crossing).  For a liquid chain that effectively only
        happens when the usable (gamma-non-null) snapshot is degraded /
        one-sided — a stale-feed / after-hours artifact — so the caller
        treats ``None`` as *flip unresolved* (persist NULL + WARN) rather
        than fabricating a grid-edge value or letting the carry-forward
        silently re-freeze a stale level.
        """
        if not profile:
            return None

        best_flip = None
        best_dist = float("inf")

        def _consider(candidate: float) -> None:
            nonlocal best_flip, best_dist
            dist = abs(candidate - underlying_price)
            if dist < best_dist:
                best_dist = dist
                best_flip = candidate

        for i in range(len(profile) - 1):
            s1, c1 = profile[i]
            s2, c2 = profile[i + 1]
            if c1 == 0.0:
                _consider(s1)
            elif c1 * c2 < 0.0:
                _consider(s1 + (s2 - s1) * (-c1) / (c2 - c1))
        # Profile ends exactly at zero => flip at the top of the grid.
        last_s, last_v = profile[-1]
        if last_v == 0.0:
            _consider(last_s)

        if best_flip is not None:
            logger.info(
                "Gamma flip point (spot-shift zero-gamma): $%.2f (nearest to spot $%.2f)",
                best_flip,
                underlying_price,
            )

        return best_flip

    def _structural_reference_from_profile(
        self,
        options: List[Dict[str, Any]],
        spot: float,
        profile: List[Tuple[float, float]],
        ref_span_pct: float = GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT,
    ) -> float:
        """Compute the active-strike-weighted p90 reference from a profile slice.

        Takes an already-built profile that covers AT LEAST
        ``spot ± ref_span_pct``, slices it to that canonical band, and
        applies the active-strike filter described in
        :meth:`_structural_reference`.  Callers must guarantee the
        coverage requirement; :meth:`_resolve_gamma_flip` enforces it
        before invoking this fast path.

        Pure function of ``(options, spot, profile slice)`` — does not
        touch the BS gamma kernel.  Behavior is byte-identical to
        building a separate ``±ref_span_pct`` profile and running the
        original filter, because the spot-shift kernel is deterministic
        and the slice contains exactly the grid points the standalone
        builder would have produced (same step, same span).

        Returns ``0.0`` when no usable slice can be built or no active
        strikes lie within the canonical band; callers treat that as
        "no structural basis available" and fall through to NULL.
        """
        if not profile:
            return 0.0

        ref_lo = spot - spot * ref_span_pct
        ref_hi = spot + spot * ref_span_pct

        # Active strikes = unique strikes with non-zero open interest.
        # Sorted to support O(log N) nearest-strike lookup via bisect.
        active_set: set = set()
        for opt in options:
            try:
                if int(opt.get("open_interest") or 0) > 0:
                    k = float(opt.get("strike") or 0.0)
                    if k > 0:
                        active_set.add(k)
            except (TypeError, ValueError):
                continue
        if not active_set:
            return 0.0
        active_strikes = sorted(active_set)

        max_distance = spot * GAMMA_PROFILE_STRUCTURAL_ACTIVE_DISTANCE_PCT
        filtered_abs: List[float] = []
        for s, v in profile:
            if s < ref_lo or s > ref_hi:
                continue
            # Binary-search the nearest active strike.  bisect_left
            # returns the insertion point; the nearest strike is
            # either at that index or at index-1.
            idx = bisect.bisect_left(active_strikes, s)
            nearest_dist = float("inf")
            if idx > 0:
                nearest_dist = min(nearest_dist, abs(s - active_strikes[idx - 1]))
            if idx < len(active_strikes):
                nearest_dist = min(nearest_dist, abs(active_strikes[idx] - s))
            if nearest_dist <= max_distance:
                filtered_abs.append(abs(v))

        if not filtered_abs:
            return 0.0
        abs_arr = np.asarray(filtered_abs, dtype=float)
        if abs_arr.max() <= 0.0:
            return 0.0
        reference = float(np.percentile(abs_arr, GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE))
        if reference <= 0.0:
            # All non-zero magnitude sits above the chosen percentile
            # (very sparse filtered set).  Fall back to max so the
            # gate still has a well-defined floor instead of
            # degenerating to "accept everything".
            reference = float(abs_arr.max())
        return reference

    def _structural_reference(
        self,
        options: List[Dict[str, Any]],
        spot: float,
        timestamp: datetime,
    ) -> float:
        """Active-strike-weighted structural reference for the resolver.

        Builds a fresh spot-shift gamma profile over a fixed
        ``±GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT`` band around
        spot, then delegates to
        :meth:`_structural_reference_from_profile`.

        This standalone path is the fallback for callers that have no
        precomputed profile, and is retained for the rare case where
        the first ladder rung is configured NARROWER than the reference
        span (the rung wouldn't cover the canonical band, so we can't
        slice from it).  In the default configuration (rung 0 = ±20%,
        reference = ±15%) :meth:`_resolve_gamma_flip` takes the fast
        slice path and this method is not called per cycle.

        Returns ``0.0`` when no usable profile can be built or no
        active strikes lie within the canonical band; callers treat
        that as "no structural basis available" and fall through to
        NULL.
        """
        ref_profile = self._gamma_exposure_profile(
            options,
            spot,
            timestamp,
            span_pct=GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT,
        )
        return self._structural_reference_from_profile(
            options,
            spot,
            ref_profile,
            GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT,
        )

    def _find_structural_interior_crossing(
        self,
        profile: List[Tuple[float, float]],
        underlying_price: float,
        structural_reference: Optional[float] = None,
    ) -> Optional[float]:
        """First-class crossing detector for the adaptive flip resolver.

        Walks the profile for adjacent sign changes (and exact zeros) and
        returns the nearest-to-spot crossing that passes BOTH of:

        * **Interior** — the linearly-interpolated crossing sits at least
          ``GAMMA_PROFILE_INTERIOR_MARGIN`` of the grid span away from
          either edge.  Forces the resolver to expand the grid rather
          than accept a brittle near-edge value.  Geometrically the same
          idea as a well-bracketed root in Brent's method: the bracket
          must have non-trivial width on both sides.

        * **Structural** — the peak ``|profile|`` value within
          ``±GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT × candidate`` of the
          crossing is at least ``GAMMA_PROFILE_STRUCTURAL_MIN_FRAC`` of
          the chain's **robust high-magnitude reference**.  The
          resolver supplies this reference precomputed (see
          :meth:`_structural_reference`) so the gate has the SAME
          floor at every ladder rung; when ``structural_reference`` is
          ``None`` (legacy callers / direct tests) the reference is
          computed from the passed profile via the same p90 rule,
          preserving the original per-profile behavior.

          Without an anchored reference, widening the grid dilutes
          p90 with deep-OTM near-zero values and lowers the floor for
          the same crossing — that's the 2026-05-20 SPX/QQQ
          pathology where the ±35% rung accepted flips the strict
          ±20% gate had correctly rejected.  Rejects noise-floor
          sign changes (profile slowly drifting through zero in a
          region where every contract's gamma has decayed near zero —
          the morning-open / extended-hours artifact where IVs spike,
          gammas collapse globally, and a sliver of imbalance can
          flip sign spuriously).

        * **Actionable-distance** — the candidate sits within
          ``GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT`` of ``underlying_price``.
          A flip further from spot than this is not actionable on any
          reasonable trading horizon, and is the failure mode that
          slipped past the structural gate during the SPX 2026-05-20
          open: structurally valid interior crossings genuinely existed
          far below spot as the chain degraded, the resolver accepted
          them, and the gamma-flip line on the heatmap walked off the
          bottom of the chart while the dashboard's latest-summary
          endpoint went NULL on the very next cycle.

        Returns ``None`` when nothing qualifies.  Unlike
        :meth:`_calculate_gamma_flip_point`, this is intentionally
        STRICT: a one-signed profile, a noise-floor crossing, an
        edge-only crossing, and a structurally valid but far-from-spot
        crossing all return ``None`` so the caller can decide whether to
        expand the grid or give up.
        """
        if not profile or len(profile) < 2:
            return None

        s_lo = profile[0][0]
        s_hi = profile[-1][0]
        width = s_hi - s_lo
        if width <= 0:
            return None
        margin_abs = GAMMA_PROFILE_INTERIOR_MARGIN * width
        interior_lo = s_lo + margin_abs
        interior_hi = s_hi - margin_abs

        if structural_reference is None:
            # Legacy / direct-call path: derive the reference from the
            # passed profile (same p90 rule that lived inline before
            # the canonical-reference refactor).
            abs_profile = np.fromiter((abs(v) for _, v in profile), dtype=float, count=len(profile))
            if abs_profile.size == 0 or abs_profile.max() <= 0.0:
                return None
            reference = float(
                np.percentile(abs_profile, GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE)
            )
            if reference <= 0.0:
                reference = float(abs_profile.max())
        else:
            reference = structural_reference
            if reference <= 0.0:
                # Canonical reference unavailable (degraded chain).
                # No basis for the structural test — conservative
                # NULL; the resolver's exhaustion path persists NULL.
                return None
        floor_abs = GAMMA_PROFILE_STRUCTURAL_MIN_FRAC * reference

        best_flip: Optional[float] = None
        best_dist = float("inf")
        for i in range(len(profile) - 1):
            s1, c1 = profile[i]
            s2, c2 = profile[i + 1]
            if c1 * c2 < 0.0:
                candidate = s1 + (s2 - s1) * (-c1) / (c2 - c1)
            elif c1 == 0.0:
                candidate = s1
            else:
                continue
            if candidate < interior_lo or candidate > interior_hi:
                continue
            if (
                underlying_price > 0
                and abs(candidate - underlying_price) / underlying_price
                > GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT
            ):
                continue
            half = GAMMA_PROFILE_STRUCTURAL_WINDOW_PCT * max(candidate, 1e-9)
            w_lo = candidate - half
            w_hi = candidate + half
            window_peak = 0.0
            for s, v in profile:
                if w_lo <= s <= w_hi:
                    av = abs(v)
                    if av > window_peak:
                        window_peak = av
            if window_peak < floor_abs:
                continue
            dist = abs(candidate - underlying_price)
            if dist < best_dist:
                best_dist = dist
                best_flip = candidate
        return best_flip

    def _resolve_gamma_flip(
        self,
        options: List[Dict[str, Any]],
        spot: float,
        timestamp: datetime,
    ) -> Tuple[List[Tuple[float, float]], Optional[float], float]:
        """Adaptive bracket-and-verify resolution of the gamma flip.

        Industry-rigorous root-finding for a function known to have a
        zero (the dealer dollar gamma profile has fixed asymptotic
        signs: → 0− as S → 0 because only puts retain gamma, dealer net
        short under this codebase's convention; → 0+ as S → ∞ because
        only calls retain gamma, dealer net long).  Since a flip
        ALWAYS exists somewhere in (0, ∞), the algorithm's job is to
        RESOLVE it inside a window where the profile signal is strong
        enough to trust — never to fabricate one at a grid edge or to
        accept a noise-floor sign change.

        Walks ``GAMMA_PROFILE_SPAN_LADDER`` in ascending order; at each
        rung builds the spot-shift profile and tries
        :meth:`_find_structural_interior_crossing` (interior +
        structural gates).  Returns the FIRST rung that yields a
        qualifying crossing.  When no rung qualifies, returns
        ``(last_profile, None, last_span)`` and the caller persists
        NULL+WARN — the honest "actionable flip beyond ±MAX% from spot
        or the chain is degraded" signal.

        Both the flip and ``net_gex_at_spot`` are read off the SAME
        returned profile, so the sign-consistency invariant
        (flip vs. net_gex_at_spot side-of-spot) holds at every rung —
        the resolver never changes which profile produces the readings.

        The structural floor that gates each rung's crossing is
        computed ONCE per cycle over a fixed canonical band around
        spot, so the significance test is identical at every rung.
        Widening the ladder only widens the geometric search; it no
        longer relaxes the noise floor by diluting p90 with deep-OTM
        near-zero values.

        The reference is sourced by SLICING the first valid ladder
        rung's profile (which is a superset of the canonical
        reference band whenever the first rung is at least as wide as
        ``GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT``, which is the
        default).  When the first rung is narrower than the reference
        span — an unusual configuration — the standalone builder is
        used as a fallback.  Slicing avoids a redundant BS gamma
        kernel call (~half of the per-cycle resolver compute at
        defaults) without changing the reference's value, since the
        kernel is deterministic and the slice contains exactly the
        grid points the standalone builder would have produced.

        Returns ``(profile, flip, span_used)``.
        """
        structural_reference: Optional[float] = None

        last_profile: List[Tuple[float, float]] = []
        last_span: float = (
            GAMMA_PROFILE_SPAN_LADDER[0] if GAMMA_PROFILE_SPAN_LADDER else GAMMA_PROFILE_SPAN_PCT
        )
        for span_pct in GAMMA_PROFILE_SPAN_LADDER:
            profile = self._gamma_exposure_profile(options, spot, timestamp, span_pct=span_pct)
            last_profile = profile
            last_span = span_pct
            if not profile:
                continue

            # Compute the structural reference once, on first valid
            # profile.  Slice the existing profile when it covers the
            # canonical reference band; fall back to building a
            # separate reference profile otherwise (rare —
            # rung_0 < ref_span isn't the default configuration).
            if structural_reference is None:
                if span_pct >= GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT:
                    structural_reference = self._structural_reference_from_profile(
                        options,
                        spot,
                        profile,
                        GAMMA_PROFILE_STRUCTURAL_REFERENCE_SPAN_PCT,
                    )
                else:
                    structural_reference = self._structural_reference(
                        options,
                        spot,
                        timestamp,
                    )

            flip = self._find_structural_interior_crossing(
                profile, spot, structural_reference=structural_reference
            )
            if flip is not None:
                return profile, flip, span_pct
        return last_profile, None, last_span

    # Default IV substituted when an option_chains row has NULL/0 IV
    # (see the snapshot fetch ~line 503).  Stale-IV pipelines cluster
    # contracts here, so we count rows sitting on this exact sentinel
    # value to surface the "IV pipeline is lagging" failure mode.
    _GAMMA_FLIP_DIAGNOSTIC_DEFAULT_IV = 0.2

    def _gamma_flip_unresolved_diagnostics(
        self,
        options: List[Dict[str, Any]],
        gamma_profile: List[Tuple[float, float]],
        underlying_price: float,
        timestamp: datetime,
    ) -> Dict[str, Any]:
        """Diagnostic snapshot for a cycle that left ``gamma_flip_point``
        unresolved.  Used only on the WARN path, so the analysis runs at
        most once per unresolved cycle.

        Fields are chosen to distinguish the four documented failure
        modes from the log alone, without cross-referencing
        ``option_chains`` or the IV-calculator state at the same
        timestamp:

        1. **IV-spike artifact** — ``iv_p50`` / ``iv_p90`` / ``iv_max``
           jump versus the recent baseline.  BS gamma is
           ``φ(d1) / (S σ √T)``, so a global σ jump collapses the
           profile peak and slumps every region into the structural
           floor (see :meth:`_find_structural_interior_crossing`).
        2. **0DTE-dominant chain × DTE weighting** — ``oi_share_0dte``
           is high *and* ``weighted_oi_share_*`` is concentrated in a
           single thin bucket.  The horizon-occupancy ramp
           (:meth:`_dte_profile_weight`) intentionally down-weights
           near-dated, so a chain whose multi-day OI is thin can drop
           below the structural floor after weighting even when the
           raw chain looks healthy.
        3. **Stale IV defaulting to 0.20** —
           ``iv_at_default_share`` is high.  Rows ingested with a
           NULL/0 IV are filled with 0.20 at fetch time, so the
           ``sigma > 0`` filter never catches them — they enter the
           profile with a constant IV that flattens the spot-shift
           curve.
        4. **One-sided chain** — ``usable_calls`` vs ``usable_puts``
           is heavily skewed, or the last-rung profile's sign
           distribution (``profile_pos_pts`` / ``profile_neg_pts``)
           is monotonic.  No crossing exists at all; the resolver
           returns None on the first gate.

        Also emits the structural-floor value the gate compares
        against, so an operator can read off "the chain HAD crossings
        but their local peak was below the floor" directly.
        """
        DEFAULT_IV = self._GAMMA_FLIP_DIAGNOSTIC_DEFAULT_IV

        usable_total = 0
        usable_calls = 0
        usable_puts = 0
        ivs: List[float] = []
        iv_at_default = 0
        oi_by_bucket = {"0dte": 0, "1_2dte": 0, "3_7dte": 0, "8plus_dte": 0}
        weighted_oi_by_bucket = dict(oi_by_bucket)
        tte_cache: Dict = {}

        for opt in options:
            sigma = float(opt.get("implied_volatility") or 0.0)
            oi = int(opt.get("open_interest") or 0)
            K = float(opt.get("strike") or 0.0)
            if sigma <= 0 or oi <= 0 or K <= 0:
                continue

            usable_total += 1
            if opt.get("option_type") == "C":
                usable_calls += 1
            elif opt.get("option_type") == "P":
                usable_puts += 1

            ivs.append(sigma)
            if abs(sigma - DEFAULT_IV) < 1e-9:
                iv_at_default += 1

            expiration = opt.get("expiration")
            if expiration is None:
                continue
            T = tte_cache.get(expiration)
            if T is None:
                T = self._calculate_time_to_expiration(timestamp, expiration)
                tte_cache[expiration] = T
            if T <= 0:
                continue
            dte = T * 365.0
            if dte < 1.0:
                bucket = "0dte"
            elif dte < 3.0:
                bucket = "1_2dte"
            elif dte < 8.0:
                bucket = "3_7dte"
            else:
                bucket = "8plus_dte"
            oi_by_bucket[bucket] += oi
            dte_w = self._dte_profile_weight(T)
            weighted_oi_by_bucket[bucket] += int(round(oi * dte_w))

        if ivs:
            ivs_arr = np.asarray(ivs, dtype=float)
            iv_p10 = float(np.percentile(ivs_arr, 10))
            iv_p50 = float(np.percentile(ivs_arr, 50))
            iv_p90 = float(np.percentile(ivs_arr, 90))
            iv_max = float(ivs_arr.max())
        else:
            iv_p10 = iv_p50 = iv_p90 = iv_max = 0.0

        total_oi = sum(oi_by_bucket.values()) or 1
        total_weighted_oi = sum(weighted_oi_by_bucket.values()) or 1

        if gamma_profile:
            vals = [v for _, v in gamma_profile]
            abs_vals = [abs(v) for v in vals]
            profile_peak = max(abs_vals) if abs_vals else 0.0
            profile_median = float(np.median(abs_vals)) if abs_vals else 0.0
            positive_pts = sum(1 for v in vals if v > 0)
            negative_pts = sum(1 for v in vals if v < 0)
            zero_pts = sum(1 for v in vals if v == 0)
        else:
            profile_peak = profile_median = 0.0
            positive_pts = negative_pts = zero_pts = 0
        # Report the same canonical reference the resolver actually
        # used to reject every rung (see _structural_reference) — not
        # the widest-rung profile's p90, which can differ materially
        # and would misrepresent why the resolver gave up.
        profile_reference = self._structural_reference(options, underlying_price, timestamp)
        if profile_reference <= 0.0 and profile_peak > 0.0:
            profile_reference = profile_peak
        structural_floor = profile_reference * GAMMA_PROFILE_STRUCTURAL_MIN_FRAC

        return {
            "usable_total": usable_total,
            "usable_calls": usable_calls,
            "usable_puts": usable_puts,
            "iv_p10": iv_p10,
            "iv_p50": iv_p50,
            "iv_p90": iv_p90,
            "iv_max": iv_max,
            "iv_at_default_count": iv_at_default,
            "iv_at_default_share": iv_at_default / max(1, usable_total),
            "oi_share_0dte": oi_by_bucket["0dte"] / total_oi,
            "oi_share_1_2dte": oi_by_bucket["1_2dte"] / total_oi,
            "oi_share_3_7dte": oi_by_bucket["3_7dte"] / total_oi,
            "oi_share_8plus_dte": oi_by_bucket["8plus_dte"] / total_oi,
            "weighted_oi_share_0dte": weighted_oi_by_bucket["0dte"] / total_weighted_oi,
            "weighted_oi_share_1_2dte": weighted_oi_by_bucket["1_2dte"] / total_weighted_oi,
            "weighted_oi_share_3_7dte": weighted_oi_by_bucket["3_7dte"] / total_weighted_oi,
            "weighted_oi_share_8plus_dte": weighted_oi_by_bucket["8plus_dte"] / total_weighted_oi,
            "profile_peak": profile_peak,
            "profile_median": profile_median,
            "profile_reference": profile_reference,
            "profile_pos_pts": positive_pts,
            "profile_neg_pts": negative_pts,
            "profile_zero_pts": zero_pts,
            "structural_floor": structural_floor,
        }

    def _net_gex_at_spot(
        self, profile: List[Tuple[float, float]], underlying_price: float
    ) -> Optional[float]:
        """Dealer net GEX at the current spot.

        Piecewise-linear sample, at the current price, of the SAME
        spot-shift gamma-exposure profile whose zero crossing defines the
        gamma flip (see :meth:`_gamma_exposure_profile`).  This is dealer
        dollar gamma *at spot* — the regime-correct headline figure — as
        opposed to ``total_net_gex`` (the whole chain summed, which can
        carry the opposite sign when far-OTM strikes dominate the tail).

        Because the flip and this value are read off one curve, the sign
        here is on the short-gamma side whenever spot is below the
        (nearest) flip and the long-gamma side when above — the headline
        figure and the spot-vs-flip regime can no longer contradict each
        other.

        Clamped to the profile's endpoints outside the ±span grid, exactly
        as the flip routine treats it (neither extrapolates beyond the
        grid).  Returns ``None`` when no profile can be built.
        """
        if not profile:
            return None

        first_s, first_v = profile[0]
        last_s, last_v = profile[-1]
        if underlying_price <= first_s:
            return first_v
        if underlying_price >= last_s:
            return last_v

        for i in range(len(profile) - 1):
            s1, c1 = profile[i]
            s2, c2 = profile[i + 1]
            if s1 <= underlying_price <= s2:
                if s2 == s1:
                    return c2
                return c1 + (c2 - c1) * (underlying_price - s1) / (s2 - s1)
        return last_v

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
            return None  # type: ignore[return-value]

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

        # Gamma flip + net-GEX-at-spot are two readings of ONE spot-shift
        # dealer gamma-exposure profile (see _gamma_exposure_profile): the
        # flip is its zero crossing, net_gex_at_spot is its value at the
        # current price.  Deriving both from the same primitive keeps the
        # headline figure and the spot-vs-flip regime from contradicting
        # each other.
        #
        # The flip is resolved by adaptive bracket-and-verify
        # (_resolve_gamma_flip): the span ladder is walked in ascending
        # order, and the first rung that yields a STRUCTURAL INTERIOR
        # crossing — well away from the grid edges, in a region where
        # the profile magnitude is meaningfully non-zero — is accepted.
        # When no rung qualifies, gamma_flip_point is None: that's the
        # honest "actionable flip is beyond ±MAX% from spot or chain is
        # degraded" signal — NOT a fabricated grid-edge value, NOT a
        # noise-floor sign change.  net_gex_at_spot is sampled at spot
        # off the same (last-built) profile, preserving sign consistency
        # regardless of which rung resolved.
        gamma_profile, gamma_flip_point, gamma_flip_span_used = self._resolve_gamma_flip(
            options, underlying_price, timestamp
        )
        net_gex_at_spot = self._net_gex_at_spot(gamma_profile, underlying_price)

        gamma_flip_unresolved = gamma_flip_point is None
        if gamma_flip_unresolved:
            # Throttle the verbose diagnostic so a persistent unresolved
            # regime (e.g. SPX with the actionable flip beyond
            # ±MAX_FLIP_DISTANCE_PCT for an entire morning) doesn't
            # produce one multi-line WARN per analytics cycle.  Emit on
            # the resolved→unresolved transition (so the FIRST log line
            # always carries the full diagnostic) and again every
            # ``_gamma_flip_unresolved_warn_throttle_seconds`` while the
            # latch is held, so operators still get a periodic refresh
            # of the chain stats instead of the log going silent.
            now_mono = _time.monotonic()
            state_transition = not self._gamma_flip_unresolved_state
            elapsed = now_mono - self._gamma_flip_unresolved_last_warn_mono
            should_warn = state_transition or (
                self._gamma_flip_unresolved_warn_throttle_seconds <= 0.0
                or elapsed >= self._gamma_flip_unresolved_warn_throttle_seconds
            )
            if should_warn:
                diag = self._gamma_flip_unresolved_diagnostics(
                    options, gamma_profile, underlying_price, timestamp
                )
                logger.warning(
                    "Gamma flip UNRESOLVED for %s @ %s: no structural interior "
                    "crossing across the span ladder (max rung ±%.0f%%, %d/%d "
                    "usable contracts, %d profile points at max rung) — "
                    "persisting NULL (no clamp, no carry-forward). "
                    "Sides usable C/P=%d/%d. "
                    "IV p10/p50/p90/max=%.3f/%.3f/%.3f/%.3f "
                    "(%d contracts at default IV=%.2f, share %.1f%% — high "
                    "share indicates stale IV pipeline). "
                    "OI share by DTE raw 0/1-2/3-7/8+=%.1f/%.1f/%.1f/%.1f%%; "
                    "DTE-weighted 0/1-2/3-7/8+=%.1f/%.1f/%.1f/%.1f%%. "
                    "Profile (widest rung) peak|GEX|=%.3g, median|GEX|=%.3g, "
                    "p%.0f reference=%.3g, structural floor=%.3g (crossings need "
                    "a local window peak above this to qualify); sign distribution "
                    "+/-/0=%d/%d/%d points (a monotonic split means no crossing "
                    "exists at all).",
                    self.db_symbol,
                    timestamp,
                    gamma_flip_span_used * 100.0,
                    diag["usable_total"],
                    len(options),
                    len(gamma_profile),
                    diag["usable_calls"],
                    diag["usable_puts"],
                    diag["iv_p10"],
                    diag["iv_p50"],
                    diag["iv_p90"],
                    diag["iv_max"],
                    diag["iv_at_default_count"],
                    self._GAMMA_FLIP_DIAGNOSTIC_DEFAULT_IV,
                    diag["iv_at_default_share"] * 100.0,
                    diag["oi_share_0dte"] * 100.0,
                    diag["oi_share_1_2dte"] * 100.0,
                    diag["oi_share_3_7dte"] * 100.0,
                    diag["oi_share_8plus_dte"] * 100.0,
                    diag["weighted_oi_share_0dte"] * 100.0,
                    diag["weighted_oi_share_1_2dte"] * 100.0,
                    diag["weighted_oi_share_3_7dte"] * 100.0,
                    diag["weighted_oi_share_8plus_dte"] * 100.0,
                    diag["profile_peak"],
                    diag["profile_median"],
                    GAMMA_PROFILE_STRUCTURAL_REFERENCE_PERCENTILE,
                    diag["profile_reference"],
                    diag["structural_floor"],
                    diag["profile_pos_pts"],
                    diag["profile_neg_pts"],
                    diag["profile_zero_pts"],
                )
                self._gamma_flip_unresolved_last_warn_mono = now_mono
            self._gamma_flip_unresolved_state = True
        else:
            if self._gamma_flip_unresolved_state:
                logger.info(
                    "Gamma flip RESOLVED for %s @ %s after persistent unresolved "
                    "period: flip=$%.2f, spot=$%.2f, span used=±%.0f%%",
                    self.db_symbol,
                    timestamp,
                    gamma_flip_point,
                    underlying_price,
                    gamma_flip_span_used * 100.0,
                )
            self._gamma_flip_unresolved_state = False
        if not gamma_flip_unresolved and (
            GAMMA_PROFILE_SPAN_LADDER and gamma_flip_span_used > GAMMA_PROFILE_SPAN_LADDER[0]
        ):
            logger.info(
                "Gamma flip resolved at expanded span ±%.0f%% for %s @ %s "
                "(flip $%.2f, spot $%.2f) — first ladder rung ±%.0f%% was "
                "insufficient (wider gamma regime than the default scan)",
                gamma_flip_span_used * 100.0,
                self.db_symbol,
                timestamp,
                gamma_flip_point,
                underlying_price,
                GAMMA_PROFILE_SPAN_LADDER[0] * 100.0,
            )

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
            "gamma_flip_unresolved": gamma_flip_unresolved,
            "gamma_flip_span_used": gamma_flip_span_used if gamma_flip_point is not None else None,
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
            "net_gex_at_spot": net_gex_at_spot,
            "call_wall": call_wall,
            "put_wall": put_wall,
            "max_pain_by_expiration": max_pain_by_exp,
            # Spot-shift dealer gamma curve used to derive both
            # gamma_flip_point (zero crossing) and net_gex_at_spot
            # (value at spot).  Persisted to gex_profile so the frontend
            # can overlay the curve on the per-strike GEX chart without
            # the API recomputing the BS gamma grid on every request.
            "gamma_profile": gamma_profile,
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
        # When the flip is explicitly unresolved (degraded/one-sided chain
        # — see _calculate_gex_summary) persist NULL: the carry-forward
        # exists to bridge a transient missing value, NOT to mask a
        # degraded snapshot as a live level (that was the original
        # flat-flip bug). A bare None with no unresolved flag still
        # carries forward (back-compatible with any non-degraded caller).
        if gamma_flip_point is None and not summary.get("gamma_flip_unresolved"):
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
        net_gex_at_spot = summary.get("net_gex_at_spot")
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
        gamma_flip_span_used = summary.get("gamma_flip_span_used")
        cursor.execute(
            """
            INSERT INTO gex_summary
            (underlying, timestamp, max_gamma_strike, max_gamma_value,
             gamma_flip_point, put_call_ratio, max_pain, total_call_volume,
             total_put_volume, total_call_oi, total_put_oi, total_net_gex,
             net_gex_at_spot, flip_distance, local_gex, convexity_risk,
             call_wall, put_wall, max_pain_by_expiration, gamma_flip_span_used)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                net_gex_at_spot = EXCLUDED.net_gex_at_spot,
                flip_distance = EXCLUDED.flip_distance,
                local_gex = EXCLUDED.local_gex,
                convexity_risk = EXCLUDED.convexity_risk,
                call_wall = EXCLUDED.call_wall,
                put_wall = EXCLUDED.put_wall,
                max_pain_by_expiration = EXCLUDED.max_pain_by_expiration,
                gamma_flip_span_used = EXCLUDED.gamma_flip_span_used
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
                OR EXCLUDED.net_gex_at_spot IS DISTINCT FROM gex_summary.net_gex_at_spot
                OR EXCLUDED.flip_distance IS DISTINCT FROM gex_summary.flip_distance
                OR EXCLUDED.local_gex IS DISTINCT FROM gex_summary.local_gex
                OR EXCLUDED.convexity_risk IS DISTINCT FROM gex_summary.convexity_risk
                OR EXCLUDED.call_wall IS DISTINCT FROM gex_summary.call_wall
                OR EXCLUDED.put_wall IS DISTINCT FROM gex_summary.put_wall
                OR EXCLUDED.max_pain_by_expiration IS DISTINCT FROM gex_summary.max_pain_by_expiration
                OR EXCLUDED.gamma_flip_span_used IS DISTINCT FROM gex_summary.gamma_flip_span_used
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
                (float(net_gex_at_spot) if net_gex_at_spot is not None else None),
                float(flip_distance) if flip_distance is not None else None,
                float(summary.get("local_gex", 0.0)),
                float(convexity_risk) if convexity_risk is not None else None,
                float(call_wall_val) if call_wall_val is not None else None,
                float(put_wall_val) if put_wall_val is not None else None,
                mp_by_exp_json,
                (float(gamma_flip_span_used) if gamma_flip_span_used is not None else None),
            ),
        )
        logger.info("✅ Stored GEX summary")

    def _store_gex_profile(self, summary: Dict[str, Any], cursor) -> None:
        """Write the spot-shift dealer gamma-exposure profile on ``cursor``.

        The profile is a list of (hypothetical_price, dealer_dollar_gex)
        tuples computed in :meth:`_calculate_gex_summary` and is the
        shared primitive behind ``gamma_flip_point`` and
        ``net_gex_at_spot``.  Persisting it here lets ``/api/gex/profile``
        serve the curve without recomputing the BS gamma grid on every
        request.  No-op when the profile is empty (degraded snapshot).

        Like the other ``_store_*`` units this owns no transaction
        boundary; the caller's ``db_connection()`` scope keeps the write
        inside the same atomic transaction as the by-strike/summary
        writes.
        """
        profile = summary.get("gamma_profile") or []
        if not profile:
            return
        import json as _json

        payload = _json.dumps(
            [{"price": float(s), "gex": float(g)} for s, g in profile]
        )
        cursor.execute(
            """
            INSERT INTO gex_profile (underlying, timestamp, spot_price, span_pct, profile)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (underlying, timestamp) DO UPDATE SET
                spot_price = EXCLUDED.spot_price,
                span_pct = EXCLUDED.span_pct,
                profile = EXCLUDED.profile
            """,
            (
                summary["underlying"],
                summary["timestamp"],
                float(summary.get("underlying_price") or 0.0),
                (
                    float(summary["gamma_flip_span_used"])
                    if summary.get("gamma_flip_span_used") is not None
                    else None
                ),
                payload,
            ),
        )

    def _store_calculation_results(
        self,
        gex_data: List[Dict[str, Any]],
        summary: Dict[str, Any],
    ) -> None:
        """Persist by-strike + summary + profile in ONE transaction (all rows land or none).

        Writes run on the same connection inside a single
        ``db_connection()`` scope.  That context manager commits exactly
        once on a clean exit and rolls back on ANY exception, so a failure
        in any one write discards the rows written earlier in the same
        transaction.  This atomicity ("all three stores commit
        together") is the invariant downstream consumers rely on, so the
        grouping must not be split into independent transactions.
        """
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                self._store_gex_by_strike(gex_data, cursor)
                self._store_gex_summary(summary, cursor)
                self._store_gex_profile(summary, cursor)
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

        return vol_tiers, prem_tiers, f"vol={vol_mode},prem={prem_mode}"  # type: ignore[return-value]

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

        The stored rows mirror what the /api/flow/series CTE would compute
        for session='current'. Closed bars are window-invariant (the
        canonical CTE's outer SUM uses ROWS UNBOUNDED PRECEDING, so once a
        5-min bar's boundary passes its cumulative values are
        mathematically fixed); only the open bar (and the bar immediately
        before it, during the boundary-crossing cycle) actually changes
        each cycle.

        Dispatch strategy
        -----------------
        * Steady-state cycles run an **incremental** UPSERT that refreshes
          only the open bar + the one immediately before it (two rows),
          using ``SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2``. ~30x cheaper
          than the full-window form because it does direct per-bar
          aggregation over flow_by_contract instead of walking the whole
          session through an 8-level CTE with LAG / cumulative window
          functions.
        * Cold-start cycles (no flow_series_5min rows exist for the
          current session, or the engine is restarting after >2 cycles
          of downtime) run the full ``SNAPSHOT_UPSERT_PSYCOPG2`` once
          to seed closed bars between session_open and prev_bar, then
          subsequent cycles fall back to incremental. This guarantees
          there are no gaps if the engine restarts mid-session.

        Best-effort and gated by the same flag as the flow-cache refresh:
        a failure here must never break the analytics cycle or the GEX
        path. Mirrors ``_refresh_flow_caches`` error handling: log, do
        not raise.
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
            curr_bar = datetime.fromtimestamp(now_floor_epoch, tz=timezone.utc)
            session_end = min(curr_bar, session_close)
            if session_end < session_start:
                session_end = session_start
            prev_bar = max(session_start, session_end - timedelta(minutes=5))

            with db_connection() as conn:
                cursor = conn.cursor()
                # Decide: incremental (steady-state) or full (cold-start /
                # gap-fill).  The cheap probe is "do we have a row in
                # flow_series_5min for prev_bar already?".  If yes, all
                # earlier bars in this session were populated by an
                # earlier cycle; refreshing just prev_bar + curr_bar is
                # sufficient.  If no, we have a gap (fresh session, or
                # missed >=2 cycles); run the full upsert to backfill.
                cursor.execute(
                    """
                    SELECT 1 FROM flow_series_5min
                    WHERE symbol = %s AND bar_start = %s
                    LIMIT 1
                    """,
                    (self.db_symbol, prev_bar),
                )
                prev_bar_known = cursor.fetchone() is not None

                if prev_bar_known and prev_bar > session_start:
                    cursor.execute(
                        SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2,
                        {
                            "symbol": self.db_symbol,
                            "prev_bar": prev_bar,
                            "curr_bar": session_end,
                        },
                    )
                    conn.commit()
                    logger.info(
                        "flow_series_5min incremental upserted %d rows for %s " "(bars [%s, %s])",
                        cursor.rowcount,
                        self.db_symbol,
                        prev_bar.isoformat(),
                        session_end.isoformat(),
                    )
                else:
                    # Cold-start / gap-fill: full session backfill.
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
                        "flow_series_5min full backfill upserted %d rows for %s "
                        "(window [%s, %s]) -- cold-start or gap detected",
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
                # Record the partial timings so the cycle-overrun warning in
                # run() reports the *current* failing cycle (just `snapshot`)
                # instead of stale stage timings from a prior successful one.
                # Without this an operator sees a snapshot=39.3s breakdown
                # next to a cycle_duration=90.0s overrun and is misled into
                # diagnosing the wrong stage.
                self._last_stage_timings = stage_timings
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
                # Off-hours the snapshot timestamp is frozen until the
                # next session, so this guard fires every interval for
                # hours.  Log the skip once per distinct frozen timestamp
                # at INFO; demote the identical repeats to DEBUG so a
                # weekend/overnight doesn't emit one INFO per worker per
                # interval.  RTH is unaffected: a new bar advances the
                # timestamp every minute so this branch isn't taken.
                if latest_timestamp != self._last_skip_logged_ts:
                    logger.info(
                        "Snapshot timestamp %s unchanged since last successful "
                        "cycle; skipping recompute (identical input -> identical "
                        "output -> no-op upsert). Suppressing identical repeats "
                        "at DEBUG until the timestamp advances.",
                        latest_timestamp,
                    )
                    self._last_skip_logged_ts = latest_timestamp
                else:
                    logger.debug(
                        "Snapshot timestamp %s still unchanged; skipping "
                        "recompute (suppressed repeat).",
                        latest_timestamp,
                    )
                return True

            logger.info(f"Running calculation for timestamp: {latest_timestamp}")
            logger.info(f"Underlying price: ${underlying_price:.2f}")

            if not options:
                # Expected closed-market state, NOT an error.  After the
                # session the underlying feed stops; ingestion still runs
                # 24x5 and keeps writing option_chains rows, but with NULL
                # Greeks once the underlying price is stale
                # (src/ingestion/main_engine.py).  Those NULL-gamma rows
                # keep advancing max(option_chains.timestamp) while no row
                # inside the ANALYTICS_SNAPSHOT_LOOKBACK_HOURS window has
                # gamma, so the snapshot query returns zero options.
                #
                # A weekday night is inside the 24x5 run window, so the
                # off-hours path never engages and this cycle re-runs every
                # interval.  Previously it logged a WARNING and returned
                # False, so run() then logged "Calculation cycle had
                # issues" every 60s all evening/overnight, and the
                # unchanged-snapshot dedupe never armed (it only records on
                # success).  Treat it as a benign no-op instead: log once
                # per closed period at INFO, latch the state so repeats are
                # silent even if max(timestamp) keeps advancing, and record
                # the snapshot timestamp so a frozen timestamp hits the
                # unchanged-snapshot skip on the next cycle.
                if not self._empty_snapshot_state:
                    logger.info(
                        "No options with Greeks for snapshot %s — expected "
                        "while the market is closed / underlying feed is "
                        "stale; skipping calculation and suppressing repeat "
                        "logs until Greek-bearing data resumes",
                        latest_timestamp,
                    )
                    self._empty_snapshot_state = True
                self._last_processed_snapshot_ts = latest_timestamp
                return True

            # Greek-bearing data is back — clear the closed-market latch so
            # the next genuine empty period logs once again.
            self._empty_snapshot_state = False

            # Calculate GEX by strike
            logger.info("Calculating GEX by strike...")
            t0 = _time.monotonic()
            gex_by_strike = self._calculate_gex_by_strike(
                options, underlying_price, latest_timestamp
            )
            stage_timings["gex_by_strike"] = _time.monotonic() - t0

            if not gex_by_strike:
                logger.warning("No GEX data calculated")
                self._last_stage_timings = stage_timings
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
                self._last_stage_timings = stage_timings
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
                    logger.warning("⚠️  Calculation cycle had issues")

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
