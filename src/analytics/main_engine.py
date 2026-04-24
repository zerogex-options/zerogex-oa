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
from datetime import datetime, timedelta, timezone
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
from src.market_calendar import (
    calculate_time_to_expiration,
    is_engine_run_window,
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
        risk_free_rate: float = RISK_FREE_RATE
    ):
        """
        Initialize analytics engine

        Args:
            underlying: Underlying symbol to analyze
            calculation_interval: Seconds between calculations
            risk_free_rate: Risk-free rate for Greeks
        """
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)  # canonical alias for DB queries (e.g. "SPX")
        self.calculation_interval = calculation_interval
        self.risk_free_rate = risk_free_rate
        self.running = False
        self.snapshot_lookback_minutes = max(1, int(os.getenv("ANALYTICS_SNAPSHOT_LOOKBACK_MINUTES", "5")))
        self.snapshot_freshness_seconds = max(30, int(os.getenv("ANALYTICS_SNAPSHOT_FRESHNESS_SECONDS", "180")))
        self.min_oi_coverage_pct_alert = float(os.getenv("ANALYTICS_MIN_OI_COVERAGE_PCT_ALERT", "0.35"))

        # Metrics
        self.calculations_completed = 0
        self.errors_count = 0
        self.last_calculation_time: Optional[datetime] = None

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

    def _get_snapshot(self) -> Optional[Dict[str, Any]]:
        """Fetch latest timestamp, underlying price, and option data in a single DB call.

        Returns dict with keys 'timestamp', 'underlying_price', 'options' or None
        if no data is available.
        """
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                # Single query: get latest timestamp + underlying price + option data
                cursor.execute("""
                    WITH latest_ts AS (
                        SELECT timestamp AS ts
                        FROM option_chains
                        WHERE underlying = %s
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ),
                    underlying AS (
                        SELECT uq.close
                        FROM underlying_quotes uq, latest_ts lt
                        WHERE uq.symbol = %s
                          AND uq.timestamp <= lt.ts
                        ORDER BY uq.timestamp DESC
                        LIMIT 1
                    ),
                    latest_per_contract AS (
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
                        FROM option_chains oc, latest_ts lt
                        WHERE oc.underlying = %s
                          AND oc.timestamp <= lt.ts
                          AND oc.timestamp >= (lt.ts - (%s * INTERVAL '1 minute'))
                          AND oc.gamma IS NOT NULL
                        ORDER BY oc.option_symbol, oc.timestamp DESC
                    )
                    SELECT
                        lt.ts,
                        u.close,
                        lpc.option_symbol,
                        lpc.strike,
                        lpc.expiration,
                        lpc.option_type,
                        lpc.last,
                        lpc.bid,
                        lpc.ask,
                        lpc.volume,
                        lpc.open_interest,
                        lpc.delta,
                        lpc.gamma,
                        lpc.theta,
                        lpc.vega,
                        lpc.implied_volatility,
                        lpc.timestamp
                    FROM latest_ts lt
                    LEFT JOIN underlying u ON TRUE
                    LEFT JOIN latest_per_contract lpc ON TRUE
                    WHERE lt.ts IS NOT NULL
                    ORDER BY lpc.expiration, lpc.strike
                    LIMIT 2000
                """, (self.db_symbol, self.db_symbol, self.db_symbol, self.snapshot_lookback_minutes))

                rows = cursor.fetchall()
                conn.commit()
                if not rows or rows[0][0] is None:
                    return None

                timestamp = rows[0][0]
                underlying_price = float(rows[0][1]) if rows[0][1] else None

                if underlying_price is None:
                    logger.warning("No underlying price found for snapshot")
                    return None

                options = []
                stale_cutoff = timestamp - timedelta(seconds=self.snapshot_freshness_seconds)
                stale_dropped = 0

                for row in rows:
                    if row[2] is None:  # no option data in this row
                        continue
                    quote_ts = row[16]
                    if quote_ts and quote_ts < stale_cutoff:
                        stale_dropped += 1
                        continue
                    options.append({
                        'option_symbol': row[2],
                        'strike': float(row[3]),
                        'expiration': row[4],
                        'option_type': row[5],
                        'last': float(row[6]) if row[6] else 0.0,
                        'bid': float(row[7]) if row[7] else 0.0,
                        'ask': float(row[8]) if row[8] else 0.0,
                        'volume': int(row[9]) if row[9] else 0,
                        'open_interest': int(row[10]) if row[10] else 0,
                        'delta': float(row[11]) if row[11] else 0.0,
                        'gamma': float(row[12]) if row[12] else 0.0,
                        'theta': float(row[13]) if row[13] else 0.0,
                        'vega': float(row[14]) if row[14] else 0.0,
                        'implied_volatility': float(row[15]) if row[15] else 0.2
                    })

                logger.info(
                    f"Fetched {len(options)} options with Greeks "
                    f"(latest-per-contract over {self.snapshot_lookback_minutes}m lookback)"
                )
                if stale_dropped > 0:
                    logger.info(
                        f"Dropped {stale_dropped} stale contracts older than "
                        f"{self.snapshot_freshness_seconds}s freshness limit"
                    )

                # Count how many have OI > 0 for informational purposes
                options_with_oi = sum(1 for opt in options if opt['open_interest'] > 0)
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
                    'timestamp': timestamp,
                    'underlying_price': underlying_price,
                    'options': options
                }

        except Exception as e:
            logger.error(f"Error fetching analytics snapshot: {e}", exc_info=True)
            return None

    # Time-to-expiration math lives in src.market_calendar — see the
    # canonical ``calculate_time_to_expiration`` function imported at
    # the top of this module.  Kept as a method-style accessor so
    # existing ``self._calculate_time_to_expiration(...)`` call sites
    # keep working without touching dozens of lines of calc code.
    def _calculate_time_to_expiration(self, current_date: datetime, expiration_date) -> float:
        return calculate_time_to_expiration(current_date, expiration_date)

    def _calculate_vanna(
        self,
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float
    ) -> float:
        """
        Calculate Vanna (∂²V/∂S∂σ)

        Vanna measures how delta changes with volatility.
        """
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return 0.0

        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        vanna = -stats.norm.pdf(d1) * d2 / sigma

        return vanna

    def _calculate_charm(
        self,
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str
    ) -> float:
        """
        Calculate Charm (∂²V/∂S∂T)

        Charm measures how delta changes with time (delta decay).
        """
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return 0.0

        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        # Call charm: -N'(d1) * [2rT - d2*sigma*sqrt(T)] / [2T*sigma*sqrt(T)]
        # Put charm adds the risk-free drift term: call_charm + r*e^{-rT}
        call_charm = -stats.norm.pdf(d1) * (
            2 * r * T - d2 * sigma * np.sqrt(T)
        ) / (2 * T * sigma * np.sqrt(T))

        if option_type == 'C':
            charm = call_charm
        else:  # Put
            charm = call_charm + r * np.exp(-r * T)

        # Convert to per day
        charm_per_day = charm / 365.0

        return charm_per_day

    def _calculate_gex_by_strike(
        self,
        options: List[Dict[str, Any]],
        underlying_price: float,
        timestamp: datetime
    ) -> List[Dict[str, Any]]:
        """
        Calculate gamma exposure by strike

        GEX = Gamma × Open Interest × 100 × Underlying Price

        This represents the notional dollar value of dealer gamma exposure.

        For dealers (who are typically short options):
        - Call GEX is POSITIVE (dealers are short gamma on calls)
        - Put GEX is NEGATIVE (dealers are long gamma on puts)

        Net GEX = Call GEX - Put GEX
        """
        # Cache time-to-expiration per expiration date to avoid redundant
        # datetime arithmetic and scipy calls inside the inner loop.
        _tte_cache: Dict = {}

        # Group by strike and expiration
        strike_data = defaultdict(lambda: {
            'calls': [],
            'puts': []
        })

        for opt in options:
            key = (opt['strike'], opt['expiration'])
            if opt['option_type'] == 'C':
                strike_data[key]['calls'].append(opt)
            else:
                strike_data[key]['puts'].append(opt)

        # Calculate GEX for each strike
        gex_results = []

        for (strike, expiration), data in strike_data.items():
            # Aggregate gamma by contract with OI weighting.
            # Note: there is typically one call/put contract per strike+expiration,
            # but we still compute this as a true weighted sum so the math remains
            # correct if upstream snapshots ever include multiple rows.
            call_gamma = sum(opt['gamma'] * opt['open_interest'] for opt in data['calls'])
            call_oi = sum(opt['open_interest'] for opt in data['calls'])
            call_volume = sum(opt['volume'] for opt in data['calls'])
            call_gex = call_gamma * 100 * underlying_price

            # Calculate put GEX (negative for dealers)
            put_gamma = sum(opt['gamma'] * opt['open_interest'] for opt in data['puts'])
            put_oi = sum(opt['open_interest'] for opt in data['puts'])
            put_volume = sum(opt['volume'] for opt in data['puts'])
            put_gex = -1 * put_gamma * 100 * underlying_price

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

            for opt in data['calls'] + data['puts']:
                vanna = self._calculate_vanna(
                    underlying_price,
                    strike,
                    T,
                    self.risk_free_rate,
                    opt['implied_volatility']
                )

                charm = self._calculate_charm(
                    underlying_price,
                    strike,
                    T,
                    self.risk_free_rate,
                    opt['implied_volatility'],
                    opt['option_type']
                )

                notional = opt['open_interest'] * 100 * underlying_price
                if opt['option_type'] == 'C':
                    call_vanna_exposure += vanna * notional
                    call_charm_exposure += charm * notional
                else:
                    put_vanna_exposure += vanna * notional
                    put_charm_exposure += charm * notional

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

            gex_results.append({
                'underlying': self.db_symbol,
                'timestamp': timestamp,
                'strike': strike,
                'expiration': expiration,
                'total_gamma': total_gamma,
                'call_gamma': call_gamma,
                'put_gamma': put_gamma,
                'net_gex': net_gex,
                'call_volume': call_volume,
                'put_volume': put_volume,
                'call_oi': call_oi,
                'put_oi': put_oi,
                'vanna_exposure': vanna_exposure,
                'charm_exposure': charm_exposure,
                'call_vanna_exposure': call_vanna_exposure,
                'put_vanna_exposure': put_vanna_exposure,
                'call_charm_exposure': call_charm_exposure,
                'put_charm_exposure': put_charm_exposure,
                'dealer_vanna_exposure': dealer_vanna_exposure,
                'dealer_charm_exposure': dealer_charm_exposure,
                'expiration_bucket': expiration_bucket,
            })

        return gex_results

    def _calculate_max_pain(
        self,
        options: List[Dict[str, Any]],
        strike_range: Optional[Tuple[float, float]] = None
    ) -> float:
        """
        Calculate Max Pain as the strike that minimizes total intrinsic payout.

        Convention used here:
        - We compute intrinsic payout to option holders at each candidate strike.
        - "Max pain" is the strike where this aggregate payout is lowest
          (i.e., minimum liability for option writers).

        Args:
            options: List of option data
            strike_range: Optional (min_strike, max_strike) to limit search

        Returns:
            Max pain strike price
        """
        # Get unique strikes
        strikes = sorted(set(opt['strike'] for opt in options))

        if strike_range:
            strikes = [s for s in strikes if strike_range[0] <= s <= strike_range[1]]

        if not strikes:
            return 0.0

        # Calculate total intrinsic payout at each candidate settlement strike.
        strike_payouts = {}

        for test_strike in strikes:
            total_payout = 0.0

            for opt in options:
                if opt['open_interest'] == 0:
                    continue

                strike = opt['strike']
                oi = opt['open_interest']

                if opt['option_type'] == 'C':
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
            return 0.0
        max_pain_strike = min(strike_payouts.items(), key=lambda x: x[1])[0]

        return max_pain_strike

    def _calculate_gamma_flip_point(
        self,
        gex_by_strike: List[Dict[str, Any]],
        underlying_price: float
    ) -> Optional[float]:
        """
        Calculate gamma flip point (zero gamma level)

        This is the strike where net GEX crosses zero.
        Above this level, dealers are long gamma (stabilizing).
        Below this level, dealers are short gamma (destabilizing).
        """
        if not gex_by_strike:
            return None

        # Aggregate net_gex by strike across all expirations.
        # The raw gex_by_strike has one entry per (strike, expiration),
        # so we must sum before looking for zero crossings.
        agg: Dict[float, float] = defaultdict(float)
        for entry in gex_by_strike:
            agg[entry['strike']] += entry['net_gex']

        strikes_sorted = sorted(agg.items())  # list of (strike, net_gex)
        if len(strikes_sorted) < 2:
            return None

        # Find the zero crossing closest to the current underlying price.
        # There can be multiple crossings; the one nearest spot is the
        # most meaningful "flip point".
        best_flip = None
        best_dist = float('inf')

        for i in range(len(strikes_sorted) - 1):
            s1, gex1 = strikes_sorted[i]
            s2, gex2 = strikes_sorted[i + 1]

            if gex1 * gex2 < 0:
                flip = s1 + (s2 - s1) * (-gex1) / (gex2 - gex1)
                dist = abs(flip - underlying_price)
                if dist < best_dist:
                    best_dist = dist
                    best_flip = flip

        if best_flip is not None:
            logger.info(f"Gamma flip point: ${best_flip:.2f} "
                       f"(nearest to spot ${underlying_price:.2f})")

        return best_flip

    def _calculate_gex_summary(
        self,
        gex_by_strike: List[Dict[str, Any]],
        options: List[Dict[str, Any]],
        underlying_price: float,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Calculate summary GEX metrics"""

        if not gex_by_strike:
            logger.warning("No GEX data to summarize")
            return None

        # Find max gamma strike
        max_gamma_strike = max(gex_by_strike, key=lambda x: abs(x['net_gex']))

        # Calculate gamma flip point
        gamma_flip_point = self._calculate_gamma_flip_point(gex_by_strike, underlying_price)

        # Calculate max pain
        max_pain = self._calculate_max_pain(options)

        # Total volumes and OI
        total_call_volume = sum(opt['volume'] for opt in options if opt['option_type'] == 'C')
        total_put_volume = sum(opt['volume'] for opt in options if opt['option_type'] == 'P')
        total_call_oi = sum(opt['open_interest'] for opt in options if opt['option_type'] == 'C')
        total_put_oi = sum(opt['open_interest'] for opt in options if opt['option_type'] == 'P')

        # Put/call ratio
        put_call_ratio = total_put_volume / total_call_volume if total_call_volume > 0 else 0

        # Total net GEX
        total_net_gex = sum(strike['net_gex'] for strike in gex_by_strike)

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
            abs(row['net_gex'])
            for row in gex_by_strike
            if abs(row['strike'] - underlying_price) <= local_band
        )

        # Convexity risk proxy:
        # large GEX imbalance while sitting near the flip implies higher
        # acceleration risk if the regime boundary breaks.
        convexity_risk = None
        if distance_to_flip is not None:
            convexity_risk = abs(total_net_gex) / max(distance_to_flip, 1e-6)

        summary = {
            'underlying': self.db_symbol,
            'timestamp': timestamp,
            'underlying_price': underlying_price,
            'max_gamma_strike': max_gamma_strike['strike'],
            'max_gamma_value': max_gamma_strike['net_gex'],
            'gamma_flip_point': gamma_flip_point,
            'flip_distance': flip_distance,
            'local_gex': local_gex,
            'convexity_risk': convexity_risk,
            'put_call_ratio': put_call_ratio,
            'max_pain': max_pain,
            'total_call_volume': total_call_volume,
            'total_put_volume': total_put_volume,
            'total_call_oi': total_call_oi,
            'total_put_oi': total_put_oi,
            'total_net_gex': total_net_gex
        }

        return summary

    def _store_gex_by_strike(
        self,
        gex_data: List[Dict[str, Any]],
        conn=None,
        cursor=None,
        commit: bool = True,
    ):
        """Store GEX by strike data in database"""
        if (conn is None) != (cursor is None):
            raise ValueError("conn and cursor must be provided together")
        if conn is None:
            with db_connection() as local_conn:
                local_cursor = local_conn.cursor()
                self._store_gex_by_strike(
                    gex_data,
                    conn=local_conn,
                    cursor=local_cursor,
                    commit=True,
                )
            return
        try:
            rows = [(
                data['underlying'],
                data['timestamp'],
                float(data['strike']),
                data['expiration'],
                float(data['total_gamma']),
                float(data['call_gamma']),
                float(data['put_gamma']),
                float(data['net_gex']),
                int(data['call_volume']),
                int(data['put_volume']),
                int(data['call_oi']),
                int(data['put_oi']),
                float(data['vanna_exposure']),
                float(data['charm_exposure']),
                float(data.get('call_vanna_exposure', 0.0)),
                float(data.get('put_vanna_exposure', 0.0)),
                float(data.get('call_charm_exposure', 0.0)),
                float(data.get('put_charm_exposure', 0.0)),
                float(data.get('dealer_vanna_exposure', -float(data['vanna_exposure']))),
                float(data.get('dealer_charm_exposure', -float(data['charm_exposure']))),
                data.get('expiration_bucket'),
            ) for data in gex_data]

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

            if commit:
                conn.commit()
            logger.info(f"✅ Stored {len(gex_data)} GEX by strike records")

        except Exception as e:
            logger.error(f"Error storing GEX by strike: {e}", exc_info=True)
            self.errors_count += 1
            if conn is not None:
                conn.rollback()
            raise

    def _store_gex_summary(
        self,
        summary: Dict[str, Any],
        conn=None,
        cursor=None,
        commit: bool = True,
    ):
        """Store GEX summary in database"""
        if (conn is None) != (cursor is None):
            raise ValueError("conn and cursor must be provided together")
        if conn is None:
            with db_connection() as local_conn:
                local_cursor = local_conn.cursor()
                self._store_gex_summary(
                    summary,
                    conn=local_conn,
                    cursor=local_cursor,
                    commit=True,
                )
            return
        try:
            gamma_flip_point = summary.get('gamma_flip_point')
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
                    (summary['underlying'], summary['timestamp']),
                )
                prev_row = cursor.fetchone()
                if prev_row and prev_row[0] is not None:
                    gamma_flip_point = float(prev_row[0])
                    logger.info(
                        "Gamma flip carry-forward applied: using prior value %.4f at %s",
                        gamma_flip_point,
                        summary['timestamp'],
                    )

            flip_distance = summary.get('flip_distance')
            convexity_risk = summary.get('convexity_risk')
            spot_price = float(summary.get('underlying_price') or 0.0)
            total_net_gex = float(summary.get('total_net_gex') or 0.0)
            if flip_distance is None and gamma_flip_point is not None and spot_price > 0:
                flip_distance = (spot_price - gamma_flip_point) / spot_price
            if convexity_risk is None and flip_distance is not None:
                convexity_risk = abs(total_net_gex) / max(abs(flip_distance), 1e-6)

            cursor.execute("""
                INSERT INTO gex_summary
                (underlying, timestamp, max_gamma_strike, max_gamma_value,
                 gamma_flip_point, put_call_ratio, max_pain, total_call_volume,
                 total_put_volume, total_call_oi, total_put_oi, total_net_gex,
                 flip_distance, local_gex, convexity_risk)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    convexity_risk = EXCLUDED.convexity_risk
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
            """, (
                summary['underlying'],
                summary['timestamp'],
                float(summary['max_gamma_strike']),
                float(summary['max_gamma_value']),
                gamma_flip_point,
                float(summary['put_call_ratio']),
                float(summary['max_pain']),
                int(summary['total_call_volume']),
                int(summary['total_put_volume']),
                int(summary['total_call_oi']),
                int(summary['total_put_oi']),
                float(summary['total_net_gex']),
                float(flip_distance) if flip_distance is not None else None,
                float(summary.get('local_gex', 0.0)),
                float(convexity_risk) if convexity_risk is not None else None,
            ))
            if commit:
                conn.commit()
            logger.info("✅ Stored GEX summary")

        except Exception as e:
            logger.error(f"Error storing GEX summary: {e}", exc_info=True)
            self.errors_count += 1
            if conn is not None:
                conn.rollback()
            raise

    def _store_calculation_results(
        self,
        gex_data: List[Dict[str, Any]],
        summary: Dict[str, Any],
    ):
        """Store by-strike + summary metrics in a single transaction."""
        with db_connection() as conn:
            cursor = conn.cursor()
            self._store_gex_by_strike(gex_data, conn=conn, cursor=cursor, commit=False)
            self._store_gex_summary(summary, conn=conn, cursor=cursor, commit=False)
            conn.commit()

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
            call_gex = row["call_gamma"] * 100 * underlying_price
            put_gex = -1 * row["put_gamma"] * 100 * underlying_price
            if abs((call_gex + put_gex) - row["net_gex"]) > 1e-6:
                mismatches += 1
            if row["call_gamma"] < 0 or row["put_gamma"] < 0:
                sign_anomalies += 1

        summary_total = sum(strike["net_gex"] for strike in gex_by_strike)
        if abs(summary_total - summary["total_net_gex"]) > 1e-6:
            mismatches += 1

        if mismatches:
            logger.warning("GEX validation: detected %d by-strike arithmetic mismatches", mismatches)
        if sign_anomalies:
            logger.warning("GEX validation: detected %d sign anomalies (negative aggregated gamma)", sign_anomalies)
        if not mismatches and not sign_anomalies:
            logger.info("GEX validation: all by-strike calculations passed")

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
                session_open_et = ET.localize(
                    datetime(ts_et.year, ts_et.month, ts_et.day, 9, 30)
                )
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
                cursor.execute("""
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
                """, (
                    prev_bucket_start, curr_bucket_start,      # row 1: (prev_start, prev_end)
                    curr_bucket_start, curr_bucket_end,        # row 2: (curr_start, curr_end)
                    session_open,                              # bucket_start >= session_open
                    underlying_price,                          # underlying_price fallback
                    self.db_symbol,                            # f.symbol filter
                    session_open,                              # f.timestamp >= session_open
                ))
                logger.info(
                    "flow_by_contract refresh upserted %d rows for %s (buckets [%s, %s])",
                    cursor.rowcount,
                    self.db_symbol,
                    prev_bucket_start.isoformat(),
                    curr_bucket_start.isoformat(),
                )

                # Refresh flow_smart_money
                logger.debug("Refreshing flow_smart_money...")
                cursor.execute("""
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
                            CASE WHEN volume_delta >= 500 THEN 4 WHEN volume_delta >= 200 THEN 3 WHEN volume_delta >= 100 THEN 2 WHEN volume_delta >= 50 THEN 1 ELSE 0 END +
                            CASE WHEN volume_delta * COALESCE(last, 0) * 100 >= 500000 THEN 4 WHEN volume_delta * COALESCE(last, 0) * 100 >= 250000 THEN 3 WHEN volume_delta * COALESCE(last, 0) * 100 >= 100000 THEN 2 WHEN volume_delta * COALESCE(last, 0) * 100 >= 50000 THEN 1 ELSE 0 END +
                            CASE WHEN implied_volatility > 1.0 THEN 2 WHEN implied_volatility > 0.6 THEN 1 ELSE 0 END
                        ))::numeric,
                        %s::numeric
                    FROM with_prev
                    WHERE timestamp = %s
                      AND volume_delta > 0
                      AND (
                        volume_delta >= 50
                        OR volume_delta * COALESCE(last, 0) * 100 >= 50000
                        OR (implied_volatility > 0.4 AND volume_delta >= 20)
                        OR (ABS(delta) < 0.15 AND volume_delta >= 20)
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
                """, (self.db_symbol, timestamp, timestamp, self.db_symbol, underlying_price, timestamp))

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

            latest_timestamp = snapshot['timestamp']
            underlying_price = snapshot['underlying_price']
            options = snapshot['options']

            logger.info(f"Running calculation for timestamp: {latest_timestamp}")
            logger.info(f"Underlying price: ${underlying_price:.2f}")

            if not options:
                logger.warning("No options with Greeks available for calculation")
                return False

            # Calculate GEX by strike
            logger.info("Calculating GEX by strike...")
            t0 = _time.monotonic()
            gex_by_strike = self._calculate_gex_by_strike(
                options,
                underlying_price,
                latest_timestamp
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
                gex_by_strike,
                options,
                underlying_price,
                latest_timestamp
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

            # Log summary
            logger.info("")
            logger.info("=" * 80)
            logger.info("GEX SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Max Gamma Strike: ${gex_summary['max_gamma_strike']:.2f}")
            logger.info(f"Max Gamma Value: {gex_summary['max_gamma_value']:,.0f}")
            logger.info(f"Gamma Flip Point: ${gex_summary['gamma_flip_point']:.2f}" if gex_summary['gamma_flip_point'] else "Gamma Flip Point: N/A")
            logger.info(
                f"Flip Distance: {gex_summary['flip_distance']:.4f}"
                if gex_summary.get('flip_distance') is not None
                else "Flip Distance: N/A"
            )
            logger.info(f"Local GEX (±1%): {gex_summary.get('local_gex', 0.0):,.0f}")
            logger.info(
                f"Convexity Risk: {gex_summary['convexity_risk']:,.0f}"
                if gex_summary.get('convexity_risk') is not None
                else "Convexity Risk: N/A"
            )
            logger.info(f"Max Pain: ${gex_summary['max_pain']:.2f}")
            logger.info(f"Put/Call Ratio: {gex_summary['put_call_ratio']:.2f}")
            logger.info(f"Total Net GEX: {gex_summary['total_net_gex']:,.0f}")
            logger.info("=" * 80)
            logger.info("")

            self.calculations_completed += 1
            self.last_calculation_time = datetime.now(ET)

            # Emit per-stage timings so cycle-overrun warnings can be
            # diagnosed without guessing which step is slow.
            self._last_stage_timings = stage_timings
            total_stage_time = sum(stage_timings.values())
            timings_str = ", ".join(
                f"{label}={secs:.2f}s" for label, secs in stage_timings.items()
            )
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
        logger.info("=" * 80 + "\n")

        self.running = True

        logger.info("Starting analytics loop...")
        logger.info("Press Ctrl+C to stop\n")

        try:
            while self.running:
                if not is_engine_run_window():
                    sleep_for = seconds_until_engine_run_window()
                    logger.info(
                        "AnalyticsEngine [%s] paused outside run window (24x5: weekdays, non-holidays); sleeping %ss",
                        self.underlying,
                        sleep_for,
                    )
                    time.sleep(max(1, sleep_for))
                    continue
                cycle_start = time.time()

                # Run calculation
                success = self.run_calculation()

                if success:
                    logger.info(f"✅ Calculation cycle {self.calculations_completed} complete")
                else:
                    logger.warning(f"⚠️  Calculation cycle had issues")

                # Calculate sleep time
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0, self.calculation_interval - cycle_duration)

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
                        self.calculation_interval,
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
                logger.info(f"Last calculation: {self.last_calculation_time.strftime('%Y-%m-%d %H:%M:%S ET')}")
            logger.info("=" * 80 + "\n")

            close_connection_pool()


def main():
    """Main entry point"""
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="ZeroGEX Analytics Engine")
    parser.add_argument("--underlying", default=None,
                       help="Single underlying symbol (backward compatible)")
    parser.add_argument(
        "--underlyings",
        default=os.getenv("ANALYTICS_UNDERLYINGS", os.getenv("ANALYTICS_UNDERLYING", "SPY")),
        help="Comma-separated underlying symbols or aliases (default: SPY)",
    )
    parser.add_argument("--interval", type=int,
                       default=int(os.getenv("ANALYTICS_INTERVAL", "60")),
                       help="Calculation interval in seconds (default: 60)")
    parser.add_argument("--risk-free-rate", type=float,
                       default=float(os.getenv("RISK_FREE_RATE", "0.05")),
                       help="Risk-free rate (default: 0.05)")
    parser.add_argument("--once", action="store_true",
                       help="Run once and exit (for testing)")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")

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
            risk_free_rate=args.risk_free_rate
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
