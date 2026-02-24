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
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
import pytz
import numpy as np
from scipy import stats

from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.config import RISK_FREE_RATE

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
        self.calculation_interval = calculation_interval
        self.risk_free_rate = risk_free_rate
        self.running = False

        # Metrics
        self.calculations_completed = 0
        self.errors_count = 0
        self.last_calculation_time: Optional[datetime] = None

        logger.info(f"Initialized AnalyticsEngine for {underlying}")
        logger.info(f"Calculation interval: {calculation_interval}s")
        logger.info(f"Risk-free rate: {risk_free_rate:.4f}")

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"\n⚠️  Received signal {signum}, shutting down...")
        self.running = False

    def _get_latest_option_timestamp(self) -> Optional[datetime]:
        """Get timestamp of most recent option data in database"""
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT MAX(timestamp) 
                    FROM option_chains 
                    WHERE underlying = %s
                """, (self.underlying,))

                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]

                return None

        except Exception as e:
            logger.error(f"Error fetching latest option timestamp: {e}")
            return None

    def _get_latest_underlying_price(self, timestamp: datetime) -> Optional[float]:
        """Get underlying price at or before given timestamp"""
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT close 
                    FROM underlying_quotes 
                    WHERE symbol = %s 
                      AND timestamp <= %s
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """, (self.underlying, timestamp))

                result = cursor.fetchone()
                if result:
                    return float(result[0])

                return None

        except Exception as e:
            logger.error(f"Error fetching underlying price: {e}")
            return None

    def _fetch_option_data(self, timestamp: datetime) -> List[Dict[str, Any]]:
        """
        Fetch all option data at given timestamp

        Returns list of options with strike, expiration, type, OI, Greeks

        Note: We fetch all options with Greeks, even if OI=0. OI is often 0 in 
        real-time data and only updates once daily after settlement. For GEX
        calculations, OI=0 simply means that strike contributes 0 to GEX.
        """
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        option_symbol,
                        strike,
                        expiration,
                        option_type,
                        last,
                        bid,
                        ask,
                        volume,
                        open_interest,
                        delta,
                        gamma,
                        theta,
                        vega,
                        implied_volatility
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp = %s
                      AND gamma IS NOT NULL
                    ORDER BY expiration, strike
                """, (self.underlying, timestamp))

                rows = cursor.fetchall()

                options = []
                for row in rows:
                    options.append({
                        'option_symbol': row[0],
                        'strike': float(row[1]),
                        'expiration': row[2],
                        'option_type': row[3],
                        'last': float(row[4]) if row[4] else 0.0,
                        'bid': float(row[5]) if row[5] else 0.0,
                        'ask': float(row[6]) if row[6] else 0.0,
                        'volume': int(row[7]) if row[7] else 0,
                        'open_interest': int(row[8]) if row[8] else 0,
                        'delta': float(row[9]) if row[9] else 0.0,
                        'gamma': float(row[10]) if row[10] else 0.0,
                        'theta': float(row[11]) if row[11] else 0.0,
                        'vega': float(row[12]) if row[12] else 0.0,
                        'implied_volatility': float(row[13]) if row[13] else 0.2
                    })

                logger.info(f"Fetched {len(options)} options with Greeks")

                # Count how many have OI > 0 for informational purposes
                options_with_oi = sum(1 for opt in options if opt['open_interest'] > 0)
                if options_with_oi > 0:
                    logger.info(f"  {options_with_oi} options have open interest > 0")
                else:
                    logger.info(f"  Note: All options have OI=0 (normal for real-time data)")
                    logger.info(f"  GEX will be calculated but will be 0 until OI updates")

                return options

        except Exception as e:
            logger.error(f"Error fetching option data: {e}", exc_info=True)
            return []

    def _calculate_time_to_expiration(
        self, 
        current_date: datetime, 
        expiration_date
    ) -> float:
        """Calculate time to expiration in years"""
        # Ensure current_date is timezone-aware
        if current_date.tzinfo is None:
            current_date = pytz.UTC.localize(current_date).astimezone(ET)
        else:
            current_date = current_date.astimezone(ET)

        # Convert expiration to datetime at market close
        expiration_dt = datetime.combine(
            expiration_date,
            datetime.strptime("16:00:00", "%H:%M:%S").time()
        )
        expiration_dt = ET.localize(expiration_dt)

        # Calculate years
        time_diff = expiration_dt - current_date
        days_to_expiration = time_diff.total_seconds() / 86400
        years_to_expiration = days_to_expiration / 365.0

        # Minimum 1 minute
        if years_to_expiration < (1 / 525600):
            years_to_expiration = 1 / 525600

        return years_to_expiration

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

        if option_type == 'C':
            charm = -stats.norm.pdf(d1) * (
                2 * r * T - d2 * sigma * np.sqrt(T)
            ) / (2 * T * sigma * np.sqrt(T))
        else:  # Put
            charm = -stats.norm.pdf(d1) * (
                2 * r * T - d2 * sigma * np.sqrt(T)
            ) / (2 * T * sigma * np.sqrt(T))

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

        GEX = Gamma × Open Interest × 100 (contract multiplier)

        For dealers (who are typically short options):
        - Call GEX is POSITIVE (dealers are short gamma on calls)
        - Put GEX is NEGATIVE (dealers are long gamma on puts)

        Net GEX = Call GEX - Put GEX
        """
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
            # Calculate call GEX (positive for dealers)
            call_gamma = sum(opt['gamma'] for opt in data['calls'])
            call_oi = sum(opt['open_interest'] for opt in data['calls'])
            call_volume = sum(opt['volume'] for opt in data['calls'])
            call_gex = call_gamma * call_oi * 100

            # Calculate put GEX (negative for dealers)
            put_gamma = sum(opt['gamma'] for opt in data['puts'])
            put_oi = sum(opt['open_interest'] for opt in data['puts'])
            put_volume = sum(opt['volume'] for opt in data['puts'])
            put_gex = -1 * put_gamma * put_oi * 100  # Negative for dealers

            # Total gamma (absolute)
            total_gamma = call_gamma + put_gamma

            # Net GEX (call - put, from dealer perspective)
            net_gex = call_gex + put_gex  # put_gex is already negative

            # Calculate Vanna and Charm exposure
            vanna_exposure = 0.0
            charm_exposure = 0.0

            for opt in data['calls'] + data['puts']:
                T = self._calculate_time_to_expiration(timestamp, opt['expiration'])

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

                # Multiply by OI and contract multiplier
                vanna_exposure += vanna * opt['open_interest'] * 100
                charm_exposure += charm * opt['open_interest'] * 100

            gex_results.append({
                'underlying': self.underlying,
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
                'charm_exposure': charm_exposure
            })

        return gex_results

    def _calculate_max_pain(
        self,
        options: List[Dict[str, Any]],
        strike_range: Optional[Tuple[float, float]] = None
    ) -> float:
        """
        Calculate Max Pain - the strike where option holders lose most money

        Max Pain is the strike price where the total value of outstanding
        options (calls + puts) is minimized.

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

        # Calculate total loss at each strike
        strike_losses = {}

        for test_strike in strikes:
            total_loss = 0.0

            for opt in options:
                if opt['open_interest'] == 0:
                    continue

                strike = opt['strike']
                oi = opt['open_interest']

                if opt['option_type'] == 'C':
                    # Call holders lose if underlying < strike
                    # Call holders gain: max(0, underlying - strike)
                    if test_strike > strike:
                        total_loss += (test_strike - strike) * oi * 100
                else:  # Put
                    # Put holders lose if underlying > strike
                    # Put holders gain: max(0, strike - underlying)
                    if test_strike < strike:
                        total_loss += (strike - test_strike) * oi * 100

            strike_losses[test_strike] = total_loss

        # Max pain is where total loss is minimized
        max_pain_strike = min(strike_losses.items(), key=lambda x: x[1])[0]

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

        # Sort by strike
        sorted_strikes = sorted(gex_by_strike, key=lambda x: x['strike'])

        # Find where net GEX changes sign
        for i in range(len(sorted_strikes) - 1):
            current = sorted_strikes[i]
            next_strike = sorted_strikes[i + 1]

            # Check for sign change
            if current['net_gex'] * next_strike['net_gex'] < 0:
                # Linear interpolation to find zero crossing
                s1, gex1 = current['strike'], current['net_gex']
                s2, gex2 = next_strike['strike'], next_strike['net_gex']

                flip_point = s1 + (s2 - s1) * (-gex1) / (gex2 - gex1)

                logger.info(f"Gamma flip point: ${flip_point:.2f} "
                           f"(between ${s1:.2f} and ${s2:.2f})")

                return flip_point

        return None

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

        summary = {
            'underlying': self.underlying,
            'timestamp': timestamp,
            'max_gamma_strike': max_gamma_strike['strike'],
            'max_gamma_value': max_gamma_strike['net_gex'],
            'gamma_flip_point': gamma_flip_point,
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
        gex_data: List[Dict[str, Any]]
    ):
        """Store GEX by strike data in database"""
        try:
            with db_connection() as conn:
                cursor = conn.cursor()

                for data in gex_data:
                    cursor.execute("""
                        INSERT INTO gex_by_strike
                        (underlying, timestamp, strike, expiration, total_gamma,
                         call_gamma, put_gamma, net_gex, call_volume, put_volume,
                         call_oi, put_oi, vanna_exposure, charm_exposure)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                            charm_exposure = EXCLUDED.charm_exposure
                    """, (
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
                        float(data['charm_exposure'])
                    ))

                conn.commit()
                logger.info(f"✅ Stored {len(gex_data)} GEX by strike records")

        except Exception as e:
            logger.error(f"Error storing GEX by strike: {e}", exc_info=True)
            self.errors_count += 1

    def _store_gex_summary(
        self,
        summary: Dict[str, Any]
    ):
        """Store GEX summary in database"""
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO gex_summary
                    (underlying, timestamp, max_gamma_strike, max_gamma_value,
                     gamma_flip_point, put_call_ratio, max_pain, total_call_volume,
                     total_put_volume, total_call_oi, total_put_oi, total_net_gex)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        total_net_gex = EXCLUDED.total_net_gex
                """, (
                    summary['underlying'],
                    summary['timestamp'],
                    float(summary['max_gamma_strike']),
                    float(summary['max_gamma_value']),
                    float(summary['gamma_flip_point']) if summary['gamma_flip_point'] else None,
                    float(summary['put_call_ratio']),
                    float(summary['max_pain']),
                    int(summary['total_call_volume']),
                    int(summary['total_put_volume']),
                    int(summary['total_call_oi']),
                    int(summary['total_put_oi']),
                    float(summary['total_net_gex'])
                ))
                conn.commit()
                logger.info(f"✅ Stored GEX summary")

        except Exception as e:
            logger.error(f"Error storing GEX summary: {e}", exc_info=True)
            self.errors_count += 1

    def run_calculation(self) -> bool:
        """
        Run one complete analytics calculation cycle

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get latest option timestamp
            latest_timestamp = self._get_latest_option_timestamp()

            if not latest_timestamp:
                logger.warning("No option data available in database")
                return False

            logger.info(f"Running calculation for timestamp: {latest_timestamp}")

            # Get underlying price
            underlying_price = self._get_latest_underlying_price(latest_timestamp)

            if not underlying_price:
                logger.warning("No underlying price available")
                return False

            logger.info(f"Underlying price: ${underlying_price:.2f}")

            # Fetch option data
            options = self._fetch_option_data(latest_timestamp)

            if not options:
                logger.warning("No options with Greeks available for calculation")
                return False

            # Calculate GEX by strike
            logger.info("Calculating GEX by strike...")
            gex_by_strike = self._calculate_gex_by_strike(
                options,
                underlying_price,
                latest_timestamp
            )

            if not gex_by_strike:
                logger.warning("No GEX data calculated")
                return False

            logger.info(f"Calculated GEX for {len(gex_by_strike)} strikes")

            # Calculate GEX summary
            logger.info("Calculating GEX summary metrics...")
            gex_summary = self._calculate_gex_summary(
                gex_by_strike,
                options,
                underlying_price,
                latest_timestamp
            )

            if not gex_summary:
                logger.warning("Failed to calculate GEX summary")
                return False

            # Store results
            logger.info("Storing results to database...")
            self._store_gex_by_strike(gex_by_strike)
            self._store_gex_summary(gex_summary)

            # Log summary
            logger.info("")
            logger.info("=" * 80)
            logger.info("GEX SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Max Gamma Strike: ${gex_summary['max_gamma_strike']:.2f}")
            logger.info(f"Max Gamma Value: {gex_summary['max_gamma_value']:,.0f}")
            logger.info(f"Gamma Flip Point: ${gex_summary['gamma_flip_point']:.2f}" if gex_summary['gamma_flip_point'] else "Gamma Flip Point: N/A")
            logger.info(f"Max Pain: ${gex_summary['max_pain']:.2f}")
            logger.info(f"Put/Call Ratio: {gex_summary['put_call_ratio']:.2f}")
            logger.info(f"Total Net GEX: {gex_summary['total_net_gex']:,.0f}")
            logger.info("=" * 80)
            logger.info("")

            self.calculations_completed += 1
            self.last_calculation_time = datetime.now(ET)

            return True

        except Exception as e:
            logger.error(f"Error in calculation cycle: {e}", exc_info=True)
            self.errors_count += 1
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
                    logger.warning(f"Calculation took {cycle_duration:.1f}s, "
                                  f"longer than interval ({self.calculation_interval}s)\n")

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
    parser.add_argument("--underlying", default=os.getenv("ANALYTICS_UNDERLYING", "SPY"),
                       help="Underlying symbol (default: SPY)")
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

    # Initialize engine
    engine = AnalyticsEngine(
        underlying=args.underlying,
        calculation_interval=args.interval,
        risk_free_rate=args.risk_free_rate
    )

    if args.once:
        # Run once for testing
        logger.info("Running single calculation cycle...")
        success = engine.run_calculation()
        sys.exit(0 if success else 1)
    else:
        # Run continuously
        engine.run()


if __name__ == "__main__":
    main()
