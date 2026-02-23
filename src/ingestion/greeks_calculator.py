"""
Options Greeks Calculator for ZeroGEX

Calculates Black-Scholes Greeks (delta, gamma, theta, vega) for options.
Integrates with the ingestion pipeline to enrich option data before storage.

Dependencies:
    - numpy
    - scipy (for stats.norm)
"""

import numpy as np
from scipy import stats
from datetime import datetime, date
from typing import Dict, Any, Optional
import pytz

from src.utils import get_logger
from src.config import RISK_FREE_RATE, IMPLIED_VOLATILITY_DEFAULT

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class GreeksCalculator:
    """
    Calculate Black-Scholes Greeks for options

    Calculates:
        - Delta: Rate of change of option price with respect to underlying price
        - Gamma: Rate of change of delta with respect to underlying price
        - Theta: Rate of change of option price with respect to time (per day)
        - Vega: Rate of change of option price with respect to volatility
    """

    def __init__(
        self, 
        risk_free_rate: float = RISK_FREE_RATE,
        default_iv: float = IMPLIED_VOLATILITY_DEFAULT
    ):
        """
        Initialize Greeks calculator

        Args:
            risk_free_rate: Annual risk-free rate (default from config)
            default_iv: Default implied volatility if not available (default from config)
        """
        self.risk_free_rate = risk_free_rate
        self.default_iv = default_iv

        logger.info(f"Initialized GreeksCalculator: r={risk_free_rate:.4f}, default_iv={default_iv:.4f}")

    def _calculate_time_to_expiration(
        self, 
        current_date: datetime, 
        expiration_date: date
    ) -> float:
        """
        Calculate time to expiration in years

        Args:
            current_date: Current datetime (timezone-aware)
            expiration_date: Option expiration date

        Returns:
            Time to expiration in years
        """
        # Ensure current_date is timezone-aware
        if current_date.tzinfo is None:
            current_date = pytz.UTC.localize(current_date).astimezone(ET)
        else:
            current_date = current_date.astimezone(ET)

        # Convert expiration_date to datetime at market close (4:00 PM ET)
        expiration_dt = datetime.combine(
            expiration_date, 
            datetime.strptime("16:00:00", "%H:%M:%S").time()
        )
        expiration_dt = ET.localize(expiration_dt)

        # Calculate time difference
        time_diff = expiration_dt - current_date

        # Convert to years
        days_to_expiration = time_diff.total_seconds() / 86400
        years_to_expiration = days_to_expiration / 365.0

        # Minimum time to expiration (avoid division by zero)
        # Set to 1 minute for options expiring very soon
        if years_to_expiration < (1 / 525600):  # 1 minute in years
            years_to_expiration = 1 / 525600

        return years_to_expiration

    def _calculate_d1_d2(
        self, 
        S: float, 
        K: float, 
        T: float, 
        r: float, 
        sigma: float
    ) -> tuple:
        """
        Calculate d1 and d2 for Black-Scholes formula

        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration (years)
            r: Risk-free rate
            sigma: Implied volatility

        Returns:
            Tuple of (d1, d2)
        """
        # Avoid log(0) or division by zero
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return (0.0, 0.0)

        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        return (d1, d2)

    def calculate_delta(
        self, 
        S: float, 
        K: float, 
        T: float, 
        r: float, 
        sigma: float, 
        option_type: str
    ) -> float:
        """
        Calculate option delta

        Delta represents the rate of change of option price with respect to 
        underlying price. Range: [0, 1] for calls, [-1, 0] for puts.

        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration (years)
            r: Risk-free rate
            sigma: Implied volatility
            option_type: 'C' for call, 'P' for put

        Returns:
            Delta value
        """
        d1, _ = self._calculate_d1_d2(S, K, T, r, sigma)

        if option_type == 'C':
            delta = stats.norm.cdf(d1)
        else:  # Put
            delta = stats.norm.cdf(d1) - 1

        return delta

    def calculate_gamma(
        self, 
        S: float, 
        K: float, 
        T: float, 
        r: float, 
        sigma: float
    ) -> float:
        """
        Calculate option gamma

        Gamma represents the rate of change of delta with respect to underlying price.
        Gamma is the same for calls and puts with the same strike and expiration.

        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration (years)
            r: Risk-free rate
            sigma: Implied volatility

        Returns:
            Gamma value
        """
        if S <= 0 or sigma <= 0 or T <= 0:
            return 0.0

        d1, _ = self._calculate_d1_d2(S, K, T, r, sigma)

        gamma = stats.norm.pdf(d1) / (S * sigma * np.sqrt(T))

        return gamma

    def calculate_theta(
        self, 
        S: float, 
        K: float, 
        T: float, 
        r: float, 
        sigma: float, 
        option_type: str
    ) -> float:
        """
        Calculate option theta (per day)

        Theta represents the rate of change of option price with respect to time.
        Typically negative for long options (time decay).
        Returned in dollars per day.

        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration (years)
            r: Risk-free rate
            sigma: Implied volatility
            option_type: 'C' for call, 'P' for put

        Returns:
            Theta value (per day)
        """
        if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
            return 0.0

        d1, d2 = self._calculate_d1_d2(S, K, T, r, sigma)

        if option_type == 'C':
            theta = (
                -S * stats.norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                - r * K * np.exp(-r * T) * stats.norm.cdf(d2)
            )
        else:  # Put
            theta = (
                -S * stats.norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                + r * K * np.exp(-r * T) * stats.norm.cdf(-d2)
            )

        # Convert from per year to per day
        theta_per_day = theta / 365.0

        return theta_per_day

    def calculate_vega(
        self, 
        S: float, 
        K: float, 
        T: float, 
        r: float, 
        sigma: float
    ) -> float:
        """
        Calculate option vega

        Vega represents the rate of change of option price with respect to 
        implied volatility. Returned per 1% change in IV.
        Vega is the same for calls and puts.

        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration (years)
            r: Risk-free rate
            sigma: Implied volatility

        Returns:
            Vega value (per 1% IV change)
        """
        if S <= 0 or sigma <= 0 or T <= 0:
            return 0.0

        d1, _ = self._calculate_d1_d2(S, K, T, r, sigma)

        # Vega per 1% change in volatility
        vega = S * stats.norm.pdf(d1) * np.sqrt(T) / 100.0

        return vega

    def calculate_all_greeks(
        self,
        underlying_price: float,
        strike: float,
        expiration: date,
        option_type: str,
        current_time: datetime,
        implied_volatility: Optional[float] = None,
        risk_free_rate: Optional[float] = None
    ) -> Dict[str, float]:
        """
        Calculate all Greeks for an option

        Args:
            underlying_price: Current price of underlying
            strike: Strike price
            expiration: Expiration date
            option_type: 'C' for call, 'P' for put
            current_time: Current datetime (timezone-aware)
            implied_volatility: IV (uses default if None)
            risk_free_rate: Risk-free rate (uses instance default if None)

        Returns:
            Dictionary with delta, gamma, theta, vega
        """
        # Use defaults if not provided
        if implied_volatility is None:
            implied_volatility = self.default_iv
            logger.debug(f"Using default IV: {implied_volatility:.4f}")

        if risk_free_rate is None:
            risk_free_rate = self.risk_free_rate

        # Calculate time to expiration
        T = self._calculate_time_to_expiration(current_time, expiration)

        # Validate inputs
        if underlying_price <= 0:
            logger.warning(f"Invalid underlying price: {underlying_price}")
            return {
                "delta": 0.0,
                "gamma": 0.0,
                "theta": 0.0,
                "vega": 0.0
            }

        if strike <= 0:
            logger.warning(f"Invalid strike: {strike}")
            return {
                "delta": 0.0,
                "gamma": 0.0,
                "theta": 0.0,
                "vega": 0.0
            }

        # Calculate Greeks
        try:
            delta = self.calculate_delta(
                underlying_price, strike, T, risk_free_rate, implied_volatility, option_type
            )
            gamma = self.calculate_gamma(
                underlying_price, strike, T, risk_free_rate, implied_volatility
            )
            theta = self.calculate_theta(
                underlying_price, strike, T, risk_free_rate, implied_volatility, option_type
            )
            vega = self.calculate_vega(
                underlying_price, strike, T, risk_free_rate, implied_volatility
            )

            greeks = {
                "delta": round(delta, 6),
                "gamma": round(gamma, 8),
                "theta": round(theta, 6),
                "vega": round(vega, 6)
            }

            logger.debug(f"Calculated Greeks for {option_type} {strike}: {greeks}")

            return greeks

        except Exception as e:
            logger.error(f"Error calculating Greeks: {e}", exc_info=True)
            return {
                "delta": 0.0,
                "gamma": 0.0,
                "theta": 0.0,
                "vega": 0.0
            }

    def enrich_option_data(
        self,
        option_data: Dict[str, Any],
        underlying_price: float
    ) -> Dict[str, Any]:
        """
        Enrich option data dictionary with calculated Greeks

        This is the main integration point with the ingestion pipeline.

        Args:
            option_data: Option data dict from ingestion (must have: strike, 
                        expiration, option_type, timestamp)
            underlying_price: Current underlying price

        Returns:
            Enriched option data with Greeks added
        """
        # Extract required fields
        strike = option_data.get("strike")
        expiration = option_data.get("expiration")
        option_type = option_data.get("option_type")
        timestamp = option_data.get("timestamp")

        # Use IV from data if available, otherwise use default
        implied_volatility = option_data.get("implied_volatility")

        # Validate required fields
        if not all([strike, expiration, option_type, timestamp]):
            logger.warning("Missing required fields for Greeks calculation")
            # Add zero Greeks
            option_data["delta"] = 0.0
            option_data["gamma"] = 0.0
            option_data["theta"] = 0.0
            option_data["vega"] = 0.0
            return option_data

        # Calculate Greeks
        greeks = self.calculate_all_greeks(
            underlying_price=underlying_price,
            strike=strike,
            expiration=expiration,
            option_type=option_type,
            current_time=timestamp,
            implied_volatility=implied_volatility
        )

        # Add Greeks to option data
        option_data.update(greeks)

        return option_data


def main():
    """Test Greeks calculator"""
    from datetime import timedelta

    print("\n" + "="*80)
    print("GREEKS CALCULATOR TEST")
    print("="*80 + "\n")

    # Initialize calculator
    calc = GreeksCalculator()

    # Test parameters
    underlying_price = 450.0
    strike = 455.0
    current_time = datetime.now(ET)
    expiration = (current_time + timedelta(days=30)).date()

    print(f"Test Parameters:")
    print(f"  Underlying: ${underlying_price:.2f}")
    print(f"  Strike: ${strike:.2f}")
    print(f"  Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S ET')}")
    print(f"  Expiration: {expiration}")
    print(f"  Days to Exp: {(expiration - current_time.date()).days}")
    print(f"  Risk-free Rate: {calc.risk_free_rate:.4f}")
    print(f"  Implied Vol: {calc.default_iv:.4f}")
    print()

    # Test call option
    print("CALL Option Greeks:")
    print("-" * 80)
    call_greeks = calc.calculate_all_greeks(
        underlying_price=underlying_price,
        strike=strike,
        expiration=expiration,
        option_type='C',
        current_time=current_time
    )

    print(f"  Delta: {call_greeks['delta']:8.6f}  (Δ)")
    print(f"  Gamma: {call_greeks['gamma']:8.6f}  (Γ)")
    print(f"  Theta: {call_greeks['theta']:8.6f}  (Θ) [$/day]")
    print(f"  Vega:  {call_greeks['vega']:8.6f}  (ν) [$/1% IV]")
    print()

    # Test put option
    print("PUT Option Greeks:")
    print("-" * 80)
    put_greeks = calc.calculate_all_greeks(
        underlying_price=underlying_price,
        strike=strike,
        expiration=expiration,
        option_type='P',
        current_time=current_time
    )

    print(f"  Delta: {put_greeks['delta']:8.6f}  (Δ)")
    print(f"  Gamma: {put_greeks['gamma']:8.6f}  (Γ)")
    print(f"  Theta: {put_greeks['theta']:8.6f}  (Θ) [$/day]")
    print(f"  Vega:  {put_greeks['vega']:8.6f}  (ν) [$/1% IV]")
    print()

    # Test enrich_option_data
    print("Testing enrich_option_data():")
    print("-" * 80)

    option_data = {
        "option_symbol": "SPY 260322C455",
        "timestamp": current_time,
        "underlying": "SPY",
        "strike": strike,
        "expiration": expiration,
        "option_type": "C",
        "last": 5.25,
        "bid": 5.20,
        "ask": 5.30,
        "volume": 1000,
        "open_interest": 5000,
        "implied_volatility": 0.18
    }

    enriched = calc.enrich_option_data(option_data, underlying_price)

    print(f"  Option: {enriched['option_symbol']}")
    print(f"  Strike: ${enriched['strike']:.2f}")
    print(f"  Last: ${enriched['last']:.2f}")
    print(f"  IV: {enriched.get('implied_volatility', 'N/A'):.4f}")
    print(f"\n  Calculated Greeks:")
    print(f"    Delta: {enriched['delta']:8.6f}")
    print(f"    Gamma: {enriched['gamma']:8.6f}")
    print(f"    Theta: {enriched['theta']:8.6f}")
    print(f"    Vega:  {enriched['vega']:8.6f}")

    print("\n" + "="*80)
    print("✅ Greeks calculator test complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
