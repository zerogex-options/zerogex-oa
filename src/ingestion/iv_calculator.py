"""
Implied Volatility Calculator for ZeroGEX

Uses Newton-Raphson method to solve for implied volatility from option prices.
Integrates with the existing GreeksCalculator for Black-Scholes calculations.

Dependencies:
    - numpy
    - scipy (for stats.norm)
"""

import numpy as np
from scipy import stats
from datetime import datetime, date
from typing import Optional
import pytz

from src.utils import get_logger
from src.config import (
    IV_CALCULATION_ENABLED,
    IV_MAX_ITERATIONS,
    IV_TOLERANCE,
    IV_MIN,
    IV_MAX,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class IVCalculator:
    """
    Calculate implied volatility using Newton-Raphson method

    Given an option's market price, solves for the volatility that would
    produce that price using the Black-Scholes model.
    """

    def __init__(
        self,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        min_iv: Optional[float] = None,
        max_iv: Optional[float] = None
    ):
        """
        Initialize IV calculator

        Args:
            max_iterations: Maximum Newton-Raphson iterations (default from config)
            tolerance: Convergence tolerance for price difference (default from config)
            min_iv: Minimum allowed IV (default from config)
            max_iv: Maximum allowed IV (default from config)
        """
        # Use config values if not provided
        self.max_iterations = max_iterations if max_iterations is not None else IV_MAX_ITERATIONS
        self.tolerance = tolerance if tolerance is not None else IV_TOLERANCE
        self.min_iv = min_iv if min_iv is not None else IV_MIN
        self.max_iv = max_iv if max_iv is not None else IV_MAX

        logger.info(f"Initialized IVCalculator: max_iter={self.max_iterations}, "
                    f"tol={self.tolerance}, range=[{self.min_iv}, {self.max_iv}]")

    def _calculate_time_to_expiration(
        self, 
        current_date: datetime, 
        expiration_date: date
    ) -> float:
        """
        Calculate time to expiration in years
        (Same logic as GreeksCalculator)
        """
        if current_date.tzinfo is None:
            current_date = pytz.UTC.localize(current_date).astimezone(ET)
        else:
            current_date = current_date.astimezone(ET)

        expiration_dt = datetime.combine(
            expiration_date, 
            datetime.strptime("16:00:00", "%H:%M:%S").time()
        )
        expiration_dt = ET.localize(expiration_dt)

        time_diff = expiration_dt - current_date
        days_to_expiration = time_diff.total_seconds() / 86400
        years_to_expiration = days_to_expiration / 365.0

        # Minimum 1 minute
        if years_to_expiration < (1 / 525600):
            years_to_expiration = 1 / 525600

        return years_to_expiration

    def _black_scholes_price(
        self,
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float,
        option_type: str
    ) -> float:
        """
        Calculate Black-Scholes option price

        Args:
            S: Underlying price
            K: Strike price
            T: Time to expiration (years)
            r: Risk-free rate
            sigma: Volatility
            option_type: 'C' for call, 'P' for put

        Returns:
            Option price
        """
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return 0.0

        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        if option_type == 'C':
            price = S * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
        else:  # Put
            price = K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * stats.norm.cdf(-d1)

        return price

    def _vega(
        self,
        S: float,
        K: float,
        T: float,
        r: float,
        sigma: float
    ) -> float:
        """
        Calculate vega for Newton-Raphson iteration
        (Derivative of price with respect to volatility)
        """
        if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
            return 0.0

        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        vega = S * stats.norm.pdf(d1) * np.sqrt(T)

        return vega

    def calculate_iv(
        self,
        option_price: float,
        underlying_price: float,
        strike: float,
        expiration: date,
        option_type: str,
        current_time: datetime,
        risk_free_rate: float = 0.05,
        initial_guess: float = 0.25
    ) -> Optional[float]:
        """
        Calculate implied volatility using Newton-Raphson method

        Args:
            option_price: Market price of option (mid, last, or bid/ask)
            underlying_price: Current underlying price
            strike: Strike price
            expiration: Expiration date
            option_type: 'C' for call, 'P' for put
            current_time: Current datetime
            risk_free_rate: Annual risk-free rate
            initial_guess: Starting IV guess (default 25%)

        Returns:
            Implied volatility as decimal (e.g., 0.25 = 25%), or None if fails
        """
        # Validate inputs
        if option_price <= 0:
            logger.debug(f"Invalid option price: {option_price}")
            return None

        if underlying_price <= 0 or strike <= 0:
            logger.debug(f"Invalid S={underlying_price} or K={strike}")
            return None

        # Calculate time to expiration
        T = self._calculate_time_to_expiration(current_time, expiration)

        if T <= 0:
            logger.debug(f"Option already expired")
            return None

        # Check for intrinsic value violations
        if option_type == 'C':
            intrinsic = max(0, underlying_price - strike)
        else:
            intrinsic = max(0, strike - underlying_price)

        if option_price < intrinsic * 0.99:  # Allow small discrepancy
            logger.debug(f"Option price ({option_price}) < intrinsic value ({intrinsic})")
            return None

        # Newton-Raphson iteration
        sigma = max(self.min_iv, min(initial_guess, self.max_iv))

        for iteration in range(self.max_iterations):
            # Calculate BS price with current sigma
            bs_price = self._black_scholes_price(
                underlying_price, strike, T, risk_free_rate, sigma, option_type
            )

            # Calculate price difference
            price_diff = bs_price - option_price

            # Check convergence
            if abs(price_diff) < self.tolerance:
                logger.debug(f"IV converged in {iteration+1} iterations: {sigma:.4f}")
                return sigma

            # Calculate vega (derivative)
            vega = self._vega(underlying_price, strike, T, risk_free_rate, sigma)

            if abs(vega) < 1e-10:
                logger.debug(f"Vega too small, cannot continue iteration")
                return None

            # Newton-Raphson update
            sigma = sigma - price_diff / vega

            # Constrain to valid range
            sigma = max(self.min_iv, min(sigma, self.max_iv))

        logger.debug(f"IV did not converge after {self.max_iterations} iterations")
        return None

    def calculate_iv_from_bid_ask(
        self,
        bid: float,
        ask: float,
        underlying_price: float,
        strike: float,
        expiration: date,
        option_type: str,
        current_time: datetime,
        risk_free_rate: float = 0.05
    ) -> Optional[float]:
        """
        Calculate IV using mid-price between bid and ask

        Args:
            bid: Bid price
            ask: Ask price
            (other args same as calculate_iv)

        Returns:
            Implied volatility or None
        """
        # Validate bid/ask
        if bid <= 0 or ask <= 0 or ask < bid:
            logger.debug(f"Invalid bid/ask: {bid}/{ask}")
            return None

        # Use mid-price
        mid_price = (bid + ask) / 2

        return self.calculate_iv(
            mid_price,
            underlying_price,
            strike,
            expiration,
            option_type,
            current_time,
            risk_free_rate
        )

    def enrich_option_data_with_iv(
        self,
        option_data: dict,
        underlying_price: float,
        risk_free_rate: float = 0.05
    ) -> dict:
        """
        Add calculated IV to option data dictionary

        This integrates with your existing ingestion pipeline.
        If API provides IV, use that. Otherwise calculate from price.

        Args:
            option_data: Option data dict from ingestion
            underlying_price: Current underlying price
            risk_free_rate: Annual risk-free rate

        Returns:
            Option data enriched with 'implied_volatility' field
        """
        # Check if IV calculation is enabled
        if not IV_CALCULATION_ENABLED:
            logger.debug("IV calculation is disabled via config")
            # Keep API-provided IV if present, otherwise None
            if not option_data.get("implied_volatility"):
                option_data["implied_volatility"] = None
            return option_data

        # If API already provided IV, use it
        if option_data.get("implied_volatility"):
            logger.debug(f"Using API-provided IV: {option_data['implied_volatility']:.4f}")
            return option_data

        # Extract required fields
        bid = option_data.get("bid")
        ask = option_data.get("ask")
        last = option_data.get("last")
        strike = option_data.get("strike")
        expiration = option_data.get("expiration")
        option_type = option_data.get("option_type")
        timestamp = option_data.get("timestamp")

        if not all([strike, expiration, option_type, timestamp]):
            logger.debug("Missing required fields for IV calculation")
            option_data["implied_volatility"] = None
            return option_data

        # Try to calculate IV from available prices
        calculated_iv = None

        # Priority 1: Use bid/ask mid-price (most reliable)
        if bid and ask and bid > 0 and ask > 0:
            calculated_iv = self.calculate_iv_from_bid_ask(
                bid, ask, underlying_price, strike, expiration,
                option_type, timestamp, risk_free_rate
            )

        # Priority 2: Use last price
        if not calculated_iv and last and last > 0:
            calculated_iv = self.calculate_iv(
                last, underlying_price, strike, expiration,
                option_type, timestamp, risk_free_rate
            )

        if calculated_iv:
            option_data["implied_volatility"] = calculated_iv
            logger.debug(f"Calculated IV for {option_data.get('option_symbol')}: {calculated_iv:.4f}")
        else:
            option_data["implied_volatility"] = None
            logger.debug(f"Could not calculate IV for {option_data.get('option_symbol')}")

        return option_data


def main():
    """Test IV calculator"""
    from datetime import timedelta

    print("\n" + "="*80)
    print("IMPLIED VOLATILITY CALCULATOR TEST")
    print("="*80 + "\n")

    # Initialize calculator
    calc = IVCalculator()

    # Test parameters
    underlying_price = 450.0
    strike = 455.0
    current_time = datetime.now(ET)
    expiration = (current_time + timedelta(days=30)).date()
    option_type = 'C'
    risk_free_rate = 0.05

    # Simulate option prices at different IVs
    print("Test 1: Calculate IV from known prices")
    print("-" * 80)

    # Generate test prices at known IVs
    test_ivs = [0.15, 0.20, 0.25, 0.30, 0.40]

    for true_iv in test_ivs:
        # Calculate what the price should be at this IV
        T = calc._calculate_time_to_expiration(current_time, expiration)
        test_price = calc._black_scholes_price(
            underlying_price, strike, T, risk_free_rate, true_iv, option_type
        )

        # Now solve for IV from that price
        calculated_iv = calc.calculate_iv(
            test_price, underlying_price, strike, expiration,
            option_type, current_time, risk_free_rate
        )

        error = abs(calculated_iv - true_iv) if calculated_iv else 999
        status = "✅" if error < 0.0001 else "❌"

        print(f"{status} True IV: {true_iv:.4f} | Price: ${test_price:.2f} | "
              f"Calculated IV: {calculated_iv:.4f if calculated_iv else 'FAIL'} | "
              f"Error: {error:.6f}")

    print("\n" + "="*80)
    print("Test 2: Calculate IV from bid/ask spread")
    print("-" * 80)

    bid = 5.20
    ask = 5.30
    mid = (bid + ask) / 2

    calculated_iv = calc.calculate_iv_from_bid_ask(
        bid, ask, underlying_price, strike, expiration,
        option_type, current_time, risk_free_rate
    )

    print(f"Bid: ${bid:.2f}")
    print(f"Ask: ${ask:.2f}")
    print(f"Mid: ${mid:.2f}")
    print(f"Calculated IV: {calculated_iv:.4f if calculated_iv else 'FAIL'} ({calculated_iv*100:.2f}%)")

    print("\n" + "="*80)
    print("Test 3: Enrich option data (integration test)")
    print("-" * 80)

    option_data = {
        "option_symbol": "SPY 260322C455",
        "timestamp": current_time,
        "underlying": "SPY",
        "strike": strike,
        "expiration": expiration,
        "option_type": option_type,
        "last": 5.25,
        "bid": 5.20,
        "ask": 5.30,
        "volume": 1000,
        "open_interest": 5000,
        "implied_volatility": None  # Not provided by API
    }

    enriched = calc.enrich_option_data_with_iv(option_data, underlying_price, risk_free_rate)

    print(f"Option: {enriched['option_symbol']}")
    print(f"Strike: ${enriched['strike']:.2f}")
    print(f"Last: ${enriched['last']:.2f}")
    print(f"Bid/Ask: ${enriched['bid']:.2f} / ${enriched['ask']:.2f}")
    print(f"\nCalculated IV: {enriched.get('implied_volatility'):.4f if enriched.get('implied_volatility') else 'None'}")
    if enriched.get('implied_volatility'):
        print(f"IV Percentage: {enriched['implied_volatility']*100:.2f}%")

    print("\n" + "="*80)
    print("✅ IV calculator test complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
