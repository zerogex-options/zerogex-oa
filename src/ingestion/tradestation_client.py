"""
TradeStation Market Data Client

Comprehensive client for TradeStation Market Data API v3 with retry logic.
"""

import os
import requests
import time
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Union
import pytz
import json

from src.ingestion.tradestation_auth import TradeStationAuth
from src.utils import get_logger
from src.validation import safe_float, safe_int
from src.config import (
    API_REQUEST_TIMEOUT,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY,
    API_RETRY_BACKOFF,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")


class TradeStationClient:
    """Comprehensive client for TradeStation Market Data API with retry logic"""

    BASE_URL = "https://api.tradestation.com/v3"
    SANDBOX_URL = "https://sim-api.tradestation.com/v3"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """Initialize TradeStation client"""
        logger.debug("Initializing TradeStationClient...")

        self.base_url = self.SANDBOX_URL if sandbox else self.BASE_URL
        self.auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)
        self.sandbox = sandbox

        # Check if market hours warnings should be suppressed
        self.warn_market_hours = os.getenv("TS_WARN_MARKET_HOURS", "true").lower() != "false"

        if sandbox:
            logger.warning(f"Using SANDBOX environment [{self.base_url}]")
        else:
            logger.info(f"Using PRODUCTION environment [{self.base_url}]")

    def _request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body
            retry_count: Current retry attempt

        Returns:
            JSON response
        """
        url = f"{self.base_url}/{endpoint}"
        headers = self.auth.get_headers()
        headers["Content-Type"] = "application/json"

        logger.debug(f"{method} {endpoint} (attempt {retry_count + 1}/{API_RETRY_ATTEMPTS})")

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data,
                timeout=API_REQUEST_TIMEOUT
            )

            if response.status_code in [200, 201]:
                result = response.json()
                logger.debug(f"Response: {json.dumps(result, indent=2)}...")
                return result

            # Handle rate limiting with exponential backoff
            if response.status_code == 429:
                if retry_count < API_RETRY_ATTEMPTS - 1:
                    retry_delay = API_RETRY_DELAY * (API_RETRY_BACKOFF ** retry_count)
                    logger.warning(f"Rate limited (429), retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    return self._request(method, endpoint, params, data, retry_count + 1)

            # Handle server errors with retry
            if response.status_code >= 500:
                if retry_count < API_RETRY_ATTEMPTS - 1:
                    retry_delay = API_RETRY_DELAY * (API_RETRY_BACKOFF ** retry_count)
                    logger.warning(f"Server error ({response.status_code}), retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    return self._request(method, endpoint, params, data, retry_count + 1)

            # Other errors
            logger.error(f"API request failed: {response.status_code}")
            logger.error(f"Response: {response.text}")
            response.raise_for_status()

        except requests.exceptions.Timeout:
            if retry_count < API_RETRY_ATTEMPTS - 1:
                retry_delay = API_RETRY_DELAY * (API_RETRY_BACKOFF ** retry_count)
                logger.warning(f"Request timeout, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                return self._request(method, endpoint, params, data, retry_count + 1)
            logger.error(f"Request timed out after {API_RETRY_ATTEMPTS} attempts")
            raise

        except requests.exceptions.RequestException as e:
            if retry_count < API_RETRY_ATTEMPTS - 1:
                retry_delay = API_RETRY_DELAY * (API_RETRY_BACKOFF ** retry_count)
                logger.warning(f"Request failed: {e}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                return self._request(method, endpoint, params, data, retry_count + 1)
            logger.error(f"Request failed after {API_RETRY_ATTEMPTS} attempts: {e}")
            raise

    # =========================================================================
    # QUOTE ENDPOINTS
    # =========================================================================

    def get_quote(self, symbols: Union[str, List[str]], warn_if_closed: bool = True) -> Dict[str, Any]:
        """Get current quote snapshots"""
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Fetching quotes for: {symbols}")

        if warn_if_closed and self.warn_market_hours and not self.is_market_open(check_extended=True):
            logger.warning("⚠️  Market is currently closed - quotes may be delayed or stale")

        return self._request("GET", f"marketdata/quotes/{symbols}")

    def get_bars(
        self, 
        symbol: str, 
        interval: int, 
        unit: str,
        barsback: Optional[int] = None,
        firstdate: Optional[str] = None,
        lastdate: Optional[str] = None,
        sessiontemplate: str = "Default",
        warn_if_closed: bool = True
    ) -> Dict[str, Any]:
        """Get historical bar data (OHLCV)"""
        params = {
            "interval": interval,
            "unit": unit,
            "sessiontemplate": sessiontemplate
        }

        if barsback:
            params["barsback"] = barsback
        if firstdate:
            params["firstdate"] = firstdate
        if lastdate:
            params["lastdate"] = lastdate

        logger.info(f"Fetching bars for {symbol}: {interval}{unit}")

        if warn_if_closed and self.warn_market_hours:
            if unit == "Minute" and not firstdate and not lastdate:
                if not self.is_market_open(check_extended=True):
                    logger.warning("⚠️  Market is closed - intraday bars may be delayed")

        return self._request("GET", f"marketdata/barcharts/{symbol}", params=params)

    # =========================================================================
    # OPTIONS ENDPOINTS
    # =========================================================================

    def get_option_expirations(self, underlying: str, strike_price: Optional[float] = None) -> List[date]:
        """Get available option expiration dates"""
        params = {}
        if strike_price:
            params["strikePrice"] = strike_price

        logger.info(f"Fetching option expirations for {underlying}")
        result = self._request("GET", f"marketdata/options/expirations/{underlying}", params=params)

        expirations = []
        if "Expirations" in result:
            for exp in result["Expirations"]:
                exp_date = datetime.strptime(exp["Date"], "%Y-%m-%dT%H:%M:%SZ").date()
                expirations.append(exp_date)

        logger.info(f"Found {len(expirations)} expirations")
        return sorted(expirations)

    def get_option_strikes(self, underlying: str, expiration: Optional[str] = None) -> List[float]:
        """Get available strike prices"""
        params = {}
        if expiration:
            params["expiration"] = expiration

        logger.info(f"Fetching option strikes for {underlying}")
        result = self._request("GET", f"marketdata/options/strikes/{underlying}", params=params)

        strikes = []
        if "Strikes" in result:
            strikes = [float(strike[0]) for strike in result["Strikes"]]

        logger.info(f"Found {len(strikes)} strikes")
        return strikes

    def get_option_quotes(self, option_symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """Get quotes for specific option symbols"""
        if isinstance(option_symbols, list):
            option_symbols = ",".join(option_symbols)

        logger.info(f"Fetching option quotes for {len(option_symbols.split(','))} symbols")
        return self._request("GET", f"marketdata/quotes/{option_symbols}")

    def search_symbols(self, search: str) -> List[Dict[str, Any]]:
        """Search for symbols by name or description"""
        logger.info(f"Searching symbols: {search}")
        params = {"search": search}
        result = self._request("GET", "marketdata/symbols/search", params=params)
        return result.get("Symbols", [])

    def get_market_depth_quotes(self, symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """Get Level 2 market depth quotes"""
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Fetching market depth for: {symbols}")
        return self._request("GET", f"marketdata/marketdepth/quotes/{symbols}")

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def build_option_symbol(self, underlying: str, expiration: date, option_type: str, strike: float) -> str:
        """
        Build TradeStation option symbol with proper formatting

        Format: UNDERLYING YYMMDD C/P STRIKE
        Strike formatting: integers as-is, decimals with 2 decimal places

        Example: SPY 260221C450 or SPY 260221P450.50
        """
        exp_str = expiration.strftime("%y%m%d")

        # Format strike with proper precision
        if strike == int(strike):
            strike_str = str(int(strike))
        else:
            strike_str = f"{strike:.2f}"

        symbol = f"{underlying} {exp_str}{option_type.upper()}{strike_str}"
        logger.debug(f"Built option symbol: {symbol}")
        return symbol

    def is_market_open(self, check_extended: bool = False) -> bool:
        """Check if US equity market is currently open"""
        now_et = datetime.now(ET)

        # Check if weekday
        if now_et.weekday() > 4:
            return False

        current_time = now_et.time()

        if check_extended:
            market_open = datetime.strptime("04:00:00", "%H:%M:%S").time()
            market_close = datetime.strptime("20:00:00", "%H:%M:%S").time()
        else:
            market_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
            market_close = datetime.strptime("16:00:00", "%H:%M:%S").time()

        return market_open <= current_time <= market_close

    def get_market_status(self) -> Dict[str, Any]:
        """Get comprehensive market status"""
        now_et = datetime.now(ET)
        is_weekend = now_et.weekday() > 4
        regular_open = self.is_market_open(check_extended=False)
        extended_open = self.is_market_open(check_extended=True)

        # Determine session
        if is_weekend:
            session = "Weekend - Market Closed"
        elif regular_open:
            session = "Regular Trading Hours"
        elif extended_open:
            current_time = now_et.time()
            pre_market_end = datetime.strptime("09:30:00", "%H:%M:%S").time()
            if current_time < pre_market_end:
                session = "Pre-Market"
            else:
                session = "After-Hours"
        else:
            session = "Market Closed"

        return {
            "is_open_regular": regular_open,
            "is_open_extended": extended_open,
            "is_weekend": is_weekend,
            "session": session,
            "current_time_et": now_et.strftime("%Y-%m-%d %H:%M:%S ET"),
            "day_of_week": now_et.strftime("%A"),
            "regular_hours": "9:30 AM - 4:00 PM ET",
            "extended_hours": "4:00 AM - 8:00 PM ET"
        }


def main():
    """Example usage and testing with command-line arguments"""
    import argparse

    parser = argparse.ArgumentParser(
        description="TradeStation Market Data Client - Test various API endpoints",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all default tests
  python -m src.ingestion.tradestation_client

  # Test specific endpoint
  python -m src.ingestion.tradestation_client --test quote --symbol AAPL
  python -m src.ingestion.tradestation_client --test bars --symbol SPY --bars-back 10
  python -m src.ingestion.tradestation_client --test options --symbol SPY
  python -m src.ingestion.tradestation_client --test search --query Apple
  python -m src.ingestion.tradestation_client --test market-hours

  # Enable debug logging
  python -m src.ingestion.tradestation_client --debug
        """
    )

    parser.add_argument("--test", 
                       choices=["all", "quote", "bars", "options", "search", "market-hours", "depth"],
                       help="Which test to run (default: all, env: TS_TEST)")

    parser.add_argument("--symbol", type=str,
                       help="Symbol(s) to test with, comma-separated (default: SPY, env: TS_SYMBOL)")

    parser.add_argument("--bars-back", type=int,
                       help="Number of bars to retrieve (default: 5, env: TS_BARS_BACK)")

    parser.add_argument("--interval", type=int,
                       help="Bar interval (default: 1, env: TS_INTERVAL)")

    parser.add_argument("--unit", type=str,
                       choices=["Minute", "Daily", "Weekly", "Monthly"],
                       help="Bar time unit (default: Daily, env: TS_UNIT)")

    parser.add_argument("--query", type=str,
                       help="Search query for symbol search (default: Apple, env: TS_QUERY)")

    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging (env: LOG_LEVEL=DEBUG)")

    args = parser.parse_args()

    # Load environment variable defaults
    test = args.test if args.test else os.getenv("TS_TEST", "all")
    symbol = args.symbol if args.symbol else os.getenv("TS_SYMBOL", "SPY")
    bars_back = args.bars_back if args.bars_back else int(os.getenv("TS_BARS_BACK", "5"))
    interval = args.interval if args.interval else int(os.getenv("TS_INTERVAL", "1"))
    unit = args.unit if args.unit else os.getenv("TS_UNIT", "Daily")
    query = args.query if args.query else os.getenv("TS_QUERY", "Apple")
    debug = args.debug or os.getenv("LOG_LEVEL", "").upper() == "DEBUG"

    # Set logging level
    if debug:
        from src.utils import set_log_level
        set_log_level("DEBUG")
        print("🐛 Debug logging enabled\n")

    print("\n" + "="*60)
    print("TradeStation Market Data Client")
    print("="*60 + "\n")

    # Initialize client
    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID"),
        os.getenv("TRADESTATION_CLIENT_SECRET"),
        os.getenv("TRADESTATION_REFRESH_TOKEN"),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true"
    )

    try:
        # Convert comma-separated symbols to list
        symbols = symbol.split(",") if "," in symbol else symbol

        # Test 1: Get quote
        if test in ["all", "quote"]:
            print("Test: Get Quote")
            print("-" * 60)
            quote = client.get_quote(symbols)
            if "Quotes" in quote and len(quote["Quotes"]) > 0:
                for q in quote["Quotes"]:
                    volume = safe_int(q.get("Volume"), field_name="Volume")
                    print(f"✅ {q['Symbol']}: ${q.get('Last', 'N/A')}")
                    print(f"   Bid: ${q.get('Bid', 'N/A')} x {q.get('BidSize', 0)}")
                    print(f"   Ask: ${q.get('Ask', 'N/A')} x {q.get('AskSize', 0)}")
                    print(f"   Volume: {volume:,}")
            print()

        # Test 2: Get bars
        if test in ["all", "bars"]:
            print(f"Test: Get {unit} Bars (last {bars_back})")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols
            bars = client.get_bars(sym, interval, unit, barsback=bars_back)
            if "Bars" in bars and len(bars["Bars"]) > 0:
                print(f"Retrieved {len(bars['Bars'])} bars for {sym}")
                for bar in bars["Bars"][-3:]:
                    open_price = safe_float(bar.get("Open"), field_name="Open")
                    high_price = safe_float(bar.get("High"), field_name="High")
                    low_price = safe_float(bar.get("Low"), field_name="Low")
                    close_price = safe_float(bar.get("Close"), field_name="Close")
                    total_vol = safe_int(bar.get("TotalVolume"), field_name="TotalVolume")

                    print(f"   {bar['TimeStamp']}: O=${open_price:.2f} H=${high_price:.2f} "
                          f"L=${low_price:.2f} C=${close_price:.2f} V={total_vol:,}")
            print()

        # Test 3: Get option expirations
        if test in ["all", "options"]:
            print("Test: Get Option Expirations (first 5)")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols
            expirations = client.get_option_expirations(sym)
            for exp in expirations[:5]:
                print(f"   {exp}")

            if expirations:
                print(f"\n   Strikes for {expirations[0]} (first 10):")
                exp_str = expirations[0].strftime("%m-%d-%Y")
                strikes = client.get_option_strikes(sym, expiration=exp_str)
                for strike in strikes[:10]:
                    print(f"      ${strike}")
            print()

        # Test 4: Symbol search
        if test in ["all", "search"]:
            print(f"Test: Search Symbols for '{query}'")
            print("-" * 60)
            results = client.search_symbols(query)
            for s in results[:5]:
                print(f"   {s.get('Symbol', 'N/A')}: {s.get('Description', 'N/A')}")
            print()

        # Test 5: Market hours
        if test in ["all", "market-hours"]:
            print("Test: Market Hours & Status")
            print("-" * 60)

            status = client.get_market_status()

            print(f"   Current Time: {status['current_time_et']}")
            print(f"   Day: {status['day_of_week']}")
            print(f"   Session: {status['session']}")
            print(f"\n   Regular Hours ({status['regular_hours']}):")
            print(f"      Open: {'✅ YES' if status['is_open_regular'] else '❌ NO'}")
            print(f"\n   Extended Hours ({status['extended_hours']}):")
            print(f"      Open: {'✅ YES' if status['is_open_extended'] else '❌ NO'}")
            print()

        # Test 6: Market depth
        if test in ["depth"]:
            print("Test: Market Depth (Level 2)")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols
            depth = client.get_market_depth_quotes(sym)
            if "MarketDepthQuotes" in depth:
                print(f"   Market depth data retrieved for {sym}")
                print(f"   Available fields: {list(depth.keys())}")
            print()

        if test == "all":
            print("="*60)
            print("✅ All tests completed successfully!")
            print("="*60)
        else:
            print(f"✅ Test '{test}' completed successfully!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == "__main__":
    main()
