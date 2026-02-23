"""
TradeStation Market Data Client

Comprehensive client for TradeStation Market Data API v3 with retry logic.
Updated with Stream Bars endpoint for real-time volume tracking.
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
                # Check if response has content
                if not response.content or len(response.content) == 0:
                    logger.warning(f"API returned 200 but empty response - likely market closed or no data available")
                    # Return empty structure based on endpoint
                    if "barcharts" in endpoint or "stream/barcharts" in endpoint:
                        return {"Bars": []}
                    elif "quotes" in endpoint:
                        return {"Quotes": []}
                    elif "expirations" in endpoint:
                        return {"Expirations": []}
                    elif "strikes" in endpoint:
                        return {"Strikes": []}
                    else:
                        return {}

                result = response.json()
                logger.debug(f"Response: {json.dumps(result, indent=2)[:1000]}...")
                return result

            # Handle 404 "No data available" - don't retry, just return empty
            if response.status_code == 404:
                try:
                    error_data = response.json()
                    if error_data.get("Message") == "No data available.":
                        logger.warning(f"No data available for request (404) - this is normal for weekends/holidays")
                        # Return empty but valid response structure based on endpoint
                        if "barcharts" in endpoint or "stream/barcharts" in endpoint:
                            return {"Bars": []}
                        elif "quotes" in endpoint:
                            return {"Quotes": []}
                        elif "expirations" in endpoint:
                            return {"Expirations": []}
                        elif "strikes" in endpoint:
                            return {"Strikes": []}
                        else:
                            return {}
                except:
                    pass

                # For other 404s, log and raise
                logger.error(f"API request failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                response.raise_for_status()

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

    def get_stream_bars(
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
        """
        Get streaming bars with volume breakdown (UpVolume/DownVolume)

        This endpoint provides UpVolume and DownVolume which are critical
        for gamma exposure calculations and directional flow analysis.

        IMPORTANT: For real-time streaming, DO NOT use barsback parameter.
        Use firstdate/lastdate for historical data, or omit all date params
        for the most recent bar.

        Key differences from get_bars():
        - Returns UpVolume and DownVolume (not in regular bars endpoint)
        - Optimized for real-time streaming
        - Better for intraday 1-minute bars
        - During market hours: returns latest completing bar
        - During closed hours: may return empty if no recent data

        Args:
            symbol: Symbol to stream
            interval: Bar interval (e.g., 1, 5, 15)
            unit: Time unit (Minute, Daily, Weekly, Monthly)
            barsback: Number of bars to retrieve (use None for latest, or for historical only)
            firstdate: Optional start date (ISO format: YYYY-MM-DDTHH:MM:SSZ)
            lastdate: Optional end date (ISO format: YYYY-MM-DDTHH:MM:SSZ)
            sessiontemplate: Session template (Default, USEQPre, USEQ24Hour, etc.)
            warn_if_closed: Warn if market is closed

        Returns:
            Dict with Bars array containing:
            - TimeStamp, Open, High, Low, Close
            - TotalVolume, UpVolume, DownVolume
            - OpenInterest (for futures)

        Example:
            # Get latest bar during market hours (RECOMMENDED for streaming)
            bars = client.get_stream_bars("SPY", 1, "Minute")

            # Get bars for specific date range (historical)
            bars = client.get_stream_bars(
                "SPY", 1, "Minute",
                firstdate="2026-02-21T09:30:00Z",
                lastdate="2026-02-21T16:00:00Z"
            )
        """
        params = {
            "interval": interval,
            "unit": unit,
            "sessiontemplate": sessiontemplate
        }

        # Only add parameters that are explicitly set
        if barsback is not None:
            params["barsback"] = barsback
        if firstdate:
            params["firstdate"] = firstdate
        if lastdate:
            params["lastdate"] = lastdate

        if barsback:
            logger.info(f"Streaming bars for {symbol}: {interval}{unit}, barsback={barsback}")
        elif firstdate or lastdate:
            logger.info(f"Streaming bars for {symbol}: {interval}{unit}, date range specified")
        else:
            logger.info(f"Streaming bars for {symbol}: {interval}{unit} (latest bar)")

        if warn_if_closed and self.warn_market_hours:
            if unit == "Minute" and not firstdate and not lastdate:
                if not self.is_market_open(check_extended=True):
                    logger.warning("⚠️  Market is closed - intraday bars may be delayed")

        return self._request("GET", f"marketdata/stream/barcharts/{symbol}", params=params)

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
        logger.debug(f"{option_symbols.split(',')}")
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
  python -m src.ingestion.tradestation_client --test stream-bars --symbol SPY
  python -m src.ingestion.tradestation_client --test options --symbol SPY
  python -m src.ingestion.tradestation_client --test search --query Apple
  python -m src.ingestion.tradestation_client --test market-hours

  # Enable debug logging
  python -m src.ingestion.tradestation_client --debug
        """
    )

    parser.add_argument("--test", 
                       choices=["all", "quote", "bars", "stream-bars", "options", "search", "market-hours", "depth"],
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

    parser.add_argument("--test-historical", action="store_true",
                       help="For stream-bars test, use historical date range (last Friday)")

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

        # Test 2.5: Get stream bars (NEW)
        if test in ["all", "stream-bars"]:
            print(f"Test: Get Stream Bars with Up/Down Volume")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols

            # If testing with historical data, use last Friday's date
            if args.test_historical:
                from datetime import datetime, timedelta
                import pytz
                ET = pytz.timezone("US/Eastern")
                now = datetime.now(ET)

                # Find last Friday
                days_since_friday = (now.weekday() - 4) % 7
                if days_since_friday == 0 and now.hour < 16:
                    days_since_friday = 7  # If before close on Friday, use previous Friday

                last_friday = now - timedelta(days=days_since_friday)

                # Use 2:00 PM - 2:05 PM ET window
                start_time = last_friday.replace(hour=14, minute=0, second=0, microsecond=0)
                end_time = start_time + timedelta(minutes=5)

                print(f"Testing with historical data from {start_time.strftime('%Y-%m-%d %H:%M ET')}")

                bars = client.get_stream_bars(
                    sym, 1, "Minute",
                    firstdate=start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    lastdate=end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    sessiontemplate="USEQ24Hour"
                )
            else:
                # For real-time during market hours, don't use barsback
                # Just get the latest bar
                print(f"Getting latest 1-minute bar for {sym} (real-time)")
                bars = client.get_stream_bars(sym, 1, "Minute", sessiontemplate="USEQ24Hour")

            if "Bars" in bars and len(bars["Bars"]) > 0:
                print(f"✅ Retrieved {len(bars['Bars'])} stream bar(s) for {sym}")
                for bar in bars["Bars"][-3:]:
                    open_price = safe_float(bar.get("Open"), field_name="Open")
                    high_price = safe_float(bar.get("High"), field_name="High")
                    low_price = safe_float(bar.get("Low"), field_name="Low")
                    close_price = safe_float(bar.get("Close"), field_name="Close")
                    total_vol = safe_int(bar.get("TotalVolume"), field_name="TotalVolume")
                    up_vol = safe_int(bar.get("UpVolume"), field_name="UpVolume")
                    down_vol = safe_int(bar.get("DownVolume"), field_name="DownVolume")

                    print(f"   {bar['TimeStamp']}: O=${open_price:.2f} H=${high_price:.2f} "
                          f"L=${low_price:.2f} C=${close_price:.2f}")
                    print(f"      Volume: {total_vol:,} (Up: {up_vol:,}, Down: {down_vol:,})")

                    # Show volume breakdown percentage
                    if total_vol > 0:
                        up_pct = (up_vol / total_vol) * 100
                        down_pct = (down_vol / total_vol) * 100
                        print(f"      Breakdown: {up_pct:.1f}% buying, {down_pct:.1f}% selling")
            else:
                print(f"⚠️  No stream bars data available for {sym}")
                if not args.test_historical:
                    print(f"   This could mean:")
                    print(f"   1. Market just opened and first bar hasn't completed yet")
                    print(f"   2. API delay in returning data")
                    print(f"   3. Market is closed (weekend/holiday)")
                    print(f"\n   Market hours: Mon-Fri 4:00 AM - 8:00 PM ET")
                    print(f"   Current time: {datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S ET')}")
                    print(f"   Try: python run.py client --test stream-bars --test-historical")
                else:
                    print(f"   No data found for requested time range")
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
