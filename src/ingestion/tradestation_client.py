"""
TradeStation Market Data Client

Comprehensive client for TradeStation Market Data API v3.
Covers all endpoints: quotes, bars, options, symbols, and more.

API Documentation: https://api.tradestation.com/docs/specification/#tag/MarketData
"""

import os
import requests
import json
from datetime import datetime, date, timezone, timedelta
from typing import Optional, List, Dict, Any, Union
from src.ingestion.tradestation_auth import TradeStationAuth
from src.utils import get_logger

logger = get_logger(__name__)


class TradeStationClient:
    """Comprehensive client for TradeStation Market Data API"""

    BASE_URL = "https://api.tradestation.com/v3"
    SANDBOX_URL = "https://sim-api.tradestation.com/v3"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize TradeStation client

        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: Refresh token for obtaining access tokens
            sandbox: Use sandbox environment (default False)
        """
        logger.debug("Initializing TradeStationClient...")

        self.base_url = self.SANDBOX_URL if sandbox else self.BASE_URL
        self.auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)
        self.sandbox = sandbox

        # Check if market hours warnings should be suppressed via env var
        self.warn_market_hours = os.getenv('TS_WARN_MARKET_HOURS', 'true').lower() != 'false'

        if sandbox:
            logger.warning(f"Using SANDBOX environment [{self.base_url}]")
        else:
            logger.info(f"Using PRODUCTION environment [{self.base_url}]")

        if not self.warn_market_hours:
            logger.debug("Market hours warnings suppressed via TS_WARN_MARKET_HOURS=false")

    def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                 data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request to TradeStation API

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (without base URL)
            params: Query parameters
            data: Request body data

        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}/{endpoint}"
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'

        logger.debug(f"{method} {endpoint}")
        if params:
            logger.debug(f"Params: {params}")
        if data:
            logger.debug(f"Data: {data}")

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data,
                timeout=30
            )

            logger.debug(f"Response status: {response.status_code}")

            if response.status_code not in [200, 201]:
                logger.error(f"API request failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                response.raise_for_status()

            result = response.json()
            logger.debug(f"Response: {json.dumps(result, indent=2)[:500]}...")
            return result

        except requests.exceptions.Timeout:
            logger.error(f"Request timed out: {method} {endpoint}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: {e}")
            raise
        except Exception as e:
            logger.critical(f"Unexpected error: {e}", exc_info=True)
            raise

    # =========================================================================
    # QUOTE ENDPOINTS
    # =========================================================================

    def get_quote(self, symbols: Union[str, List[str]], warn_if_closed: bool = True) -> Dict[str, Any]:
        """
        Get current quote snapshots for one or more symbols

        Args:
            symbols: Single symbol string or list of symbols (max 500)
            warn_if_closed: Log warning if market is closed (default True)
                          Can be suppressed globally via TS_WARN_MARKET_HOURS=false

        Returns:
            Quote data with bid, ask, last, volume, etc.

        Example:
            >>> client.get_quote("SPY")
            >>> client.get_quote(["SPY", "QQQ", "AAPL"])
            >>> client.get_quote("SPY", warn_if_closed=False)  # Suppress warning for this call
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Fetching quotes for: {symbols}")

        # Warn if market is closed (respects both parameter and env var)
        if warn_if_closed and self.warn_market_hours and not self.is_market_open(check_extended=True):
            logger.warning("⚠️  Market is currently closed - quotes may be delayed or stale")

        return self._request("GET", f"marketdata/quotes/{symbols}")

    def stream_quotes(self, symbols: Union[str, List[str]], warn_if_closed: bool = True) -> Dict[str, Any]:
        """
        Get streaming quote endpoint URL (use with WebSocket)

        Args:
            symbols: Single symbol or list of symbols
            warn_if_closed: Log warning if market is closed (default True)

        Returns:
            Streaming endpoint information
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Getting stream endpoint for quotes: {symbols}")

        # Warn if market is closed
        if warn_if_closed and self.warn_market_hours and not self.is_market_open(check_extended=True):
            logger.warning("⚠️  Market is currently closed - streaming may not provide real-time updates")

        return self._request("GET", f"marketdata/stream/quotes/{symbols}")

    # =========================================================================
    # BAR CHART ENDPOINTS
    # =========================================================================

    def get_bars(self, symbol: str, interval: int, unit: str, 
                 barsback: Optional[int] = None,
                 firstdate: Optional[str] = None,
                 lastdate: Optional[str] = None,
                 sessiontemplate: str = "Default",
                 warn_if_closed: bool = True) -> Dict[str, Any]:
        """
        Get historical bar data (OHLCV)

        Args:
            symbol: Symbol to fetch bars for
            interval: Bar interval (e.g., 1, 5, 15, 60)
            unit: Time unit - 'Minute', 'Daily', 'Weekly', 'Monthly'
            barsback: Number of bars to retrieve (alternative to date range)
            firstdate: Start date (ISO format: YYYY-MM-DDTHH:MM:SSZ)
            lastdate: End date (ISO format: YYYY-MM-DDTHH:MM:SSZ)
            sessiontemplate: Session template - 'Default', 'USEQPre', 'USEQPost', 
                           'USEQPreAndPost', 'USEQ24Hour'
            warn_if_closed: Log warning if requesting real-time data while market is closed

        Returns:
            Bar data with High, Low, Open, Close, Volume

        Example:
            >>> # Get last 100 daily bars
            >>> client.get_bars("SPY", 1, "Daily", barsback=100)
            >>> # Get 5-min bars for date range
            >>> client.get_bars("SPY", 5, "Minute", 
            ...                 firstdate="2026-02-01T09:30:00Z",
            ...                 lastdate="2026-02-01T16:00:00Z")
        """
        params = {
            'interval': interval,
            'unit': unit,
            'sessiontemplate': sessiontemplate
        }

        if barsback:
            params['barsback'] = barsback
        if firstdate:
            params['firstdate'] = firstdate
        if lastdate:
            params['lastdate'] = lastdate

        logger.info(f"Fetching bars for {symbol}: {interval}{unit}, {params}")

        # Warn if requesting recent/real-time bars while market is closed
        if warn_if_closed and self.warn_market_hours:
            # Only warn for intraday bars or if no date specified (implies recent data)
            if unit == 'Minute' and not firstdate and not lastdate:
                if not self.is_market_open(check_extended=True):
                    logger.warning("⚠️  Market is currently closed - intraday bars may be delayed or stale")

        return self._request("GET", f"marketdata/barcharts/{symbol}", params=params)

    def stream_bars(self, symbol: str, interval: int, unit: str,
                    sessiontemplate: str = "Default",
                    warn_if_closed: bool = True) -> Dict[str, Any]:
        """
        Get streaming bars endpoint (use with WebSocket)

        Args:
            symbol: Symbol to stream
            interval: Bar interval
            unit: Time unit
            sessiontemplate: Session template
            warn_if_closed: Log warning if market is closed

        Returns:
            Streaming endpoint information
        """
        params = {
            'interval': interval,
            'unit': unit,
            'sessiontemplate': sessiontemplate
        }

        logger.info(f"Getting stream endpoint for bars: {symbol}")

        # Warn if market is closed
        if warn_if_closed and self.warn_market_hours and not self.is_market_open(check_extended=True):
            logger.warning("⚠️  Market is currently closed - bar streaming may not provide real-time updates")

        return self._request("GET", f"marketdata/stream/barcharts/{symbol}", params=params)

    # =========================================================================
    # OPTIONS ENDPOINTS
    # =========================================================================

    def get_option_expirations(self, underlying: str, 
                              strike_price: Optional[float] = None) -> List[date]:
        """
        Get available option expiration dates for an underlying

        Args:
            underlying: Underlying symbol (e.g., 'SPY')
            strike_price: Optional strike price filter

        Returns:
            List of expiration dates (sorted)

        Example:
            >>> client.get_option_expirations("SPY")
            >>> client.get_option_expirations("SPY", strike_price=450.0)
        """
        params = {}
        if strike_price:
            params['strikePrice'] = strike_price

        logger.info(f"Fetching option expirations for {underlying}")
        result = self._request("GET", f"marketdata/options/expirations/{underlying}", 
                              params=params)

        expirations = []
        if 'Expirations' in result:
            for exp in result['Expirations']:
                exp_date = datetime.strptime(exp['Date'], '%Y-%m-%dT%H:%M:%SZ').date()
                expirations.append(exp_date)

        logger.info(f"Found {len(expirations)} expirations")
        return sorted(expirations)

    def get_option_strikes(self, underlying: str, 
                          expiration: Optional[str] = None) -> List[float]:
        """
        Get available strike prices for an underlying

        Args:
            underlying: Underlying symbol
            expiration: Optional expiration date (MM-DD-YYYY format)

        Returns:
            List of strike prices

        Example:
            >>> client.get_option_strikes("SPY")
            >>> client.get_option_strikes("SPY", expiration="02-21-2026")
        """
        params = {}
        if expiration:
            params['expiration'] = expiration

        logger.info(f"Fetching option strikes for {underlying}")
        result = self._request("GET", f"marketdata/options/strikes/{underlying}", 
                              params=params)

        strikes = []
        if 'Strikes' in result:
            strikes = [float(strike[0]) for strike in result['Strikes']]

        logger.info(f"Found {len(strikes)} strikes")
        return strikes

    def get_option_chain(self, underlying: str, expiration: str) -> Dict[str, Any]:
        """
        Get full option chain for an underlying and expiration

        Args:
            underlying: Underlying symbol
            expiration: Expiration date (MM-DD-YYYY format)

        Returns:
            Complete option chain with calls and puts

        Example:
            >>> client.get_option_chain("SPY", "02-21-2026")
        """
        logger.info(f"Fetching option chain for {underlying} exp {expiration}")
        params = {'expiration': expiration}
        return self._request("GET", f"marketdata/options/chain/{underlying}", 
                           params=params)

    def get_option_quotes(self, option_symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get quotes for specific option symbols

        Args:
            option_symbols: Single option symbol or list (max 500)
                          Format: SPY 260221C450 (underlying YYMMDD C/P strike)

        Returns:
            Option quote data

        Example:
            >>> client.get_option_quotes("SPY 260221C450")
            >>> client.get_option_quotes(["SPY 260221C450", "SPY 260221P450"])
        """
        if isinstance(option_symbols, list):
            option_symbols = ",".join(option_symbols)

        logger.info(f"Fetching option quotes for: {option_symbols}")
        return self._request("GET", f"marketdata/options/quotes/{option_symbols}")

    def spread_quote(self, spread: str, legs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Get quote for an option spread

        Args:
            spread: Spread type - 'VERTICAL', 'CALENDAR', 'DIAGONAL', 'CONDOR', etc.
            legs: List of leg dictionaries with 'symbol', 'quantity', 'side'

        Returns:
            Spread quote with theoretical value

        Example:
            >>> legs = [
            ...     {'symbol': 'SPY 260221C450', 'quantity': 1, 'side': 'BUY'},
            ...     {'symbol': 'SPY 260221C455', 'quantity': 1, 'side': 'SELL'}
            ... ]
            >>> client.spread_quote("VERTICAL", legs)
        """
        data = {
            'spread': spread,
            'legs': legs
        }
        logger.info(f"Fetching {spread} spread quote")
        return self._request("POST", "marketdata/options/spread/quote", data=data)

    # =========================================================================
    # SYMBOL & MARKET INFO ENDPOINTS
    # =========================================================================

    def search_symbols(self, search: str) -> List[Dict[str, Any]]:
        """
        Search for symbols by name or description

        Args:
            search: Search string (company name, symbol, etc.)

        Returns:
            List of matching symbols with details

        Example:
            >>> client.search_symbols("Apple")
            >>> client.search_symbols("SPY")
        """
        logger.info(f"Searching symbols: {search}")
        params = {'search': search}
        result = self._request("GET", "marketdata/symbols/search", params=params)
        return result.get('Symbols', [])

    def get_symbol_details(self, symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get detailed information about symbols

        Args:
            symbols: Single symbol or list of symbols

        Returns:
            Symbol details (exchange, asset type, tick size, etc.)

        Example:
            >>> client.get_symbol_details("SPY")
            >>> client.get_symbol_details(["SPY", "QQQ", "AAPL"])
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Fetching symbol details for: {symbols}")
        return self._request("GET", f"marketdata/symbols/{symbols}")

    def suggest_symbols(self, text: str, filter_type: Optional[str] = None) -> List[str]:
        """
        Get symbol suggestions (autocomplete)

        Args:
            text: Partial symbol or name
            filter_type: Optional filter - 'All', 'Stock', 'StockOption', 'Future', etc.

        Returns:
            List of suggested symbols

        Example:
            >>> client.suggest_symbols("APP")
            >>> client.suggest_symbols("tech", filter_type="Stock")
        """
        params = {'text': text}
        if filter_type:
            params['filter'] = filter_type

        logger.info(f"Getting symbol suggestions for: {text}")
        result = self._request("GET", "marketdata/symbols/suggest", params=params)
        return result.get('Suggestions', [])

    # =========================================================================
    # MARKET DEPTH (LEVEL 2) ENDPOINTS
    # =========================================================================

    def get_market_depth_quotes(self, symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get Level 2 market depth quotes (bid/ask ladder)

        Args:
            symbols: Single symbol or list of symbols

        Returns:
            Market depth data with multiple bid/ask levels

        Example:
            >>> client.get_market_depth_quotes("SPY")
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Fetching market depth for: {symbols}")
        return self._request("GET", f"marketdata/marketdepth/quotes/{symbols}")

    def get_market_depth_aggregates(self, symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get aggregated market depth (total size at each price level)

        Args:
            symbols: Single symbol or list of symbols

        Returns:
            Aggregated market depth data

        Example:
            >>> client.get_market_depth_aggregates("SPY")
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Fetching aggregated market depth for: {symbols}")
        return self._request("GET", f"marketdata/marketdepth/aggregates/{symbols}")

    # =========================================================================
    # CRYPTOCURRENCY ENDPOINTS
    # =========================================================================

    def get_crypto_quote(self, symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get cryptocurrency quotes

        Args:
            symbols: Crypto symbol(s) - e.g., 'BTCUSD', 'ETHUSD'

        Returns:
            Crypto quote data

        Example:
            >>> client.get_crypto_quote("BTCUSD")
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Fetching crypto quotes for: {symbols}")
        return self._request("GET", f"marketdata/crypto/quotes/{symbols}")

    def stream_crypto(self, symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get streaming crypto endpoint

        Args:
            symbols: Crypto symbol(s)

        Returns:
            Streaming endpoint information
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Getting crypto stream endpoint for: {symbols}")
        return self._request("GET", f"marketdata/stream/crypto/quotes/{symbols}")

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def build_option_symbol(self, underlying: str, expiration: date, 
                           option_type: str, strike: float) -> str:
        """
        Build TradeStation option symbol format

        Args:
            underlying: Underlying symbol (e.g., 'SPY')
            expiration: Expiration date
            option_type: 'C' for call, 'P' for put
            strike: Strike price

        Returns:
            Formatted option symbol

        Example:
            >>> from datetime import date
            >>> client.build_option_symbol("SPY", date(2026, 2, 21), "C", 450.0)
            'SPY 260221C450'
        """
        # Format: UNDERLYING YYMMDD C/P STRIKE
        exp_str = expiration.strftime('%y%m%d')
        strike_str = str(int(strike)) if strike == int(strike) else str(strike)
        symbol = f"{underlying} {exp_str}{option_type.upper()}{strike_str}"
        logger.debug(f"Built option symbol: {symbol}")
        return symbol

    def is_market_open(self, check_extended: bool = False) -> bool:
        """
        Check if US equity market is currently open based on standard market hours

        Regular Hours: 9:30 AM - 4:00 PM ET (Monday-Friday)
        Extended Hours: 4:00 AM - 8:00 PM ET (Monday-Friday)

        Args:
            check_extended: If True, checks extended hours (pre/post market)
                          If False, checks only regular trading hours

        Returns:
            True if market is open, False otherwise

        Example:
            >>> client.is_market_open()  # Regular hours
            >>> client.is_market_open(check_extended=True)  # Include pre/post
        """
        from datetime import timedelta

        now_utc = datetime.now(timezone.utc)

        # Convert UTC to ET (UTC-5 or UTC-4 depending on DST)
        # Simplified: assume EST (UTC-5) - for precise DST handling, use pytz
        et_offset = timedelta(hours=-5)
        now_et = now_utc + et_offset

        # Check if it's a weekday (Monday=0, Sunday=6)
        if now_et.weekday() > 4:  # Saturday or Sunday
            logger.debug(f"Market closed: Weekend (day={now_et.weekday()})")
            return False

        current_time = now_et.time()

        if check_extended:
            # Extended hours: 4:00 AM - 8:00 PM ET
            market_open = datetime.strptime('04:00:00', '%H:%M:%S').time()
            market_close = datetime.strptime('20:00:00', '%H:%M:%S').time()
        else:
            # Regular hours: 9:30 AM - 4:00 PM ET
            market_open = datetime.strptime('09:30:00', '%H:%M:%S').time()
            market_close = datetime.strptime('16:00:00', '%H:%M:%S').time()

        is_open = market_open <= current_time <= market_close

        logger.debug(f"Market open check ({'extended' if check_extended else 'regular'}): {is_open} "
                    f"(ET time={now_et.strftime('%H:%M:%S')}, "
                    f"hours={market_open.strftime('%H:%M')}-{market_close.strftime('%H:%M')})")

        return is_open

    def get_market_status(self) -> Dict[str, Any]:
        """
        Get comprehensive market status information

        Returns:
            Dictionary with market status, current time, and hours info

        Example:
            >>> status = client.get_market_status()
            >>> print(status['is_open'])
            >>> print(status['session'])
        """
        now_utc = datetime.now(timezone.utc)
        et_offset = timedelta(hours=-5)
        now_et = now_utc + et_offset

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
            pre_market_end = datetime.strptime('09:30:00', '%H:%M:%S').time()
            if current_time < pre_market_end:
                session = "Pre-Market"
            else:
                session = "After-Hours"
        else:
            session = "Market Closed"

        return {
            'is_open_regular': regular_open,
            'is_open_extended': extended_open,
            'is_weekend': is_weekend,
            'session': session,
            'current_time_et': now_et.strftime('%Y-%m-%d %H:%M:%S ET'),
            'current_time_utc': now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'day_of_week': now_et.strftime('%A'),
            'regular_hours': '9:30 AM - 4:00 PM ET',
            'extended_hours': '4:00 AM - 8:00 PM ET'
        }


def main():
    """Example usage and testing with command-line arguments"""
    import argparse

    parser = argparse.ArgumentParser(
        description='TradeStation Market Data Client - Test various API endpoints',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
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
  python -m src.ingestion.tradestation_client --test quote --symbol SPY --debug

  # Multiple symbols
  python -m src.ingestion.tradestation_client --test quote --symbol SPY,QQQ,AAPL

Environment Variables (.env):
  All command-line options can be set via environment variables.
  Command-line arguments override environment variables.

  Required:
    TRADESTATION_CLIENT_ID           Your API client ID
    TRADESTATION_CLIENT_SECRET       Your API client secret
    TRADESTATION_REFRESH_TOKEN       Your refresh token

  Optional - API Configuration:
    TRADESTATION_USE_SANDBOX=false   Use sandbox/sim environment (default: false)
    LOG_LEVEL=INFO                   Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)
    TS_WARN_MARKET_HOURS=true        Show market closed warnings (default: true)

  Optional - Test Configuration:
    TS_TEST=all                      Which test: all, quote, bars, options, search, market-hours, depth (default: all)
    TS_SYMBOL=SPY                    Symbol(s) to test, comma-separated (default: SPY)
    TS_BARS_BACK=5                   Number of bars to retrieve (default: 5)
    TS_INTERVAL=1                    Bar interval: 1, 5, 15, 60, etc. (default: 1)
    TS_UNIT=Daily                    Time unit: Minute, Daily, Weekly, Monthly (default: Daily)
    TS_QUERY=Apple                   Search query for symbol search (default: Apple)

  Note: See .env.example for complete configuration template
        '''
    )

    parser.add_argument('--test', 
                       choices=['all', 'quote', 'bars', 'options', 'search', 'market-hours', 'depth'],
                       help='Which test to run (default: all, env: TS_TEST)')

    parser.add_argument('--symbol', type=str,
                       help='Symbol(s) to test with, comma-separated (default: SPY, env: TS_SYMBOL)')

    parser.add_argument('--bars-back', type=int,
                       help='Number of bars to retrieve (default: 5, env: TS_BARS_BACK)')

    parser.add_argument('--interval', type=int,
                       help='Bar interval (default: 1, env: TS_INTERVAL)')

    parser.add_argument('--unit', type=str,
                       choices=['Minute', 'Daily', 'Weekly', 'Monthly'],
                       help='Bar time unit (default: Daily, env: TS_UNIT)')

    parser.add_argument('--query', type=str,
                       help='Search query for symbol search (default: Apple, env: TS_QUERY)')

    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging (env: LOG_LEVEL=DEBUG)')

    args = parser.parse_args()

    # Load environment variable defaults - command line args override these
    # Use the pattern: args.value if args.value is not None else os.getenv('ENV_VAR', 'default')

    test = args.test if args.test else os.getenv('TS_TEST', 'all')
    symbol = args.symbol if args.symbol else os.getenv('TS_SYMBOL', 'SPY')
    bars_back = args.bars_back if args.bars_back else int(os.getenv('TS_BARS_BACK', '5'))
    interval = args.interval if args.interval else int(os.getenv('TS_INTERVAL', '1'))
    unit = args.unit if args.unit else os.getenv('TS_UNIT', 'Daily')
    query = args.query if args.query else os.getenv('TS_QUERY', 'Apple')
    debug = args.debug or os.getenv('LOG_LEVEL', '').upper() == 'DEBUG'

    # Validate unit choice when loaded from env
    if unit not in ['Minute', 'Daily', 'Weekly', 'Monthly']:
        print(f"Warning: Invalid TS_UNIT '{unit}', using 'Daily'")
        unit = 'Daily'

    # Validate test choice when loaded from env
    valid_tests = ['all', 'quote', 'bars', 'options', 'search', 'market-hours', 'depth']
    if test not in valid_tests:
        print(f"Warning: Invalid TS_TEST '{test}', using 'all'")
        test = 'all'

    # Set logging level
    if debug:
        from src.utils import set_log_level
        set_log_level('DEBUG')
        print("🐛 Debug logging enabled\n")

    print("\n" + "="*60)
    print("TradeStation Market Data Client")
    print("="*60 + "\n")

    # Initialize client
    client = TradeStationClient(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    )

    try:
        # Convert comma-separated symbols to list
        symbols = symbol.split(',') if ',' in symbol else symbol

        # Test 1: Get quote
        if test in ['all', 'quote']:
            print("Test: Get Quote")
            print("-" * 60)
            quote = client.get_quote(symbols)
            if 'Quotes' in quote and len(quote['Quotes']) > 0:
                for q in quote['Quotes']:
                    # Safely convert volume to int for formatting
                    volume = q.get('Volume', 0)
                    try:
                        volume = int(volume) if volume else 0
                    except (ValueError, TypeError):
                        volume = 0

                    print(f"✅ {q['Symbol']}: ${q.get('Last', 'N/A')}")
                    print(f"   Bid: ${q.get('Bid', 'N/A')} x {q.get('BidSize', 0)}")
                    print(f"   Ask: ${q.get('Ask', 'N/A')} x {q.get('AskSize', 0)}")
                    print(f"   Volume: {volume:,}")
            print()

        # Test 2: Get bars
        if test in ['all', 'bars']:
            print(f"Test: Get {unit} Bars (last {bars_back})")
            print("-" * 60)
            # Use first symbol if multiple provided
            sym = symbols[0] if isinstance(symbols, list) else symbols
            bars = client.get_bars(sym, interval, unit, barsback=bars_back)
            if 'Bars' in bars and len(bars['Bars']) > 0:
                print(f"Retrieved {len(bars['Bars'])} bars for {sym}")
                # Show last 3 bars
                for bar in bars['Bars'][-3:]:
                    # Safely convert all numeric values
                    try:
                        open_price = float(bar.get('Open', 0))
                        high_price = float(bar.get('High', 0))
                        low_price = float(bar.get('Low', 0))
                        close_price = float(bar.get('Close', 0))
                        total_vol = int(bar.get('TotalVolume', 0)) if bar.get('TotalVolume') else 0
                    except (ValueError, TypeError):
                        open_price = high_price = low_price = close_price = 0.0
                        total_vol = 0

                    print(f"   {bar['TimeStamp']}: O=${open_price:.2f} H=${high_price:.2f} "
                          f"L=${low_price:.2f} C=${close_price:.2f} V={total_vol:,}")
            print()

        # Test 3: Get option expirations
        if test in ['all', 'options']:
            print("Test: Get Option Expirations (first 5)")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols
            expirations = client.get_option_expirations(sym)
            for exp in expirations[:5]:
                print(f"   {exp}")

            # Also get strikes for first expiration
            if expirations:
                print(f"\n   Strikes for {expirations[0]} (first 10):")
                exp_str = expirations[0].strftime('%m-%d-%Y')
                strikes = client.get_option_strikes(sym, expiration=exp_str)
                for strike in strikes[:10]:
                    print(f"      ${strike}")
            print()

        # Test 4: Symbol search
        if test in ['all', 'search']:
            print(f"Test: Search Symbols for '{query}'")
            print("-" * 60)
            results = client.search_symbols(query)
            for s in results[:5]:
                print(f"   {s.get('Symbol', 'N/A')}: {s.get('Description', 'N/A')}")
            print()

        # Test 5: Market hours
        if test in ['all', 'market-hours']:
            print("Test: Market Hours & Status")
            print("-" * 60)

            # Get comprehensive market status
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
        if test in ['depth']:
            print("Test: Market Depth (Level 2)")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols
            depth = client.get_market_depth_quotes(sym)
            if 'MarketDepthQuotes' in depth:
                print(f"   Market depth data retrieved for {sym}")
                print(f"   Available fields: {list(depth.keys())}")
            print()

        if test == 'all':
            print("="*60)
            print("✅ All tests completed successfully!")
            print("="*60)
        else:
            print(f"✅ Test '{test}' completed successfully!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == '__main__':
    main()
