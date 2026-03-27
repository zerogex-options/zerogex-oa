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
from requests import Response
from threading import Lock

from src.ingestion.tradestation_auth import TradeStationAuth
from src.utils import get_logger
from src.validation import safe_float, safe_int
from src.symbols import parse_underlyings, resolve_option_root
from src.config import (
    API_REQUEST_TIMEOUT,
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY,
    API_RETRY_BACKOFF,
)

logger = get_logger(__name__)

# Eastern Time timezone
ET = pytz.timezone("US/Eastern")
STREAM_READ_TIMEOUT_SECONDS = int(os.getenv("TS_STREAM_READ_TIMEOUT", "300"))
STREAM_REUSE_CONNECTIONS = os.getenv("TS_STREAM_REUSE_CONNECTIONS", "false").lower() == "true"
STREAM_REUSE_QUOTES = os.getenv("TS_STREAM_REUSE_QUOTES", "false").lower() == "true"


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
        self._stream_lock = Lock()
        self._stream_state: Dict[str, Dict[str, Any]] = {}

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
            response = self._build_request_response(method, url, headers, params, data)

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

            # Handle expired/invalid token - force refresh and retry once
            if response.status_code == 401:
                if retry_count < API_RETRY_ATTEMPTS - 1:
                    logger.warning("TradeStation returned 401; forcing token refresh and retrying")
                    self.auth.force_refresh_access_token()
                    return self._request(method, endpoint, params, data, retry_count + 1)
                logger.error("TradeStation returned 401 after retries")
                response.raise_for_status()

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

            # Handle quota exceeded (403) - do not retry, log actionable guidance
            if response.status_code == 403:
                try:
                    error_data = response.json()
                    if error_data.get("Message", "").lower() == "quota exceeded":
                        logger.error(
                            "TradeStation API quota exceeded (403). Your account has hit its daily "
                            "API call limit. Reduce INGEST_STRIKE_COUNT (e.g. 5) or INGEST_EXPIRATIONS "
                            "to lower call volume. Quota resets daily."
                        )
                        response.raise_for_status()
                except Exception:
                    pass
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

    def _build_request_response(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        params: Optional[Dict],
        data: Optional[Dict]
    ) -> Response:
        """Build and execute a standard JSON API request."""
        return requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=data,
            timeout=API_REQUEST_TIMEOUT
        )

    def _request_stream_snapshot(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """
        Read a single JSON payload from a TradeStation stream endpoint.

        TradeStation stream endpoints may keep the connection open. For ingestion
        compatibility we consume one payload and return immediately.
        """
        stream_key = self._build_stream_key(endpoint, params)

        try:
            state = self._get_or_open_stream(stream_key, endpoint, params)
            line = self._next_stream_json_line(stream_key, state)
            if line is None:
                return {}
            return json.loads(line)
        except StopIteration:
            # Stream endpoints may rotate/terminate connections. Treat this as a
            # normal reconnect event instead of warning-level noise.
            self._close_stream(stream_key)
            if retry_count < API_RETRY_ATTEMPTS - 1:
                return self._request_stream_snapshot(endpoint, params, retry_count + 1)
            logger.debug("Stream ended repeatedly; returning empty snapshot")
            return {}
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            self._close_stream(stream_key)
            if retry_count < API_RETRY_ATTEMPTS - 1:
                retry_delay = API_RETRY_DELAY * (API_RETRY_BACKOFF ** retry_count)
                logger.warning(f"Stream request failed: {e}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                return self._request_stream_snapshot(endpoint, params, retry_count + 1)
            logger.error(f"Stream request failed after {API_RETRY_ATTEMPTS} attempts: {e}")
            raise
        finally:
            # Snapshot mode should not hold the stream open by default.
            # Reusing a stream for one-line snapshots can block until the *next*
            # event on subsequent reads, which introduces write lag.
            # Quote streams are reused by default to avoid reconnect churn/rate limits.
            reuse_stream = STREAM_REUSE_CONNECTIONS or (
                STREAM_REUSE_QUOTES and endpoint.startswith("marketdata/stream/quotes/")
            )
            if not reuse_stream:
                self._close_stream(stream_key)

    def _build_stream_key(self, endpoint: str, params: Optional[Dict]) -> str:
        params_key = json.dumps(params or {}, sort_keys=True)
        return f"{endpoint}?{params_key}"

    def _get_or_open_stream(self, stream_key: str, endpoint: str, params: Optional[Dict]) -> Dict[str, Any]:
        with self._stream_lock:
            existing = self._stream_state.get(stream_key)
            if existing:
                return existing

            url = f"{self.base_url}/{endpoint}"
            headers = self.auth.get_headers()
            response = requests.get(
                url,
                headers=headers,
                params=params,
                stream=True,
                timeout=(API_REQUEST_TIMEOUT, STREAM_READ_TIMEOUT_SECONDS),
            )

            if response.status_code == 401:
                response.close()
                self.auth.force_refresh_access_token()
                headers = self.auth.get_headers()
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    stream=True,
                    timeout=(API_REQUEST_TIMEOUT, STREAM_READ_TIMEOUT_SECONDS),
                )

            if response.status_code not in [200, 201]:
                logger.error(f"Stream request failed: {response.status_code} [{endpoint}]")
                logger.error(f"Response: {response.text}")
                response.raise_for_status()

            state = {
                "response": response,
                "iterator": response.iter_lines(decode_unicode=True),
            }
            self._stream_state[stream_key] = state
            return state

    def _next_stream_json_line(self, stream_key: str, state: Dict[str, Any]) -> Optional[str]:
        iterator = state["iterator"]
        for raw_line in iterator:
            if not raw_line:
                continue
            if isinstance(raw_line, bytes):
                line = raw_line.decode("utf-8", errors="ignore").strip()
            else:
                line = raw_line.strip()
            if not line:
                continue
            if line.startswith("data:"):
                line = line[5:].strip()
            if line in {"[DONE]", "heartbeat"}:
                continue
            return line

        # Iterator exhausted -> close and signal caller to retry.
        self._close_stream(stream_key)
        raise StopIteration("Stream ended")

    def _close_stream(self, stream_key: str):
        with self._stream_lock:
            state = self._stream_state.pop(stream_key, None)
            if state and state.get("response") is not None:
                try:
                    state["response"].close()
                except Exception:
                    pass

    def close_all_streams(self):
        """Close all open stream HTTP connections."""
        with self._stream_lock:
            keys = list(self._stream_state.keys())
        for key in keys:
            self._close_stream(key)

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

        result = self._request_stream_snapshot(f"marketdata/stream/barcharts/{symbol}", params=params)

        if isinstance(result, dict) and "Bars" in result:
            return result
        if isinstance(result, dict) and "Bar" in result and isinstance(result["Bar"], dict):
            return {"Bars": [result["Bar"]]}
        if isinstance(result, dict) and "TimeStamp" in result:
            return {"Bars": [result]}
        return {"Bars": []}

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

    def get_stream_quotes(self, symbols: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get quote updates from TradeStation streaming quotes endpoint.

        The caller can consume this exactly like get_quote/get_option_quotes
        because this method normalizes one read into {"Quotes": [...]}.
        """
        if isinstance(symbols, list):
            symbols = ",".join(symbols)

        logger.info(f"Streaming quotes for {len(symbols.split(','))} symbols")
        result = self._request_stream_snapshot(f"marketdata/stream/quotes/{symbols}")

        if isinstance(result, dict) and "Quotes" in result:
            quotes = result.get("Quotes")
            if isinstance(quotes, list):
                return {"Quotes": quotes}
            if isinstance(quotes, dict):
                return {"Quotes": [quotes]}
            return {"Quotes": []}

        # Defensive normalization in case stream response comes back as a single quote object
        if isinstance(result, dict) and "Symbol" in result:
            return {"Quotes": [result]}

        if isinstance(result, list):
            # Defensive handling if stream emits line-delimited quote objects
            return {"Quotes": [q for q in result if isinstance(q, dict)]}

        return {"Quotes": []}

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
        option_root = resolve_option_root(underlying)

        # Format strike with proper precision
        if strike == int(strike):
            strike_str = str(int(strike))
        else:
            strike_str = f"{strike:.2f}"

        symbol = f"{option_root} {exp_str}{option_type.upper()}{strike_str}"
        if option_root != underlying:
            logger.debug(f"Option root override: {underlying} -> {option_root}")
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
  python -m src.ingestion.tradestation_client --test options-expirations --symbol '$SPX.X'
  python -m src.ingestion.tradestation_client --test options-strikes --symbol '$SPX.X' --expiration 03-20-2026
  python -m src.ingestion.tradestation_client --test option-quote --option-symbol 'SPXW 260320C6630'
  python -m src.ingestion.tradestation_client --test search --query Apple
  python -m src.ingestion.tradestation_client --test market-hours

  # Enable debug logging
  python -m src.ingestion.tradestation_client --debug
        """
    )

    parser.add_argument("--test", 
                       choices=["all", "quote", "bars", "stream-bars", "options", "options-expirations", "options-strikes", "option-quote", "search", "market-hours", "depth"],
                       help="Which test to run (default: all, env: TS_TEST)")

    parser.add_argument("--symbol", type=str,
                       help="Symbol(s) to test with, comma-separated (default: SPY, env: TS_SYMBOL)")

    parser.add_argument("--option-symbol", type=str,
                       help="Direct option contract symbol for option-quote test (e.g., 'SPXW 260320C6630')")

    parser.add_argument("--expiration", type=str,
                       help="Expiration for options-strikes test (MM-DD-YYYY)")

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
    expiration = args.expiration if args.expiration else None
    option_symbol_arg = args.option_symbol if args.option_symbol else None
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
        # Convert comma-separated symbols to list and resolve aliases
        resolved_symbols = parse_underlyings(symbol)
        if not resolved_symbols:
            raise ValueError("No valid symbols provided")

        symbols = resolved_symbols if len(resolved_symbols) > 1 else resolved_symbols[0]

        if symbol.strip().upper() != ",".join(resolved_symbols):
            print(f"Resolved symbols: {symbol} -> {', '.join(resolved_symbols)}")

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

        # Test 3a: Option expirations
        if test in ["all", "options", "options-expirations"]:
            print("Test: Get Option Expirations (first 5)")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols
            expirations = client.get_option_expirations(sym)
            for exp in expirations[:5]:
                print(f"   {exp}")
            print()

        # Test 3b: Option strikes
        if test in ["all", "options", "options-strikes"]:
            print("Test: Get Option Strikes")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols

            if expiration:
                exp_str = expiration
            else:
                expirations = client.get_option_expirations(sym)
                exp_str = expirations[0].strftime("%m-%d-%Y") if expirations else None

            if exp_str:
                print(f"   Expiration: {exp_str}")
                strikes = client.get_option_strikes(sym, expiration=exp_str)
                print("   First 10 strikes:")
                for strike in strikes[:10]:
                    print(f"      ${strike}")
            else:
                print("   ⚠️  No expirations available to fetch strikes")
            print()

        # Test 3c: Direct option quote (or sampled from first expiration/strikes)
        if test in ["all", "options", "option-quote"]:
            print("Test: Option Quote")
            print("-" * 60)
            sym = symbols[0] if isinstance(symbols, list) else symbols

            if option_symbol_arg:
                sample_option = option_symbol_arg
            else:
                expirations = client.get_option_expirations(sym)
                if not expirations:
                    print("   ⚠️  No expirations available for quote test")
                    sample_option = None
                else:
                    exp_str = expiration if expiration else expirations[0].strftime("%m-%d-%Y")
                    strikes = client.get_option_strikes(sym, expiration=exp_str)
                    if not strikes:
                        print("   ⚠️  No strikes available for quote test")
                        sample_option = None
                    else:
                        sample_strike = strikes[len(strikes) // 2]
                        exp_date = datetime.strptime(exp_str, "%m-%d-%Y").date()
                        sample_option = client.build_option_symbol(sym, exp_date, "C", sample_strike)

            if sample_option:
                print(f"   Quote test option: {sample_option}")
                option_quote = client.get_option_quotes([sample_option])

                if "Errors" in option_quote and option_quote["Errors"]:
                    err = option_quote["Errors"][0]
                    print(f"   ❌ Quote error: {err.get('Error', 'Unknown error')}")
                elif "Quotes" in option_quote and option_quote["Quotes"]:
                    q = option_quote["Quotes"][0]
                    print(f"   ✅ {q.get('Symbol', sample_option)}")
                    print(f"      Last: ${q.get('Last', 'N/A')}")
                    print(f"      Bid/Ask: ${q.get('Bid', 'N/A')} / ${q.get('Ask', 'N/A')}")
                    print(f"      Volume: {safe_int(q.get('Volume'), field_name='Volume'):,}")
                else:
                    print("   ⚠️  Quote request returned no data and no explicit error")
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
