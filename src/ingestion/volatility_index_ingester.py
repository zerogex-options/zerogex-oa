"""
Volatility-Index Ingester

Generic streaming ingester that pulls 5-minute bars for a CBOE-style cash
volatility index from TradeStation's ``/stream/barcharts`` endpoint and
upserts them into a per-ticker bars table (``vix_bars``, ``vxn_bars``, …).
The ``/api/market/vix`` and ``/api/market/volatility`` endpoints read from
those tables instead of calling TradeStation directly, so the endpoints
stay fast and a single long-running ingester process per index keeps the
window fresh.

This module owns the streaming + persistence + retention loop; the
per-ticker entry points (see ``vix_ingester.py`` and ``vxn_ingester.py``)
are thin wrappers that instantiate :class:`VolatilityIndexIngester` with
ticker-specific parameters.

Design notes:
- 5-minute bars are used because the endpoint's level + momentum scores
  were tuned against 5-minute bars (see volatility_gauge.py).
- The ingester opens a persistent HTTP streaming connection and reads
  bar payloads as they arrive.  TradeStation's barchart stream sends
  partial-bar updates and a final payload at bar close, so intraday
  state always matches what a polling client would observe.
- On first connect we request ``initial_barsback`` bars to seed the
  table.  On reconnect we ask for only ``poll_barsback`` so a short
  outage still replays the bars we might have missed.
- Rows older than ``retention_days`` are pruned periodically to keep the
  table bounded.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytz
import requests as _requests

from src.ingestion.tradestation_client import TradeStationClient
from src.database import db_connection, close_connection_pool
from src.config import _getenv_int, _getenv_bool
from src.utils import get_logger
from src.validation import (
    safe_float,
    safe_datetime,
    is_engine_run_window,
    seconds_until_engine_run_window,
)
from src.config import API_REQUEST_TIMEOUT

logger = get_logger(__name__)

ET = pytz.timezone("US/Eastern")

VOLATILITY_BAR_INTERVAL = 5
VOLATILITY_BAR_UNIT = "Minute"

# How long the stream reader waits for the next event before timing out.
# Shared env var with the main stream manager so operators tune one knob.
_STREAM_READ_TIMEOUT = _getenv_int("TS_STREAM_READ_TIMEOUT", 300)

# Session template for the bar stream; "Default" matches the prior REST poll.
_SESSION_TEMPLATE = "Default"

# Backoff between reconnect attempts when the stream drops.
_RECONNECT_BACKOFF_SEC = 2

# Prune at startup and then roughly every this many bar upserts.
_PRUNE_EVERY_N_UPSERTS = 120

# Allowed bars-table names — guards the SQL string interpolation in
# _upsert_bars / _prune_old_bars against ever using an attacker-controlled
# identifier.  Add new tables here when a new volatility index is wired up.
_ALLOWED_TABLES = frozenset({"vix_bars", "vxn_bars"})


def _parse_bar(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a raw TradeStation bar into our DB row shape, or None if invalid."""
    ts = safe_datetime(raw.get("TimeStamp"), field_name="TimeStamp")  # type: ignore[arg-type]
    if ts is None:
        return None
    close = safe_float(raw.get("Close"), field_name="Close", default=None)
    if close is None:
        return None
    return {
        "timestamp": ts,
        "open": safe_float(raw.get("Open"), field_name="Open", default=None),
        "high": safe_float(raw.get("High"), field_name="High", default=None),
        "low": safe_float(raw.get("Low"), field_name="Low", default=None),
        "close": close,
    }


def _is_auth_error_payload(payload: Dict[str, Any]) -> bool:
    """Best-effort detection of auth-expiry messages inside stream payloads."""
    fields = (
        str(payload.get("Error", "")),
        str(payload.get("Message", "")),
        str(payload.get("Description", "")),
        str(payload.get("Code", "")),
    )
    text = " ".join(fields).lower()
    return any(token in text for token in ("unauthorized", "401", "token", "forbidden"))


class VolatilityIndexIngester:
    """Streams an index's 5-min bars and persists them to ``<ticker>_bars``.

    Parameters
    ----------
    client:
        Authenticated TradeStation client.
    ticker:
        Short display name (e.g. ``"VIX"``, ``"VXN"``) — used only for log
        lines so an operator can tell the two child processes apart.
    symbol:
        TradeStation cash-index symbol (e.g. ``"$VIX.X"``, ``"$VXN.X"``).
    table_name:
        Destination upsert table.  Must be in :data:`_ALLOWED_TABLES`; the
        constructor refuses any other value so the SQL identifier
        interpolation in :meth:`_upsert_bars` / :meth:`_prune_old_bars`
        can never be steered to an unintended table.
    initial_barsback:
        Bars requested on the very first stream connect (used to seed the
        rolling window).
    poll_barsback:
        Bars requested on each reconnect after the initial seed (just
        enough to replay anything missed during a short outage).
    retention_days:
        Bars older than this are pruned periodically.
    """

    def __init__(
        self,
        client: TradeStationClient,
        *,
        ticker: str,
        symbol: str,
        table_name: str,
        initial_barsback: int,
        poll_barsback: int,
        retention_days: int,
    ):
        if table_name not in _ALLOWED_TABLES:
            raise ValueError(
                f"table_name {table_name!r} not in allowlist {_ALLOWED_TABLES!r}; "
                "add it explicitly to keep SQL identifier interpolation safe."
            )
        self.client = client
        self.ticker = ticker
        self.symbol = symbol
        self.table_name = table_name
        self.initial_barsback = initial_barsback
        self.poll_barsback = poll_barsback
        self.retention_days = retention_days
        self.running = False
        self._seeded = False
        self._upserts_since_prune = 0
        self._current_response: Optional[_requests.Response] = None
        self._response_lock = threading.Lock()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("%s ingester received signal %s, shutting down...", self.ticker, signum)
        self.running = False
        # Close any in-flight stream so iter_lines returns promptly.
        with self._response_lock:
            if self._current_response is not None:
                try:
                    self._current_response.close()
                except Exception:
                    pass

    # -- DB helpers --------------------------------------------------------

    def _upsert_bars(self, bars: List[Dict[str, Any]]) -> int:
        """Upsert a list of bars. Returns the number of rows written."""
        if not bars:
            return 0
        query = (
            f"INSERT INTO {self.table_name} (timestamp, open, high, low, close) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (timestamp) DO UPDATE SET "
            "    open = EXCLUDED.open, "
            "    high = EXCLUDED.high, "
            "    low = EXCLUDED.low, "
            "    close = EXCLUDED.close, "
            "    updated_at = NOW()"
        )
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    query,
                    [
                        (
                            b["timestamp"],
                            b["open"],
                            b["high"],
                            b["low"],
                            b["close"],
                        )
                        for b in bars
                    ],
                )
                conn.commit()
            return len(bars)
        except Exception as e:
            logger.error("%s bar upsert failed: %s", self.ticker, e, exc_info=True)
            return 0

    def _prune_old_bars(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"DELETE FROM {self.table_name} WHERE timestamp < %s",
                    (cutoff,),
                )
                conn.commit()
        except Exception as e:
            logger.warning("%s bar prune failed: %s", self.ticker, e)

    # -- stream reader -----------------------------------------------------

    def _extract_bars(self, payload: Any) -> List[Dict[str, Any]]:
        """Normalize the various bar payload shapes TradeStation emits."""
        if not isinstance(payload, dict):
            return []
        if "Bars" in payload and isinstance(payload["Bars"], list):
            return payload["Bars"]
        if "Bar" in payload and isinstance(payload["Bar"], dict):
            return [payload["Bar"]]
        if "TimeStamp" in payload:
            return [payload]
        return []

    def _handle_payload(self, payload: Any) -> None:
        if isinstance(payload, dict) and _is_auth_error_payload(payload):
            logger.warning(
                "%s stream reported auth error payload; refreshing token and reconnecting",
                self.ticker,
            )
            self.client.auth.force_refresh_access_token()
            # Closing the response forces iter_lines to exit so we reconnect.
            with self._response_lock:
                if self._current_response is not None:
                    try:
                        self._current_response.close()
                    except Exception:
                        pass
            return

        raw_bars = self._extract_bars(payload)
        if not raw_bars:
            return

        parsed = [b for b in (_parse_bar(r) for r in raw_bars) if b is not None]
        written = self._upsert_bars(parsed)
        if written <= 0:
            return

        if not self._seeded:
            self._seeded = True
            logger.info("%s cache seeded with %d bars", self.ticker, written)
            self._prune_old_bars()
            self._upserts_since_prune = 0
        else:
            logger.debug("%s bars upserted: %d", self.ticker, written)
            self._upserts_since_prune += written
            if self._upserts_since_prune >= _PRUNE_EVERY_N_UPSERTS:
                self._prune_old_bars()
                self._upserts_since_prune = 0

    def _read_stream(self) -> None:
        """Open one stream connection and read bar events until it ends."""
        barsback = self.poll_barsback if self._seeded else self.initial_barsback
        url = f"{self.client.base_url}/marketdata/stream/barcharts/{self.symbol}"
        headers = self.client.auth.get_headers()
        params = {
            "interval": str(VOLATILITY_BAR_INTERVAL),
            "unit": VOLATILITY_BAR_UNIT,
            "barsback": str(barsback),
            "sessiontemplate": _SESSION_TEMPLATE,
        }

        response = _requests.get(
            url,
            headers=headers,
            params=params,
            stream=True,
            timeout=(API_REQUEST_TIMEOUT, _STREAM_READ_TIMEOUT),
        )

        try:
            if response.status_code == 401:
                response.close()
                logger.warning(
                    "%s stream: 401 auth failure, forcing token refresh and retrying",
                    self.ticker,
                )
                self.client.auth.force_refresh_access_token()
                return
            response.raise_for_status()
        except Exception:
            response.close()
            raise

        logger.info(
            "%s stream: connected (HTTP %s, barsback=%d)",
            self.ticker,
            response.status_code,
            barsback,
        )

        with self._response_lock:
            self._current_response = response

        heartbeat_count = 0
        data_line_count = 0

        try:
            for raw_line in response.iter_lines(decode_unicode=True):
                if not self.running:
                    break
                if not raw_line:
                    continue
                line = (
                    raw_line.strip()
                    if isinstance(raw_line, str)
                    else raw_line.decode("utf-8", errors="ignore").strip()
                )
                if not line:
                    continue
                if line in ("[DONE]", "heartbeat"):
                    heartbeat_count += 1
                    if heartbeat_count % 50 == 0:
                        logger.debug(
                            "%s stream: %d heartbeats, %d data lines so far",
                            self.ticker,
                            heartbeat_count,
                            data_line_count,
                        )
                    continue
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line:
                    continue

                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(
                        "%s stream: JSON decode failed, skipping line: %s",
                        self.ticker,
                        line[:200],
                    )
                    continue

                data_line_count += 1
                self._handle_payload(payload)
        finally:
            with self._response_lock:
                self._current_response = None
            response.close()

    # -- run loop ----------------------------------------------------------

    def run(self) -> None:
        logger.info("=" * 80)
        logger.info(
            "%s INGESTER — streaming %s %d-%s bars",
            self.ticker,
            self.symbol,
            VOLATILITY_BAR_INTERVAL,
            VOLATILITY_BAR_UNIT,
        )
        logger.info("=" * 80)

        self.running = True
        try:
            while self.running:
                if not is_engine_run_window():
                    sleep_for = seconds_until_engine_run_window()
                    logger.info(
                        "%s ingester paused outside run window; sleeping %ss",
                        self.ticker,
                        sleep_for,
                    )
                    # Sleep in short chunks so shutdown signals stay responsive.
                    slept = 0
                    target = max(1, sleep_for)
                    while slept < target and self.running:
                        time.sleep(1)
                        slept += 1
                    continue

                try:
                    self._read_stream()
                except Exception as e:
                    if self.running:
                        logger.warning(
                            "%s stream disconnected (%s), reconnecting in %ds...",
                            self.ticker,
                            e,
                            _RECONNECT_BACKOFF_SEC,
                        )
                        # Responsive sleep so SIGTERM interrupts the backoff.
                        slept = 0
                        while slept < _RECONNECT_BACKOFF_SEC and self.running:
                            time.sleep(1)
                            slept += 1

        except Exception as e:
            logger.error("Fatal error in %s ingester: %s", self.ticker, e, exc_info=True)
            sys.exit(1)
        finally:
            close_connection_pool()
            logger.info("%s ingester stopped", self.ticker)


def run_ingester(
    *,
    ticker: str,
    symbol: str,
    table_name: str,
    initial_barsback: int,
    poll_barsback: int,
    retention_days: int,
) -> None:
    """Shared child-process entry point.

    Loads ``.env``, builds an authenticated TradeStation client, attaches
    the API-call DB writer, and runs the ingester until shutdown.  The
    per-ticker modules (``vix_ingester.py`` / ``vxn_ingester.py``) call
    this with their own config so every index follows the identical
    spawn → seed → stream → prune lifecycle.
    """
    from dotenv import load_dotenv

    load_dotenv()

    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID", ""),
        os.getenv("TRADESTATION_CLIENT_SECRET", ""),
        os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
        sandbox=_getenv_bool("TRADESTATION_USE_SANDBOX", False),
    )

    # Wire up the API-calls DB writer so this child process also contributes
    # its API usage to the tradestation_api_calls table.
    try:
        from src.ingestion.api_call_tracker import attach_db_writer

        attach_db_writer(client)
    except Exception as e:
        logger.warning("Failed to attach API-call DB writer: %s", e)

    ingester = VolatilityIndexIngester(
        client,
        ticker=ticker,
        symbol=symbol,
        table_name=table_name,
        initial_barsback=initial_barsback,
        poll_barsback=poll_barsback,
        retention_days=retention_days,
    )
    ingester.run()
