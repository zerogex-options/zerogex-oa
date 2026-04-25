"""
VIX Ingester

Streams $VIX.X 5-minute bars from TradeStation's /stream/barcharts endpoint
and upserts them into the `vix_bars` table.  The /api/market/vix endpoint
reads from that table instead of calling TradeStation directly, so the
endpoint stays fast and a single running ingestion process keeps the VIX
window fresh.

Design notes:
- 5-minute bars are used because the endpoint's level + momentum scores
  were tuned against 5-minute bars (see volatility_gauge.py).
- The ingester opens a persistent HTTP streaming connection and reads
  bar payloads as they arrive.  TradeStation's barchart stream sends
  partial-bar updates and a final payload at bar close, so intraday
  state always matches what a polling client would observe.
- On first connect we request VIX_INITIAL_BARSBACK bars to seed the
  table.  On reconnect we ask for only VIX_POLL_BARSBACK so a short
  outage still replays the bars we might have missed.
- Rows older than VIX_BARS_RETENTION_DAYS are pruned periodically to
  keep the table bounded.
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

VIX_SYMBOL = "$VIX.X"
VIX_BAR_INTERVAL = 5
VIX_BAR_UNIT = "Minute"
# Seed enough bars on first connect to cover ~2 trading sessions (≈156 bars).
VIX_INITIAL_BARSBACK = int(os.getenv("VIX_INITIAL_BARSBACK", "160"))
# On reconnect, request just enough history to cover short outages.
VIX_POLL_BARSBACK = int(os.getenv("VIX_POLL_BARSBACK", "3"))
VIX_BARS_RETENTION_DAYS = int(os.getenv("VIX_BARS_RETENTION_DAYS", "7"))

# How long the stream reader waits for the next event before timing out.
# Shared env var with the main stream manager so operators tune one knob.
_STREAM_READ_TIMEOUT = int(os.getenv("TS_STREAM_READ_TIMEOUT", "300"))

# Session template for the bar stream; "Default" matches the prior REST poll.
_SESSION_TEMPLATE = "Default"

# Backoff between reconnect attempts when the stream drops.
_RECONNECT_BACKOFF_SEC = 2

# Prune at startup and then roughly every this many bar upserts.
_PRUNE_EVERY_N_UPSERTS = 120


def _parse_bar(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a raw TradeStation bar into our DB row shape, or None if invalid."""
    ts = safe_datetime(raw.get("TimeStamp"), field_name="TimeStamp")
    if ts is None:
        return None
    close = safe_float(raw.get("Close"), field_name="Close")
    if close is None:
        return None
    return {
        "timestamp": ts,
        "open": safe_float(raw.get("Open"), field_name="Open"),
        "high": safe_float(raw.get("High"), field_name="High"),
        "low": safe_float(raw.get("Low"), field_name="Low"),
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


class VIXIngester:
    """Streams VIX 5-min bars and persists them to `vix_bars`."""

    def __init__(self, client: TradeStationClient):
        self.client = client
        self.running = False
        self._seeded = False
        self._upserts_since_prune = 0
        self._current_response: Optional[_requests.Response] = None
        self._response_lock = threading.Lock()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("VIX ingester received signal %s, shutting down...", signum)
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
        """Upsert a list of VIX bars. Returns the number of rows written."""
        if not bars:
            return 0
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    """
                    INSERT INTO vix_bars (timestamp, open, high, low, close)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        updated_at = NOW()
                    """,
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
            logger.error("VIX bar upsert failed: %s", e, exc_info=True)
            return 0

    def _prune_old_bars(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=VIX_BARS_RETENTION_DAYS)
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM vix_bars WHERE timestamp < %s",
                    (cutoff,),
                )
                conn.commit()
        except Exception as e:
            logger.warning("VIX bar prune failed: %s", e)

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
                "VIX stream reported auth error payload; refreshing token and reconnecting"
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
            logger.info("VIX cache seeded with %d bars", written)
            self._prune_old_bars()
            self._upserts_since_prune = 0
        else:
            logger.debug("VIX bars upserted: %d", written)
            self._upserts_since_prune += written
            if self._upserts_since_prune >= _PRUNE_EVERY_N_UPSERTS:
                self._prune_old_bars()
                self._upserts_since_prune = 0

    def _read_stream(self) -> None:
        """Open one stream connection and read bar events until it ends."""
        barsback = VIX_POLL_BARSBACK if self._seeded else VIX_INITIAL_BARSBACK
        url = f"{self.client.base_url}/marketdata/stream/barcharts/{VIX_SYMBOL}"
        headers = self.client.auth.get_headers()
        params = {
            "interval": str(VIX_BAR_INTERVAL),
            "unit": VIX_BAR_UNIT,
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
                logger.warning("VIX stream: 401 auth failure, forcing token refresh and retrying")
                self.client.auth.force_refresh_access_token()
                return
            response.raise_for_status()
        except Exception:
            response.close()
            raise

        logger.info(
            "VIX stream: connected (HTTP %s, barsback=%d)",
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
                            "VIX stream: %d heartbeats, %d data lines so far",
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
                        "VIX stream: JSON decode failed, skipping line: %s",
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
            "VIX INGESTER — streaming %s %d-%s bars", VIX_SYMBOL, VIX_BAR_INTERVAL, VIX_BAR_UNIT
        )
        logger.info("=" * 80)

        self.running = True
        try:
            while self.running:
                if not is_engine_run_window():
                    sleep_for = seconds_until_engine_run_window()
                    logger.info(
                        "VIX ingester paused outside run window; sleeping %ss",
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
                            "VIX stream disconnected (%s), reconnecting in %ds...",
                            e,
                            _RECONNECT_BACKOFF_SEC,
                        )
                        # Responsive sleep so SIGTERM interrupts the backoff.
                        slept = 0
                        while slept < _RECONNECT_BACKOFF_SEC and self.running:
                            time.sleep(1)
                            slept += 1

        except Exception as e:
            logger.error("Fatal error in VIX ingester: %s", e, exc_info=True)
            sys.exit(1)
        finally:
            close_connection_pool()
            logger.info("VIX ingester stopped")


def main() -> None:
    """Entry point used when spawned as a child process from main_engine."""
    from dotenv import load_dotenv

    load_dotenv()

    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID", ""),
        os.getenv("TRADESTATION_CLIENT_SECRET", ""),
        os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
        sandbox=os.getenv("TRADESTATION_USE_SANDBOX", "false").lower() == "true",
    )

    # Wire up the API-calls DB writer so this child process also contributes
    # its API usage to the tradestation_api_calls table.
    try:
        from src.ingestion.api_call_tracker import attach_db_writer

        attach_db_writer(client)
    except Exception as e:
        logger.warning("Failed to attach API-call DB writer: %s", e)

    ingester = VIXIngester(client)
    ingester.run()


if __name__ == "__main__":
    main()
