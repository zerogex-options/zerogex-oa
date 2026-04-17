"""
VIX Ingester

Polls $VIX.X 5-minute bars from TradeStation on a timer and upserts them
into the `vix_bars` table.  The /api/market/vix endpoint reads from that
table instead of calling TradeStation directly, so the endpoint stays
fast and a single running ingestion process keeps the VIX window fresh.

Design notes:
- 5-minute bars are used because the endpoint's level + momentum scores
  were tuned against 5-minute bars (see volatility_gauge.py).
- The ingester polls at the same cadence as the main stream loop
  (MARKET_HOURS_POLL_INTERVAL during regular hours). 5-min bars don't
  need sub-second latency so polling is simpler than a persistent stream.
- Each poll fetches the latest ~3 bars so a partial-bar update and any
  bar we missed on the previous tick both land.
- Rows older than VIX_BARS_RETENTION_DAYS are pruned after each write
  to keep the table bounded.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytz

from src.ingestion.tradestation_client import TradeStationClient
from src.database import db_connection, close_connection_pool
from src.utils import get_logger
from src.validation import (
    safe_float,
    safe_datetime,
    is_engine_run_window,
    seconds_until_engine_run_window,
)
from src.config import (
    MARKET_HOURS_POLL_INTERVAL,
    EXTENDED_HOURS_POLL_INTERVAL,
    CLOSED_HOURS_POLL_INTERVAL,
)


logger = get_logger(__name__)

ET = pytz.timezone("US/Eastern")

VIX_SYMBOL = "$VIX.X"
VIX_BAR_INTERVAL = 5
VIX_BAR_UNIT = "Minute"
# Seed enough bars on first run to cover ~2 trading sessions (≈156 bars).
VIX_INITIAL_BARSBACK = int(os.getenv("VIX_INITIAL_BARSBACK", "160"))
# Incremental polls overlap the last few bars so partial-bar updates land.
VIX_POLL_BARSBACK = int(os.getenv("VIX_POLL_BARSBACK", "3"))
VIX_BARS_RETENTION_DAYS = int(os.getenv("VIX_BARS_RETENTION_DAYS", "7"))


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


class VIXIngester:
    """Polls VIX 5-min bars and persists them to `vix_bars`."""

    def __init__(self, client: TradeStationClient):
        self.client = client
        self.running = False
        self._seeded = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("VIX ingester received signal %s, shutting down...", signum)
        self.running = False

    def _fetch_bars(self, barsback: int) -> List[Dict[str, Any]]:
        try:
            result = self.client.get_bars(
                symbol=VIX_SYMBOL,
                interval=VIX_BAR_INTERVAL,
                unit=VIX_BAR_UNIT,
                barsback=barsback,
                sessiontemplate="Default",
                warn_if_closed=False,
            )
        except Exception as e:
            logger.warning("VIX bar fetch failed: %s", e)
            return []
        raw_bars = result.get("Bars", []) if isinstance(result, dict) else []
        parsed = [b for b in (_parse_bar(r) for r in raw_bars) if b is not None]
        return parsed

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

    def _poll_once(self) -> None:
        if not self._seeded:
            bars = self._fetch_bars(VIX_INITIAL_BARSBACK)
            written = self._upsert_bars(bars)
            if written > 0:
                self._seeded = True
                logger.info("VIX cache seeded with %d bars", written)
                self._prune_old_bars()
            else:
                logger.warning("VIX seeding returned no bars, will retry next cycle")
            return

        bars = self._fetch_bars(VIX_POLL_BARSBACK)
        written = self._upsert_bars(bars)
        if written > 0:
            logger.debug("VIX bars upserted: %d", written)
        # Prune sparingly so we don't hammer the DB.
        if datetime.now(timezone.utc).minute % 30 == 0:
            self._prune_old_bars()

    def run(self) -> None:
        logger.info("=" * 80)
        logger.info("VIX INGESTER — polling %s bars every poll interval", VIX_SYMBOL)
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
                    time.sleep(max(1, sleep_for))
                    continue

                try:
                    self._poll_once()
                except Exception as e:
                    logger.error("VIX poll iteration error: %s", e, exc_info=True)

                # Reuse the main engine's poll cadence so the VIX window
                # stays as fresh as the underlying streams.
                if self.client.is_market_open(check_extended=False):
                    wait = MARKET_HOURS_POLL_INTERVAL
                elif self.client.is_market_open(check_extended=True):
                    wait = EXTENDED_HOURS_POLL_INTERVAL
                else:
                    wait = CLOSED_HOURS_POLL_INTERVAL

                # During regular/extended hours the bar only advances every 5
                # min, so there's no value in polling faster than ~30s.
                wait = max(wait, 30)

                # Sleep in short chunks so shutdown signals are responsive.
                slept = 0
                while slept < wait and self.running:
                    chunk = min(1, wait - slept)
                    time.sleep(chunk)
                    slept += chunk

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
