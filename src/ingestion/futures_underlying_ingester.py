"""
Futures Underlying Ingester

Streams 1-minute bars for a CME equity-index future (``@ES``, ``@NQ``, …)
from TradeStation's ``/stream/barcharts`` endpoint and upserts them into
the ``futures_quotes`` table, keyed by the CASH INDEX the future stands in
for (``SPX``, ``NDX``, …).

``futures_quotes`` has two consumers, and between them they want the feed
running for the WHOLE CME session (Sun 18:00 → Fri 17:00 ET, minus the
daily 17:00-18:00 maintenance break — see
:func:`src.market_calendar.is_futures_ingest_window`):

* The **basis engine** (:mod:`src.jobs.futures_projection`), which makes
  ES / NQ first-class symbols by carrying SPX / NDX levels across to the
  futures price axis.  It measures the carry ratio by comparing an ES
  print against the SPX print at the same minute — which is only possible
  during the CASH session, precisely the window this ingester used to
  sleep through.
* The **overnight display swap**, the original consumer: during the
  overnight window only, ``/api/market/quote`` + ``/api/market/historical``
  attach the future's price under the index label.  That gate
  (:func:`src.market_calendar.should_display_future`) is unchanged and
  stays overnight-only — a wider ingest window does not widen the swap.

Design mirrors :mod:`src.ingestion.volatility_index_ingester` (the VIX/VXN
ingesters): one long-running child process per configured index, a
persistent HTTP streaming connection, partial-bar updates folded in as
they arrive, and periodic retention pruning so the table stays a bounded
rolling window (default 7 days).

Isolation guarantee: this writes ONLY to ``futures_quotes``.  It never
touches ``underlying_quotes`` or emits the ``zgx_quote_updates`` NOTIFY,
so GEX / greeks / signals / settlement — which all key on the cash-index
symbol — are completely unaffected.
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
from src.config import _getenv_int, _getenv_bool, _getenv_str, API_REQUEST_TIMEOUT
from src.market_calendar import is_futures_ingest_window
from src.symbols import resolve_index_future
from src.utils import get_logger
from src.validation import bucket_timestamp, safe_float, safe_datetime

logger = get_logger(__name__)

ET = pytz.timezone("US/Eastern")

FUTURES_BAR_INTERVAL = 1
FUTURES_BAR_UNIT = "Minute"

# How long the stream reader waits for the next event before timing out.
_STREAM_READ_TIMEOUT = _getenv_int("TS_STREAM_READ_TIMEOUT", 300)

# TradeStation session template for the futures stream.  "Default" returns
# the future's full electronic session; expose it as a knob in case a
# deployment needs a 24h template to surface every overnight bar.
_SESSION_TEMPLATE = _getenv_str("FUTURES_SESSION_TEMPLATE", "Default")

# Backoff between reconnect attempts when the stream drops.
_RECONNECT_BACKOFF_SEC = 2

# Poll cadence (seconds) while sleeping outside the futures display window.
_WINDOW_POLL_SEC = 30

# Prune at seed and then roughly every this many bar upserts.
_PRUNE_EVERY_N_UPSERTS = 120


def _safe_bigint(value: Any) -> int:
    """Coerce a TradeStation volume field to a non-negative int (0 on error)."""
    v = safe_float(value, field_name="volume", default=0.0)
    if v is None or v < 0:
        return 0
    return int(v)


def _parse_bar(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a raw TradeStation bar into a ``futures_quotes`` row, or None.

    Requires a full, positive OHLC set — the table enforces NOT NULL +
    positive-price + high>=low, so partial/degenerate bars are skipped
    rather than allowed to raise on insert.
    """
    ts = safe_datetime(raw.get("TimeStamp"), field_name="TimeStamp")  # type: ignore[arg-type]
    if ts is None:
        return None
    # Stamp the bar with its OWN minute, matching underlying_quotes (see
    # main_engine.py, same expression and the same reason). TradeStation
    # stamps a bar at its CLOSE, so storing it raw put futures_quotes one
    # minute ahead of the contemporaneous underlying_quotes row — and the
    # basis join then paired each futures bar with the NEXT minute's index
    # print, biasing the measured ratio by a minute of drift whenever the
    # market was trending. A bar stamped mid-interval floors unchanged.
    ts = bucket_timestamp(ts - timedelta(seconds=1), 60)
    o = safe_float(raw.get("Open"), field_name="Open", default=None)
    h = safe_float(raw.get("High"), field_name="High", default=None)
    low = safe_float(raw.get("Low"), field_name="Low", default=None)
    c = safe_float(raw.get("Close"), field_name="Close", default=None)
    if None in (o, h, low, c):
        return None
    if o <= 0 or h <= 0 or low <= 0 or c <= 0 or h < low:
        return None
    return {
        "timestamp": ts,
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "up_volume": _safe_bigint(raw.get("UpVolume")),
        "down_volume": _safe_bigint(raw.get("DownVolume")),
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


class FuturesUnderlyingIngester:
    """Streams a future's 1-min bars into ``futures_quotes`` for one index.

    Parameters
    ----------
    client:
        Authenticated TradeStation client.
    index_symbol:
        Cash-index alias the future stands in for (e.g. ``"SPX"``).  Rows
        are keyed by this so the API reads by the index symbol.
    future_symbol:
        TradeStation continuous-contract symbol streamed (e.g. ``"@ES"``).
    initial_barsback / poll_barsback:
        Bars requested on first connect (seed) vs each reconnect (replay).
    retention_days:
        Rows older than this (for this index) are pruned periodically.
    """

    def __init__(
        self,
        client: TradeStationClient,
        *,
        index_symbol: str,
        future_symbol: str,
        initial_barsback: int,
        poll_barsback: int,
        retention_days: int,
    ):
        self.client = client
        self.index_symbol = index_symbol.strip().upper()
        self.future_symbol = future_symbol.strip().upper()
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
        logger.info(
            "%s futures ingester received signal %s, shutting down...",
            self.index_symbol,
            signum,
        )
        self.running = False
        with self._response_lock:
            if self._current_response is not None:
                try:
                    self._current_response.close()
                except Exception:
                    pass

    # -- DB helpers --------------------------------------------------------

    def _upsert_bars(self, bars: List[Dict[str, Any]]) -> int:
        if not bars:
            return 0
        query = (
            "INSERT INTO futures_quotes "
            "(index_symbol, future_symbol, timestamp, open, high, low, close, "
            " up_volume, down_volume) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (index_symbol, timestamp) DO UPDATE SET "
            "    future_symbol = EXCLUDED.future_symbol, "
            "    open = EXCLUDED.open, "
            "    high = EXCLUDED.high, "
            "    low = EXCLUDED.low, "
            "    close = EXCLUDED.close, "
            "    up_volume = EXCLUDED.up_volume, "
            "    down_volume = EXCLUDED.down_volume, "
            "    updated_at = NOW()"
        )
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    query,
                    [
                        (
                            self.index_symbol,
                            self.future_symbol,
                            b["timestamp"],
                            b["open"],
                            b["high"],
                            b["low"],
                            b["close"],
                            b["up_volume"],
                            b["down_volume"],
                        )
                        for b in bars
                    ],
                )
                conn.commit()
            return len(bars)
        except Exception as e:
            logger.error("%s futures bar upsert failed: %s", self.index_symbol, e, exc_info=True)
            return 0

    def _prune_old_bars(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM futures_quotes WHERE index_symbol = %s AND timestamp < %s",
                    (self.index_symbol, cutoff),
                )
                conn.commit()
        except Exception as e:
            logger.warning("%s futures bar prune failed: %s", self.index_symbol, e)

    # -- stream reader -----------------------------------------------------

    def _extract_bars(self, payload: Any) -> List[Dict[str, Any]]:
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
                "%s futures stream reported auth error; refreshing token and reconnecting",
                self.index_symbol,
            )
            self.client.auth.force_refresh_access_token()
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
            logger.info(
                "%s futures cache seeded with %d bars (%s)",
                self.index_symbol,
                written,
                self.future_symbol,
            )
            self._prune_old_bars()
            self._upserts_since_prune = 0
        else:
            self._upserts_since_prune += written
            if self._upserts_since_prune >= _PRUNE_EVERY_N_UPSERTS:
                self._prune_old_bars()
                self._upserts_since_prune = 0

    def _read_stream(self) -> None:
        """Open one stream connection and read bar events until it ends or
        the futures display window closes (at which point we flip back to
        the index and sleep)."""
        barsback = self.poll_barsback if self._seeded else self.initial_barsback
        url = f"{self.client.base_url}/marketdata/stream/barcharts/{self.future_symbol}"
        headers = self.client.auth.get_headers()
        params = {
            "interval": str(FUTURES_BAR_INTERVAL),
            "unit": FUTURES_BAR_UNIT,
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
                    "%s futures stream: 401 auth failure, forcing token refresh",
                    self.index_symbol,
                )
                self.client.auth.force_refresh_access_token()
                return
            response.raise_for_status()
        except Exception:
            response.close()
            raise

        logger.info(
            "%s futures stream: connected (%s, HTTP %s, barsback=%d)",
            self.index_symbol,
            self.future_symbol,
            response.status_code,
            barsback,
        )

        with self._response_lock:
            self._current_response = response

        try:
            for raw_line in response.iter_lines(decode_unicode=True):
                if not self.running or not is_futures_ingest_window():
                    break
                if not raw_line:
                    continue
                line = (
                    raw_line.strip()
                    if isinstance(raw_line, str)
                    else raw_line.decode("utf-8", errors="ignore").strip()
                )
                if not line or line in ("[DONE]", "heartbeat"):
                    continue
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(
                        "%s futures stream: JSON decode failed, skipping: %s",
                        self.index_symbol,
                        line[:200],
                    )
                    continue
                self._handle_payload(payload)
        finally:
            with self._response_lock:
                self._current_response = None
            response.close()

    # -- run loop ----------------------------------------------------------

    def _sleep_until_window(self) -> None:
        """Sleep in short chunks until the futures ingest window opens."""
        slept = 0
        while self.running and not is_futures_ingest_window():
            time.sleep(1)
            slept += 1
            if slept % _WINDOW_POLL_SEC == 0:
                logger.debug(
                    "%s futures ingester idle (outside 18:00-09:30 ET window)",
                    self.index_symbol,
                )

    def _backoff(self, reason: str) -> None:
        """Sleep the reconnect backoff, in 1s slices so shutdown stays prompt."""
        if not self.running or not is_futures_ingest_window():
            return
        logger.warning(
            "%s futures %s, reconnecting in %ds...",
            self.index_symbol,
            reason,
            _RECONNECT_BACKOFF_SEC,
        )
        slept = 0
        while slept < _RECONNECT_BACKOFF_SEC and self.running:
            time.sleep(1)
            slept += 1

    def run(self) -> None:
        logger.info("=" * 80)
        logger.info(
            "%s FUTURES INGESTER — streaming %s %d-%s bars for the whole "
            "CME session (Sun 18:00 -> Fri 17:00 ET)",
            self.index_symbol,
            self.future_symbol,
            FUTURES_BAR_INTERVAL,
            FUTURES_BAR_UNIT,
        )
        logger.info("=" * 80)

        self.running = True
        try:
            while self.running:
                if not is_futures_ingest_window():
                    # CME is closed (weekend / maintenance break): nothing to
                    # pull, wait it out.
                    self._seeded = False  # re-seed the window on next open
                    self._sleep_until_window()
                    continue

                try:
                    self._read_stream()
                except Exception as e:
                    self._backoff(f"stream disconnected ({e})")
                else:
                    # A NORMAL return also needs the backoff. _read_stream
                    # returns without raising when the connection is refused
                    # for a reason that is not an exception here — a 401 that
                    # triggers a token refresh, or an account stream-slot cap
                    # that closes the connection immediately. Without a sleep
                    # on this path the loop reconnects flat out, which turns a
                    # transient cap exhaustion into a hot spin against
                    # TradeStation.
                    if self.running and is_futures_ingest_window():
                        self._backoff("stream ended without an error")
        except Exception as e:
            logger.error(
                "Fatal error in %s futures ingester: %s",
                self.index_symbol,
                e,
                exc_info=True,
            )
            sys.exit(1)
        finally:
            close_connection_pool()
            logger.info("%s futures ingester stopped", self.index_symbol)


def _futures_retention_days() -> int:
    """How many days of futures bars to keep.

    Defaults to ``DATA_RETENTION_DAYS`` — the same knob that bounds
    ``underlying_quotes`` for SPY / QQQ / SPX / NDX — so futures history
    matches its index counterpart without an operator having to keep two
    numbers in sync. ``FUTURES_BARS_RETENTION_DAYS`` still overrides it, which
    is what a long backfill wants: see ``src/tools/futures_backfill.py``.
    """
    explicit = os.getenv("FUTURES_BARS_RETENTION_DAYS")
    if explicit and explicit.strip():
        try:
            return max(1, int(explicit))
        except ValueError:
            logger.warning(
                "FUTURES_BARS_RETENTION_DAYS=%r is not an integer; "
                "falling back to DATA_RETENTION_DAYS",
                explicit,
            )
    return _getenv_int("DATA_RETENTION_DAYS", 90)


def run_futures_ingester(index_symbol: str) -> None:
    """Child-process entry point for one index's futures feed.

    Resolves the future via ``INDEX_FUTURES_MAP`` (SPX->@ES, …), builds an
    authenticated TradeStation client, and runs the ingester until
    shutdown.  Exits quietly if the index has no mapped future.
    """
    from dotenv import load_dotenv

    load_dotenv()

    future_symbol = resolve_index_future(index_symbol)
    if not future_symbol:
        logger.warning(
            "No INDEX_FUTURES_MAP entry for %s; futures ingester not starting.",
            index_symbol,
        )
        return

    client = TradeStationClient(
        os.getenv("TRADESTATION_CLIENT_ID", ""),
        os.getenv("TRADESTATION_CLIENT_SECRET", ""),
        os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
        sandbox=_getenv_bool("TRADESTATION_USE_SANDBOX", False),
    )

    try:
        from src.ingestion.api_call_tracker import attach_db_writer

        attach_db_writer(client)
    except Exception as e:
        logger.warning("Failed to attach API-call DB writer: %s", e)

    ingester = FuturesUnderlyingIngester(
        client,
        index_symbol=index_symbol,
        future_symbol=future_symbol,
        initial_barsback=_getenv_int("FUTURES_INITIAL_BARSBACK", 960),
        poll_barsback=_getenv_int("FUTURES_POLL_BARSBACK", 3),
        retention_days=_futures_retention_days(),
    )
    ingester.run()
