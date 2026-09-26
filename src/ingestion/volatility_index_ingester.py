"""
Volatility-Index Ingester

Generic streaming ingester that pulls 5-minute bars for a CBOE-style cash
volatility index from TradeStation's ``/stream/barcharts`` endpoint and
upserts them into a per-ticker bars table (``vix_bars``, ``vxn_bars``, …).
The ``/api/market/volatility?ticker=…`` endpoint reads from those tables
instead of calling TradeStation directly, so the endpoint stays fast and
a single long-running ingester process per index keeps the window fresh.

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
from src.config import (
    _getenv_int,
    _getenv_bool,
    configured_provider_name,
    VOLATILITY_INDEX_PROVIDER_ENV,
)
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

# How often the PROVIDER path drains its bar stream. The stream does its own
# polling underneath; this only bounds how long a SIGTERM waits, so it is
# deliberately far shorter than the 5-minute bar it is collecting.
_PROVIDER_DRAIN_SLEEP_SEC = 1.0

# Allowed bars-table names — guards the SQL string interpolation in
# _upsert_bars / _accumulate_mark / _prune_old_bars against ever using an
# attacker-controlled identifier.  Add new tables here when a new volatility
# index is wired up.
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


def _bar_close_timestamp(ts: datetime) -> datetime:
    """The 5-minute bar a mark observed at ``ts`` belongs to, stamped at its CLOSE.

    TradeStation stamps a bar at the END of its interval -- the 09:30-09:35
    bar arrives stamped 09:35 -- and every row already in these tables follows
    that convention, so the provider path has to as well.  Stamping at the
    bucket's start instead would put each new row one bar earlier than the
    history sitting beside it, and ``get_vix_z_score_20d`` reads both at once.

    A mark landing exactly on a boundary closes that bar rather than opening
    the next: the interval is (09:30, 09:35], not [09:30, 09:35).
    """
    floored = ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)
    return floored if floored == ts else floored + timedelta(minutes=5)


def _mark_row(bar: Any) -> Optional[Dict[str, Any]]:
    """One provider mark as a degenerate bar in its 5-minute bucket.

    ``None`` when there is no usable close or timestamp, mirroring
    ``_parse_bar``: ``close`` is NOT NULL in both bar tables, so a mark
    without one would fail its whole write rather than skip its own row.

    Only ``close`` is read.  The Market Value index endpoint answers a mark
    and nothing else, and the provider's ``_bar_from_row`` fills open/high/low
    from that same close -- but the realtime endpoint answers a running DAILY bar,
    whose high and low would otherwise be written into every 5-minute row as
    if they were the interval's.  Taking the close alone makes the candle a
    function of the observed sequence under either endpoint, which is what
    ``_accumulate_mark``'s conflict clause then builds it out of.

    No volume: a cash index is a calculation over its constituents, not
    something that trades, and these tables have no volume column.
    """
    close = safe_float(getattr(bar, "close", None), field_name="close", default=None)
    if close is None:
        return None
    ts = getattr(bar, "timestamp", None)
    if not isinstance(ts, datetime):
        return None
    bucket = _bar_close_timestamp(ts)
    return {
        "timestamp": bucket,
        "open": close,
        "high": close,
        "low": close,
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
        client: Optional[TradeStationClient],
        *,
        provider: Any = None,
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
        # EXACTLY ONE of these drives the stream, and which one is decided by
        # MARKET_DATA_PROVIDER in run_ingester below.
        #
        # client is not None -> the original TradeStation reader, byte for
        # byte. That path is what production has run since this ingester was
        # written and this change does not touch it; a rewrite would have put
        # a fresh implementation of a working feed into production with no
        # way to validate it until the next open.
        #
        # client is None -> _read_provider_stream, via the MarketDataProvider
        # seam. VIX and VXN are Cboe CGIF data, licensed separately from
        # OPRA, so moving them off TradeStation is the point of this: leaving
        # them behind would keep the redistribution exposure the migration
        # exists to remove.
        self.client = client
        self.provider = provider
        self.ticker = ticker
        self.symbol = symbol
        self.table_name = table_name
        self.initial_barsback = initial_barsback
        self.poll_barsback = poll_barsback
        self.retention_days = retention_days
        self.running = False
        self._seeded = False
        self._upserts_since_prune = 0
        #: Bucket of the last mark written by the provider path, so the
        #: retention counter below counts BARS and not marks.
        self._last_mark_bucket: Optional[datetime] = None
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

    def _accumulate_mark(self, row: Dict[str, Any]) -> int:
        """Fold one mark into its 5-minute bar. Returns rows written (0 or 1).

        Separate from :meth:`_upsert_bars` because the two feeds deliver
        different things. TradeStation sends a COMPLETE bar, so overwriting
        all four prices with it is right. The provider path sends a mark
        several times per bar, and that same overwrite would leave every row
        with open = high = low = close of whichever mark happened to land
        last -- candles with no bodies and no wicks, on a customer-facing
        gauge.

        So take the period-correct aggregate instead: first-seen open,
        running high and low, last close. The identical conflict clause
        ``IngestionEngine._upsert_underlying_quote`` uses to build a minute
        candle out of a sequence of marks.

        ``table_name`` is allowlist-checked in ``__init__``, which is what
        makes the interpolation below safe.
        """
        query = (
            f"INSERT INTO {self.table_name} (timestamp, open, high, low, close) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (timestamp) DO UPDATE SET "
            f"    open = COALESCE({self.table_name}.open, EXCLUDED.open), "
            f"    high = GREATEST({self.table_name}.high, EXCLUDED.high), "
            f"    low = LEAST({self.table_name}.low, EXCLUDED.low), "
            "    close = EXCLUDED.close, "
            "    updated_at = NOW()"
        )
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    query,
                    (
                        row["timestamp"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                    ),
                )
                conn.commit()
            return 1
        except Exception as e:
            logger.error("%s mark upsert failed: %s", self.ticker, e, exc_info=True)
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
        self._record_upsert(self._upsert_bars(parsed))

    def _record_upsert(self, written: int) -> None:
        """Seed marker and retention bookkeeping after a successful upsert.

        Shared by both readers so the retention policy has one implementation
        -- two copies of "prune every N upserts" drift, and the one that
        drifts is the one nobody is watching.
        """
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

    def _read_provider_stream(self) -> None:
        """Collect bars through the MarketDataProvider seam.

        Used for every feed but TradeStation. Raises when the stream dies so
        run()'s existing reconnect backoff handles it, exactly as the
        TradeStation reader does -- the two readers differ in where bars come
        from and in nothing else.

        The provider delivers a MARK several times per bar rather than a
        finished bar, so each one is folded into its 5-minute bucket and
        _accumulate_mark builds the candle. Two consequences to know before
        this runs in production:

        * There is no backfill. TradeStation replays ``poll_barsback`` bars
          on every reconnect; a mark feed only has now. A restart inside one
          bucket costs nothing, but an outage spanning several leaves those
          bars missing rather than late.
        * The first bucket after a start is partial -- its open is the first
          mark seen, not the interval's true open.
        """
        stream = self.provider.stream_index_bars(
            self.symbol,
            db_symbol=self.ticker,
            interval=VOLATILITY_BAR_INTERVAL,
            unit=VOLATILITY_BAR_UNIT,
            initial_barsback=self.initial_barsback,
            poll_barsback=self.poll_barsback,
        )
        stream.start()
        logger.info(
            "%s stream: connected via %s",
            self.ticker,
            type(self.provider).__name__,
        )
        try:
            while self.running and is_engine_run_window():
                bar = stream.drain()
                if bar is None and not stream.is_alive():
                    raise RuntimeError(f"{self.ticker} index bar stream stopped")
                row = _mark_row(bar) if bar is not None else None
                if row is not None and self._accumulate_mark(row) > 0:
                    # Count BARS, not marks. _PRUNE_EVERY_N_UPSERTS is a bar
                    # budget; at one mark a second, counting marks would run
                    # the retention DELETE every two minutes instead of
                    # every ten hours.
                    if row["timestamp"] != self._last_mark_bucket:
                        self._last_mark_bucket = row["timestamp"]
                        self._record_upsert(1)
                time.sleep(_PROVIDER_DRAIN_SLEEP_SEC)
        finally:
            try:
                stream.stop()
            except Exception as e:  # noqa: BLE001 - never mask the real error
                logger.warning("%s stream stop failed: %s", self.ticker, e)

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
                    if self.client is not None:
                        self._read_stream()
                    else:
                        self._read_provider_stream()
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

    Loads ``.env``, builds whichever feed ``MARKET_DATA_PROVIDER`` names, and
    runs the ingester until shutdown.  The per-ticker modules
    (``vix_ingester.py`` / ``vxn_ingester.py``) call this with their own
    config so every index follows the identical spawn → seed → stream →
    prune lifecycle.

    A TradeStationClient is built ONLY for the TradeStation feed -- the same
    rule ``main_engine`` follows, and for the same reason: its three
    credentials stop existing when TradeStation is decommissioned, so
    constructing one unconditionally would make every other feed depend on
    the old one's secrets to start at all.
    """
    from dotenv import load_dotenv

    load_dotenv()

    # Through the override, so VIX and VXN can be pinned to one vendor while
    # the options move to another.
    provider_name = configured_provider_name(VOLATILITY_INDEX_PROVIDER_ENV)
    deployment_name = configured_provider_name()
    client: Optional[TradeStationClient] = None
    provider: Any = None

    if provider_name == "tradestation":
        client = TradeStationClient(
            os.getenv("TRADESTATION_CLIENT_ID", ""),
            os.getenv("TRADESTATION_CLIENT_SECRET", ""),
            os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
            sandbox=_getenv_bool("TRADESTATION_USE_SANDBOX", False),
        )

        # Wire up the API-calls DB writer so this child process also
        # contributes its API usage to the tradestation_api_calls table.
        try:
            from src.ingestion.api_call_tracker import attach_db_writer

            attach_db_writer(client)
        except Exception as e:
            logger.warning("Failed to attach API-call DB writer: %s", e)
    else:
        from src.ingestion.providers import get_provider

        # BY NAME. get_provider() with no argument reads MARKET_DATA_PROVIDER
        # for itself, so under an override it would hand back the deployment
        # feed while the log line below announced the pinned one -- an
        # override that reads as applied and is not.
        provider = get_provider(name=provider_name)

    if provider_name == deployment_name:
        logger.info("%s feed: %s", ticker, provider_name)
    else:
        logger.info(
            "%s feed: %s (pinned by %s; the deployment feed is %s)",
            ticker,
            provider_name,
            VOLATILITY_INDEX_PROVIDER_ENV,
            deployment_name,
        )

    ingester = VolatilityIndexIngester(
        client,
        provider=provider,
        ticker=ticker,
        symbol=symbol,
        table_name=table_name,
        initial_barsback=initial_barsback,
        poll_barsback=poll_barsback,
        retention_days=retention_days,
    )
    ingester.run()
