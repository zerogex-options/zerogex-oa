"""
TradeStation API-call window → database writer.

`TradeStationClient` already tracks API calls in 5-minute UTC windows
(see `_record_api_https_session_open`).  When a window rolls over, the
client logs the completed count.  This helper attaches a DB writer so
the same count is also upserted into the `tradestation_api_calls`
table, where the ON CONFLICT clause accumulates counts from multiple
ingestion processes that share a window.

It also exposes a reader the client's rate-limit governor calls to
learn how many calls other processes have already contributed to the
current in-flight 5-minute window.
"""

from __future__ import annotations

from datetime import datetime

from src.database import db_connection
from src.ingestion.tradestation_client import TradeStationClient
from src.utils import get_logger

logger = get_logger(__name__)


def write_api_call_window(window_start: datetime, call_count: int) -> None:
    """Upsert (window_start, call_count) into tradestation_api_calls.

    Summing via ON CONFLICT lets multiple ingestion processes (one per
    underlying, plus the VIX ingester) each contribute their per-window
    totals without overwriting each other.
    """
    if call_count <= 0:
        return
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO tradestation_api_calls (window_start, call_count)
                VALUES (%s, %s)
                ON CONFLICT (window_start) DO UPDATE SET
                    call_count = tradestation_api_calls.call_count + EXCLUDED.call_count,
                    updated_at = NOW()
                """,
                (window_start, call_count),
            )
            conn.commit()
    except Exception as e:
        # DB failure here must never break the API call path.
        logger.warning("Failed to persist tradestation_api_calls row: %s", e)


def read_api_call_window(window_start: datetime) -> int:
    """Return the persisted call_count for ``window_start``, or 0 if none.

    Reads only the single row for the given window so the query is a
    primary-key lookup.  Any DB error is logged and treated as 0 — the
    rate-limit governor falls back to the local in-process counter in
    that case, which is the safe direction (slightly under-counts vs.
    over-counts and blocks unnecessarily).
    """
    try:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT call_count FROM tradestation_api_calls WHERE window_start = %s",
                (window_start,),
            )
            row = cursor.fetchone()
            if row is None:
                return 0
            return int(row[0] or 0)
    except Exception as e:
        logger.debug("Failed to read tradestation_api_calls row: %s", e)
        return 0


def attach_db_writer(client: TradeStationClient) -> None:
    """Install the rollover writer AND the rate-limit governor reader.

    Name kept for backwards compat with existing call sites; this now also
    wires ``read_api_call_window`` so each process's
    ``_gate_for_rate_limit`` can see the other processes' contributions
    to the current 5-min window.
    """
    client.set_api_call_window_writer(write_api_call_window)
    client.set_api_call_window_reader(read_api_call_window)
