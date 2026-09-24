"""Read-only access to the Trade Bias archive and the minute bars.

Every statement here is a ``SELECT``. Readings come from ``trade_bias_scores``
(``tenor = 'swing'``: the structural market state is computed from the same
nine inputs on both tenors, so reading one avoids double-counting every
minute). Only ``payload->'inputs'`` is fetched, not the whole payload, which
carries playbook copy and tactical detail that would multiply the transfer
for nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from research.msi_regime_excursion.excursion import Bar
from research.msi_regime_excursion.sources import instrument, load_bars as _load_bars
from research.short_gamma_trend.rule import bias_input_from_payload
from src.signals.trade_bias.bias import BiasInput


@dataclass
class Reading:
    """One persisted Trade Bias reading."""

    timestamp: datetime
    #: ``None`` when the row predates ``payload.inputs`` or it is unreadable.
    inputs: Optional[BiasInput]
    #: ``market_state`` as the engine stored it at the time.
    stored_state: Optional[str]


def load_readings(conn, symbol: str, start: datetime, end: datetime) -> list[Reading]:
    """Cash-session Trade Bias readings for ``symbol`` in ``[start, end)``.

    One reading per minute -- the last one written in it -- whatever the
    engine's stamping, so no minute is weighted by how many cycles it ran.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (date_trunc('minute', timestamp))
               timestamp, payload->'inputs', market_state
        FROM trade_bias_scores
        WHERE underlying = %s AND tenor = 'swing'
          AND timestamp >= %s AND timestamp < %s
          AND (timestamp AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
          AND (timestamp AT TIME ZONE 'America/New_York')::time <  TIME '16:00'
        ORDER BY date_trunc('minute', timestamp) ASC, timestamp DESC
        """,
        (symbol.upper(), start, end),
    )
    out: list[Reading] = []
    for ts, inputs, stored in cur.fetchall():
        if ts is None:
            continue
        out.append(
            Reading(timestamp=ts, inputs=bias_input_from_payload(inputs), stored_state=stored)
        )
    return out


def load_bars(conn, symbol: str, start: datetime, end: datetime) -> list[Bar]:
    """``underlying_quotes`` minute bars for ``symbol`` in ``[start, end]``."""
    return _load_bars(conn, instrument(symbol), start, end)


def archive_span(conn, symbol: str) -> tuple[Optional[datetime], Optional[datetime], int]:
    """``(first, last, rows)`` of the swing-tenor Trade Bias archive for ``symbol``."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
        FROM trade_bias_scores
        WHERE underlying = %s AND tenor = 'swing'
        """,
        (symbol.upper(),),
    )
    row = cur.fetchone()
    return (row[0], row[1], int(row[2] or 0)) if row else (None, None, 0)
