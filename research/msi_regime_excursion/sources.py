"""Read-only access to the production archive, and the instrument table.

Every statement here is a ``SELECT``. Nothing in this package writes to the
database.

The instrument table is where the ES / NQ design decision is written down, so
it is worth reading carefully. ZeroGEX computes gamma from INDEX option chains
and never from options on futures, and
``src/jobs/futures_projection.py`` is explicit that projection is read-side
only: "no projected value ever reaches GEX, greeks, signals, settlement or any
DB write". ``docs/runbooks/es_nq_futures_rollout.md`` says the same in table
form -- **scores are not projected**.

So there is no ES row in ``signal_scores`` and never has been. An ES trader
looking at the regime gauge is looking at the SPX MSI, unchanged, with ES
prices on the chart. That is not a gap in the archive to be worked around; it
is the product's actual behavior, and testing it means exactly what this table
does: take the SPX score, and measure the excursion that followed **on the ES
tape**. If the SPX-derived regime read does not describe ES price action, that
is a finding about what ES users are being shown.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from research.msi_regime_excursion.excursion import Bar

__all__ = ["Instrument", "INSTRUMENTS", "instrument", "Reading", "load_readings", "load_bars"]


@dataclass(frozen=True)
class Instrument:
    key: str
    #: Symbol whose ``signal_scores`` rows carry the MSI for this instrument.
    score_symbol: str
    #: ``cash`` -> ``underlying_quotes.symbol``; ``futures`` -> ``futures_quotes.index_symbol``.
    bar_source: str
    bar_symbol: str
    #: Excursion targets in the instrument's own price units. For ES/NQ these
    #: are the sizes the churned trader named ("4 point, 8 points, 10 points
    #: most typically"); for the cash names they are the rough equivalents.
    point_targets: tuple[float, ...] = ()

    @property
    def inherits_score(self) -> bool:
        """True when the score is borrowed from another symbol's option book."""
        return self.score_symbol != self.bar_symbol or self.bar_source == "futures"


INSTRUMENTS: dict[str, Instrument] = {
    "SPY": Instrument("SPY", "SPY", "cash", "SPY", (0.4, 0.8, 1.0)),
    "SPX": Instrument("SPX", "SPX", "cash", "SPX", (4.0, 8.0, 10.0)),
    "QQQ": Instrument("QQQ", "QQQ", "cash", "QQQ", (0.4, 0.8, 1.0)),
    "NDX": Instrument("NDX", "NDX", "cash", "NDX", (15.0, 30.0, 40.0)),
    # Score from the SPX / NDX option book; bars from the future's own feed.
    "ES": Instrument("ES", "SPX", "futures", "SPX", (4.0, 8.0, 10.0)),
    "NQ": Instrument("NQ", "NDX", "futures", "NDX", (15.0, 30.0, 40.0)),
}

DEFAULT_INSTRUMENTS: tuple[str, ...] = ("SPY", "SPX", "QQQ", "NDX", "ES", "NQ")


def instrument(key: str) -> Instrument:
    try:
        return INSTRUMENTS[key.upper()]
    except KeyError:
        raise SystemExit(
            f"unknown instrument {key!r}; known: {', '.join(sorted(INSTRUMENTS))}"
        )


@dataclass
class Reading:
    """One persisted MSI reading."""

    timestamp: datetime
    msi: float
    #: ``signal_scores.direction`` as persisted by the engine that wrote it.
    persisted_band: Optional[str]
    #: Per-component ``{name: {score, max_points, contribution}}``.
    components: dict[str, Any] = field(default_factory=dict)


def _as_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def load_readings(
    conn,
    score_symbol: str,
    start: datetime,
    end: datetime,
) -> list[Reading]:
    """Every persisted MSI reading for ``score_symbol`` in ``[start, end]``."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, composite_score, direction, components
        FROM signal_scores
        WHERE underlying = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp ASC
        """,
        (score_symbol.upper(), start, end),
    )
    out: list[Reading] = []
    for ts, score, direction, components in cur.fetchall():
        if ts is None or score is None:
            continue
        out.append(
            Reading(
                timestamp=ts,
                msi=float(score),
                persisted_band=direction,
                components=_as_dict(components),
            )
        )
    return out


def load_bars(
    conn,
    inst: Instrument,
    start: datetime,
    end: datetime,
) -> list[Bar]:
    """Minute bars for ``inst`` over ``[start, end]``.

    The window is widened by the caller, not here -- ``study`` pads the end so
    the last readings still have a full forward window to be measured over.
    """
    cur = conn.cursor()
    if inst.bar_source == "futures":
        cur.execute(
            """
            SELECT timestamp, open, high, low, close
            FROM futures_quotes
            WHERE index_symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
            """,
            (inst.bar_symbol.upper(), start, end),
        )
    else:
        cur.execute(
            """
            SELECT timestamp, open, high, low, close
            FROM underlying_quotes
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
            """,
            (inst.bar_symbol.upper(), start, end),
        )
    bars: list[Bar] = []
    for ts, o, h, l, c in cur.fetchall():
        if ts is None or None in (o, h, l, c):
            continue
        bars.append(Bar(ts=ts, open=float(o), high=float(h), low=float(l), close=float(c)))
    return bars


def archive_span(
    conn, table: str, symbol_column: str, symbol: str
) -> tuple[Optional[datetime], Optional[datetime], int]:
    """``(min_ts, max_ts, row_count)`` for one symbol -- used by ``describe``.

    ``table`` and ``symbol_column`` are validated against a fixed allowlist
    rather than interpolated blind, so this stays a closed set of reads.
    """
    allowed = {
        ("signal_scores", "underlying"),
        ("underlying_quotes", "symbol"),
        ("futures_quotes", "index_symbol"),
    }
    if (table, symbol_column) not in allowed:
        raise ValueError(f"archive_span refuses {table}.{symbol_column}")
    cur = conn.cursor()
    cur.execute(
        f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM {table} WHERE {symbol_column} = %s",
        (symbol.upper(),),
    )
    row = cur.fetchone()
    return (row[0], row[1], int(row[2] or 0)) if row else (None, None, 0)
