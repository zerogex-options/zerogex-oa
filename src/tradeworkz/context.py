"""Per-tick MarketSnapshot the engine hands to every bot.

The snapshot pulls together the pieces every strategy needs from the
existing analytics stack — walls, gamma flip, GEX regime, dealer positioning,
VIX regime, VWAP, session progress — and hides the SQL. Bots read attributes;
they never open a cursor.

The snapshot is cheap to build (single tick of ``gex_summary`` plus a
handful of scalars) and is passed by reference to every bot in the fleet
each cycle. If a field is unavailable the attribute is ``None`` and bots
must handle it — a snapshot is best-effort, never partial-crash.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


@dataclass
class MarketSnapshot:
    """Everything a bot needs to make an entry / exit decision this tick."""

    underlying: str
    timestamp: datetime
    spot: float

    # Wall structure (SpotGamma-style; see src/analytics/walls.py).
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    call_wall_strength: Optional[float] = None
    put_wall_strength: Optional[float] = None

    # Prior tick's walls — used to detect wall migration (a bot expecting
    # rejection at a wall should cut when the wall itself moves the wrong way).
    prior_call_wall: Optional[float] = None
    prior_put_wall: Optional[float] = None

    # Dealer positioning.
    net_gex: Optional[float] = None
    gamma_flip: Optional[float] = None
    max_gamma_strike: Optional[float] = None
    max_pain: Optional[float] = None
    put_call_ratio: Optional[float] = None
    dealer_net_delta: Optional[float] = None

    # Intraday context.
    vwap: Optional[float] = None
    vwap_deviation_pct: Optional[float] = None
    session_high: Optional[float] = None
    session_low: Optional[float] = None
    open_price: Optional[float] = None

    # Volatility regime.
    vix: Optional[float] = None
    iv_rank: Optional[float] = None

    # Recent price series (oldest -> newest).
    recent_closes: list[float] = field(default_factory=list)
    recent_highs: list[float] = field(default_factory=list)
    recent_lows: list[float] = field(default_factory=list)

    extra: Dict[str, Any] = field(default_factory=dict)

    # ---- Convenience derivations ---------------------------------------

    @property
    def et_time(self) -> time:
        """Snapshot timestamp as an ET wall-clock time."""
        if self.timestamp.tzinfo is None:
            ts = self.timestamp.replace(tzinfo=timezone.utc)
        else:
            ts = self.timestamp
        return ts.astimezone(_ET).time()

    @property
    def et_date(self) -> date:
        if self.timestamp.tzinfo is None:
            ts = self.timestamp.replace(tzinfo=timezone.utc)
        else:
            ts = self.timestamp
        return ts.astimezone(_ET).date()

    @property
    def minutes_since_open(self) -> Optional[float]:
        """Minutes elapsed since 09:30 ET, or ``None`` before the open."""
        t = self.et_time
        m = (t.hour * 60 + t.minute) - (9 * 60 + 30)
        return float(m) if m >= 0 else None

    @property
    def minutes_to_close(self) -> Optional[float]:
        """Minutes remaining until 16:00 ET, or ``None`` after close."""
        t = self.et_time
        m = (16 * 60) - (t.hour * 60 + t.minute)
        return float(m) if m > 0 else None

    def distance_to_call_wall_pct(self) -> Optional[float]:
        if self.call_wall is None or self.spot <= 0:
            return None
        return (self.call_wall - self.spot) / self.spot

    def distance_to_put_wall_pct(self) -> Optional[float]:
        if self.put_wall is None or self.spot <= 0:
            return None
        return (self.spot - self.put_wall) / self.spot

    def gex_regime(self) -> str:
        """Coarse regime label from ``net_gex`` sign / magnitude.

        The bots use this to gate strategies whose edge is regime-conditional
        (e.g. wall bouncing works in positive-gamma; breakout works when net
        gamma is thin or negative).
        """
        if self.net_gex is None:
            return "unknown"
        if self.net_gex >= 1.5e9:
            return "positive_strong"
        if self.net_gex >= 0.0:
            return "positive_weak"
        if self.net_gex > -1.5e9:
            return "negative_weak"
        return "negative_strong"


def build_snapshot(conn: Any, underlying: str) -> Optional[MarketSnapshot]:
    """Build the current ``MarketSnapshot`` for ``underlying`` from the DB.

    Uses the existing ``gex_summary`` row (canonical wall + flip + max-pain
    source), the latest ``underlying_quotes`` price, and the most recent
    ``vix_bars`` close. Returns ``None`` when there is no usable spot price
    or gex_summary row for the symbol.
    """
    cur = conn.cursor()

    cur.execute(
        """
        SELECT timestamp, close, session_high, session_low, session_open
        FROM underlying_quotes
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (underlying,),
    )
    row = cur.fetchone()
    if row is None:
        logger.debug("build_snapshot(%s): no underlying_quotes row", underlying)
        return None
    ts, spot, sess_hi, sess_lo, sess_open = row
    if spot is None or float(spot) <= 0:
        return None

    cur.execute(
        """
        SELECT net_gex, gamma_flip, max_pain, put_call_ratio, call_wall, put_wall,
               max_gamma_strike, dealer_net_delta
        FROM gex_summary
        WHERE underlying = %s
        ORDER BY timestamp DESC
        LIMIT 2
        """,
        (underlying,),
    )
    gex_rows = cur.fetchall()
    if gex_rows:
        gx = gex_rows[0]
        prior = gex_rows[1] if len(gex_rows) > 1 else None
    else:
        gx = (None,) * 8
        prior = None

    cur.execute(
        """
        SELECT close FROM vix_bars
        ORDER BY timestamp DESC LIMIT 1
        """
    )
    vix_row = cur.fetchone()
    vix = float(vix_row[0]) if vix_row and vix_row[0] is not None else None

    cur.execute(
        """
        SELECT close FROM underlying_quotes
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT 30
        """,
        (underlying,),
    )
    closes = [float(r[0]) for r in cur.fetchall() if r[0] is not None][::-1]

    return MarketSnapshot(
        underlying=underlying,
        timestamp=ts if isinstance(ts, datetime) else datetime.utcnow(),
        spot=float(spot),
        net_gex=_maybe_float(gx[0]),
        gamma_flip=_maybe_float(gx[1]),
        max_pain=_maybe_float(gx[2]),
        put_call_ratio=_maybe_float(gx[3]),
        call_wall=_maybe_float(gx[4]),
        put_wall=_maybe_float(gx[5]),
        max_gamma_strike=_maybe_float(gx[6]),
        dealer_net_delta=_maybe_float(gx[7]),
        prior_call_wall=_maybe_float(prior[4]) if prior else None,
        prior_put_wall=_maybe_float(prior[5]) if prior else None,
        session_high=_maybe_float(sess_hi),
        session_low=_maybe_float(sess_lo),
        open_price=_maybe_float(sess_open),
        vix=vix,
        recent_closes=closes,
    )


def _maybe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
