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

    # underlying_quotes is per-bar OHLC (open/high/low/close per timestamp)
    # — there is no session_high / session_low / session_open on the row.
    # Fetch the latest tick for the current spot + timestamp, then aggregate
    # today's session bounds in a second query.
    cur.execute(
        """
        SELECT timestamp, close
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
    ts, spot = row
    if spot is None or float(spot) <= 0:
        return None

    # Session bounds: MIN/MAX of per-bar low/high, plus the earliest open in
    # today's session window (09:30-16:15 ET, expressed as the last 24h to
    # keep the query timezone-agnostic — the bots that care about session
    # bounds only look at intraday behavior anyway).
    cur.execute(
        """
        SELECT MIN(low), MAX(high),
               (SELECT open FROM underlying_quotes
                WHERE symbol = %s AND timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY timestamp ASC LIMIT 1)
        FROM underlying_quotes
        WHERE symbol = %s AND timestamp >= NOW() - INTERVAL '24 hours'
        """,
        (underlying, underlying),
    )
    session_row = cur.fetchone()
    sess_lo = session_row[0] if session_row else None
    sess_hi = session_row[1] if session_row else None
    sess_open = session_row[2] if session_row else None

    # gex_summary columns:
    #   * net_gex_at_spot   — DTE-horizon-occupancy-weighted gamma sampled
    #                         at spot. This is the REGIME-CORRECT headline
    #                         figure the rest of the site displays (see
    #                         src/analytics/main_engine.py::_calculate_gex_summary:
    #                         "net_gex_at_spot is the regime-correct
    #                         headline figure; total_net_gex is retained
    #                         as the raw chain aggregate. Downstream
    #                         consumers must not treat them as
    #                         interchangeable."). TradeWorkz reads it into
    #                         snap.net_gex so bot regime gates match what
    #                         a customer sees on /gex-heatmap and every
    #                         other GEX-consuming page. Also used as the
    #                         dealer_net_delta proxy since it's already
    #                         signed and same order of magnitude.
    #   * total_net_gex     — unweighted whole-chain sum. Kept in the
    #                         SELECT for parity with the analytics engine
    #                         but NOT surfaced on the MarketSnapshot.
    #                         Occasionally disagrees in sign with the
    #                         at-spot value when far-OTM long-dated
    #                         strikes dominate the unweighted tail; those
    #                         are the exact moments TradeWorkz used to
    #                         classify the regime wrong.
    #   * gamma_flip_point  (not gamma_flip)
    cur.execute(
        """
        SELECT net_gex_at_spot, gamma_flip_point, max_pain, put_call_ratio,
               call_wall, put_wall, max_gamma_strike, total_net_gex
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

    # Session VWAP + deviation from the existing view
    # (underlying_vwap_deviation). The view is keyed by symbol +
    # timestamp and returns the running per-session VWAP across the
    # current trading day; we pick the latest row.
    cur.execute(
        """
        SELECT vwap, vwap_deviation_pct
        FROM underlying_vwap_deviation
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (underlying,),
    )
    vwap_row = cur.fetchone()
    vwap_value = _maybe_float(vwap_row[0]) if vwap_row else None
    vwap_deviation = _maybe_float(vwap_row[1]) if vwap_row else None

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

    # dealer_net_delta proxy: net_gex_at_spot is the value of the
    # spot-shift gamma profile at the current spot — signed, same order
    # of magnitude the DealerDeltaPressureRider bot's ~5e8 threshold is
    # calibrated to. A true dealer_net_delta isn't persisted; this is
    # the closest directional proxy we have. Since we ALSO use
    # net_gex_at_spot for snap.net_gex (regime), the two are the same
    # number by construction — DealerDeltaPressureRider's own logic then
    # filters on regime + recent_trend agreement, so the sign of
    # dealer_net_delta only affects entries where recent_trend confirms
    # a same-sign move.
    dealer_net_delta = _maybe_float(gx[0])

    return MarketSnapshot(
        underlying=underlying,
        timestamp=ts if isinstance(ts, datetime) else datetime.utcnow(),
        spot=float(spot),
        # gx[0] is net_gex_at_spot (regime-correct); gx[7] is the
        # unweighted total_net_gex kept for parity with analytics but
        # NOT surfaced. Do not swap without updating the SELECT above.
        net_gex=_maybe_float(gx[0]),
        gamma_flip=_maybe_float(gx[1]),
        max_pain=_maybe_float(gx[2]),
        put_call_ratio=_maybe_float(gx[3]),
        call_wall=_maybe_float(gx[4]),
        put_wall=_maybe_float(gx[5]),
        max_gamma_strike=_maybe_float(gx[6]),
        dealer_net_delta=dealer_net_delta,
        prior_call_wall=_maybe_float(prior[4]) if prior else None,
        prior_put_wall=_maybe_float(prior[5]) if prior else None,
        session_high=_maybe_float(sess_hi),
        session_low=_maybe_float(sess_lo),
        open_price=_maybe_float(sess_open),
        vwap=vwap_value,
        vwap_deviation_pct=vwap_deviation,
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
