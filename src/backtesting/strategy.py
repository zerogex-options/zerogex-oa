"""Custom strategy builder (Phase 3).

Compiles a user-defined condition rule (``StrategySpec``) into synthetic Action
Cards so the existing forward-walk engine can price it exactly like a built-in
playbook pattern.

Pipeline:
  1. Materialize a per-minute indicator series by as-of joining
     ``underlying_quotes`` (price) with ``gex_summary`` (gamma structure) and
     ``signal_scores`` (MSI / regime). As-of (not exact-timestamp) because the
     three are written by different engines on slightly different clocks.
  2. On every price bar where ALL conditions hold, emit a synthetic CardRow: a
     directional ATM entry with level-offset target/stop (if configured). The
     premium overlay on the top-level ``exit`` still applies on top.
  3. ``engine.run_backtest`` then cooldown-collapses and prices these cards
     through the same ``_build_candidate`` path as playbook cards.
"""

from __future__ import annotations

import logging
import operator
from datetime import datetime, timedelta
from typing import Optional

from src.backtesting.models import BacktestSpec, Condition, StrategySpec
from src.signals.playbook.backtest import CardRow

logger = logging.getLogger(__name__)

# An indicator row older than this (relative to the price bar) is treated as
# stale and not carried forward — conditions on its fields then fail rather than
# evaluate against a number from before a data gap.
_INDICATOR_STALENESS_MIN = 15

_OPS = {
    "<": operator.lt, "<=": operator.le, ">": operator.gt,
    ">=": operator.ge, "==": operator.eq, "!=": operator.ne,
}

# Synthetic Cards all share this pattern id, so the engine's per-pattern
# cooldown collapses the continuous match stream into discrete entries.
_STRATEGY_PATTERN = "custom_strategy"


def _fetchall(conn, sql: str, params: tuple) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def _asof_index(rows: list[tuple], price_ts: datetime, ptr: int) -> tuple[Optional[tuple], int]:
    """Advance ``ptr`` to the last row whose ts <= price_ts; return (row, ptr).

    ``rows`` are ascending by timestamp (row[0]); ``ptr`` is monotonic across the
    outer price walk, so the whole merge is O(n).
    """
    while ptr + 1 < len(rows) and rows[ptr + 1][0] <= price_ts:
        ptr += 1
    if ptr < len(rows) and rows[ptr][0] <= price_ts:
        return rows[ptr], ptr
    return None, ptr


def _fresh(row: Optional[tuple], price_ts: datetime) -> Optional[tuple]:
    if row is None:
        return None
    if (price_ts - row[0]) > timedelta(minutes=_INDICATOR_STALENESS_MIN):
        return None
    return row


def _f(v) -> Optional[float]:
    return float(v) if v is not None else None


def load_indicator_bars(conn, underlying: str, start: datetime, end: datetime) -> list[dict]:
    """As-of merge price + gamma structure + MSI into per-minute indicator bars."""
    prices = _fetchall(
        conn,
        "SELECT timestamp, close FROM underlying_quotes "
        "WHERE symbol = %s AND timestamp BETWEEN %s AND %s AND close IS NOT NULL "
        "ORDER BY timestamp",
        (underlying, start, end),
    )
    gex = _fetchall(
        conn,
        "SELECT timestamp, total_net_gex, net_gex_at_spot, flip_distance, gamma_flip_point, "
        "       call_wall, put_wall, put_call_ratio, max_pain, convexity_risk "
        "FROM gex_summary WHERE underlying = %s AND timestamp BETWEEN %s AND %s "
        "ORDER BY timestamp",
        (underlying, start, end),
    )
    scores = _fetchall(
        conn,
        "SELECT timestamp, composite_score, direction FROM signal_scores "
        "WHERE underlying = %s AND timestamp BETWEEN %s AND %s ORDER BY timestamp",
        (underlying, start, end),
    )

    bars: list[dict] = []
    gptr = sptr = 0
    for ts, close in prices:
        price = _f(close)
        if not price or price <= 0:
            continue
        g_row, gptr = _asof_index(gex, ts, gptr)
        s_row, sptr = _asof_index(scores, ts, sptr)
        g = _fresh(g_row, ts)
        s = _fresh(s_row, ts)

        bar: dict = {"ts": ts, "price": price}
        if g is not None:
            net_gex = _f(g[1])
            net_gex_at_spot = _f(g[2])
            gamma_flip = _f(g[4])
            call_wall = _f(g[5])
            put_wall = _f(g[6])
            bar.update({
                "net_gex": net_gex,
                "net_gex_at_spot": net_gex_at_spot,
                "flip_distance": _f(g[3]),
                "gamma_flip_point": gamma_flip,
                "call_wall": call_wall,
                "put_wall": put_wall,
                "put_call_ratio": _f(g[7]),
                "max_pain": _f(g[8]),
                "convexity_risk": _f(g[9]),
            })
            sign_src = net_gex_at_spot if net_gex_at_spot is not None else net_gex
            if sign_src is not None:
                bar["net_gex_sign"] = (
                    "positive" if sign_src > 0 else "negative" if sign_src < 0 else "zero"
                )
            if gamma_flip is not None:
                bar["flip_distance_pct"] = abs(price - gamma_flip) / price * 100.0
            if call_wall is not None:
                bar["dist_to_call_wall_pct"] = (call_wall - price) / price * 100.0
            if put_wall is not None:
                bar["dist_to_put_wall_pct"] = (price - put_wall) / price * 100.0
        if s is not None:
            bar["msi"] = _f(s[1])
            bar["msi_regime"] = s[2]
        bars.append(bar)
    return bars


def _passes(conditions: list[Condition], bar: dict) -> bool:
    """All conditions hold; a condition on a field absent from the bar fails."""
    for c in conditions:
        if c.field not in bar or bar[c.field] is None:
            return False
        try:
            if not _OPS[c.op](bar[c.field], c.value):
                return False
        except TypeError:
            return False
    return True


def _build_legs(strategy: StrategySpec, price: float, expiry: str) -> list[dict]:
    """ATM single, or a defined-risk vertical (long ATM + short OTM by width)."""
    right = "C" if strategy.direction == "bullish" else "P"
    atm = round(price)
    long_leg = {"expiry": expiry, "strike": atm, "right": right, "side": "BUY"}
    if strategy.structure != "vertical":
        return [long_leg]
    # Bullish call vertical: short the higher-strike call. Bearish put vertical:
    # short the lower-strike put. Both are debit spreads in the trade direction.
    short_strike = atm + strategy.width if strategy.direction == "bullish" else atm - strategy.width
    short_leg = {"expiry": expiry, "strike": short_strike, "right": right, "side": "SELL"}
    return [long_leg, short_leg]


def _synth_card(strategy: StrategySpec, bar: dict, *, underlying: str, max_hold: int) -> CardRow:
    price = bar["price"]
    direction = strategy.direction
    right = "C" if direction == "bullish" else "P"
    expiry = (bar["ts"].date() + timedelta(days=strategy.dte)).isoformat()

    payload: dict = {
        "direction": direction,
        "entry": {"ref_price": price, "trigger": "at_market"},
        "max_hold_minutes": max_hold,
        "legs": _build_legs(strategy, price, expiry),
    }
    # Level-offset exits (favorable target / adverse stop), if configured.
    if strategy.target_offset_pct is not None:
        tgt = (
            price * (1 + strategy.target_offset_pct) if direction == "bullish"
            else price * (1 - strategy.target_offset_pct)
        )
        payload["target"] = {"ref_price": tgt, "kind": "level", "level_name": "custom_target"}
    else:
        payload["target"] = {"ref_price": None, "kind": "premium_pct"}
    if strategy.stop_offset_pct is not None:
        stp = (
            price * (1 - strategy.stop_offset_pct) if direction == "bullish"
            else price * (1 + strategy.stop_offset_pct)
        )
        payload["stop"] = {"ref_price": stp, "kind": "level", "level_name": "custom_stop"}
    else:
        payload["stop"] = {"ref_price": None, "kind": "premium_pct"}

    return CardRow(
        underlying=underlying,
        timestamp=bar["ts"],
        pattern=_STRATEGY_PATTERN,
        action="BUY_CALL" if right == "C" else "BUY_PUT",
        tier="custom",
        direction=direction,
        confidence=0.0,
        payload=payload,
    )


def generate_strategy_cards(conn, spec: BacktestSpec, *, max_hold: int) -> list[CardRow]:
    """Evaluate the strategy over history → synthetic CardRows (pre-cooldown)."""
    strategy = spec.strategy
    assert strategy is not None
    start = datetime.combine(spec.start_date, datetime.min.time())
    end = datetime.combine(spec.end_date, datetime.max.time())
    bars = load_indicator_bars(conn, spec.underlying, start, end)
    cards = [
        _synth_card(strategy, bar, underlying=spec.underlying, max_hold=max_hold)
        for bar in bars
        if _passes(strategy.conditions, bar)
    ]
    logger.info(
        "strategy backtest: %d indicator bars, %d condition matches", len(bars), len(cards)
    )
    return cards
