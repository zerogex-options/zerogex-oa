"""Always-on signal scoring engine.

Computes one weighted score per underlying symbol each cycle and stores a
full component breakdown as a time series.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)


WEIGHTS = {
    "gex_regime": 3,
    "dealer_hedging": 3,
    "smart_money": 2,
    "vwap": 2,
    "orb": 1,
    "pcr": 2,
    "unusual_volume": 1,
    "momentum_divergence": 1,
    "vanna_charm": 1,
    "exhaustion": 3,
    "vol_instability": 2,
}


@dataclass
class ScoreComponent:
    key: str
    name: str
    weight: int
    raw_score: int
    weighted_score: int
    value: Optional[float]
    description: str


@dataclass
class ScoreSnapshot:
    underlying: str
    timestamp: datetime
    composite_score: int
    max_possible_score: int
    normalized_score: float
    direction: str
    strength: str
    regime: str
    recommended_trade_type: str
    recommended_timeframe: str
    components: list[ScoreComponent]


@dataclass
class ScoreContext:
    timestamp: datetime
    current_price: float
    net_gex: float
    gamma_flip: float
    put_call_ratio: float
    max_gamma_strike: float
    vwap_deviation_pct: float
    orb_status: str
    dealer_net_delta: float
    smart_call_premium: float
    smart_put_premium: float
    unusual_call_volume: bool
    price_change_5min: float
    net_option_flow_5min: float
    vanna_exposure: float
    charm_exposure: float
    recent_closes: list[float]


class SignalScoringEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)

    @staticmethod
    def _to_direction(score: int) -> str:
        if score > 0:
            return "bullish"
        if score < 0:
            return "bearish"
        return "neutral"

    @staticmethod
    def _to_strength(normalized: float) -> str:
        if normalized >= 0.67:
            return "high"
        if normalized >= 0.40:
            return "medium"
        return "low"

    @staticmethod
    def _compute_rsi(closes: list[float], period: int = 14) -> Optional[float]:
        if len(closes) < period + 1:
            return None
        gains = losses = 0.0
        for i in range(-period, 0):
            delta = closes[i] - closes[i - 1]
            if delta >= 0:
                gains += delta
            else:
                losses += abs(delta)
        if losses == 0:
            return 100.0
        rs = (gains / period) / (losses / period)
        return 100 - (100 / (1 + rs))

    def _compute_exhaustion(self, ctx: ScoreContext) -> tuple[float, bool]:
        closes = ctx.recent_closes
        if len(closes) < 12:
            return 0.0, False
        dist = abs(ctx.current_price - ctx.max_gamma_strike) / max(ctx.current_price, 1e-6) if ctx.max_gamma_strike else 1.0
        max_gamma_score = 1 - min(dist / 0.005, 1.0)

        rsi_now = self._compute_rsi(closes)
        rsi_prev = self._compute_rsi(closes[:-3]) if len(closes) >= 18 else None
        divergence = 1.0 if (rsi_now is not None and rsi_prev is not None and rsi_now < rsi_prev and max(closes[-3:]) > max(closes[-7:-3])) else 0.0

        velocity_now = abs(closes[-1] - closes[-6])
        velocity_prev = abs(closes[-6] - closes[-11])
        velocity = 1 - min((velocity_now / velocity_prev), 1.0) if velocity_prev > 0 else 0.0

        gamma_context = 0.3 if ctx.net_gex > 0 else 1.0
        zes = (0.35 * max_gamma_score + 0.30 * divergence + 0.20 * velocity + 0.15 * gamma_context) * 100
        return round(zes, 2), zes >= 85

    def _fetch_context(self) -> Optional[ScoreContext]:
        try:
            with db_connection() as conn:
                cur = conn.cursor()

                cur.execute(
                    """
                    SELECT timestamp, total_net_gex, gamma_flip_point, put_call_ratio, max_gamma_strike
                    FROM gex_summary
                    WHERE underlying = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol,),
                )
                gex = cur.fetchone()
                if not gex:
                    return None
                ts, net_gex, gamma_flip, pcr, max_gamma = gex

                cur.execute(
                    """
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                current_price = float(row[0])

                cur.execute(
                    """
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 30
                    """,
                    (self.db_symbol,),
                )
                recent_closes = [float(r[0]) for r in reversed(cur.fetchall())]

                cur.execute(
                    """
                    SELECT vwap_deviation_pct
                    FROM underlying_vwap_deviation
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol,),
                )
                vwap_row = cur.fetchone()
                vwap_dev = float(vwap_row[0]) if vwap_row else 0.0

                cur.execute(
                    """
                    SELECT orb_status
                    FROM opening_range_breakout
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol,),
                )
                orb = cur.fetchone()
                orb_status = orb[0] if orb else ""

                cur.execute(
                    """
                    SELECT option_type, SUM(total_premium)
                    FROM flow_smart_money
                    WHERE symbol = %s
                      AND timestamp >= NOW() - INTERVAL '30 minutes'
                    GROUP BY option_type
                    """,
                    (self.db_symbol,),
                )
                sm_call = sm_put = 0.0
                for option_type, prem in cur.fetchall():
                    if option_type == "C":
                        sm_call = float(prem or 0.0)
                    elif option_type == "P":
                        sm_put = float(prem or 0.0)

                cur.execute(
                    """
                    SELECT SUM(delta * open_interest * 100)
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp = (
                        SELECT timestamp
                        FROM option_chains
                        WHERE underlying = %s
                        ORDER BY timestamp DESC
                        LIMIT 1
                      )
                    """,
                    (self.db_symbol, self.db_symbol),
                )
                dealer_delta = -(float(cur.fetchone()[0] or 0.0))

                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM option_chains
                        WHERE underlying = %s
                          AND option_type = 'C'
                          AND open_interest > 0
                          AND volume::float / open_interest > 3.0
                          AND timestamp >= NOW() - INTERVAL '30 minutes'
                    )
                    """,
                    (self.db_symbol,),
                )
                unusual = bool(cur.fetchone()[0])

                cur.execute(
                    """
                    SELECT close - LAG(close, 5) OVER (PARTITION BY symbol ORDER BY timestamp)
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol,),
                )
                pc5 = float(cur.fetchone()[0] or 0.0)

                cur.execute(
                    """
                    SELECT SUM(CASE WHEN option_type='C' THEN total_premium ELSE -total_premium END)
                    FROM flow_by_type
                    WHERE symbol = %s
                      AND timestamp >= NOW() - INTERVAL '5 minutes'
                    """,
                    (self.db_symbol,),
                )
                net_flow = float(cur.fetchone()[0] or 0.0)

                cur.execute(
                    """
                    SELECT SUM(vanna_exposure), SUM(charm_exposure)
                    FROM gex_by_strike
                    WHERE underlying = %s
                      AND timestamp = (
                        SELECT timestamp
                        FROM gex_by_strike
                        WHERE underlying = %s
                        ORDER BY timestamp DESC
                        LIMIT 1
                      )
                    """,
                    (self.db_symbol, self.db_symbol),
                )
                vc = cur.fetchone() or (0.0, 0.0)

                return ScoreContext(
                    timestamp=ts,
                    current_price=current_price,
                    net_gex=float(net_gex or 0.0),
                    gamma_flip=float(gamma_flip or 0.0),
                    put_call_ratio=float(pcr or 1.0),
                    max_gamma_strike=float(max_gamma or 0.0),
                    vwap_deviation_pct=vwap_dev,
                    orb_status=orb_status,
                    dealer_net_delta=dealer_delta,
                    smart_call_premium=sm_call,
                    smart_put_premium=sm_put,
                    unusual_call_volume=unusual,
                    price_change_5min=pc5,
                    net_option_flow_5min=net_flow,
                    vanna_exposure=float(vc[0] or 0.0),
                    charm_exposure=float(vc[1] or 0.0),
                    recent_closes=recent_closes,
                )
        except Exception:
            logger.exception("SignalScoringEngine context fetch failed")
            return None

    def compute_score(self, ctx: ScoreContext) -> ScoreSnapshot:
        components: list[ScoreComponent] = []
        total = 0

        def add(key: str, name: str, raw: int, value: Optional[float], desc: str) -> None:
            nonlocal total
            weighted = raw * WEIGHTS[key]
            total += weighted
            components.append(ScoreComponent(key=key, name=name, weight=WEIGHTS[key], raw_score=raw, weighted_score=weighted, value=value, description=desc))

        add("gex_regime", "GEX Regime", 1 if ctx.net_gex > 0 else -1, ctx.net_gex, "Positive GEX favors mean reversion; negative GEX favors trend expansion.")
        add("dealer_hedging", "Dealer Hedging", 1 if ctx.dealer_net_delta > 0 else (-1 if ctx.dealer_net_delta < 0 else 0), ctx.dealer_net_delta, "Derived dealer hedge pressure from delta*OI.")
        add("smart_money", "Smart Money", 1 if ctx.smart_call_premium > ctx.smart_put_premium * 1.2 else (-1 if ctx.smart_put_premium > ctx.smart_call_premium * 1.2 else 0), ctx.smart_call_premium - ctx.smart_put_premium, "30-minute smart money premium imbalance.")
        add("vwap", "VWAP Deviation", 1 if ctx.vwap_deviation_pct > 0.2 else (-1 if ctx.vwap_deviation_pct < -0.2 else 0), ctx.vwap_deviation_pct, "Price extension vs VWAP.")
        orb_raw = 1 if "breakout" in (ctx.orb_status or "").lower() or "long" in (ctx.orb_status or "").lower() else (-1 if "breakdown" in (ctx.orb_status or "").lower() or "short" in (ctx.orb_status or "").lower() else 0)
        add("orb", "ORB", orb_raw, None, f"ORB status: {ctx.orb_status or 'none'}")
        add("pcr", "Put/Call Ratio", 1 if ctx.put_call_ratio < 0.7 else (-1 if ctx.put_call_ratio > 1.3 else 0), ctx.put_call_ratio, "Sentiment extreme based on put/call ratio.")
        add("unusual_volume", "Unusual Call Volume", 1 if ctx.unusual_call_volume else 0, 1.0 if ctx.unusual_call_volume else 0.0, "Volume/OI call spike.")
        div_raw = 1 if (ctx.price_change_5min < 0 and ctx.net_option_flow_5min > 50_000) else (-1 if (ctx.price_change_5min > 0 and ctx.net_option_flow_5min < -50_000) else 0)
        add("momentum_divergence", "Momentum Divergence", div_raw, ctx.net_option_flow_5min, "Short-horizon flow/price divergence.")
        vc_raw = 1 if (ctx.vanna_exposure > 0 and ctx.charm_exposure < 0) else (-1 if (ctx.vanna_exposure < 0 and ctx.charm_exposure > 0) else 0)
        add("vanna_charm", "Vanna/Charm", vc_raw, ctx.vanna_exposure + ctx.charm_exposure, "Dealer flow drift from higher-order greeks.")

        zes, trap = self._compute_exhaustion(ctx)
        ex_raw = -1 if (zes >= 70 and ctx.price_change_5min >= 0) else (1 if (zes >= 85 and ctx.price_change_5min < 0) else 0)
        add("exhaustion", "Exhaustion", ex_raw, zes, "ZeroGEX exhaustion state with trap trigger." if trap else "ZeroGEX exhaustion state.")

        near_flip = (abs(ctx.current_price - ctx.gamma_flip) / max(ctx.current_price, 1e-6)) <= 0.003 if ctx.gamma_flip else False
        vol_raw = 1 if (ctx.net_gex < 0 or near_flip or abs(ctx.vwap_deviation_pct) > 1.0) else 0
        add("vol_instability", "Volatility Instability", vol_raw, 1.0 if vol_raw else 0.0, "Combined vol expansion proxy.")

        mx = sum(WEIGHTS.values())
        normalized = round(abs(total) / mx, 4) if mx else 0.0
        direction = self._to_direction(total)
        strength = self._to_strength(normalized)
        regime = "pin" if ctx.net_gex > 0 else "expand"

        if direction == "neutral" or strength == "low":
            trade_type, timeframe = "no_trade", "intraday"
        elif direction == "bullish" and ctx.net_gex > 0:
            trade_type, timeframe = "bull_put_credit", "intraday"
        elif direction == "bullish":
            trade_type, timeframe = "bull_call_debit", "swing"
        elif ctx.net_gex > 0:
            trade_type, timeframe = "bear_call_credit", "intraday"
        else:
            trade_type, timeframe = "bear_put_debit", "swing"

        return ScoreSnapshot(
            underlying=self.db_symbol,
            timestamp=ctx.timestamp,
            composite_score=total,
            max_possible_score=mx,
            normalized_score=normalized,
            direction=direction,
            strength=strength,
            regime=regime,
            recommended_trade_type=trade_type,
            recommended_timeframe=timeframe,
            components=components,
        )

    def store_score(self, snapshot: ScoreSnapshot) -> None:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO signal_scores (
                    underlying, timestamp, composite_score, max_possible_score,
                    normalized_score, direction, strength, regime,
                    recommended_trade_type, recommended_timeframe, components
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (underlying, timestamp) DO UPDATE SET
                    composite_score = EXCLUDED.composite_score,
                    max_possible_score = EXCLUDED.max_possible_score,
                    normalized_score = EXCLUDED.normalized_score,
                    direction = EXCLUDED.direction,
                    strength = EXCLUDED.strength,
                    regime = EXCLUDED.regime,
                    recommended_trade_type = EXCLUDED.recommended_trade_type,
                    recommended_timeframe = EXCLUDED.recommended_timeframe,
                    components = EXCLUDED.components,
                    updated_at = NOW()
                """,
                (
                    snapshot.underlying,
                    snapshot.timestamp,
                    snapshot.composite_score,
                    snapshot.max_possible_score,
                    snapshot.normalized_score,
                    snapshot.direction,
                    snapshot.strength,
                    snapshot.regime,
                    snapshot.recommended_trade_type,
                    snapshot.recommended_timeframe,
                    json.dumps([asdict(c) for c in snapshot.components], default=str),
                ),
            )
            conn.commit()

    def run_cycle(self) -> Optional[ScoreSnapshot]:
        ctx = self._fetch_context()
        if ctx is None:
            logger.warning("SignalScoringEngine: no context available")
            return None
        snapshot = self.compute_score(ctx)
        self.store_score(snapshot)
        return snapshot
