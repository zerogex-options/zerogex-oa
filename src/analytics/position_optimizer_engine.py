"""Position optimizer engine for risk-adjusted options spread selection."""

import argparse
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

import pytz

from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)
ET = pytz.timezone("US/Eastern")

POSITION_OPTIMIZER_WEIGHTS: dict[str, int] = {
    "cost_efficiency": 3,
    "probability_of_profit": 4,
    "risk_reward": 3,
    "greek_alignment": 2,
    "liquidity": 2,
    "market_structure": 2,
    "edge_quality": 3,
}

CADENCE_POP_DEFAULTS = {
    "high": 0.66,
    "medium": 0.58,
    "low": 0.52,
}

TARGET_DTE_WINDOWS = {
    "intraday": (0, 2),
    "swing": (1, 7),
    "multi_day": (3, 14),
}

RISK_PROFILE_BUDGETS = {
    "conservative": 0.010,
    "optimal": 0.020,
    "aggressive": 0.035,
}

ASSUMED_ACCOUNT_EQUITY = 100_000.0
KELLY_FRACTION = 0.25
MAX_CANDIDATES = 3


@dataclass
class CandidateComponent:
    name: str
    weight: int
    raw_score: int
    weighted_score: int
    description: str
    value: Optional[float]


@dataclass
class SizingProfile:
    profile: str
    contracts: int
    max_risk_dollars: float
    expected_value_dollars: float
    constrained_by: str


@dataclass
class SpreadCandidate:
    rank: int
    strategy_type: str
    expiry: date
    dte: int
    strikes: str
    option_type: str
    entry_debit: float
    entry_credit: float
    width: float
    max_profit: float
    max_loss: float
    risk_reward_ratio: float
    probability_of_profit: float
    expected_value: float
    sharpe_like_ratio: float
    liquidity_score: float
    net_delta: float
    net_gamma: float
    net_theta: float
    premium_efficiency: float
    market_structure_fit: float
    greek_alignment_score: float
    edge_score: float
    kelly_fraction: float
    sizing_profiles: list[SizingProfile] = field(default_factory=list)
    components: list[CandidateComponent] = field(default_factory=list)
    reasoning: list[str] = field(default_factory=list)


@dataclass
class PositionOptimizerContext:
    timestamp: datetime
    signal_timestamp: datetime
    signal_timeframe: str
    signal_direction: str
    signal_strength: str
    trade_type: str
    current_price: float
    net_gex: float
    gamma_flip: Optional[float]
    put_call_ratio: float
    max_pain: Optional[float]
    smart_call_premium: float
    smart_put_premium: float
    dealer_net_delta: float
    target_dte_min: int
    target_dte_max: int
    option_rows: list[dict] = field(default_factory=list)


@dataclass
class PositionOptimizerSignal:
    underlying: str
    timestamp: datetime
    signal_timestamp: datetime
    signal_timeframe: str
    signal_direction: str
    signal_strength: str
    trade_type: str
    current_price: float
    composite_score: float
    max_possible_score: int
    normalized_score: float
    top_strategy_type: str
    top_expiry: date
    top_dte: int
    top_strikes: str
    top_probability_of_profit: float
    top_expected_value: float
    top_max_profit: float
    top_max_loss: float
    top_kelly_fraction: float
    top_sharpe_like_ratio: float
    top_liquidity_score: float
    top_market_structure_fit: float
    top_reasoning: list[str] = field(default_factory=list)
    candidates: list[SpreadCandidate] = field(default_factory=list)


@dataclass
class PositionOptimizerAccuracySnapshot:
    realized_move_pct: float
    realized_direction: str
    realized_close: float
    proxy_return_pct: float
    profitable: bool


class PositionOptimizerEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self._last_accuracy_update: Optional[date] = None

    def _fetch_context(self, as_of: Optional[datetime] = None) -> Optional[PositionOptimizerContext]:
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                anchor_ts = as_of

                if anchor_ts is None:
                    cur.execute(
                        """
                        SELECT timestamp, timeframe, direction, strength, trade_type
                        FROM trade_signals
                        WHERE underlying = %s
                        ORDER BY timestamp DESC,
                                 CASE timeframe
                                   WHEN 'intraday' THEN 1
                                   WHEN 'swing' THEN 2
                                   ELSE 3
                                 END ASC
                        LIMIT 1
                        """,
                        (self.db_symbol,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT timestamp, timeframe, direction, strength, trade_type
                        FROM trade_signals
                        WHERE underlying = %s
                          AND timestamp <= %s
                        ORDER BY timestamp DESC,
                                 CASE timeframe
                                   WHEN 'intraday' THEN 1
                                   WHEN 'swing' THEN 2
                                   ELSE 3
                                 END ASC
                        LIMIT 1
                        """,
                        (self.db_symbol, anchor_ts),
                    )
                signal_row = cur.fetchone()
                if not signal_row:
                    logger.warning("PositionOptimizerEngine: no trade_signals rows found")
                    return None
                signal_ts, signal_timeframe, signal_direction, signal_strength, trade_type = signal_row
                anchor_ts = anchor_ts or signal_ts

                cur.execute(
                    """
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s
                      AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, anchor_ts),
                )
                price_row = cur.fetchone()
                if not price_row:
                    logger.warning("PositionOptimizerEngine: no underlying price rows found")
                    return None
                current_price = float(price_row[0])

                cur.execute(
                    """
                    SELECT total_net_gex, gamma_flip_point, put_call_ratio, max_pain
                    FROM gex_summary
                    WHERE underlying = %s
                      AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, anchor_ts),
                )
                gex_row = cur.fetchone()
                if not gex_row:
                    logger.warning("PositionOptimizerEngine: no gex_summary rows found")
                    return None
                net_gex, gamma_flip, pcr, max_pain = gex_row

                cur.execute(
                    """
                    SELECT option_type, SUM(total_premium)
                    FROM flow_smart_money
                    WHERE symbol = %s
                      AND timestamp BETWEEN %s - INTERVAL '30 minutes' AND %s
                    GROUP BY option_type
                    """,
                    (self.db_symbol, anchor_ts, anchor_ts),
                )
                smart_call = smart_put = 0.0
                for option_type, premium in cur.fetchall():
                    if option_type == "C":
                        smart_call = float(premium or 0.0)
                    elif option_type == "P":
                        smart_put = float(premium or 0.0)

                dte_min, dte_max = TARGET_DTE_WINDOWS.get(signal_timeframe, (1, 7))
                trade_date = anchor_ts.astimezone(ET).date() if anchor_ts.tzinfo else anchor_ts.date()
                cur.execute(
                    """
                    SELECT MAX(timestamp)
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp <= %s
                      AND expiration BETWEEN (%s::date + (%s * INTERVAL '1 day'))
                                          AND (%s::date + (%s * INTERVAL '1 day'))
                      AND (
                          (bid IS NOT NULL AND ask IS NOT NULL AND ask > 0)
                          OR (last IS NOT NULL AND last > 0)
                      )
                    """,
                    (self.db_symbol, anchor_ts, trade_date, dte_min, trade_date, dte_max),
                )
                snapshot_row = cur.fetchone()
                snapshot_ts = snapshot_row[0] if snapshot_row else None

                if snapshot_ts is not None:
                    cur.execute(
                        """
                        SELECT SUM(delta * open_interest * 100)
                        FROM option_chains
                        WHERE underlying = %s
                          AND timestamp = %s
                          AND delta IS NOT NULL
                          AND open_interest > 0
                        """,
                        (self.db_symbol, snapshot_ts),
                    )
                    delta_row = cur.fetchone()
                    dealer_net_delta = -(float(delta_row[0]) if delta_row and delta_row[0] else 0.0)
                else:
                    dealer_net_delta = 0.0

                cur.execute(
                    """
                    SELECT
                        expiration,
                        strike,
                        option_type,
                        bid,
                        ask,
                        last,
                        delta,
                        gamma,
                        theta,
                        implied_volatility,
                        volume,
                        open_interest
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp = %s
                      AND expiration BETWEEN (%s::date + (%s * INTERVAL '1 day'))
                                          AND (%s::date + (%s * INTERVAL '1 day'))
                      AND (
                          (bid IS NOT NULL AND ask IS NOT NULL AND ask > 0)
                          OR (last IS NOT NULL AND last > 0)
                      )
                    ORDER BY expiration, option_type, strike
                    """,
                    (self.db_symbol, snapshot_ts, trade_date, dte_min, trade_date, dte_max),
                )
                raw_option_rows = cur.fetchall()

                if not raw_option_rows:
                    cur.execute(
                        """
                        SELECT option_symbol
                        FROM option_chains
                        WHERE underlying = %s
                          AND timestamp <= %s
                          AND expiration BETWEEN (%s::date + (%s * INTERVAL '1 day'))
                                              AND (%s::date + (%s * INTERVAL '1 day'))
                        ORDER BY timestamp DESC, option_symbol
                        LIMIT 5
                        """,
                        (self.db_symbol, anchor_ts, trade_date, dte_min, trade_date, dte_max),
                    )
                    candidate_contracts = [row[0] for row in cur.fetchall()]
                    logger.warning(
                        "PositionOptimizerEngine: no option rows in %s DTE window (%s-%s) at snapshot=%s; "
                        "candidate contracts=%s; widening window",
                        signal_timeframe,
                        dte_min,
                        dte_max,
                        snapshot_ts,
                        ", ".join(candidate_contracts) if candidate_contracts else "none",
                    )
                    cur.execute(
                        """
                        SELECT MAX(timestamp)
                        FROM option_chains
                        WHERE underlying = %s
                          AND timestamp <= %s
                          AND expiration BETWEEN %s::date AND (%s::date + INTERVAL '45 day')
                          AND (
                              (bid IS NOT NULL AND ask IS NOT NULL AND ask > 0)
                              OR (last IS NOT NULL AND last > 0)
                          )
                        """,
                        (self.db_symbol, anchor_ts, trade_date, trade_date),
                    )
                    snapshot_row = cur.fetchone()
                    snapshot_ts = snapshot_row[0] if snapshot_row else None
                    cur.execute(
                        """
                        SELECT
                            expiration,
                            strike,
                            option_type,
                            bid,
                            ask,
                            last,
                            delta,
                            gamma,
                            theta,
                            implied_volatility,
                            volume,
                            open_interest
                        FROM option_chains
                        WHERE underlying = %s
                          AND timestamp = %s
                          AND expiration BETWEEN %s::date AND (%s::date + INTERVAL '45 day')
                          AND (
                              (bid IS NOT NULL AND ask IS NOT NULL AND ask > 0)
                              OR (last IS NOT NULL AND last > 0)
                          )
                        ORDER BY expiration, option_type, strike
                        """,
                        (self.db_symbol, snapshot_ts, trade_date, trade_date),
                    )
                    raw_option_rows = cur.fetchall()

                option_rows = []
                for row in raw_option_rows:
                    option_rows.append(
                        {
                            "expiration": row[0],
                            "strike": float(row[1]),
                            "option_type": row[2],
                            "bid": float(row[3] or 0.0),
                            "ask": float(row[4] or 0.0),
                            "last": float(row[5] or 0.0),
                            "delta": float(row[6] or 0.0),
                            "gamma": float(row[7] or 0.0),
                            "theta": float(row[8] or 0.0),
                            "iv": float(row[9] or 0.0),
                            "volume": int(row[10] or 0),
                            "open_interest": int(row[11] or 0),
                        }
                    )
                if not option_rows:
                    logger.warning("PositionOptimizerEngine: no option rows in target expiry window")
                    return None

                available_dtes = sorted({max((row["expiration"] - trade_date).days, 0) for row in option_rows})
                effective_dte_min = available_dtes[0] if available_dtes else dte_min
                effective_dte_max = available_dtes[-1] if available_dtes else dte_max

                return PositionOptimizerContext(
                    timestamp=anchor_ts,
                    signal_timestamp=signal_ts,
                    signal_timeframe=signal_timeframe,
                    signal_direction=signal_direction,
                    signal_strength=signal_strength,
                    trade_type=trade_type,
                    current_price=current_price,
                    net_gex=float(net_gex or 0.0),
                    gamma_flip=float(gamma_flip) if gamma_flip is not None else None,
                    put_call_ratio=float(pcr or 1.0),
                    max_pain=float(max_pain) if max_pain is not None else None,
                    smart_call_premium=smart_call,
                    smart_put_premium=smart_put,
                    dealer_net_delta=dealer_net_delta,
                    target_dte_min=effective_dte_min,
                    target_dte_max=effective_dte_max,
                    option_rows=option_rows,
                )
        except Exception as exc:
            logger.error("PositionOptimizerEngine._fetch_context failed: %s", exc, exc_info=True)
            return None

    @staticmethod
    def _mid(row: dict) -> float:
        bid = row["bid"]
        ask = row["ask"]
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return max(row.get("last", 0.0), ask, bid, 0.0)

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(value, high))

    def _liquidity_score(self, short_leg: dict, long_leg: dict) -> tuple[float, str]:
        mids = [max(self._mid(short_leg), 0.01), max(self._mid(long_leg), 0.01)]
        widths = [(short_leg["ask"] - short_leg["bid"]) / mids[0], (long_leg["ask"] - long_leg["bid"]) / mids[1]]
        avg_width = sum(max(w, 0.0) for w in widths) / len(widths)
        volume_oi = short_leg["volume"] + long_leg["volume"] + short_leg["open_interest"] + long_leg["open_interest"]
        width_score = self._clamp(1.0 - avg_width, 0.0, 1.0)
        depth_bonus = self._clamp(math.log10(max(volume_oi, 1)) / 4.0, 0.0, 1.0)
        score = round((width_score * 0.7 + depth_bonus * 0.3), 4)
        desc = f"Average relative bid/ask width is {avg_width:.2%}; combined vol+OI {volume_oi}."
        return score, desc

    def _structure_adjustment(self, ctx: PositionOptimizerContext, strategy_type: str, short_strike: float, long_strike: float) -> tuple[float, str]:
        adjustments = 0.0
        reasons = []
        if ctx.gamma_flip:
            distance = abs(ctx.current_price - ctx.gamma_flip) / ctx.gamma_flip
            if distance < 0.01:
                adjustments += 0.03 if ctx.net_gex < 0 else -0.02
                reasons.append("gamma flip proximity")
        if ctx.max_pain:
            distance_mp = abs(ctx.current_price - ctx.max_pain) / ctx.max_pain
            if distance_mp < 0.01 and "credit" in strategy_type:
                adjustments += 0.02 if ctx.net_gex > 0 else -0.01
                reasons.append("max pain magnet")
        flow_bias = ctx.smart_call_premium - ctx.smart_put_premium
        if ctx.signal_direction == "bullish" and flow_bias > 0:
            adjustments += 0.02
            reasons.append("bullish smart-money flow")
        elif ctx.signal_direction == "bearish" and flow_bias < 0:
            adjustments += 0.02
            reasons.append("bearish smart-money flow")
        elif ctx.signal_direction == "neutral" and abs(flow_bias) < 5_000_000:
            adjustments += 0.01
            reasons.append("balanced flow")
        if ctx.signal_direction == "bullish" and short_strike < ctx.current_price < long_strike:
            adjustments -= 0.01
        if ctx.signal_direction == "bearish" and long_strike < ctx.current_price < short_strike:
            adjustments -= 0.01
        return adjustments, ", ".join(reasons) if reasons else "baseline market-structure fit"

    def _kelly_fraction(self, probability: float, max_profit: float, max_loss: float) -> float:
        if max_profit <= 0 or max_loss <= 0:
            return 0.0
        b = max_profit / max_loss
        q = 1.0 - probability
        k = (b * probability - q) / b if b > 0 else 0.0
        return round(self._clamp(k, 0.0, 1.0) * KELLY_FRACTION, 4)

    def _build_sizing_profiles(self, candidate: SpreadCandidate) -> list[SizingProfile]:
        effective_risk = max(candidate.max_loss, candidate.entry_debit, 1.0)
        sizing = []
        for profile, heat_pct in RISK_PROFILE_BUDGETS.items():
            budget = ASSUMED_ACCOUNT_EQUITY * heat_pct
            kelly_adjusted_budget = max(budget * max(candidate.kelly_fraction, 0.10), min(budget, effective_risk))
            contracts = max(1, int(kelly_adjusted_budget // effective_risk)) if candidate.expected_value > 0 else 0
            constrained_by = "edge filter" if candidate.expected_value <= 0 else (
                "kelly fraction" if kelly_adjusted_budget < budget else "portfolio heat cap"
            )
            sizing.append(
                SizingProfile(
                    profile=profile,
                    contracts=contracts,
                    max_risk_dollars=round(contracts * effective_risk, 2),
                    expected_value_dollars=round(contracts * candidate.expected_value, 2),
                    constrained_by=constrained_by,
                )
            )
        return sizing

    def _score_candidate(
        self,
        ctx: PositionOptimizerContext,
        strategy_type: str,
        expiry: date,
        option_type: str,
        short_leg: dict,
        long_leg: dict,
        iron_call: Optional[tuple[dict, dict]] = None,
    ) -> Optional[SpreadCandidate]:
        dte = max((expiry - (ctx.timestamp.astimezone(ET).date() if ctx.timestamp.tzinfo else ctx.timestamp.date())).days, 0)
        width = abs(short_leg["strike"] - long_leg["strike"])
        if width <= 0:
            return None

        debit = credit = 0.0
        max_profit = max_loss = 0.0
        net_delta = net_gamma = net_theta = 0.0
        strikes_label = ""
        reasoning = []

        if strategy_type == "bull_call_debit":
            long_call, short_call = long_leg, short_leg
            debit = max(self._mid(long_call) - self._mid(short_call), 0.01) * 100.0
            max_profit = max(width * 100.0 - debit, 0.0)
            max_loss = debit
            short_strike, long_strike = short_call["strike"], long_call["strike"]
            net_delta = (long_call["delta"] - short_call["delta"]) * 100.0
            net_gamma = (long_call["gamma"] - short_call["gamma"]) * 100.0
            net_theta = (long_call["theta"] - short_call["theta"]) * 100.0
            base_pop = self._clamp(abs(long_call["delta"]), 0.15, 0.85)
            strikes_label = f"Long {long_call['strike']:.0f}C / Short {short_call['strike']:.0f}C"
        elif strategy_type == "bear_put_debit":
            long_put, short_put = long_leg, short_leg
            debit = max(self._mid(long_put) - self._mid(short_put), 0.01) * 100.0
            max_profit = max(width * 100.0 - debit, 0.0)
            max_loss = debit
            short_strike, long_strike = short_put["strike"], long_put["strike"]
            net_delta = (long_put["delta"] - short_put["delta"]) * 100.0
            net_gamma = (long_put["gamma"] - short_put["gamma"]) * 100.0
            net_theta = (long_put["theta"] - short_put["theta"]) * 100.0
            base_pop = self._clamp(abs(long_put["delta"]), 0.15, 0.85)
            strikes_label = f"Long {long_put['strike']:.0f}P / Short {short_put['strike']:.0f}P"
        elif strategy_type == "bull_put_credit":
            short_put, long_put = short_leg, long_leg
            credit = max(self._mid(short_put) - self._mid(long_put), 0.01) * 100.0
            max_profit = credit
            max_loss = max(width * 100.0 - credit, 0.01)
            short_strike, long_strike = short_put["strike"], long_put["strike"]
            net_delta = -(short_put["delta"] - long_put["delta"]) * 100.0
            net_gamma = -(short_put["gamma"] - long_put["gamma"]) * 100.0
            net_theta = -(short_put["theta"] - long_put["theta"]) * 100.0
            base_pop = self._clamp(1.0 - abs(short_put["delta"]), 0.2, 0.92)
            strikes_label = f"Short {short_put['strike']:.0f}P / Long {long_put['strike']:.0f}P"
        elif strategy_type == "bear_call_credit":
            short_call, long_call = short_leg, long_leg
            credit = max(self._mid(short_call) - self._mid(long_call), 0.01) * 100.0
            max_profit = credit
            max_loss = max(width * 100.0 - credit, 0.01)
            short_strike, long_strike = short_call["strike"], long_call["strike"]
            net_delta = -(short_call["delta"] - long_call["delta"]) * 100.0
            net_gamma = -(short_call["gamma"] - long_call["gamma"]) * 100.0
            net_theta = -(short_call["theta"] - long_call["theta"]) * 100.0
            base_pop = self._clamp(1.0 - abs(short_call["delta"]), 0.2, 0.92)
            strikes_label = f"Short {short_call['strike']:.0f}C / Long {long_call['strike']:.0f}C"
        elif strategy_type == "iron_condor" and iron_call is not None:
            short_put, long_put = short_leg, long_leg
            short_call, long_call = iron_call
            put_credit = max(self._mid(short_put) - self._mid(long_put), 0.01)
            call_credit = max(self._mid(short_call) - self._mid(long_call), 0.01)
            credit = (put_credit + call_credit) * 100.0
            call_width = abs(long_call["strike"] - short_call["strike"])
            put_width = abs(short_put["strike"] - long_put["strike"])
            width = max(call_width, put_width)
            max_profit = credit
            max_loss = max(width * 100.0 - credit, 0.01)
            net_delta = (-(short_put["delta"] - long_put["delta"]) - (short_call["delta"] - long_call["delta"])) * 100.0
            net_gamma = (-(short_put["gamma"] - long_put["gamma"]) - (short_call["gamma"] - long_call["gamma"])) * 100.0
            net_theta = (-(short_put["theta"] - long_put["theta"]) - (short_call["theta"] - long_call["theta"])) * 100.0
            base_pop = self._clamp(1.0 - (abs(short_put["delta"]) + abs(short_call["delta"])) / 2.0, 0.2, 0.93)
            short_strike, long_strike = short_put["strike"], short_call["strike"]
            strikes_label = (
                f"Short {short_put['strike']:.0f}P/{short_call['strike']:.0f}C | "
                f"Long {long_put['strike']:.0f}P/{long_call['strike']:.0f}C"
            )
        else:
            return None

        liquidity_score, liquidity_desc = self._liquidity_score(short_leg, long_leg)
        structure_adj, structure_desc = self._structure_adjustment(ctx, strategy_type, short_strike, long_strike)
        probability = self._clamp(base_pop + structure_adj, 0.05, 0.95)
        expected_value = round(probability * max_profit - (1.0 - probability) * max_loss, 2)
        rr_ratio = round(max_profit / max_loss, 4) if max_loss > 0 else 0.0
        premium_efficiency = round(max_profit / max(debit or credit, 1.0), 4)
        market_structure_fit = round(self._clamp(0.5 + structure_adj * 5.0, 0.0, 1.0), 4)
        greek_alignment = round(self._clamp(1.0 - abs(net_theta) / 100.0, 0.0, 1.0) * 0.4 + self._clamp(abs(net_delta) / 50.0, 0.0, 1.0) * (0.6 if ctx.signal_direction != "neutral" else 0.1), 4)
        edge_score = round(self._clamp((expected_value / max(max_loss, 1.0) + probability) / 2.0, 0.0, 1.0), 4)
        sharpe_like = round(expected_value / max(max_loss, 1.0), 4)
        kelly_fraction = self._kelly_fraction(probability, max_profit, max_loss)

        component_values = {
            "cost_efficiency": (premium_efficiency, f"Premium efficiency is {premium_efficiency:.2f}x max-profit per net premium."),
            "probability_of_profit": (probability, f"Estimated POP is {probability:.1%} after market-structure adjustments."),
            "risk_reward": (rr_ratio, f"Max profit / max loss is {rr_ratio:.2f}."),
            "greek_alignment": (greek_alignment, f"Net delta {net_delta:+.1f}, gamma {net_gamma:+.3f}, theta {net_theta:+.2f}."),
            "liquidity": (liquidity_score, liquidity_desc),
            "market_structure": (market_structure_fit, f"Structure fit reflects {structure_desc}."),
            "edge_quality": (edge_score, f"Expected value is ${expected_value:,.2f} per spread with Kelly {kelly_fraction:.2%}."),
        }
        components = []
        total = 0
        for key, (value, desc) in component_values.items():
            normalized = value if key not in {"cost_efficiency", "risk_reward"} else self._clamp(value / 3.0, 0.0, 1.0)
            raw = int(round(self._clamp(normalized, 0.0, 1.0) * 10))
            weight = POSITION_OPTIMIZER_WEIGHTS[key]
            weighted = raw * weight
            total += weighted
            components.append(CandidateComponent(key.replace("_", " ").title(), weight, raw, weighted, desc, round(value, 4) if isinstance(value, float) else value))

        reasoning.extend([
            f"{strategy_type} targets the {ctx.signal_direction} {ctx.signal_timeframe} signal from {ctx.signal_timestamp.isoformat()}.",
            f"POP {probability:.1%} with EV ${expected_value:,.2f} and max loss ${max_loss:,.2f}.",
            f"Liquidity {liquidity_score:.2f}; market structure fit {market_structure_fit:.2f}; premium efficiency {premium_efficiency:.2f}.",
        ])
        candidate = SpreadCandidate(
            rank=0,
            strategy_type=strategy_type,
            expiry=expiry,
            dte=dte,
            strikes=strikes_label,
            option_type=option_type,
            entry_debit=round(debit, 2),
            entry_credit=round(credit, 2),
            width=round(width, 2),
            max_profit=round(max_profit, 2),
            max_loss=round(max_loss, 2),
            risk_reward_ratio=rr_ratio,
            probability_of_profit=round(probability, 4),
            expected_value=expected_value,
            sharpe_like_ratio=sharpe_like,
            liquidity_score=liquidity_score,
            net_delta=round(net_delta, 4),
            net_gamma=round(net_gamma, 6),
            net_theta=round(net_theta, 4),
            premium_efficiency=premium_efficiency,
            market_structure_fit=market_structure_fit,
            greek_alignment_score=greek_alignment,
            edge_score=edge_score,
            kelly_fraction=kelly_fraction,
            components=components,
            reasoning=reasoning,
        )
        candidate.sizing_profiles = self._build_sizing_profiles(candidate)
        return candidate

    def _generate_candidates(self, ctx: PositionOptimizerContext) -> list[SpreadCandidate]:
        grouped: dict[tuple[date, str], list[dict]] = {}
        for row in ctx.option_rows:
            grouped.setdefault((row["expiration"], row["option_type"]), []).append(row)

        candidates: list[SpreadCandidate] = []

        for (expiry, option_type), rows in grouped.items():
            rows = sorted(rows, key=lambda x: x["strike"])
            calls = rows if option_type == "C" else []
            puts = rows if option_type == "P" else []

            if ctx.signal_direction == "bullish" and option_type == "C":
                atm_calls = [r for r in calls if r["strike"] >= ctx.current_price * 0.98 and r["strike"] <= ctx.current_price * 1.03]
                for i in range(min(len(atm_calls), max(len(calls) - 1, 0))):
                    long_call = atm_calls[i]
                    higher = [r for r in calls if r["strike"] > long_call["strike"]][:3]
                    for short_call in higher:
                        cand = self._score_candidate(ctx, "bull_call_debit", expiry, "C", short_call, long_call)
                        if cand:
                            candidates.append(cand)
            if ctx.signal_direction == "bullish" and option_type == "P":
                otm_puts = [r for r in puts if r["strike"] < ctx.current_price]
                for idx, short_put in enumerate(reversed(otm_puts[-6:])):
                    lower = [r for r in otm_puts if r["strike"] < short_put["strike"]][-3:]
                    for long_put in lower:
                        cand = self._score_candidate(ctx, "bull_put_credit", expiry, "P", short_put, long_put)
                        if cand:
                            candidates.append(cand)
            if ctx.signal_direction == "bearish" and option_type == "P":
                atm_puts = [r for r in puts if r["strike"] <= ctx.current_price * 1.02 and r["strike"] >= ctx.current_price * 0.97]
                for i in range(min(len(atm_puts), max(len(puts) - 1, 0))):
                    long_put = list(reversed(atm_puts))[i]
                    lower = [r for r in puts if r["strike"] < long_put["strike"]][-3:]
                    for short_put in reversed(lower):
                        cand = self._score_candidate(ctx, "bear_put_debit", expiry, "P", short_put, long_put)
                        if cand:
                            candidates.append(cand)
            if ctx.signal_direction == "bearish" and option_type == "C":
                otm_calls = [r for r in calls if r["strike"] > ctx.current_price]
                for short_call in otm_calls[:6]:
                    higher = [r for r in otm_calls if r["strike"] > short_call["strike"]][:3]
                    for long_call in higher:
                        cand = self._score_candidate(ctx, "bear_call_credit", expiry, "C", short_call, long_call)
                        if cand:
                            candidates.append(cand)

        if ctx.signal_direction == "neutral":
            expiries = sorted({row["expiration"] for row in ctx.option_rows})
            for expiry in expiries:
                puts = sorted([r for r in ctx.option_rows if r["expiration"] == expiry and r["option_type"] == "P" and r["strike"] < ctx.current_price], key=lambda x: x["strike"])
                calls = sorted([r for r in ctx.option_rows if r["expiration"] == expiry and r["option_type"] == "C" and r["strike"] > ctx.current_price], key=lambda x: x["strike"])
                if len(puts) < 2 or len(calls) < 2:
                    continue
                for short_put in reversed(puts[-4:]):
                    long_puts = [r for r in puts if r["strike"] < short_put["strike"]][-2:]
                    for short_call in calls[:4]:
                        long_calls = [r for r in calls if r["strike"] > short_call["strike"]][:2]
                        for long_put in long_puts:
                            for long_call in long_calls:
                                cand = self._score_candidate(ctx, "iron_condor", expiry, "IC", short_put, long_put, iron_call=(short_call, long_call))
                                if cand:
                                    candidates.append(cand)

        filtered = [c for c in candidates if c.max_profit > 0 and c.max_loss > 0 and c.probability_of_profit >= 0.25]
        filtered.sort(key=lambda c: (c.edge_score, c.expected_value, c.probability_of_profit, c.liquidity_score), reverse=True)
        top = filtered[:MAX_CANDIDATES]
        for idx, candidate in enumerate(top, start=1):
            candidate.rank = idx
        return top

    def compute_signal(self, ctx: PositionOptimizerContext) -> Optional[PositionOptimizerSignal]:
        candidates = self._generate_candidates(ctx)
        if not candidates:
            return None
        best = candidates[0]
        composite = sum(component.weighted_score for component in best.components)
        max_possible = sum(weight * 10 for weight in POSITION_OPTIMIZER_WEIGHTS.values())
        normalized = round(composite / max_possible, 4) if max_possible else 0.0
        return PositionOptimizerSignal(
            underlying=self.db_symbol,
            timestamp=ctx.timestamp,
            signal_timestamp=ctx.signal_timestamp,
            signal_timeframe=ctx.signal_timeframe,
            signal_direction=ctx.signal_direction,
            signal_strength=ctx.signal_strength,
            trade_type=ctx.trade_type,
            current_price=ctx.current_price,
            composite_score=float(composite),
            max_possible_score=max_possible,
            normalized_score=normalized,
            top_strategy_type=best.strategy_type,
            top_expiry=best.expiry,
            top_dte=best.dte,
            top_strikes=best.strikes,
            top_probability_of_profit=best.probability_of_profit,
            top_expected_value=best.expected_value,
            top_max_profit=best.max_profit,
            top_max_loss=best.max_loss,
            top_kelly_fraction=best.kelly_fraction,
            top_sharpe_like_ratio=best.sharpe_like_ratio,
            top_liquidity_score=best.liquidity_score,
            top_market_structure_fit=best.market_structure_fit,
            top_reasoning=best.reasoning,
            candidates=candidates,
        )

    def _store_signal(self, signal: PositionOptimizerSignal) -> None:
        candidates_json = json.dumps([asdict(candidate) for candidate in signal.candidates], default=str)
        top_reasoning_json = json.dumps(signal.top_reasoning)
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO position_optimizer_signals (
                        underlying, timestamp, signal_timestamp, signal_timeframe, signal_direction,
                        signal_strength, trade_type, current_price, composite_score, max_possible_score,
                        normalized_score, top_strategy_type, top_expiry, top_dte, top_strikes,
                        top_probability_of_profit, top_expected_value, top_max_profit, top_max_loss,
                        top_kelly_fraction, top_sharpe_like_ratio, top_liquidity_score,
                        top_market_structure_fit, top_reasoning, candidates
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (underlying, timestamp) DO UPDATE SET
                        signal_timestamp = EXCLUDED.signal_timestamp,
                        signal_timeframe = EXCLUDED.signal_timeframe,
                        signal_direction = EXCLUDED.signal_direction,
                        signal_strength = EXCLUDED.signal_strength,
                        trade_type = EXCLUDED.trade_type,
                        current_price = EXCLUDED.current_price,
                        composite_score = EXCLUDED.composite_score,
                        max_possible_score = EXCLUDED.max_possible_score,
                        normalized_score = EXCLUDED.normalized_score,
                        top_strategy_type = EXCLUDED.top_strategy_type,
                        top_expiry = EXCLUDED.top_expiry,
                        top_dte = EXCLUDED.top_dte,
                        top_strikes = EXCLUDED.top_strikes,
                        top_probability_of_profit = EXCLUDED.top_probability_of_profit,
                        top_expected_value = EXCLUDED.top_expected_value,
                        top_max_profit = EXCLUDED.top_max_profit,
                        top_max_loss = EXCLUDED.top_max_loss,
                        top_kelly_fraction = EXCLUDED.top_kelly_fraction,
                        top_sharpe_like_ratio = EXCLUDED.top_sharpe_like_ratio,
                        top_liquidity_score = EXCLUDED.top_liquidity_score,
                        top_market_structure_fit = EXCLUDED.top_market_structure_fit,
                        top_reasoning = EXCLUDED.top_reasoning,
                        candidates = EXCLUDED.candidates,
                        updated_at = NOW()
                    """,
                    (
                        signal.underlying,
                        signal.timestamp,
                        signal.signal_timestamp,
                        signal.signal_timeframe,
                        signal.signal_direction,
                        signal.signal_strength,
                        signal.trade_type,
                        signal.current_price,
                        signal.composite_score,
                        signal.max_possible_score,
                        signal.normalized_score,
                        signal.top_strategy_type,
                        signal.top_expiry,
                        signal.top_dte,
                        signal.top_strikes,
                        signal.top_probability_of_profit,
                        signal.top_expected_value,
                        signal.top_max_profit,
                        signal.top_max_loss,
                        signal.top_kelly_fraction,
                        signal.top_sharpe_like_ratio,
                        signal.top_liquidity_score,
                        signal.top_market_structure_fit,
                        top_reasoning_json,
                        candidates_json,
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.error("PositionOptimizerEngine._store_signal failed: %s", exc, exc_info=True)

    @staticmethod
    def _extract_strikes(candidate: dict) -> list[float]:
        import re
        return [float(match) for match in re.findall(r"(\d+(?:\.\d+)?)", candidate.get("strikes", ""))]

    def _proxy_realized_return(self, candidate: dict, close_px: float) -> float:
        strategy = candidate.get("strategy_type")
        entry_debit = float(candidate.get("entry_debit") or 0.0)
        entry_credit = float(candidate.get("entry_credit") or 0.0)
        max_loss = max(float(candidate.get("max_loss") or 0.0), 0.01)
        parsed = self._extract_strikes(candidate)
        if strategy == "bull_call_debit" and len(parsed) >= 2:
            long_strike, short_strike = parsed[0], parsed[1]
            intrinsic = max(min(close_px - long_strike, short_strike - long_strike), 0.0) * 100.0
            pnl = intrinsic - entry_debit
            return round((pnl / max_loss) * 100.0, 2)
        if strategy == "bear_put_debit" and len(parsed) >= 2:
            long_strike, short_strike = parsed[0], parsed[1]
            intrinsic = max(min(long_strike - close_px, long_strike - short_strike), 0.0) * 100.0
            pnl = intrinsic - entry_debit
            return round((pnl / max_loss) * 100.0, 2)
        if strategy == "bull_put_credit" and len(parsed) >= 2:
            short_strike, long_strike = parsed[0], parsed[1]
            intrinsic_loss = max(long_strike - close_px, 0.0) - max(short_strike - close_px, 0.0)
            pnl = entry_credit - abs(intrinsic_loss * 100.0)
            return round((pnl / max_loss) * 100.0, 2)
        if strategy == "bear_call_credit" and len(parsed) >= 2:
            short_strike, long_strike = parsed[0], parsed[1]
            intrinsic_loss = max(close_px - short_strike, 0.0) - max(close_px - long_strike, 0.0)
            pnl = entry_credit - abs(intrinsic_loss * 100.0)
            return round((pnl / max_loss) * 100.0, 2)
        if strategy == "iron_condor" and len(parsed) >= 4:
            short_put, short_call, long_put, long_call = parsed[0], parsed[1], parsed[2], parsed[3]
            put_loss = max(long_put - close_px, 0.0) - max(short_put - close_px, 0.0)
            call_loss = max(close_px - short_call, 0.0) - max(close_px - long_call, 0.0)
            pnl = entry_credit - abs(max(put_loss, call_loss) * 100.0)
            return round((pnl / max_loss) * 100.0, 2)
        return 0.0

    def _snapshot_accuracy(self, signal_ts: datetime, candidate: dict) -> Optional[PositionOptimizerAccuracySnapshot]:
        trade_date = signal_ts.astimezone(ET).date() if signal_ts.tzinfo else signal_ts.date()
        next_date = trade_date + timedelta(days=1)
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    WITH day_quotes AS (
                        SELECT close, high, low, timestamp
                        FROM underlying_quotes
                        WHERE symbol = %s
                          AND DATE(timestamp AT TIME ZONE 'America/New_York') = %s
                    )
                    SELECT
                        (SELECT close FROM day_quotes ORDER BY timestamp ASC LIMIT 1),
                        (SELECT close FROM day_quotes ORDER BY timestamp DESC LIMIT 1),
                        (SELECT MAX(high) FROM day_quotes),
                        (SELECT MIN(low) FROM day_quotes)
                    """,
                    (self.db_symbol, next_date),
                )
                row = cur.fetchone()
                if not row or row[0] is None or row[1] is None:
                    return None
                open_px, close_px, high_px, low_px = map(float, row)
                realized_move_pct = max(abs(high_px - open_px), abs(low_px - open_px), abs(close_px - open_px)) / open_px * 100.0
                realized_direction = "bullish" if close_px > open_px else ("bearish" if close_px < open_px else "neutral")
                proxy_return = self._proxy_realized_return(candidate, close_px)
                return PositionOptimizerAccuracySnapshot(
                    realized_move_pct=round(realized_move_pct, 4),
                    realized_direction=realized_direction,
                    realized_close=close_px,
                    proxy_return_pct=proxy_return,
                    profitable=proxy_return > 0,
                )
        except Exception as exc:
            logger.error("PositionOptimizerEngine._snapshot_accuracy failed: %s", exc, exc_info=True)
            return None

    def _update_accuracy(self) -> None:
        today = datetime.now(ET).date()
        if self._last_accuracy_update == today:
            return
        eval_date = today - timedelta(days=1)
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT timestamp, signal_direction, signal_strength, top_strategy_type, top_probability_of_profit, top_expected_value, candidates
                    FROM position_optimizer_signals
                    WHERE underlying = %s
                      AND DATE(timestamp AT TIME ZONE 'America/New_York') = %s
                    """,
                    (self.db_symbol, eval_date),
                )
                rows = cur.fetchall()
                if not rows:
                    self._last_accuracy_update = today
                    return
                buckets: dict[tuple[str, str], dict[str, float]] = {}
                for signal_ts, direction, strength, strategy_type, top_pop, top_ev, candidates in rows:
                    candidate_list = json.loads(candidates) if isinstance(candidates, str) else (candidates or [])
                    best_candidate = candidate_list[0] if candidate_list else {"strategy_type": strategy_type}
                    snapshot = self._snapshot_accuracy(signal_ts, best_candidate)
                    if snapshot is None:
                        continue
                    key = (direction, strategy_type)
                    bucket = buckets.setdefault(
                        key,
                        {"total": 0, "profitable": 0, "realized_sum": 0.0, "predicted_pop_sum": 0.0, "expected_ev_sum": 0.0, "realized_move_sum": 0.0},
                    )
                    bucket["total"] += 1
                    bucket["profitable"] += int(snapshot.profitable)
                    bucket["realized_sum"] += snapshot.proxy_return_pct
                    bucket["predicted_pop_sum"] += float(top_pop or CADENCE_POP_DEFAULTS.get(strength, 0.5))
                    bucket["expected_ev_sum"] += float(top_ev or 0.0)
                    bucket["realized_move_sum"] += snapshot.realized_move_pct

                for (direction, strategy_type), stats in buckets.items():
                    total = stats["total"]
                    if total == 0:
                        continue
                    cur.execute(
                        """
                        INSERT INTO position_optimizer_accuracy (
                            underlying, trade_date, signal_direction, strategy_type,
                            total_signals, profitable_signals, avg_realized_return_pct,
                            avg_expected_value, avg_predicted_pop, avg_realized_move_pct
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (underlying, trade_date, signal_direction, strategy_type)
                        DO UPDATE SET
                            total_signals = EXCLUDED.total_signals,
                            profitable_signals = EXCLUDED.profitable_signals,
                            avg_realized_return_pct = EXCLUDED.avg_realized_return_pct,
                            avg_expected_value = EXCLUDED.avg_expected_value,
                            avg_predicted_pop = EXCLUDED.avg_predicted_pop,
                            avg_realized_move_pct = EXCLUDED.avg_realized_move_pct,
                            updated_at = NOW()
                        """,
                        (
                            self.db_symbol,
                            eval_date,
                            direction,
                            strategy_type,
                            total,
                            stats["profitable"],
                            round(stats["realized_sum"] / total, 4),
                            round(stats["expected_ev_sum"] / total, 4),
                            round(stats["predicted_pop_sum"] / total, 4),
                            round(stats["realized_move_sum"] / total, 4),
                        ),
                    )
                conn.commit()
                self._last_accuracy_update = today
        except Exception as exc:
            logger.error("PositionOptimizerEngine._update_accuracy failed: %s", exc, exc_info=True)

    def run_calculation(self) -> bool:
        ctx = self._fetch_context()
        if ctx is None:
            logger.warning("PositionOptimizerEngine: no context available, skipping")
            return False
        signal = self.compute_signal(ctx)
        if signal is None:
            logger.warning("PositionOptimizerEngine: no viable spread candidates generated")
            return False
        self._store_signal(signal)
        self._update_accuracy()
        logger.info(
            "✅ Position optimizer [%s] %s | best=%s | POP=%.0f%% | EV=$%.2f | Kelly=%.1f%%",
            self.underlying,
            signal.signal_direction.upper(),
            signal.top_strategy_type,
            signal.top_probability_of_profit * 100,
            signal.top_expected_value,
            signal.top_kelly_fraction * 100,
        )
        return True


def main() -> None:
    parser = argparse.ArgumentParser(description="ZeroGEX position optimizer engine")
    parser.add_argument("--underlying", default="SPY")
    args = parser.parse_args()
    ok = PositionOptimizerEngine(underlying=args.underlying).run_calculation()
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
