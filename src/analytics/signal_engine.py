"""
ZeroGEX Signal Engine
=====================
Runs on a separate 5-minute interval after the main AnalyticsEngine cycle.

Responsibilities
----------------
1. Pull latest computed data from the DB (gex_summary, gex_by_strike,
   flow_cache_*, underlying_quotes, opening_range_breakout,
   underlying_vwap_deviation).
2. Derive two signals that have no DB view:
     - dealer_net_delta  (from gex_by_strike delta * OI aggregation)
     - unusual_volume    (volume/OI ratio spike detection from option_chains)
3. Score all 9 signal components for each timeframe (intraday/swing/multi_day).
4. Write one trade_signals row per timeframe to the DB.
5. Update signal_accuracy for yesterday's signals once per day.
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from typing import Optional
import pytz
import requests

from src.database import db_connection
from src.utils import get_logger
from src.symbols import get_canonical_symbol

logger = get_logger(__name__)

ET = pytz.timezone("US/Eastern")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SIGNAL_WEIGHTS: dict[str, dict[str, int]] = {
    #                             intraday  swing  multi_day
    "gex_regime":         {"intraday": 3, "swing": 3, "multi_day": 3},
    "dealer_hedging":     {"intraday": 3, "swing": 3, "multi_day": 2},
    "smart_money_flow":   {"intraday": 2, "swing": 2, "multi_day": 2},
    "vwap_position":      {"intraday": 2, "swing": 2, "multi_day": 0},
    "orb_direction":      {"intraday": 2, "swing": 1, "multi_day": 0},
    "put_call_ratio":     {"intraday": 1, "swing": 2, "multi_day": 3},
    "unusual_volume":     {"intraday": 1, "swing": 1, "multi_day": 0},
    "momentum_divergence":{"intraday": 1, "swing": 1, "multi_day": 0},
    "vanna_charm_drift":  {"intraday": 0, "swing": 1, "multi_day": 2},
    "exhaustion_score":   {"intraday": 2, "swing": 2, "multi_day": 1},
}

WIN_PCT_DEFAULTS: dict[str, dict[str, float]] = {
    "intraday":  {"high": 0.68, "medium": 0.60, "low": 0.50},
    "swing":     {"high": 0.65, "medium": 0.58, "low": 0.50},
    "multi_day": {"high": 0.63, "medium": 0.57, "low": 0.50},
}

TARGET_EXPIRY: dict[str, dict[str, str]] = {
    "intraday":  {"high": "0DTE",   "medium": "0DTE",  "low": "0DTE"},
    "swing":     {"high": "1DTE",   "medium": "2DTE",  "low": "2DTE"},
    "multi_day": {"high": "3-5DTE", "medium": "5DTE",  "low": "5DTE"},
}

STRIKE_GUIDANCE: dict[str, dict[str, str]] = {
    "intraday": {
        "bull_credit": "Current price - 0.5% / - 1.0%",
        "bear_credit": "Current price + 0.5% / + 1.0%",
        "bull_debit":  "ATM / ATM + 0.5%",
        "bear_debit":  "ATM / ATM - 0.5%",
    },
    "swing": {
        "bull_credit": "Current price - 0.75% / - 1.5%",
        "bear_credit": "Current price + 0.75% / + 1.5%",
        "bull_debit":  "ATM / ATM + 1.0%",
        "bear_debit":  "ATM / ATM - 1.0%",
    },
    "multi_day": {
        "bull_credit": "Current price - 1.0% / - 2.0%",
        "bear_credit": "Current price + 1.0% / + 2.0%",
        "bull_debit":  "ATM / ATM + 1.5%",
        "bear_debit":  "ATM / ATM - 1.5%",
    },
}

# Calibration knobs (env-overridable) so thresholds can be tuned by symbol/regime.
SMART_MONEY_DOMINANCE_RATIO = float(os.getenv("SIGNAL_SMART_MONEY_DOMINANCE_RATIO", "1.2"))
VWAP_DEV_BULL_THRESHOLD = float(os.getenv("SIGNAL_VWAP_DEV_BULL_THRESHOLD_PCT", "0.2"))
VWAP_DEV_BEAR_THRESHOLD = float(os.getenv("SIGNAL_VWAP_DEV_BEAR_THRESHOLD_PCT", "-0.2"))
PCR_BULLISH_THRESHOLD = float(os.getenv("SIGNAL_PCR_BULLISH_THRESHOLD", "0.7"))
PCR_BEARISH_THRESHOLD = float(os.getenv("SIGNAL_PCR_BEARISH_THRESHOLD", "1.3"))


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class SignalComponent:
    name: str
    weight: int
    score: int
    description: str
    value: Optional[float]
    applicable: bool


@dataclass
class SignalContext:
    """All raw values needed to score signals. Populated from DB queries."""
    timestamp: datetime
    current_price: float = 0.0
    max_gamma_strike: float = 0.0
    net_gex: float = 0.0
    gamma_flip: float = 0.0
    put_call_ratio: float = 1.0
    vwap: float = 0.0
    vwap_deviation_pct: float = 0.0
    orb_status: str = ""           # raw orb_status string from the view
    smart_call_premium: float = 0.0
    smart_put_premium: float = 0.0
    # Derived in Python (no DB view)
    dealer_net_delta: float = 0.0
    unusual_call_volume: bool = False
    # Divergence
    price_change_5min: float = 0.0
    net_option_flow: float = 0.0
    # Greeks
    vanna_exposure: float = 0.0
    charm_exposure: float = 0.0
    recent_closes: list[float] = field(default_factory=list)
    recent_highs: list[float] = field(default_factory=list)
    recent_up_volumes: list[int] = field(default_factory=list)
    recent_down_volumes: list[int] = field(default_factory=list)


@dataclass
class TradeSignal:
    underlying: str
    timestamp: datetime
    timeframe: str
    composite_score: int
    max_possible_score: int
    normalized_score: float
    direction: str
    strength: str
    estimated_win_pct: float
    trade_type: str
    trade_rationale: str
    target_expiry: str
    suggested_strikes: str
    current_price: float
    net_gex: float
    gamma_flip: Optional[float]
    price_vs_flip: Optional[float]
    vwap: Optional[float]
    vwap_deviation_pct: Optional[float]
    put_call_ratio: Optional[float]
    dealer_net_delta: Optional[float]
    smart_money_direction: str
    unusual_volume_detected: bool
    orb_breakout_direction: Optional[str]
    components: list = field(default_factory=list)


@dataclass
class SignalThresholds:
    smart_money_ratio: float = SMART_MONEY_DOMINANCE_RATIO
    vwap_bull_threshold: float = VWAP_DEV_BULL_THRESHOLD
    vwap_bear_threshold: float = VWAP_DEV_BEAR_THRESHOLD
    pcr_bullish_threshold: float = PCR_BULLISH_THRESHOLD
    pcr_bearish_threshold: float = PCR_BEARISH_THRESHOLD


class SignalSmsNotifier:
    """Optional SMS notifier for high-conviction signal hits."""

    def __init__(self, underlying: str):
        self.underlying = underlying.upper()
        self.enabled = os.getenv("SIGNAL_SMS_ENABLED", "false").lower() == "true"
        self.provider = os.getenv("SIGNAL_SMS_PROVIDER", "twilio").lower()
        self.from_number = os.getenv("SIGNAL_SMS_FROM_NUMBER", "").strip()
        self.to_numbers = [
            n.strip() for n in os.getenv("SIGNAL_SMS_TO_NUMBERS", "").split(",") if n.strip()
        ]
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
        self.min_normalized = float(os.getenv("SIGNAL_SMS_MIN_NORMALIZED_SCORE", "0.67"))
        self.min_strength = os.getenv("SIGNAL_SMS_MIN_STRENGTH", "high").lower()
        self.allowed_timeframes = {
            tf.strip() for tf in os.getenv("SIGNAL_SMS_TIMEFRAMES", "intraday,swing").split(",")
            if tf.strip()
        }
        self.cooldown_seconds = int(os.getenv("SIGNAL_SMS_COOLDOWN_SECONDS", "900"))
        self._last_sent_by_key: dict[str, float] = {}

    def maybe_send(self, sig: TradeSignal) -> None:
        if not self.enabled:
            return
        if self.provider != "twilio":
            logger.warning("SignalSmsNotifier: unsupported provider=%s", self.provider)
            return
        if not self._is_eligible(sig):
            return
        if not (self.account_sid and self.auth_token and self.from_number and self.to_numbers):
            logger.warning("SignalSmsNotifier: missing Twilio credentials or numbers")
            return

        key = f"{sig.underlying}:{sig.timeframe}:{sig.direction}:{sig.strength}"
        now = time.time()
        last_sent = self._last_sent_by_key.get(key, 0.0)
        if now - last_sent < self.cooldown_seconds:
            return

        zes_value = next(
            (
                c.value for c in sig.components
                if c.name == "ZeroGEX Exhaustion Score" and c.value is not None
            ),
            None,
        )
        message = (
            f"ZeroGEX {sig.underlying} {sig.timeframe} {sig.direction.upper()} "
            f"{sig.strength.upper()} signal | score {sig.composite_score}/{sig.max_possible_score} "
            f"({sig.normalized_score * 100:.1f}%) | trade={sig.trade_type}"
        )
        if zes_value is not None:
            message += f" | ZES={float(zes_value):.1f}"

        for phone in self.to_numbers:
            self._send_twilio_sms(phone, message)
        self._last_sent_by_key[key] = now

    def _is_eligible(self, sig: TradeSignal) -> bool:
        if sig.timeframe not in self.allowed_timeframes:
            return False
        if sig.direction == "neutral":
            return False
        if sig.normalized_score < self.min_normalized:
            return False
        rank = {"low": 0, "medium": 1, "high": 2}
        return rank.get(sig.strength, 0) >= rank.get(self.min_strength, 2)

    def _send_twilio_sms(self, to_number: str, body: str) -> None:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}/Messages.json"
        payload = {"From": self.from_number, "To": to_number, "Body": body[:1500]}
        try:
            response = requests.post(
                url,
                data=payload,
                auth=(self.account_sid, self.auth_token),
                timeout=10,
            )
            response.raise_for_status()
            logger.info("SignalSmsNotifier: sent SMS to %s", to_number)
        except Exception as exc:
            logger.error("SignalSmsNotifier: SMS send failed for %s: %s", to_number, exc)


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _max_possible(tf: str) -> int:
    return sum(w[tf] for w in SIGNAL_WEIGHTS.values())


def _normalize(score: int, tf: str) -> float:
    mx = _max_possible(tf)
    return round(abs(score) / mx, 4) if mx else 0.0


def _to_strength(normalized: float) -> str:
    if normalized >= 0.67:
        return "high"
    if normalized >= 0.40:
        return "medium"
    return "low"


def _to_direction(score: int) -> str:
    if score > 0:
        return "bullish"
    if score < 0:
        return "bearish"
    return "neutral"


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


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
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0  # Pure uptrend vs flat market
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _compute_zes(ctx: SignalContext) -> tuple[float, str, bool]:
    """
    ZeroGEX Exhaustion Score (ZES), scaled 0..100.
    Returns (score, state_label, trap_triggered).
    """
    closes = ctx.recent_closes
    highs = ctx.recent_highs
    if len(closes) < 12:
        return 0.0, "Trend Control", False

    # 1) Distance to max gamma (25%)
    if ctx.current_price > 0 and ctx.max_gamma_strike > 0:
        dist = abs(ctx.current_price - ctx.max_gamma_strike) / ctx.current_price
        max_gamma_score = 1 - min(dist / 0.005, 1.0)
    else:
        max_gamma_score = 0.0

    # 2) Momentum divergence (20%) via RSI swing-high divergence fallback
    rsi_now = _compute_rsi(closes, 14)
    rsi_prev = _compute_rsi(closes[:-3], 14) if len(closes) >= 18 else None
    price_higher_high = max(closes[-3:]) > max(closes[-7:-3])
    rsi_lower_high = (
        rsi_now is not None
        and rsi_prev is not None
        and rsi_now < rsi_prev
    )
    if price_higher_high and rsi_lower_high:
        divergence_score = 1.0
    elif rsi_now is not None and rsi_prev is not None:
        rsi_decay = max(rsi_prev - rsi_now, 0.0) / 15.0
        divergence_score = _clamp(rsi_decay)
    else:
        divergence_score = 0.0

    # 3) Velocity decay (15%) over two 5-bar windows
    velocity_now = abs(closes[-1] - closes[-6])
    velocity_prev = abs(closes[-6] - closes[-11])
    if velocity_prev > 0:
        velocity_score = 1 - min(velocity_now / velocity_prev, 1.0)
    else:
        velocity_score = 0.0

    # 4) Gamma regime context (20%)
    near_gamma_flip = (
        ctx.gamma_flip > 0
        and (abs(ctx.current_price - ctx.gamma_flip) / max(ctx.current_price, 1e-9)) <= 0.003
    )
    if ctx.net_gex > 0:
        gamma_score = 0.3
    elif near_gamma_flip:
        gamma_score = 0.7
    else:
        gamma_score = 1.0
    if (
        ctx.gamma_flip > 0
        and closes[-1] > ctx.gamma_flip
        and closes[-1] < closes[-2]
    ):
        gamma_score = _clamp(gamma_score + 0.1)

    # 5) Structure failure (15%)
    prior_high = max(highs[-10:-3]) if len(highs) >= 10 else max(highs[:-3], default=0.0)
    recent_push_high = max(highs[-3:])
    failed_breakout = recent_push_high > prior_high and closes[-1] < prior_high
    lost_vwap = ctx.vwap > 0 and closes[-2] > ctx.vwap and closes[-1] < ctx.vwap
    lower_high = len(highs) >= 6 and max(highs[-3:]) < max(highs[-6:-3])

    structure_score = 0.0
    if failed_breakout:
        structure_score += 0.4
    if lost_vwap:
        structure_score += 0.3
    if lower_high:
        structure_score += 0.3
    structure_score = _clamp(structure_score)

    # 6) Volume divergence (10%): rising price on declining up-volume is bearish exhaustion;
    #    falling price on declining down-volume is bullish exhaustion (sellers drying up).
    volume_divergence_score = 0.0
    up_vols = ctx.recent_up_volumes
    dn_vols = ctx.recent_down_volumes
    if len(up_vols) >= 10 and len(closes) >= 10:
        recent_up_avg = sum(up_vols[-5:]) / 5 if up_vols[-5:] else 0.0
        prior_up_avg = sum(up_vols[-10:-5]) / 5 if up_vols[-10:-5] else 0.0
        price_advancing = closes[-1] > closes[-5]
        if price_advancing and prior_up_avg > 0 and recent_up_avg < prior_up_avg * 0.75:
            # Price going up but up-volume shrinking = exhaustion on up-move
            volume_divergence_score = _clamp(1.0 - recent_up_avg / prior_up_avg)
    if len(dn_vols) >= 10 and len(closes) >= 10 and volume_divergence_score == 0.0:
        recent_dn_avg = sum(dn_vols[-5:]) / 5 if dn_vols[-5:] else 0.0
        prior_dn_avg = sum(dn_vols[-10:-5]) / 5 if dn_vols[-10:-5] else 0.0
        price_declining = closes[-1] < closes[-5]
        if price_declining and prior_dn_avg > 0 and recent_dn_avg < prior_dn_avg * 0.75:
            # Price going down but down-volume shrinking = exhaustion on down-move (buyers emerging)
            volume_divergence_score = _clamp(1.0 - recent_dn_avg / prior_dn_avg)

    zes = (
        0.25 * max_gamma_score +
        0.20 * divergence_score +
        0.15 * velocity_score +
        0.20 * gamma_score +
        0.15 * structure_score +
        0.05 * volume_divergence_score
    ) * 100

    trap_triggered = zes >= 85 and structure_score >= 0.7
    if trap_triggered:
        state = "Trap Triggered"
    elif zes >= 70:
        state = "Exhaustion Zone"
    elif zes >= 40:
        state = "Late Trend"
    else:
        state = "Trend Control"
    return round(zes, 2), state, trap_triggered


def _orb_direction(orb_status: str) -> Optional[str]:
    s = orb_status.lower()
    if "breakout" in s or "long" in s:
        return "bullish"
    if "breakdown" in s or "short" in s:
        return "bearish"
    return None


def _sm_direction(
    call_prem: float,
    put_prem: float,
    smart_money_ratio: float = SMART_MONEY_DOMINANCE_RATIO,
) -> str:
    if call_prem > put_prem * smart_money_ratio:
        return "bullish"
    if put_prem > call_prem * smart_money_ratio:
        return "bearish"
    return "neutral"


def _build_trade_idea(
    direction: str, strength: str, tf: str, positive_gex: bool
) -> tuple[str, str, str, str]:
    """Returns (trade_type, rationale, target_expiry, suggested_strikes)."""
    expiry = TARGET_EXPIRY[tf][strength]
    sg = STRIKE_GUIDANCE[tf]

    if direction == "neutral" or strength == "low":
        return (
            "no_trade",
            "Composite score below threshold for this timeframe — no edge identified.",
            "N/A",
            "N/A",
        )

    if direction == "bullish":
        if positive_gex:
            return (
                "short_put_spread",
                f"Positive GEX pins/supports price ({tf}). "
                "Sell OTM put spread below dealer support.",
                expiry,
                sg["bull_credit"],
            )
        return (
            "long_call_spread",
            f"Negative GEX amplifies trend ({tf}). "
            "Buy ATM call spread for directional continuation.",
            expiry,
            sg["bull_debit"],
        )

    # bearish
    if positive_gex:
        return (
            "short_call_spread",
            f"Positive GEX caps/resists price ({tf}). "
            "Sell OTM call spread above dealer resistance.",
            expiry,
            sg["bear_credit"],
        )
    return (
        "long_put_spread",
        f"Negative GEX amplifies trend ({tf}). "
        "Buy ATM put spread for directional continuation.",
        expiry,
        sg["bear_debit"],
    )


# ---------------------------------------------------------------------------
# Component scorer
# ---------------------------------------------------------------------------

def _score_components(
    ctx: SignalContext,
    tf: str,
    thresholds: Optional[SignalThresholds] = None,
) -> tuple[int, list[SignalComponent]]:
    thresholds = thresholds or SignalThresholds()
    comps: list[SignalComponent] = []
    total = 0

    def add(name: str, key: str, raw: int, desc: str, value: Optional[float]):
        w = SIGNAL_WEIGHTS[key][tf]
        sc = raw * w
        nonlocal total
        total += sc
        comps.append(SignalComponent(
            name=name, weight=w, score=sc,
            description=desc, value=value, applicable=(w > 0)
        ))

    # 1. GEX Regime
    gf = ctx.gamma_flip or 0.0
    pvf = ((ctx.current_price - gf) / gf * 100) if gf else 0.0
    add("GEX Regime", "gex_regime",
        1 if ctx.net_gex > 0 else -1,
        f"{'Positive' if ctx.net_gex > 0 else 'Negative'} GEX "
        f"({'pin/mean-revert' if ctx.net_gex > 0 else 'trend/vol-expand'}). "
        f"Price {pvf:+.2f}% vs gamma flip.",
        round(ctx.net_gex, 2))

    # 2. Dealer Hedging Pressure
    add("Dealer Hedging Pressure", "dealer_hedging",
        1 if ctx.dealer_net_delta > 0 else (-1 if ctx.dealer_net_delta < 0 else 0),
        f"Dealers net {'buying' if ctx.dealer_net_delta > 0 else 'selling'} "
        f"underlying (derived from delta×OI). Net delta: {ctx.dealer_net_delta:,.0f}.",
        round(ctx.dealer_net_delta, 2))

    # 3. Smart Money Flow
    diff = ctx.smart_call_premium - ctx.smart_put_premium
    if ctx.smart_call_premium > ctx.smart_put_premium * thresholds.smart_money_ratio:
        sm_raw, sm_desc = 1, (f"Call sweeps dominate "
            f"(${ctx.smart_call_premium:,.0f} vs ${ctx.smart_put_premium:,.0f} put).")
    elif ctx.smart_put_premium > ctx.smart_call_premium * thresholds.smart_money_ratio:
        sm_raw, sm_desc = -1, (f"Put sweeps dominate "
            f"(${ctx.smart_put_premium:,.0f} vs ${ctx.smart_call_premium:,.0f} call).")
    else:
        sm_raw, sm_desc = 0, "No clear smart money bias in last 30 min."
    add("Smart Money Flow", "smart_money_flow", sm_raw, sm_desc, round(diff, 2))

    # 4. VWAP Position
    vd = ctx.vwap_deviation_pct
    add("VWAP Position", "vwap_position",
        1 if vd > thresholds.vwap_bull_threshold else (-1 if vd < thresholds.vwap_bear_threshold else 0),
        f"Price is {vd:+.2f}% {'above' if vd > 0 else 'below'} VWAP."
        if SIGNAL_WEIGHTS["vwap_position"][tf] > 0
        else "Not applicable for multi-day timeframe.",
        round(vd, 4))

    # 5. ORB Direction
    orb_dir = _orb_direction(ctx.orb_status)
    orb_raw = 1 if orb_dir == "bullish" else (-1 if orb_dir == "bearish" else 0)
    add("Opening Range Breakout", "orb_direction",
        orb_raw,
        f"ORB status: {ctx.orb_status or 'no breakout detected'}."
        if SIGNAL_WEIGHTS["orb_direction"][tf] > 0
        else "Not applicable for multi-day timeframe.",
        None)

    # 6. Put/Call Ratio
    pcr = ctx.put_call_ratio or 1.0
    add("Put/Call Ratio", "put_call_ratio",
        1 if pcr < thresholds.pcr_bullish_threshold else (-1 if pcr > thresholds.pcr_bearish_threshold else 0),
        f"P/C ratio: {pcr:.2f} "
        f"({'call-heavy/bullish' if pcr < thresholds.pcr_bullish_threshold else 'put-heavy/bearish' if pcr > thresholds.pcr_bearish_threshold else 'neutral'}).",
        round(pcr, 3))

    # 7. Unusual Volume
    add("Unusual Volume Spike", "unusual_volume",
        1 if ctx.unusual_call_volume else 0,
        "Unusual call volume detected (vol/OI ratio spike)." if ctx.unusual_call_volume
        else ("No unusual call volume in last 30 min."
              if SIGNAL_WEIGHTS["unusual_volume"][tf] > 0
              else "Not applicable for multi-day timeframe."),
        None)

    # 8. Momentum Divergence
    pc5 = ctx.price_change_5min
    nof = ctx.net_option_flow
    if pc5 < 0 and nof > 50_000:
        div_raw, div_desc = 1, "Dip + call flow surging — accumulation signal."
    elif pc5 > 0 and nof < -50_000:
        div_raw, div_desc = -1, "Rip + put flow surging — distribution signal."
    else:
        div_raw, div_desc = 0, (
            "No momentum divergence detected."
            if SIGNAL_WEIGHTS["momentum_divergence"][tf] > 0
            else "Not applicable for multi-day timeframe."
        )
    add("Momentum Divergence", "momentum_divergence", div_raw, div_desc, None)

    # 9. Vanna/Charm Drift
    if ctx.vanna_exposure > 0 and ctx.charm_exposure < 0:
        vc_raw = 1
        vc_desc = (f"Positive vanna ({ctx.vanna_exposure:.2f}) + "
                   f"negative charm ({ctx.charm_exposure:.2f}): dealer buying pressure building.")
    elif ctx.vanna_exposure < 0 and ctx.charm_exposure > 0:
        vc_raw = -1
        vc_desc = (f"Negative vanna ({ctx.vanna_exposure:.2f}) + "
                   f"positive charm ({ctx.charm_exposure:.2f}): dealer selling pressure building.")
    else:
        vc_raw = 0
        vc_desc = (
            f"Vanna/charm not clearly directional "
            f"(vanna={ctx.vanna_exposure:.2f}, charm={ctx.charm_exposure:.2f})."
            if SIGNAL_WEIGHTS["vanna_charm_drift"][tf] > 0
            else "Not applicable for intraday timeframe."
        )
    add("Vanna/Charm Drift", "vanna_charm_drift", vc_raw, vc_desc,
        round(ctx.vanna_exposure, 4))

    # 10. ZeroGEX Exhaustion Score (ZES)
    zes, state_label, trap_triggered = _compute_zes(ctx)
    if zes >= 85:
        ex_raw = -1 if ctx.price_change_5min >= 0 else 1
    elif zes >= 70:
        ex_raw = -1 if ctx.price_change_5min >= 0 else 0
    else:
        ex_raw = 0
    trigger_text = "Reversal trigger active." if trap_triggered else "Awaiting structure break confirmation."
    add(
        "ZeroGEX Exhaustion Score",
        "exhaustion_score",
        ex_raw,
        f"ZES={zes:.1f}/100 ({state_label}). {trigger_text}",
        zes,
    )

    return total, comps


# ---------------------------------------------------------------------------
# Main engine class
# ---------------------------------------------------------------------------

class SignalEngine:
    """
    Computes composite trade signals from already-calculated GEX/flow data
    and writes them to the trade_signals table.
    """

    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)  # canonical alias for DB queries (e.g. "SPX")
        self._last_accuracy_update: Optional[date] = None
        self.auto_tune_enabled = os.getenv("SIGNAL_AUTO_TUNE_ENABLED", "true").lower() == "true"
        self.auto_tune_lookback_days = max(5, int(os.getenv("SIGNAL_AUTO_TUNE_LOOKBACK_DAYS", "20")))
        self.auto_tune_min_samples = max(50, int(os.getenv("SIGNAL_AUTO_TUNE_MIN_SAMPLES", "250")))
        self._last_auto_tune_date: Optional[date] = None
        self._defaults = {
            "sm_ratio": SMART_MONEY_DOMINANCE_RATIO,
            "vwap_bull": VWAP_DEV_BULL_THRESHOLD,
            "vwap_bear": VWAP_DEV_BEAR_THRESHOLD,
            "pcr_bull": PCR_BULLISH_THRESHOLD,
            "pcr_bear": PCR_BEARISH_THRESHOLD,
        }
        self.thresholds = SignalThresholds(
            smart_money_ratio=self._defaults["sm_ratio"],
            vwap_bull_threshold=self._defaults["vwap_bull"],
            vwap_bear_threshold=self._defaults["vwap_bear"],
            pcr_bullish_threshold=self._defaults["pcr_bull"],
            pcr_bearish_threshold=self._defaults["pcr_bear"],
        )
        self.sms_notifier = SignalSmsNotifier(self.db_symbol)

    def _auto_tune_thresholds(self) -> None:
        """Blend defaults with recent distribution stats for regime-adaptive thresholds."""
        if not self.auto_tune_enabled:
            return
        today = datetime.now(ET).date()
        if self._last_auto_tune_date == today:
            return

        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS n,
                        PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY put_call_ratio) AS pcr_bull,
                        PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY put_call_ratio) AS pcr_bear
                    FROM gex_summary
                    WHERE underlying = %s
                      AND timestamp >= NOW() - (%s * INTERVAL '1 day')
                      AND put_call_ratio IS NOT NULL
                    """,
                    (self.db_symbol, self.auto_tune_lookback_days),
                )
                row = cur.fetchone() or (0, None, None)
                if int(row[0] or 0) < self.auto_tune_min_samples:
                    logger.info("SignalEngine auto-tune skipped: insufficient GEX samples")
                    return
                pcr_bull = float(row[1]) if row[1] is not None else self._defaults["pcr_bull"]
                pcr_bear = float(row[2]) if row[2] is not None else self._defaults["pcr_bear"]

                cur.execute(
                    """
                    SELECT
                        PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY vwap_deviation_pct) FILTER (WHERE vwap_deviation_pct > 0) AS bull_dev,
                        PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY vwap_deviation_pct) FILTER (WHERE vwap_deviation_pct < 0) AS bear_dev
                    FROM underlying_vwap_deviation
                    WHERE symbol = %s
                      AND timestamp >= NOW() - (%s * INTERVAL '1 day')
                    """,
                    (self.db_symbol, self.auto_tune_lookback_days),
                )
                row = cur.fetchone() or (None, None)
                vwap_bull = float(row[0]) if row[0] is not None else self._defaults["vwap_bull"]
                vwap_bear = float(row[1]) if row[1] is not None else self._defaults["vwap_bear"]

                cur.execute(
                    """
                    WITH sm AS (
                        SELECT
                            timestamp,
                            SUM(CASE WHEN option_type='C' THEN total_premium ELSE 0 END)::float AS call_p,
                            SUM(CASE WHEN option_type='P' THEN total_premium ELSE 0 END)::float AS put_p
                        FROM flow_smart_money
                        WHERE symbol = %s
                          AND timestamp >= NOW() - (%s * INTERVAL '1 day')
                        GROUP BY timestamp
                    )
                    SELECT
                        PERCENTILE_CONT(0.6) WITHIN GROUP (
                            ORDER BY GREATEST(call_p, put_p) / NULLIF(LEAST(call_p, put_p), 0)
                        )
                    FROM sm
                    WHERE call_p > 0 AND put_p > 0
                    """,
                    (self.db_symbol, self.auto_tune_lookback_days),
                )
                row = cur.fetchone()
                sm_ratio = float(row[0]) if row and row[0] is not None else self._defaults["sm_ratio"]

            alpha = 0.20
            self.thresholds.smart_money_ratio = max(1.05, min(1.80, (1 - alpha) * self._defaults["sm_ratio"] + alpha * sm_ratio))
            self.thresholds.vwap_bull_threshold = max(0.05, min(1.00, (1 - alpha) * self._defaults["vwap_bull"] + alpha * vwap_bull))
            self.thresholds.vwap_bear_threshold = min(-0.05, max(-1.00, (1 - alpha) * self._defaults["vwap_bear"] + alpha * vwap_bear))
            self.thresholds.pcr_bullish_threshold = max(0.40, min(1.00, (1 - alpha) * self._defaults["pcr_bull"] + alpha * pcr_bull))
            self.thresholds.pcr_bearish_threshold = max(1.00, min(2.20, (1 - alpha) * self._defaults["pcr_bear"] + alpha * pcr_bear))
            self._last_auto_tune_date = today

            if abs(self.thresholds.pcr_bullish_threshold - self._defaults["pcr_bull"]) > 0.20:
                logger.warning("SignalEngine auto-tune drift alert: bullish PCR threshold moved materially.")

        except Exception as e:
            logger.error(f"SignalEngine auto-tune failed: {e}")

    # ------------------------------------------------------------------
    # DB reads — all use the synchronous db_connection() context manager
    # that your existing code uses throughout AnalyticsEngine.
    # ------------------------------------------------------------------

    def _fetch_context(self) -> Optional[SignalContext]:
        """Pull every value needed to score signals in one synchronous pass."""
        try:
            with db_connection() as conn:
                cur = conn.cursor()

                # --- Latest GEX summary ---
                cur.execute("""
                    SELECT
                        timestamp,
                        total_net_gex,
                        gamma_flip_point,
                        put_call_ratio,
                        max_gamma_strike   -- used as a proxy price anchor
                    FROM gex_summary
                    WHERE underlying = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (self.db_symbol,))
                gex_row = cur.fetchone()
                if not gex_row:
                    logger.warning("SignalEngine: no gex_summary rows found")
                    return None

                ts, net_gex, gamma_flip, pcr, max_gamma_strike = gex_row

                # --- Latest underlying price ---
                cur.execute("""
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (self.db_symbol,))
                price_row = cur.fetchone()
                if not price_row:
                    logger.warning("SignalEngine: no underlying_quotes rows found")
                    return None
                current_price = float(price_row[0])

                # --- Recent bars for RSI/velocity/structure exhaustion state ---
                cur.execute("""
                    SELECT close, high, COALESCE(up_volume, 0), COALESCE(down_volume, 0)
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 30
                """, (self.db_symbol,))
                history_rows = cur.fetchall()
                recent_closes = [float(r[0]) for r in reversed(history_rows)] if history_rows else []
                recent_highs = [float(r[1]) for r in reversed(history_rows)] if history_rows else []
                recent_up_volumes = [int(r[2]) for r in reversed(history_rows)] if history_rows else []
                recent_down_volumes = [int(r[3]) for r in reversed(history_rows)] if history_rows else []

                # --- Latest VWAP deviation ---
                cur.execute("""
                    SELECT vwap, vwap_deviation_pct
                    FROM underlying_vwap_deviation
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (self.db_symbol,))
                vwap_row = cur.fetchone()
                vwap = float(vwap_row[0]) if vwap_row else 0.0
                vwap_dev = float(vwap_row[1]) if vwap_row else 0.0

                # --- Latest ORB status ---
                cur.execute("""
                    SELECT orb_status
                    FROM opening_range_breakout
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (self.db_symbol,))
                orb_row = cur.fetchone()
                orb_status = orb_row[0] if orb_row else ""

                # --- Smart money: call vs put premium (last 30 min) ---
                cur.execute("""
                    SELECT option_type, SUM(total_premium)
                    FROM flow_smart_money
                    WHERE symbol = %s
                      AND timestamp >= NOW() - INTERVAL '30 minutes'
                    GROUP BY option_type
                """, (self.db_symbol,))
                sm_call = sm_put = 0.0
                for row in cur.fetchall():
                    if row[0] == 'C':
                        sm_call = float(row[1] or 0)
                    else:
                        sm_put = float(row[1] or 0)

                # --- Dealer net delta (derived: sum delta×OI across all strikes) ---
                # Call delta is positive, put delta is negative.
                # Dealers are net short options, so their hedge delta is the inverse.
                # net_dealer_delta = -(sum of delta×OI×100 for all live options)
                cur.execute("""
                    SELECT
                        SUM(oc.delta * oc.open_interest * 100) AS gross_delta
                    FROM option_chains oc
                    WHERE oc.underlying = %s
                      AND oc.timestamp = (
                          SELECT timestamp
                          FROM option_chains
                          WHERE underlying = %s
                          ORDER BY timestamp DESC
                          LIMIT 1
                      )
                      AND oc.delta IS NOT NULL
                      AND oc.open_interest > 0
                """, (self.db_symbol, self.db_symbol))
                delta_row = cur.fetchone()
                # Dealer delta is the negative of aggregate customer delta
                dealer_net_delta = -(float(delta_row[0]) if delta_row and delta_row[0] else 0.0)

                # --- Unusual call volume: any option with vol/OI ratio > 3 in last 30 min ---
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM option_chains oc
                        WHERE oc.underlying = %s
                          AND oc.option_type = 'C'
                          AND oc.open_interest > 0
                          AND oc.volume::float / oc.open_interest > 3.0
                          AND oc.timestamp >= NOW() - INTERVAL '30 minutes'
                    )
                """, (self.db_symbol,))
                unusual_call_volume = bool(cur.fetchone()[0])

                # --- Momentum divergence: 5-min price change + net option flow ---
                cur.execute("""
                    SELECT
                        close - LAG(close, 5) OVER (
                            PARTITION BY symbol ORDER BY timestamp
                        ) AS price_change_5min
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (self.db_symbol,))
                pc5_row = cur.fetchone()
                price_change_5min = float(pc5_row[0]) if (pc5_row and pc5_row[0]) else 0.0

                cur.execute("""
                    SELECT
                        SUM(CASE WHEN option_type = 'C'
                                 THEN total_premium
                                 ELSE -total_premium END) AS net_flow
                    FROM flow_by_type
                    WHERE symbol = %s
                      AND timestamp >= NOW() - INTERVAL '5 minutes'
                """, (self.db_symbol,))
                nof_row = cur.fetchone()
                net_option_flow = float(nof_row[0]) if (nof_row and nof_row[0]) else 0.0

                # --- Vanna / charm from latest gex_by_strike aggregate ---
                cur.execute("""
                    SELECT
                        SUM(vanna_exposure) AS total_vanna,
                        SUM(charm_exposure) AS total_charm
                    FROM gex_by_strike
                    WHERE underlying = %s
                      AND timestamp = (
                          SELECT timestamp
                          FROM gex_by_strike
                          WHERE underlying = %s
                          ORDER BY timestamp DESC
                          LIMIT 1
                      )
                """, (self.db_symbol, self.db_symbol))
                vc_row = cur.fetchone()
                vanna = float(vc_row[0]) if (vc_row and vc_row[0]) else 0.0
                charm = float(vc_row[1]) if (vc_row and vc_row[1]) else 0.0

                return SignalContext(
                    timestamp=ts,
                    current_price=current_price,
                    max_gamma_strike=float(max_gamma_strike or 0),
                    net_gex=float(net_gex or 0),
                    gamma_flip=float(gamma_flip or 0),
                    put_call_ratio=float(pcr or 1.0),
                    vwap=vwap,
                    vwap_deviation_pct=vwap_dev,
                    orb_status=orb_status,
                    smart_call_premium=sm_call,
                    smart_put_premium=sm_put,
                    dealer_net_delta=dealer_net_delta,
                    unusual_call_volume=unusual_call_volume,
                    price_change_5min=price_change_5min,
                    net_option_flow=net_option_flow,
                    vanna_exposure=vanna,
                    charm_exposure=charm,
                    recent_closes=recent_closes,
                    recent_highs=recent_highs,
                    recent_up_volumes=recent_up_volumes,
                    recent_down_volumes=recent_down_volumes,
                )

        except Exception as e:
            logger.error(f"SignalEngine._fetch_context failed: {e}", exc_info=True)
            return None

    def _fetch_calibrated_win_pct(
        self, tf: str, strength: str, lookback_days: int = 30
    ) -> Optional[float]:
        """Read empirical win % from signal_accuracy table if enough data exists."""
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT
                        SUM(total_signals)   AS total,
                        SUM(correct_signals) AS correct
                    FROM signal_accuracy
                    WHERE underlying      = %s
                      AND timeframe       = %s
                      AND strength_bucket = %s
                      AND trade_date      >= CURRENT_DATE - %s
                """, (self.db_symbol, tf, strength, lookback_days))
                row = cur.fetchone()
                if row and row[0] and row[0] > 0:
                    return round(float(row[1]) / float(row[0]), 4)
                return None
        except Exception as e:
            logger.error(f"_fetch_calibrated_win_pct failed: {e}")
            return None

    # ------------------------------------------------------------------
    # DB writes
    # ------------------------------------------------------------------

    def _store_signal(self, sig: TradeSignal) -> None:
        components_json = json.dumps([asdict(c) for c in sig.components])
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO trade_signals (
                        underlying, timestamp, timeframe,
                        composite_score, max_possible_score, normalized_score,
                        direction, strength, estimated_win_pct,
                        trade_type, trade_rationale, target_expiry, suggested_strikes,
                        current_price, net_gex, gamma_flip, price_vs_flip,
                        vwap, vwap_deviation_pct, put_call_ratio,
                        dealer_net_delta, smart_money_direction,
                        unusual_volume_detected, orb_breakout_direction,
                        components
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    ON CONFLICT (underlying, timestamp, timeframe) DO UPDATE SET
                        composite_score        = EXCLUDED.composite_score,
                        max_possible_score     = EXCLUDED.max_possible_score,
                        normalized_score       = EXCLUDED.normalized_score,
                        direction              = EXCLUDED.direction,
                        strength               = EXCLUDED.strength,
                        estimated_win_pct      = EXCLUDED.estimated_win_pct,
                        trade_type             = EXCLUDED.trade_type,
                        trade_rationale        = EXCLUDED.trade_rationale,
                        target_expiry          = EXCLUDED.target_expiry,
                        suggested_strikes      = EXCLUDED.suggested_strikes,
                        current_price          = EXCLUDED.current_price,
                        net_gex                = EXCLUDED.net_gex,
                        gamma_flip             = EXCLUDED.gamma_flip,
                        price_vs_flip          = EXCLUDED.price_vs_flip,
                        vwap                   = EXCLUDED.vwap,
                        vwap_deviation_pct     = EXCLUDED.vwap_deviation_pct,
                        put_call_ratio         = EXCLUDED.put_call_ratio,
                        dealer_net_delta       = EXCLUDED.dealer_net_delta,
                        smart_money_direction  = EXCLUDED.smart_money_direction,
                        unusual_volume_detected= EXCLUDED.unusual_volume_detected,
                        orb_breakout_direction = EXCLUDED.orb_breakout_direction,
                        components             = EXCLUDED.components
                """, (
                    self.db_symbol, sig.timestamp, sig.timeframe,
                    sig.composite_score, sig.max_possible_score, sig.normalized_score,
                    sig.direction, sig.strength, sig.estimated_win_pct,
                    sig.trade_type, sig.trade_rationale, sig.target_expiry, sig.suggested_strikes,
                    sig.current_price, sig.net_gex,
                    sig.gamma_flip, sig.price_vs_flip,
                    sig.vwap, sig.vwap_deviation_pct, sig.put_call_ratio,
                    sig.dealer_net_delta, sig.smart_money_direction,
                    sig.unusual_volume_detected, sig.orb_breakout_direction,
                    components_json,
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"_store_signal failed ({sig.timeframe}): {e}", exc_info=True)

    def _update_accuracy(self) -> None:
        """
        For each signal written yesterday, compare the direction to the actual
        daily outcome (open vs close) and update signal_accuracy.
        Only runs once per calendar day.
        """
        today = datetime.now(ET).date()
        if self._last_accuracy_update == today:
            return
        yesterday = today - timedelta(days=1)

        try:
            with db_connection() as conn:
                cur = conn.cursor()
                # Get yesterday's signals
                cur.execute("""
                    SELECT
                        ts.timeframe,
                        ts.strength,
                        ts.direction,
                        ts.timestamp
                    FROM trade_signals ts
                    WHERE ts.underlying = %s
                      AND DATE(ts.timestamp AT TIME ZONE 'America/New_York') = %s
                """, (self.db_symbol, yesterday))
                signals = cur.fetchall()
                if not signals:
                    self._last_accuracy_update = today
                    return

                # Get yesterday's open and close
                cur.execute("""
                    SELECT
                        FIRST_VALUE(close) OVER (ORDER BY timestamp ASC)  AS day_open,
                        LAST_VALUE(close)  OVER (
                            ORDER BY timestamp ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) AS day_close
                    FROM underlying_quotes
                    WHERE symbol = %s
                      AND DATE(timestamp AT TIME ZONE 'America/New_York') = %s
                    LIMIT 1
                """, (self.db_symbol, yesterday))
                outcome_row = cur.fetchone()
                if not outcome_row:
                    return
                day_open, day_close = float(outcome_row[0]), float(outcome_row[1])
                up_day = day_close > day_open

                # Tally per (timeframe, strength)
                buckets: dict[tuple, list[bool]] = {}
                for tf, strength, direction, _ in signals:
                    key = (tf, strength)
                    correct = (direction == "bullish" and up_day) or \
                              (direction == "bearish" and not up_day)
                    buckets.setdefault(key, []).append(correct)

                for (tf, strength), results in buckets.items():
                    total   = len(results)
                    correct = sum(results)
                    cur.execute("""
                        INSERT INTO signal_accuracy
                            (underlying, trade_date, timeframe, strength_bucket,
                             total_signals, correct_signals, win_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (underlying, trade_date, timeframe, strength_bucket)
                        DO UPDATE SET
                            total_signals   = EXCLUDED.total_signals,
                            correct_signals = EXCLUDED.correct_signals,
                            win_pct         = EXCLUDED.win_pct,
                            updated_at      = NOW()
                    """, (
                        self.db_symbol, yesterday, tf, strength,
                        total, correct,
                        round(correct / total, 4) if total > 0 else None,
                    ))
                conn.commit()
                logger.info(
                    f"SignalEngine: updated accuracy for {self.underlying} on {yesterday}"
                )
                self._last_accuracy_update = today

        except Exception as e:
            logger.error(f"_update_accuracy failed: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_calculation(self) -> bool:
        """
        Fetch context, score all three timeframes, store to DB.
        Returns True if at least one signal was successfully stored.
        """
        ctx = self._fetch_context()
        if ctx is None:
            logger.warning("SignalEngine: no context available, skipping")
            return False
        self._auto_tune_thresholds()

        stored = 0
        for tf in ("intraday", "swing", "multi_day"):
            try:
                composite, comps = _score_components(ctx, tf, self.thresholds)
                comps.append(
                    SignalComponent(
                        name="Calibration Snapshot",
                        weight=0,
                        score=0,
                        description=(
                            f"sm_ratio={self.thresholds.smart_money_ratio:.3f}, "
                            f"vwap_bull={self.thresholds.vwap_bull_threshold:.3f}, "
                            f"vwap_bear={self.thresholds.vwap_bear_threshold:.3f}, "
                            f"pcr_bull={self.thresholds.pcr_bullish_threshold:.3f}, "
                            f"pcr_bear={self.thresholds.pcr_bearish_threshold:.3f}"
                        ),
                        value=None,
                        applicable=False,
                    )
                )
                mx      = _max_possible(tf)
                normed  = _normalize(composite, tf)
                direction = _to_direction(composite)
                strength  = _to_strength(normed)

                # Win pct: calibrated > default
                win_pct = (
                    self._fetch_calibrated_win_pct(tf, strength)
                    or WIN_PCT_DEFAULTS[tf][strength]
                )

                positive_gex = ctx.net_gex > 0
                trade_type, rationale, expiry, strikes = _build_trade_idea(
                    direction, strength, tf, positive_gex
                )

                gf  = ctx.gamma_flip or None
                pvf = round((ctx.current_price - gf) / gf * 100, 4) if gf else None
                orb_dir = _orb_direction(ctx.orb_status) if ctx.orb_status else None
                sm_dir  = _sm_direction(
                    ctx.smart_call_premium,
                    ctx.smart_put_premium,
                    self.thresholds.smart_money_ratio,
                )

                sig = TradeSignal(
                    underlying=self.db_symbol,
                    timestamp=ctx.timestamp,
                    timeframe=tf,
                    composite_score=composite,
                    max_possible_score=mx,
                    normalized_score=normed,
                    direction=direction,
                    strength=strength,
                    estimated_win_pct=win_pct,
                    trade_type=trade_type,
                    trade_rationale=rationale,
                    target_expiry=expiry,
                    suggested_strikes=strikes,
                    current_price=ctx.current_price,
                    net_gex=ctx.net_gex,
                    gamma_flip=gf,
                    price_vs_flip=pvf,
                    vwap=ctx.vwap or None,
                    vwap_deviation_pct=ctx.vwap_deviation_pct or None,
                    put_call_ratio=ctx.put_call_ratio,
                    dealer_net_delta=ctx.dealer_net_delta,
                    smart_money_direction=sm_dir,
                    unusual_volume_detected=ctx.unusual_call_volume,
                    orb_breakout_direction=orb_dir,
                    components=comps,
                )
                self._store_signal(sig)
                self.sms_notifier.maybe_send(sig)
                stored += 1
                logger.info(
                    f"✅ Signal [{tf}] {direction.upper()} | "
                    f"score={composite}/{mx} ({normed:.0%}) | "
                    f"strength={strength} | win_pct={win_pct:.0%} | "
                    f"trade={trade_type}"
                )
            except Exception as e:
                logger.error(f"SignalEngine: error scoring {tf}: {e}", exc_info=True)

        # Daily accuracy update (no-ops if already done today)
        self._update_accuracy()

        return stored > 0
