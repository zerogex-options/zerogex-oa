"""
Volatility expansion prediction engine.

Builds on the existing trade signal stack and reframes the same market microstructure
inputs around one question: when is the market most vulnerable to a 0.5%+ move?
"""

import argparse
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

import pytz

from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)
ET = pytz.timezone("US/Eastern")

VOL_SIGNAL_WEIGHTS: dict[str, int] = {
    "gamma_instability": 4,
    "dealer_pressure": 3,
    "smart_money_flow": 3,
    "vanna_risk": 2,
    "max_pain_breakdown": 3,
    "put_call_extreme": 2,
    "vol_oi_surge": 2,
    "charm_decay": 2,
    "orb_confirmation": 2,
    "vwap_extension": 1,
}

MOVE_PROBABILITY_DEFAULTS = {
    "high": 0.72,
    "medium": 0.58,
    "low": 0.44,
}

# Env-overridable calibration knobs for regime tuning.
VOL_SMART_MONEY_DOMINANCE_RATIO = float(os.getenv("VOL_SMART_MONEY_DOMINANCE_RATIO", "1.2"))
VOL_GAMMA_DEEP_NEGATIVE = float(os.getenv("VOL_GAMMA_DEEP_NEGATIVE", "-5000000000"))
VOL_GAMMA_NEGATIVE = float(os.getenv("VOL_GAMMA_NEGATIVE", "-3000000000"))
VOL_GAMMA_FLIP_NEAR_PCT = float(os.getenv("VOL_GAMMA_FLIP_NEAR_PCT", "0.003"))
VOL_PCR_HIGH = float(os.getenv("VOL_PCR_HIGH", "1.8"))
VOL_PCR_LOW = float(os.getenv("VOL_PCR_LOW", "0.4"))


@dataclass
class VolComponent:
    name: str
    weight: int
    raw_score: int
    weighted_score: int
    description: str
    value: Optional[float]


@dataclass
class VolExpansionContext:
    timestamp: datetime
    current_price: float
    net_gex: float
    gamma_flip: Optional[float]
    dealer_net_delta: float
    smart_call_premium: float
    smart_put_premium: float
    put_call_ratio: float
    vanna_exposure: float
    charm_exposure: float
    max_pain: float
    orb_status: str
    vwap: float
    vwap_deviation_pct: float
    unusual_volume_count: int
    price_change_5min: float
    net_option_flow_5min: float
    hours_to_next_expiry: float


@dataclass
class VolExpansionSignal:
    underlying: str
    timestamp: datetime
    composite_score: int
    max_possible_score: int
    normalized_score: float
    move_probability: float
    expected_direction: str
    expected_magnitude_pct: float
    confidence: str
    catalyst_type: str
    time_horizon: str
    strategy_type: str
    entry_window: str
    current_price: float
    net_gex: float
    gamma_flip: Optional[float]
    max_pain: Optional[float]
    put_call_ratio: float
    dealer_net_delta: float
    smart_money_direction: str
    vwap_deviation_pct: float
    hours_to_next_expiry: float
    components: list[VolComponent] = field(default_factory=list)


@dataclass
class AccuracySnapshot:
    move_threshold_pct: float
    actual_move_pct: float
    actual_direction: str
    hit_large_move: bool
    direction_correct: bool
    magnitude_bucket: str


@dataclass
class VolThresholds:
    smart_money_ratio: float = VOL_SMART_MONEY_DOMINANCE_RATIO
    deep_negative_gex: float = VOL_GAMMA_DEEP_NEGATIVE
    negative_gex: float = VOL_GAMMA_NEGATIVE
    gamma_flip_near_pct: float = VOL_GAMMA_FLIP_NEAR_PCT
    pcr_high: float = VOL_PCR_HIGH
    pcr_low: float = VOL_PCR_LOW


class VolExpansionEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self._last_accuracy_update: Optional[date] = None
        self.auto_tune_enabled = os.getenv("VOL_AUTO_TUNE_ENABLED", "true").lower() == "true"
        self.auto_tune_lookback_days = max(5, int(os.getenv("VOL_AUTO_TUNE_LOOKBACK_DAYS", "30")))
        self.auto_tune_min_samples = max(50, int(os.getenv("VOL_AUTO_TUNE_MIN_SAMPLES", "250")))
        self._last_auto_tune_date: Optional[date] = None
        self._defaults = {
            "sm_ratio": VOL_SMART_MONEY_DOMINANCE_RATIO,
            "deep_neg_gex": VOL_GAMMA_DEEP_NEGATIVE,
            "neg_gex": VOL_GAMMA_NEGATIVE,
            "flip_near": VOL_GAMMA_FLIP_NEAR_PCT,
            "pcr_high": VOL_PCR_HIGH,
            "pcr_low": VOL_PCR_LOW,
        }
        self.thresholds = VolThresholds(
            smart_money_ratio=self._defaults["sm_ratio"],
            deep_negative_gex=self._defaults["deep_neg_gex"],
            negative_gex=self._defaults["neg_gex"],
            gamma_flip_near_pct=self._defaults["flip_near"],
            pcr_high=self._defaults["pcr_high"],
            pcr_low=self._defaults["pcr_low"],
        )

    def _auto_tune_thresholds(self) -> None:
        """Adapt vol thresholds from recent regime distributions while anchoring to defaults."""
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
                        PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY total_net_gex) AS deep_neg,
                        PERCENTILE_CONT(0.30) WITHIN GROUP (ORDER BY total_net_gex) AS neg_gex,
                        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY put_call_ratio) AS pcr_hi,
                        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY put_call_ratio) AS pcr_lo
                    FROM gex_summary
                    WHERE underlying = %s
                      AND timestamp >= NOW() - (%s * INTERVAL '1 day')
                    """,
                    (self.db_symbol, self.auto_tune_lookback_days),
                )
                row = cur.fetchone() or (0, None, None, None, None)
                if int(row[0] or 0) < self.auto_tune_min_samples:
                    logger.info("VolExpansionEngine auto-tune skipped: insufficient GEX samples")
                    return
                deep_neg = float(row[1]) if row[1] is not None else self._defaults["deep_neg_gex"]
                neg_gex = float(row[2]) if row[2] is not None else self._defaults["neg_gex"]
                pcr_hi = float(row[3]) if row[3] is not None else self._defaults["pcr_high"]
                pcr_lo = float(row[4]) if row[4] is not None else self._defaults["pcr_low"]

                cur.execute(
                    """
                    WITH joined AS (
                        SELECT
                            gs.timestamp,
                            ABS((uq.close - gs.gamma_flip_point) / NULLIF(gs.gamma_flip_point, 0)) AS flip_dist
                        FROM gex_summary gs
                        JOIN LATERAL (
                            SELECT close
                            FROM underlying_quotes uq
                            WHERE uq.symbol = gs.underlying
                              AND uq.timestamp <= gs.timestamp
                            ORDER BY uq.timestamp DESC
                            LIMIT 1
                        ) uq ON TRUE
                        WHERE gs.underlying = %s
                          AND gs.gamma_flip_point IS NOT NULL
                          AND gs.timestamp >= NOW() - (%s * INTERVAL '1 day')
                    )
                    SELECT PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY flip_dist) FROM joined
                    """,
                    (self.db_symbol, self.auto_tune_lookback_days),
                )
                row = cur.fetchone()
                flip_near = float(row[0]) if row and row[0] is not None else self._defaults["flip_near"]

            alpha = 0.20
            self.thresholds.deep_negative_gex = (1 - alpha) * self._defaults["deep_neg_gex"] + alpha * min(deep_neg, -1e9)
            self.thresholds.negative_gex = (1 - alpha) * self._defaults["neg_gex"] + alpha * min(neg_gex, -5e8)
            self.thresholds.gamma_flip_near_pct = max(0.001, min(0.01, (1 - alpha) * self._defaults["flip_near"] + alpha * flip_near))
            self.thresholds.pcr_high = max(1.10, min(2.50, (1 - alpha) * self._defaults["pcr_high"] + alpha * pcr_hi))
            self.thresholds.pcr_low = max(0.20, min(0.90, (1 - alpha) * self._defaults["pcr_low"] + alpha * pcr_lo))
            self.thresholds.smart_money_ratio = self._defaults["sm_ratio"]
            self._last_auto_tune_date = today
            if abs(self.thresholds.gamma_flip_near_pct - self._defaults["flip_near"]) > 0.003:
                logger.warning("VolExpansionEngine auto-tune drift alert: gamma flip near threshold moved materially.")

        except Exception as exc:
            logger.error("VolExpansionEngine auto-tune failed: %s", exc)

    def _fetch_context(self, as_of: Optional[datetime] = None) -> Optional[VolExpansionContext]:
        try:
            with db_connection() as conn:
                cur = conn.cursor()

                anchor_ts = as_of
                if anchor_ts is None:
                    cur.execute(
                        """
                        SELECT MAX(timestamp)
                        FROM gex_summary
                        WHERE underlying = %s
                        """,
                        (self.db_symbol,),
                    )
                    anchor_row = cur.fetchone()
                    anchor_ts = anchor_row[0] if anchor_row else None
                if anchor_ts is None:
                    logger.warning("VolExpansionEngine: no anchor timestamp available")
                    return None

                cur.execute(
                    """
                    SELECT timestamp, total_net_gex, gamma_flip_point, put_call_ratio, max_pain
                    FROM gex_summary
                    WHERE underlying = %s
                      AND timestamp = %s
                    LIMIT 1
                    """,
                    (self.db_symbol, anchor_ts),
                )
                gex_row = cur.fetchone()
                if not gex_row:
                    logger.warning("VolExpansionEngine: no gex_summary rows found")
                    return None
                ts, net_gex, gamma_flip, pcr, max_pain = gex_row

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
                    logger.warning("VolExpansionEngine: no underlying_quotes rows found")
                    return None
                current_price = float(price_row[0])

                cur.execute(
                    """
                    SELECT vwap, vwap_deviation_pct
                    FROM underlying_vwap_deviation
                    WHERE symbol = %s
                      AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, anchor_ts),
                )
                vwap_row = cur.fetchone()
                vwap = float(vwap_row[0]) if vwap_row else current_price
                vwap_dev = float(vwap_row[1]) if vwap_row else 0.0

                cur.execute(
                    """
                    SELECT orb_status
                    FROM opening_range_breakout
                    WHERE symbol = %s
                      AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, anchor_ts),
                )
                orb_row = cur.fetchone()
                orb_status = orb_row[0] if orb_row else ""

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

                cur.execute(
                    """
                    SELECT SUM(delta * open_interest * 100) AS gross_delta
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp = (
                          SELECT MAX(timestamp) FROM option_chains WHERE underlying = %s AND timestamp <= %s
                      )
                      AND delta IS NOT NULL
                      AND open_interest > 0
                    """,
                    (self.db_symbol, self.db_symbol, anchor_ts),
                )
                delta_row = cur.fetchone()
                dealer_net_delta = -(float(delta_row[0]) if delta_row and delta_row[0] else 0.0)

                cur.execute(
                    """
                    SELECT
                        SUM(vanna_exposure) AS total_vanna,
                        SUM(charm_exposure) AS total_charm
                    FROM gex_by_strike
                    WHERE underlying = %s
                      AND timestamp = (
                          SELECT MAX(timestamp) FROM gex_by_strike WHERE underlying = %s AND timestamp <= %s
                      )
                    """,
                    (self.db_symbol, self.db_symbol, anchor_ts),
                )
                greek_row = cur.fetchone()
                vanna = float(greek_row[0]) if greek_row and greek_row[0] else 0.0
                charm = float(greek_row[1]) if greek_row and greek_row[1] else 0.0

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp BETWEEN %s - INTERVAL '15 minutes' AND %s
                      AND open_interest > 0
                      AND volume > 200
                      AND volume::float / open_interest > 8.0
                    """,
                    (self.db_symbol, anchor_ts, anchor_ts),
                )
                unusual_volume_count = int(cur.fetchone()[0] or 0)

                cur.execute(
                    """
                    SELECT close - LAG(close, 5) OVER (PARTITION BY symbol ORDER BY timestamp)
                    FROM underlying_quotes
                    WHERE symbol = %s
                      AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, anchor_ts),
                )
                pc5_row = cur.fetchone()
                price_change_5min = float(pc5_row[0]) if pc5_row and pc5_row[0] else 0.0

                cur.execute(
                    """
                    SELECT SUM(CASE WHEN option_type = 'C' THEN total_premium ELSE -total_premium END)
                    FROM flow_by_type
                    WHERE symbol = %s
                      AND timestamp BETWEEN %s - INTERVAL '5 minutes' AND %s
                    """,
                    (self.db_symbol, anchor_ts, anchor_ts),
                )
                flow_row = cur.fetchone()
                net_option_flow_5min = float(flow_row[0]) if flow_row and flow_row[0] else 0.0

                cur.execute(
                    """
                    SELECT MIN(expiration)
                    FROM option_chains
                    WHERE underlying = %s
                      AND timestamp = (
                          SELECT MAX(timestamp) FROM option_chains WHERE underlying = %s AND timestamp <= %s
                      )
                      AND expiration >= DATE(%s AT TIME ZONE 'America/New_York')
                    """,
                    (self.db_symbol, self.db_symbol, anchor_ts, ts),
                )
                expiry_row = cur.fetchone()
                next_expiry = expiry_row[0] if expiry_row else None
                hours_to_expiry = self._hours_to_expiry(ts, next_expiry)

                return VolExpansionContext(
                    timestamp=ts,
                    current_price=current_price,
                    net_gex=float(net_gex or 0.0),
                    gamma_flip=float(gamma_flip) if gamma_flip is not None else None,
                    dealer_net_delta=dealer_net_delta,
                    smart_call_premium=smart_call,
                    smart_put_premium=smart_put,
                    put_call_ratio=float(pcr or 1.0),
                    vanna_exposure=vanna,
                    charm_exposure=charm,
                    max_pain=float(max_pain or current_price),
                    orb_status=orb_status,
                    vwap=vwap,
                    vwap_deviation_pct=vwap_dev,
                    unusual_volume_count=unusual_volume_count,
                    price_change_5min=price_change_5min,
                    net_option_flow_5min=net_option_flow_5min,
                    hours_to_next_expiry=hours_to_expiry,
                )
        except Exception as exc:
            logger.error("VolExpansionEngine._fetch_context failed: %s", exc, exc_info=True)
            return None

    @staticmethod
    def _hours_to_expiry(ts: datetime, expiry_date) -> float:
        if not expiry_date:
            return 999.0
        local_ts = ts.astimezone(ET) if ts.tzinfo else ET.localize(ts)
        expiry_dt = ET.localize(datetime.combine(expiry_date, datetime.strptime("16:00:00", "%H:%M:%S").time()))
        return max((expiry_dt - local_ts).total_seconds() / 3600.0, 0.0)

    @staticmethod
    def _normalize(composite_score: int, max_possible: int) -> float:
        return round(min(abs(composite_score) / max_possible, 1.0), 4) if max_possible else 0.0

    def _smart_money_direction(self, call_premium: float, put_premium: float) -> str:
        if call_premium > put_premium * self.thresholds.smart_money_ratio:
            return "up"
        if put_premium > call_premium * self.thresholds.smart_money_ratio:
            return "down"
        return "neutral"

    def _score_gamma_instability(self, ctx: VolExpansionContext) -> tuple[int, str, Optional[float]]:
        if not ctx.gamma_flip:
            return 0, "No gamma flip available.", None
        distance = abs(ctx.current_price - ctx.gamma_flip) / ctx.gamma_flip
        if distance < self.thresholds.gamma_flip_near_pct:
            if ctx.net_gex < self.thresholds.deep_negative_gex:
                return 10, "Price is sitting on the gamma flip with deeply negative GEX.", distance
            if ctx.net_gex < 0:
                return 7, "Price is near the gamma flip in a short-gamma regime.", distance
            return -3, "Price is near the flip but positive GEX should damp volatility.", distance
        if ctx.current_price < ctx.gamma_flip and ctx.net_gex < self.thresholds.negative_gex:
            return -5, "Below gamma flip with negative GEX increases downside chase risk.", distance
        if ctx.current_price > ctx.gamma_flip and ctx.net_gex < self.thresholds.negative_gex:
            return 5, "Above gamma flip with negative GEX increases upside squeeze risk.", distance
        return 0, "Gamma regime is not near an unstable transition.", distance

    def _score_dealer_pressure(self, ctx: VolExpansionContext) -> tuple[int, str, float]:
        normalized_delta = ctx.dealer_net_delta / 1e9
        if abs(normalized_delta) > 10:
            raw = 8 if normalized_delta > 0 else -8
        elif abs(normalized_delta) > 5:
            raw = 5 if normalized_delta > 0 else -5
        else:
            raw = 0
        desc = f"Dealer hedge pressure is {normalized_delta:+.2f}B delta-equivalent."
        return raw, desc, normalized_delta

    def _score_smart_money_flow(self, ctx: VolExpansionContext) -> tuple[int, str, float]:
        imbalance = ctx.smart_call_premium - ctx.smart_put_premium
        if imbalance > 50e6:
            raw = 7
        elif imbalance < -50e6:
            raw = -7
        elif abs(imbalance) > 25e6:
            raw = 4 if imbalance > 0 else -4
        else:
            raw = 0
        desc = (
            f"30-minute smart-money premium imbalance is ${imbalance:,.0f} "
            f"(calls ${ctx.smart_call_premium:,.0f} vs puts ${ctx.smart_put_premium:,.0f})."
        )
        return raw, desc, imbalance

    def _score_vanna_risk(self, ctx: VolExpansionContext) -> tuple[int, str, float]:
        exposure = ctx.vanna_exposure
        if exposure > 5e9:
            raw = 4
        elif exposure < -5e9:
            raw = -4
        else:
            raw = 0
        desc = f"Aggregate vanna exposure is {exposure:,.0f}."
        return raw, desc, exposure

    def _score_max_pain_breakdown(self, ctx: VolExpansionContext) -> tuple[int, str, float]:
        if not ctx.max_pain:
            return 0, "No max pain anchor available.", 0.0
        deviation = abs(ctx.current_price - ctx.max_pain) / ctx.max_pain
        if deviation > 0.015:
            raw = 6 if ctx.current_price > ctx.max_pain else -6
        elif deviation > 0.01:
            raw = 3 if ctx.current_price > ctx.max_pain else -3
        else:
            raw = -2 if ctx.net_gex > 0 else 0
        desc = f"Price is {deviation:.2%} away from max pain."
        return raw, desc, deviation

    def _score_put_call_extreme(self, ctx: VolExpansionContext) -> tuple[int, str, float]:
        pcr = ctx.put_call_ratio or 1.0
        if pcr > self.thresholds.pcr_high:
            raw = 5
        elif pcr < self.thresholds.pcr_low:
            raw = -5
        else:
            raw = 0
        desc = f"Put/call ratio is {pcr:.2f}."
        return raw, desc, pcr

    def _score_vol_oi_surge(self, ctx: VolExpansionContext) -> tuple[int, str, int]:
        count = ctx.unusual_volume_count
        if count > 10:
            raw = 6
        elif count > 5:
            raw = 3
        else:
            raw = 0
        desc = f"Detected {count} high-turnover strikes in the last 15 minutes."
        return raw, desc, count

    def _score_charm_decay(self, ctx: VolExpansionContext) -> tuple[int, str, float]:
        if ctx.hours_to_next_expiry > 24:
            return 0, "Charm decay not yet acute.", ctx.hours_to_next_expiry
        if abs(ctx.charm_exposure) > 5e9 and ctx.hours_to_next_expiry < 4:
            raw = 7 if ctx.charm_exposure < 0 else -7
        elif abs(ctx.charm_exposure) > 3e9 and ctx.hours_to_next_expiry < 8:
            raw = 4 if ctx.charm_exposure < 0 else -4
        else:
            raw = 0
        desc = (
            f"Charm exposure is {ctx.charm_exposure:,.0f} with "
            f"{ctx.hours_to_next_expiry:.1f} hours to expiry."
        )
        return raw, desc, ctx.charm_exposure

    def _score_orb_confirmation(self, ctx: VolExpansionContext) -> tuple[int, str, Optional[float]]:
        status = (ctx.orb_status or "").lower()
        if "breakout" in status or "long" in status:
            return 4, f"ORB confirms upside continuation ({ctx.orb_status}).", None
        if "breakdown" in status or "short" in status:
            return -4, f"ORB confirms downside continuation ({ctx.orb_status}).", None
        return 0, "No ORB expansion signal.", None

    def _score_vwap_extension(self, ctx: VolExpansionContext) -> tuple[int, str, float]:
        dev = ctx.vwap_deviation_pct
        if abs(dev) > 1.0:
            if ctx.net_gex < 0:
                raw = 5 if dev > 0 else -5
            else:
                raw = -3 if dev > 0 else 3
        else:
            raw = 0
        desc = f"Price is {dev:+.2f}% vs VWAP."
        return raw, desc, dev

    def compute_signal(self, ctx: VolExpansionContext) -> VolExpansionSignal:
        scorers = [
            ("gamma_instability", "Gamma Instability", self._score_gamma_instability),
            ("dealer_pressure", "Dealer Pressure", self._score_dealer_pressure),
            ("smart_money_flow", "Smart Money Flow", self._score_smart_money_flow),
            ("vanna_risk", "Vanna Risk", self._score_vanna_risk),
            ("max_pain_breakdown", "Max Pain Breakdown", self._score_max_pain_breakdown),
            ("put_call_extreme", "Put/Call Extreme", self._score_put_call_extreme),
            ("vol_oi_surge", "Volume/OI Surge", self._score_vol_oi_surge),
            ("charm_decay", "Charm Decay", self._score_charm_decay),
            ("orb_confirmation", "ORB Confirmation", self._score_orb_confirmation),
            ("vwap_extension", "VWAP Extension", self._score_vwap_extension),
        ]
        components: list[VolComponent] = []
        composite = 0
        catalyst_rank: list[tuple[str, int]] = []

        for key, label, scorer in scorers:
            raw, desc, value = scorer(ctx)
            weight = VOL_SIGNAL_WEIGHTS[key]
            weighted = raw * weight
            composite += weighted
            components.append(
                VolComponent(
                    name=label,
                    weight=weight,
                    raw_score=raw,
                    weighted_score=weighted,
                    description=desc,
                    value=float(value) if isinstance(value, (int, float)) else value,
                )
            )
            catalyst_rank.append((key, abs(weighted)))

        components.append(
            VolComponent(
                name="Calibration Snapshot",
                weight=0,
                raw_score=0,
                weighted_score=0,
                description=(
                    f"deep_neg_gex={self.thresholds.deep_negative_gex:.0f}, "
                    f"neg_gex={self.thresholds.negative_gex:.0f}, "
                    f"flip_near={self.thresholds.gamma_flip_near_pct:.4f}, "
                    f"pcr_hi={self.thresholds.pcr_high:.3f}, "
                    f"pcr_lo={self.thresholds.pcr_low:.3f}"
                ),
                value=None,
            )
        )

        max_possible = sum(weight * 10 for weight in VOL_SIGNAL_WEIGHTS.values())
        normalized = self._normalize(composite, max_possible)

        if normalized >= 0.70:
            move_prob, magnitude, confidence = 0.85, 0.75, "high"
        elif normalized >= 0.50:
            move_prob, magnitude, confidence = 0.65, 0.50, "medium"
        elif normalized >= 0.35:
            move_prob, magnitude, confidence = 0.45, 0.35, "low"
        else:
            move_prob, magnitude, confidence = 0.20, 0.15, "low"

        expected_direction = "up" if composite > 0 else ("down" if composite < 0 else "neutral")
        smart_money_direction = self._smart_money_direction(ctx.smart_call_premium, ctx.smart_put_premium)
        top_catalyst = max(catalyst_rank, key=lambda item: item[1])[0] if catalyst_rank else "mixed"
        catalyst_map = {
            "gamma_instability": "gamma_squeeze",
            "dealer_pressure": "dealer_hedging",
            "smart_money_flow": "informed_flow",
            "max_pain_breakdown": "liquidity_void",
            "charm_decay": "expiry_unwind",
        }
        catalyst_type = catalyst_map.get(top_catalyst, "mixed")

        if move_prob >= 0.65 and expected_direction == "neutral":
            strategy_type = "long_straddle"
        elif move_prob >= 0.65:
            strategy_type = "directional_debit_spread"
        elif ctx.net_gex > 0 and move_prob < 0.35:
            strategy_type = "premium_selling_range_trade"
        else:
            strategy_type = "wait"

        if ctx.hours_to_next_expiry <= 8:
            time_horizon = "intraday"
        elif move_prob >= 0.65:
            time_horizon = "overnight"
        else:
            time_horizon = "multi_day"

        if ctx.hours_to_next_expiry <= 6:
            entry_window = "Now through 14:00 ET"
        elif expected_direction == "neutral":
            entry_window = "10:00-14:00 ET"
        else:
            entry_window = "After first 30 minutes of cash session"

        return VolExpansionSignal(
            underlying=self.db_symbol,
            timestamp=ctx.timestamp,
            composite_score=composite,
            max_possible_score=max_possible,
            normalized_score=normalized,
            move_probability=move_prob,
            expected_direction=expected_direction,
            expected_magnitude_pct=magnitude,
            confidence=confidence,
            catalyst_type=catalyst_type,
            time_horizon=time_horizon,
            strategy_type=strategy_type,
            entry_window=entry_window,
            current_price=ctx.current_price,
            net_gex=ctx.net_gex,
            gamma_flip=ctx.gamma_flip,
            max_pain=ctx.max_pain,
            put_call_ratio=ctx.put_call_ratio,
            dealer_net_delta=ctx.dealer_net_delta,
            smart_money_direction=smart_money_direction,
            vwap_deviation_pct=ctx.vwap_deviation_pct,
            hours_to_next_expiry=ctx.hours_to_next_expiry,
            components=components,
        )

    def _fetch_calibrated_move_probability(self, confidence: str, lookback_days: int = 60) -> Optional[float]:
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT SUM(total_signals), SUM(large_move_hits)
                    FROM vol_expansion_accuracy
                    WHERE underlying = %s
                      AND confidence = %s
                      AND trade_date >= CURRENT_DATE - %s
                    """,
                    (self.db_symbol, confidence, lookback_days),
                )
                row = cur.fetchone()
                if row and row[0] and row[1] is not None:
                    return round(float(row[1]) / float(row[0]), 4)
        except Exception as exc:
            logger.error("_fetch_calibrated_move_probability failed: %s", exc)
        return None

    def _store_signal(self, signal: VolExpansionSignal) -> None:
        components_json = json.dumps([asdict(component) for component in signal.components])
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO volatility_expansion_signals (
                        underlying, timestamp, composite_score, max_possible_score,
                        normalized_score, move_probability, expected_direction,
                        expected_magnitude_pct, confidence, catalyst_type, time_horizon,
                        strategy_type, entry_window, current_price, net_gex, gamma_flip,
                        max_pain, put_call_ratio, dealer_net_delta, smart_money_direction,
                        vwap_deviation_pct, hours_to_next_expiry, components
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (underlying, timestamp) DO UPDATE SET
                        composite_score = EXCLUDED.composite_score,
                        max_possible_score = EXCLUDED.max_possible_score,
                        normalized_score = EXCLUDED.normalized_score,
                        move_probability = EXCLUDED.move_probability,
                        expected_direction = EXCLUDED.expected_direction,
                        expected_magnitude_pct = EXCLUDED.expected_magnitude_pct,
                        confidence = EXCLUDED.confidence,
                        catalyst_type = EXCLUDED.catalyst_type,
                        time_horizon = EXCLUDED.time_horizon,
                        strategy_type = EXCLUDED.strategy_type,
                        entry_window = EXCLUDED.entry_window,
                        current_price = EXCLUDED.current_price,
                        net_gex = EXCLUDED.net_gex,
                        gamma_flip = EXCLUDED.gamma_flip,
                        max_pain = EXCLUDED.max_pain,
                        put_call_ratio = EXCLUDED.put_call_ratio,
                        dealer_net_delta = EXCLUDED.dealer_net_delta,
                        smart_money_direction = EXCLUDED.smart_money_direction,
                        vwap_deviation_pct = EXCLUDED.vwap_deviation_pct,
                        hours_to_next_expiry = EXCLUDED.hours_to_next_expiry,
                        components = EXCLUDED.components,
                        updated_at = NOW()
                    """,
                    (
                        signal.underlying,
                        signal.timestamp,
                        signal.composite_score,
                        signal.max_possible_score,
                        signal.normalized_score,
                        signal.move_probability,
                        signal.expected_direction,
                        signal.expected_magnitude_pct,
                        signal.confidence,
                        signal.catalyst_type,
                        signal.time_horizon,
                        signal.strategy_type,
                        signal.entry_window,
                        signal.current_price,
                        signal.net_gex,
                        signal.gamma_flip,
                        signal.max_pain,
                        signal.put_call_ratio,
                        signal.dealer_net_delta,
                        signal.smart_money_direction,
                        signal.vwap_deviation_pct,
                        signal.hours_to_next_expiry,
                        components_json,
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.error("VolExpansionEngine._store_signal failed: %s", exc, exc_info=True)

    def _snapshot_accuracy(self, signal_ts: datetime, move_threshold_pct: float = 0.5) -> Optional[AccuracySnapshot]:
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
                        (SELECT close FROM day_quotes ORDER BY timestamp ASC LIMIT 1) AS open_px,
                        (SELECT close FROM day_quotes ORDER BY timestamp DESC LIMIT 1) AS close_px,
                        (SELECT MAX(high) FROM day_quotes) AS day_high,
                        (SELECT MIN(low) FROM day_quotes) AS day_low
                    """,
                    (self.db_symbol, next_date),
                )
                row = cur.fetchone()
                if not row or row[0] is None or row[1] is None:
                    return None
                open_px, close_px, day_high, day_low = map(float, row)
                close_to_close = ((close_px - open_px) / open_px) * 100.0
                intraday_range = max(abs(day_high - open_px), abs(day_low - open_px)) / open_px * 100.0
                actual_move_pct = max(abs(close_to_close), intraday_range)
                actual_direction = "up" if close_to_close > 0 else ("down" if close_to_close < 0 else "neutral")
                hit_large_move = actual_move_pct >= move_threshold_pct
                magnitude_bucket = "large" if actual_move_pct >= 0.5 else "small"
                return AccuracySnapshot(
                    move_threshold_pct=move_threshold_pct,
                    actual_move_pct=round(actual_move_pct, 4),
                    actual_direction=actual_direction,
                    hit_large_move=hit_large_move,
                    direction_correct=False,
                    magnitude_bucket=magnitude_bucket,
                )
        except Exception as exc:
            logger.error("VolExpansionEngine._snapshot_accuracy failed: %s", exc, exc_info=True)
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
                    SELECT timestamp, confidence, expected_direction, catalyst_type, move_probability
                    FROM volatility_expansion_signals
                    WHERE underlying = %s
                      AND DATE(timestamp AT TIME ZONE 'America/New_York') = %s
                    """,
                    (self.db_symbol, eval_date),
                )
                rows = cur.fetchall()
                if not rows:
                    self._last_accuracy_update = today
                    return

                buckets: dict[tuple[str, str], dict[str, int]] = {}
                for signal_ts, confidence, expected_direction, catalyst_type, move_probability in rows:
                    snapshot = self._snapshot_accuracy(signal_ts)
                    if snapshot is None:
                        continue
                    direction_correct = expected_direction in ("neutral", snapshot.actual_direction) or (
                        expected_direction == snapshot.actual_direction
                    )
                    bucket = buckets.setdefault(
                        (confidence, catalyst_type),
                        {"total": 0, "large_move_hits": 0, "direction_correct": 0, "prob_sum": 0.0},
                    )
                    bucket["total"] += 1
                    bucket["large_move_hits"] += int(snapshot.hit_large_move)
                    bucket["direction_correct"] += int(direction_correct)
                    bucket["prob_sum"] += float(move_probability)

                for (confidence, catalyst_type), stats in buckets.items():
                    total = stats["total"]
                    if total == 0:
                        continue
                    cur.execute(
                        """
                        INSERT INTO vol_expansion_accuracy (
                            underlying, trade_date, confidence, catalyst_type,
                            total_signals, large_move_hits, direction_correct_hits,
                            empirical_move_pct, avg_predicted_probability
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (underlying, trade_date, confidence, catalyst_type)
                        DO UPDATE SET
                            total_signals = EXCLUDED.total_signals,
                            large_move_hits = EXCLUDED.large_move_hits,
                            direction_correct_hits = EXCLUDED.direction_correct_hits,
                            empirical_move_pct = EXCLUDED.empirical_move_pct,
                            avg_predicted_probability = EXCLUDED.avg_predicted_probability,
                            updated_at = NOW()
                        """,
                        (
                            self.db_symbol,
                            eval_date,
                            confidence,
                            catalyst_type,
                            total,
                            stats["large_move_hits"],
                            stats["direction_correct"],
                            round(stats["large_move_hits"] / total, 4),
                            round(stats["prob_sum"] / total, 4),
                        ),
                    )
                conn.commit()
                self._last_accuracy_update = today
        except Exception as exc:
            logger.error("VolExpansionEngine._update_accuracy failed: %s", exc, exc_info=True)

    def run_calculation(self) -> bool:
        self._auto_tune_thresholds()
        ctx = self._fetch_context()
        if ctx is None:
            logger.warning("VolExpansionEngine: no context available, skipping")
            return False

        signal = self.compute_signal(ctx)
        calibrated = self._fetch_calibrated_move_probability(signal.confidence)
        if calibrated is not None:
            signal.move_probability = calibrated
        else:
            signal.move_probability = MOVE_PROBABILITY_DEFAULTS.get(signal.confidence, signal.move_probability)

        self._store_signal(signal)
        self._update_accuracy()

        logger.info(
            "✅ Vol expansion [%s] %s | score=%s/%s (%.0f%%) | move_prob=%.0f%% | catalyst=%s | strategy=%s",
            self.underlying,
            signal.expected_direction.upper(),
            signal.composite_score,
            signal.max_possible_score,
            signal.normalized_score * 100,
            signal.move_probability * 100,
            signal.catalyst_type,
            signal.strategy_type,
        )
        return True


def main() -> None:
    parser = argparse.ArgumentParser(description="ZeroGEX volatility expansion engine")
    parser.add_argument("--underlying", default="SPY")
    args = parser.parse_args()
    ok = VolExpansionEngine(underlying=args.underlying).run_calculation()
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
