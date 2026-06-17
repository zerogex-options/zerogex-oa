"""Regime Narrative Classifier.

Translates the internal MSI regime + GEX posture into one of five novice-
comprehensible labels with confidence, expected behavior, and what-to-avoid.

See ``docs/design/gex_copilot_architecture.md`` §3 for the spec. This module
implements the deterministic rule set; thresholds in this PR are the spec
priors and are intended to be tuned against backtest in a later PR.

The classifier is a pure function over ``PlaybookContext`` and an optional
prior ``RegimeNarrative`` (for hysteresis). It does no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from src.signals.playbook.context import PlaybookContext


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------


LABEL_LONG_GAMMA_PIN = "LONG_GAMMA_PIN"
LABEL_SHORT_GAMMA_TREND = "SHORT_GAMMA_TREND"
LABEL_VOL_EXPANSION = "VOL_EXPANSION"
LABEL_VANNA_GLIDE = "VANNA_GLIDE"
LABEL_CHARM_DRIFT = "CHARM_DRIFT"
LABEL_TRANSITION = "TRANSITION"
LABEL_UNDEFINED = "UNDEFINED"


# Per-label descriptors. Surfaced verbatim in the output so the LLM and the
# UI both read from the same source of truth.
_DESCRIPTORS: dict[str, dict[str, Any]] = {
    LABEL_LONG_GAMMA_PIN: {
        "expected_behavior": (
            "Dealers are long gamma — expect chop and mean-reversion toward a "
            "magnet level (max pain or the nearest wall) with low realized vol."
        ),
        "favored_patterns": ["call_wall_fade", "put_wall_bounce", "pin_risk_premium_sell"],
        "avoid": [
            "Chasing breakouts",
            "Holding directional 0DTEs through chop",
            "Buying expensive premium near a wall",
        ],
    },
    LABEL_SHORT_GAMMA_TREND: {
        "expected_behavior": (
            "Dealers are short gamma — their hedging amplifies moves. Expect "
            "trend continuation, wider ranges, and vol-expansion risk."
        ),
        "favored_patterns": ["gamma_flip_break", "gex_gradient_trend", "squeeze_breakout"],
        "avoid": [
            "Fading the move",
            "Selling naked premium",
            "Tight time-based stops",
        ],
    },
    LABEL_VOL_EXPANSION: {
        "expected_behavior": (
            "Volatility is breaking out of its recent regime. Expect wider candles, "
            "gap risk, and chaotic dealer hedging."
        ),
        "favored_patterns": ["squeeze_breakout", "gamma_flip_break"],
        "avoid": [
            "Tight stops",
            "Selling premium without a hedge",
            "Overnight long gamma without protection",
        ],
    },
    LABEL_VANNA_GLIDE: {
        "expected_behavior": (
            "Vol direction is dragging spot via vanna. Expect a smooth directional "
            "drift with realized vol below implied."
        ),
        "favored_patterns": ["vanna_charm_glide"],
        "avoid": [
            "Fading the drift",
            "Mean-reversion plays into the direction of glide",
        ],
    },
    LABEL_CHARM_DRIFT: {
        "expected_behavior": (
            "Time decay is pulling spot toward max pain into the close. Expect "
            "slow drift to the OI pivot and last-hour close-direction skew."
        ),
        "favored_patterns": ["eod_pressure_drift", "zero_dte_imbalance_drift"],
        "avoid": [
            "Holding 0DTE OTM hoping for a move",
            "Initiating new directional swing entries late in the day",
        ],
    },
    LABEL_TRANSITION: {
        "expected_behavior": (
            "Conditions are ambiguous; the regime is in flux. Pattern edges are "
            "reduced until one regime resolves."
        ),
        "favored_patterns": [],
        "avoid": ["All new entries — wait for resolution"],
    },
    LABEL_UNDEFINED: {
        "expected_behavior": "Insufficient data to classify the regime.",
        "favored_patterns": [],
        "avoid": [],
    },
}


# Spec priors. PR-3 will replace with empirically tuned values.
NET_GEX_LONG_THRESHOLD = 1.0e9
NET_GEX_SHORT_THRESHOLD = -1.0e9
NET_GEX_NEAR_ZERO = 5.0e8

VIX_CHANGE_VOL_EXPANSION = 5.0  # percent day-over-day
VIX_CHANGE_VANNA = 2.0  # percent day-over-day, either direction
REALIZED_VS_IMPLIED_RATIO = 0.85

VANNA_CHARM_SCORE_GLIDE = 60.0  # abs score
ZERO_DTE_IMBALANCE_SCORE = 60.0
EOD_PRESSURE_SCORE = 60.0
TAPE_BIAS_CONFIRM = 0.0  # sign-of-score must match sign(spot - gamma_flip)

PIN_PROXIMITY_PCT = 0.003  # within 0.3%
CHARM_DRIFT_PROXIMITY_PCT = 0.005  # within 0.5% and converging
REALIZED_VOL_PIN_MAX = 0.10  # annualized
CHARM_DRIFT_WINDOW_MIN = 90

HYSTERESIS_CONFIDENCE_DELTA = 0.10


# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RegimeNarrative:
    """Plain-English regime label with confidence and audit trail.

    See ``docs/design/gex_copilot_architecture.md`` §3.7.
    """

    timestamp: datetime
    symbol: str
    label: str
    confidence: float
    spot: float
    expected_behavior: str
    favored_patterns: list[str]
    avoid: list[str]
    what_would_flip_it: str
    msi_regime: str
    inputs_snapshot: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "symbol": self.symbol,
            "label": self.label,
            "confidence": round(self.confidence, 4),
            "spot": self.spot,
            "expected_behavior": self.expected_behavior,
            "favored_patterns": list(self.favored_patterns),
            "avoid": list(self.avoid),
            "what_would_flip_it": self.what_would_flip_it,
            "msi_regime": self.msi_regime,
            "inputs": self.inputs_snapshot,
        }


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


def classify_regime(
    ctx: PlaybookContext,
    *,
    prior: Optional[RegimeNarrative] = None,
    vix_change_pct: Optional[float] = None,
    realized_vol_30m: Optional[float] = None,
    max_pain_convergence: Optional[float] = None,
) -> RegimeNarrative:
    """Classify the current regime from a ``PlaybookContext``.

    Parameters
    ----------
    ctx
        Built upstream in ``PlaybookEngine.cycle()``; already carries MSI
        snapshot, signal snapshots, and live levels.
    prior
        The previous cycle's narrative for this symbol, for hysteresis.
    vix_change_pct
        Day-over-day VIX change in percent. The caller computes this from
        ``vix_ingester`` history; passed in so the classifier stays pure.
    realized_vol_30m
        Annualized 30-minute realized vol. Computed upstream from the
        rolling stdev of 1-min closes.
    max_pain_convergence
        Signed change in ``|spot - max_pain|`` over the last 10 minutes.
        Negative means spot is converging toward max pain. The caller
        computes this from the ``regime_narratives`` history.

    Returns
    -------
    RegimeNarrative
        Always returns a value; ``UNDEFINED`` when inputs insufficient.
    """

    inputs = _snapshot_inputs(
        ctx,
        vix_change_pct=vix_change_pct,
        realized_vol_30m=realized_vol_30m,
        max_pain_convergence=max_pain_convergence,
    )

    if not _has_minimum_inputs(inputs):
        return _make(ctx, LABEL_UNDEFINED, confidence=0.0, inputs=inputs,
                    what_flips="Missing required inputs (GEX, spot, or MSI regime).")

    candidates = [
        _eval_vol_expansion(ctx, inputs),
        _eval_vanna_glide(ctx, inputs),
        _eval_short_gamma_trend(ctx, inputs),
        _eval_charm_drift(ctx, inputs),
        _eval_long_gamma_pin(ctx, inputs),
    ]

    matched = next((c for c in candidates if c is not None and c[1] == "matched"), None)
    if matched is None:
        near_miss = any(c is not None and c[1] == "near_miss" for c in candidates)
        if near_miss:
            return _make(
                ctx,
                LABEL_TRANSITION,
                confidence=0.40,
                inputs=inputs,
                what_flips="One of the active regime rules is close to crossing — watch the input nearest its threshold.",
            )
        return _make(
            ctx,
            LABEL_UNDEFINED,
            confidence=0.0,
            inputs=inputs,
            what_flips="No regime rule near its trigger.",
        )

    label, _, fit, what_flips = matched
    confidence = _confidence(0.70, fit)
    candidate = _make(ctx, label, confidence=confidence, inputs=inputs, what_flips=what_flips)

    return _apply_hysteresis(candidate, prior)


# ---------------------------------------------------------------------------
# Internal: per-rule evaluators
#
# Each returns (label, status, fit, what_would_flip_it) where status is
# "matched" | "near_miss" | None. Ordered evaluation: first matched wins.
# ---------------------------------------------------------------------------


def _eval_vol_expansion(ctx: PlaybookContext, inp: dict[str, Any]) -> Optional[tuple[str, str, float, str]]:
    vol_exp = ctx.advanced("vol_expansion")
    triggered = bool(vol_exp.triggered) if vol_exp else False
    vix_chg = inp.get("vix_change_pct")
    realized = inp.get("realized_vol_30m")
    vix = inp.get("vix_level")

    criteria_pass = 0
    criteria_total = 3
    margins: list[float] = []

    if triggered:
        criteria_pass += 1
        margins.append(1.0)

    if vix_chg is not None and vix_chg >= VIX_CHANGE_VOL_EXPANSION:
        criteria_pass += 1
        margins.append(_margin(vix_chg, VIX_CHANGE_VOL_EXPANSION))
    elif realized is not None and vix and vix > 0 and (realized / (vix / 100.0)) >= REALIZED_VS_IMPLIED_RATIO:
        criteria_pass += 1
        margins.append(_margin(realized / (vix / 100.0), REALIZED_VS_IMPLIED_RATIO))

    net_gex = inp.get("net_gex") or 0.0
    if abs(net_gex) < NET_GEX_NEAR_ZERO or net_gex < 0:
        criteria_pass += 1
        margins.append(1.0)

    if criteria_pass == criteria_total:
        return (LABEL_VOL_EXPANSION, "matched", _avg(margins),
                "VIX change drops below 5% AND realized vol stays under implied.")
    if criteria_pass / criteria_total >= 0.70:
        return (LABEL_VOL_EXPANSION, "near_miss", 0.0, "")
    return None


def _eval_vanna_glide(ctx: PlaybookContext, inp: dict[str, Any]) -> Optional[tuple[str, str, float, str]]:
    vc = ctx.basic("vanna_charm_flow")
    vc_score = abs(vc.score) if vc else 0.0
    vix_chg = inp.get("vix_change_pct")

    criteria_pass = 0
    criteria_total = 2
    margins: list[float] = []

    if vc_score >= VANNA_CHARM_SCORE_GLIDE:
        criteria_pass += 1
        margins.append(_margin(vc_score, VANNA_CHARM_SCORE_GLIDE))

    if vix_chg is not None and abs(vix_chg) >= VIX_CHANGE_VANNA:
        criteria_pass += 1
        margins.append(_margin(abs(vix_chg), VIX_CHANGE_VANNA))

    if criteria_pass == criteria_total:
        return (LABEL_VANNA_GLIDE, "matched", _avg(margins),
                "Vanna-charm flow weakens below 60 or VIX stops moving.")
    if criteria_pass / criteria_total >= 0.70:
        return (LABEL_VANNA_GLIDE, "near_miss", 0.0, "")
    return None


def _eval_short_gamma_trend(ctx: PlaybookContext, inp: dict[str, Any]) -> Optional[tuple[str, str, float, str]]:
    net_gex = inp.get("net_gex")
    spot = inp.get("spot")
    flip = inp.get("gamma_flip")
    msi_regime = inp.get("msi_regime") or ""
    tape = ctx.basic("tape_flow_bias")
    tape_score = tape.score if tape else 0.0

    if net_gex is None or spot is None or flip is None:
        return None

    criteria_pass = 0
    criteria_total = 4
    margins: list[float] = []

    if net_gex < NET_GEX_SHORT_THRESHOLD:
        criteria_pass += 1
        margins.append(_margin(abs(net_gex), abs(NET_GEX_SHORT_THRESHOLD)))

    direction = 1 if spot > flip else -1 if spot < flip else 0
    if direction != 0:
        criteria_pass += 1
        margins.append(1.0)

    tape_dir = 1 if tape_score > 0 else -1 if tape_score < 0 else 0
    if direction != 0 and tape_dir == direction:
        criteria_pass += 1
        margins.append(min(abs(tape_score) / 50.0, 1.5))

    if msi_regime in {"trend_expansion", "controlled_trend"}:
        criteria_pass += 1
        margins.append(1.0)

    if criteria_pass == criteria_total:
        return (LABEL_SHORT_GAMMA_TREND, "matched", _avg(margins),
                "Net GEX rises above -$1B or tape flow flips against the move.")
    if criteria_pass / criteria_total >= 0.70:
        return (LABEL_SHORT_GAMMA_TREND, "near_miss", 0.0, "")
    return None


def _eval_charm_drift(ctx: PlaybookContext, inp: dict[str, Any]) -> Optional[tuple[str, str, float, str]]:
    minutes_to_close = ctx.minutes_to_close
    zdi = ctx.advanced("zero_dte_position_imbalance")
    eod = ctx.advanced("eod_pressure")
    zdi_score = zdi.score if zdi else 0.0
    eod_score = eod.score if eod else 0.0
    spot = inp.get("spot")
    mp = inp.get("max_pain")
    convergence = inp.get("max_pain_convergence")

    if minutes_to_close > CHARM_DRIFT_WINDOW_MIN or minutes_to_close < 0:
        return None
    if spot is None or mp is None:
        return None

    criteria_pass = 0
    criteria_total = 3
    margins: list[float] = []

    criteria_pass += 1
    margins.append(1.0)

    if zdi_score >= ZERO_DTE_IMBALANCE_SCORE or eod_score >= EOD_PRESSURE_SCORE:
        criteria_pass += 1
        margins.append(max(zdi_score, eod_score) / ZERO_DTE_IMBALANCE_SCORE)

    proximity = abs(spot - mp) / spot if spot else 1.0
    if proximity <= CHARM_DRIFT_PROXIMITY_PCT and (convergence is None or convergence <= 0):
        criteria_pass += 1
        margins.append(_margin(CHARM_DRIFT_PROXIMITY_PCT, proximity))

    if criteria_pass == criteria_total:
        return (LABEL_CHARM_DRIFT, "matched", _avg(margins),
                "Spot diverges from max pain or imbalance score collapses.")
    if criteria_pass / criteria_total >= 0.70:
        return (LABEL_CHARM_DRIFT, "near_miss", 0.0, "")
    return None


def _eval_long_gamma_pin(ctx: PlaybookContext, inp: dict[str, Any]) -> Optional[tuple[str, str, float, str]]:
    net_gex = inp.get("net_gex")
    spot = inp.get("spot")
    mp = inp.get("max_pain")
    cw = inp.get("call_wall")
    pw = inp.get("put_wall")
    realized = inp.get("realized_vol_30m")
    msi_regime = inp.get("msi_regime") or ""

    if net_gex is None or spot is None:
        return None

    criteria_pass = 0
    criteria_total = 4
    margins: list[float] = []

    if net_gex > NET_GEX_LONG_THRESHOLD:
        criteria_pass += 1
        margins.append(_margin(net_gex, NET_GEX_LONG_THRESHOLD))

    proximities = [
        abs(spot - lvl) / spot for lvl in (mp, cw, pw) if lvl is not None and spot
    ]
    nearest = min(proximities) if proximities else 1.0
    if nearest <= PIN_PROXIMITY_PCT:
        criteria_pass += 1
        margins.append(_margin(PIN_PROXIMITY_PCT, nearest))

    if realized is not None and realized <= REALIZED_VOL_PIN_MAX:
        criteria_pass += 1
        margins.append(_margin(REALIZED_VOL_PIN_MAX, realized))

    if msi_regime in {"chop_range", "high_risk_reversal"}:
        criteria_pass += 1
        margins.append(1.0)

    if criteria_pass == criteria_total:
        return (LABEL_LONG_GAMMA_PIN, "matched", _avg(margins),
                "Net GEX drops below +$1B, spot escapes a magnet level, or realized vol expands.")
    if criteria_pass / criteria_total >= 0.70:
        return (LABEL_LONG_GAMMA_PIN, "near_miss", 0.0, "")
    return None


# ---------------------------------------------------------------------------
# Internal: helpers
# ---------------------------------------------------------------------------


def _snapshot_inputs(
    ctx: PlaybookContext,
    *,
    vix_change_pct: Optional[float],
    realized_vol_30m: Optional[float],
    max_pain_convergence: Optional[float],
) -> dict[str, Any]:
    return {
        "net_gex": _safe(ctx, "net_gex"),
        "spot": _safe(ctx, "close"),
        "gamma_flip": ctx.level("gamma_flip"),
        "max_pain": ctx.level("max_pain"),
        "call_wall": ctx.level("call_wall"),
        "put_wall": ctx.level("put_wall"),
        "vix_level": getattr(ctx.market, "vix_level", None),
        "vix_change_pct": vix_change_pct,
        "realized_vol_30m": realized_vol_30m,
        "max_pain_convergence": max_pain_convergence,
        "msi_regime": ctx.msi_regime,
        "msi_score": ctx.msi_score,
        "minutes_to_close": ctx.minutes_to_close,
    }


def _has_minimum_inputs(inp: dict[str, Any]) -> bool:
    return inp["net_gex"] is not None and inp["spot"] is not None and inp["msi_regime"] is not None


def _safe(ctx: PlaybookContext, attr: str) -> Optional[float]:
    try:
        return getattr(ctx, attr)
    except (AttributeError, TypeError):
        return None


def _margin(actual: float, threshold: float) -> float:
    if threshold == 0:
        return 1.0
    return min(max(actual / threshold, 0.6), 1.3)


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 1.0


def _confidence(base: float, fit: float) -> float:
    raw = base * fit
    if raw != raw:  # NaN
        return 0.0
    return max(0.0, min(0.95, raw))


def _make(
    ctx: PlaybookContext,
    label: str,
    *,
    confidence: float,
    inputs: dict[str, Any],
    what_flips: str,
) -> RegimeNarrative:
    desc = _DESCRIPTORS[label]
    return RegimeNarrative(
        timestamp=ctx.timestamp,
        symbol=ctx.underlying,
        label=label,
        confidence=confidence,
        spot=ctx.close,
        expected_behavior=desc["expected_behavior"],
        favored_patterns=list(desc["favored_patterns"]),
        avoid=list(desc["avoid"]),
        what_would_flip_it=what_flips,
        msi_regime=ctx.msi_regime or "",
        inputs_snapshot=inputs,
    )


def _apply_hysteresis(
    candidate: RegimeNarrative,
    prior: Optional[RegimeNarrative],
) -> RegimeNarrative:
    """Damp label flapping at boundaries — see spec §3.6."""
    if prior is None or candidate.label == prior.label:
        return candidate
    if candidate.confidence >= prior.confidence + HYSTERESIS_CONFIDENCE_DELTA:
        return candidate
    # Emit TRANSITION until the new candidate clears prior + 0.10 confidence.
    desc = _DESCRIPTORS[LABEL_TRANSITION]
    return RegimeNarrative(
        timestamp=candidate.timestamp,
        symbol=candidate.symbol,
        label=LABEL_TRANSITION,
        confidence=min(prior.confidence, 0.40),
        spot=candidate.spot,
        expected_behavior=desc["expected_behavior"],
        favored_patterns=list(desc["favored_patterns"]),
        avoid=list(desc["avoid"]),
        what_would_flip_it=(
            f"Candidate label {candidate.label} needs ~"
            f"{prior.confidence + HYSTERESIS_CONFIDENCE_DELTA:.2f} confidence to commit."
        ),
        msi_regime=candidate.msi_regime,
        inputs_snapshot=candidate.inputs_snapshot,
    )
