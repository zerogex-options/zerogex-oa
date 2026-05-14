"""Pattern: ``gamma_flip_bounce`` — Test of the Gamma Flip as Support/Resistance.

The mirror of ``gamma_flip_break``.  Where ``gamma_flip_break`` trades the
cross *through* the flip (dealer regime change), this pattern trades the
*defense* of the flip when price tags it from one side and rejects back.

Setup (bullish leg):
  * Long-gamma backdrop (``net_gex`` above floor).
  * Price has been trading mostly above the flip in the prior window.
  * Most recent bars dipped to the flip (close at/below it on at least
    one bar, within a small tolerance), then the current close is back
    above the flip by a buffer.
  * Bullish flow corroboration (``tape_flow_bias`` or
    ``order_flow_imbalance``).
  * No active range-break signal (``range_break_imminence`` in
    "Breakout Mode" or ``vol_expansion`` triggered overrides the
    bounce — that's a different regime).

The bearish leg is the symmetric mirror (test from below, reject down).

Entry buffer is added past the flip in the bounce direction.  Stops sit
on the other side of the flip — if the flip fails to hold, the setup is
invalidated.  Targets walk to the nearest structural level in the bounce
direction (call_wall / put_wall / max_gamma_strike).

Time gate: 9:35 ET — earlier than the 10:00 ET gate on flip-related
break/bounce patterns because flip-defense in the first 30 minutes is a
highest-conviction setup, not noise.
"""

from __future__ import annotations

import math
import os
from datetime import time
from typing import Literal, Optional

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import (
    ActionCard,
    ActionEnum,
    Entry,
    Leg,
    Stop,
    Target,
)

# Env-overridable thresholds.
_NET_GEX_FLOOR = float(os.getenv("PLAYBOOK_GFBO_NET_GEX_FLOOR", "1.5e9"))
_FLIP_DISTANCE_MIN = float(os.getenv("PLAYBOOK_GFBO_FLIP_DISTANCE_MIN", "0.6"))
_TOUCH_TOLERANCE_PCT = float(os.getenv("PLAYBOOK_GFBO_TOUCH_TOL_PCT", "0.0010"))  # 0.10%
_REJECT_BUFFER_PCT = float(os.getenv("PLAYBOOK_GFBO_REJECT_BUFFER_PCT", "0.0005"))  # 0.05%
_PRIOR_MODE_BARS = int(os.getenv("PLAYBOOK_GFBO_PRIOR_MODE_BARS", "30"))
_PRIOR_MODE_THRESHOLD = float(os.getenv("PLAYBOOK_GFBO_PRIOR_MODE_THRESHOLD", "0.60"))
_RECENT_BARS = int(os.getenv("PLAYBOOK_GFBO_RECENT_BARS", "3"))
_FLOW_THRESHOLD = float(os.getenv("PLAYBOOK_GFBO_FLOW_THRESHOLD", "20.0"))
_STOP_PCT = float(os.getenv("PLAYBOOK_GFBO_STOP_PCT", "0.0030"))
_VOL_DEBIT_SWITCH = float(os.getenv("PLAYBOOK_GFBO_VOL_DEBIT_SWITCH", "0.0025"))
_SPREAD_WIDTH_POINTS = float(os.getenv("PLAYBOOK_GFBO_SPREAD_WIDTH", "5.0"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_GFBO_MAX_HOLD_MIN", "60"))
_OPEN_GATE = time(9, 35)


BounceDirection = Literal["bullish", "bearish"]


def _realized_sigma_30min(closes: list[float]) -> float:
    usable = [c for c in (closes or []) if c and c > 0][-30:]
    if len(usable) < 5:
        return 0.0
    rets = [
        (usable[i] - usable[i - 1]) / usable[i - 1]
        for i in range(1, len(usable))
        if usable[i - 1] > 0
    ]
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    return math.sqrt(max(var, 0.0))


def _detect_bounce(
    closes: list[float],
    flip: float,
    *,
    lows: Optional[list[float]] = None,
    highs: Optional[list[float]] = None,
    prior_bars: int = _PRIOR_MODE_BARS,
    recent_bars: int = _RECENT_BARS,
    prior_threshold: float = _PRIOR_MODE_THRESHOLD,
    touch_tol_pct: float = _TOUCH_TOLERANCE_PCT,
    reject_buffer_pct: float = _REJECT_BUFFER_PCT,
) -> Optional[BounceDirection]:
    """Detect a flip-tag-and-reject from trailing bars.

    Uses ``lows`` for bullish-touch detection and ``highs`` for
    bearish-touch detection when available — that's the real wick
    rejection signal.  Falls back to ``closes`` for both touch and
    rejection when the OHL arrays are empty or misaligned (older test
    fixtures, async API path that doesn't fetch bar data).

    Bullish: prior window mostly *above* flip, recent window's lowest
    low pierced flip (low <= flip * (1 + touch_tol_pct)), and the most
    recent close is back above flip by reject_buffer_pct.

    Bearish: mirror — prior mostly below, recent window's highest high
    pierced flip from below, latest close back below by buffer.

    Returns None if neither pattern holds (e.g., clean cross-through,
    no test, or insufficient data).
    """
    usable = [c for c in (closes or []) if c and c > 0]
    if flip is None or flip <= 0:
        return None
    if len(usable) < recent_bars + 5:
        return None

    if len(usable) > recent_bars:
        prior_window = usable[-(prior_bars + recent_bars) : -recent_bars]
        recent_window = usable[-recent_bars:]
    else:
        return None
    if len(prior_window) < 5:
        return None

    # Pull the same window from lows/highs if available and aligned.
    def _aligned_tail(series: Optional[list[float]]) -> Optional[list[float]]:
        if not series or len(series) != len(closes):
            return None
        return series[-recent_bars:]

    recent_lows = _aligned_tail(lows)
    recent_highs = _aligned_tail(highs)

    above_prior = sum(1 for c in prior_window if c > flip)
    above_share = above_prior / len(prior_window)
    below_share = 1.0 - above_share

    last_close = recent_window[-1]
    touch_low_band = flip * (1.0 + touch_tol_pct)
    touch_high_band = flip * (1.0 - touch_tol_pct)
    reject_above = flip * (1.0 + reject_buffer_pct)
    reject_below = flip * (1.0 - reject_buffer_pct)

    # True wick check when lows/highs are present; otherwise fall back
    # to closes (close at/below band approximates a touch).
    bullish_touch_source = recent_lows if recent_lows is not None else recent_window
    bearish_touch_source = recent_highs if recent_highs is not None else recent_window
    bullish_touch = any(v <= touch_low_band for v in bullish_touch_source)
    bearish_touch = any(v >= touch_high_band for v in bearish_touch_source)

    if above_share >= prior_threshold and bullish_touch and last_close > reject_above:
        return "bullish"
    if below_share >= prior_threshold and bearish_touch and last_close < reject_below:
        return "bearish"
    return None


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


class GammaFlipBouncePattern(PatternBase):
    id = "gamma_flip_bounce"
    name = "Bounce Off the Gamma Flip"
    tier = "0DTE"
    direction = "context_dependent"
    valid_regimes = ("trend_expansion", "controlled_trend", "chop_range")
    preferred_regime = "controlled_trend"
    pattern_base = 0.65

    confluence_signals_for = (
        "positioning_trap",
        "dealer_delta_pressure",
        "gex_gradient",
        "tape_flow_bias",
        "order_flow_imbalance",
    )
    confluence_signals_against = (
        "vol_expansion",
        "range_break_imminence",
    )

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        flip = ctx.market.gamma_flip
        close = ctx.close
        bounce = _detect_bounce(
            ctx.market.recent_closes,
            flip,
            lows=ctx.market.recent_lows or None,
            highs=ctx.market.recent_highs or None,
        )
        if bounce is None:
            return None  # defensive; trigger check already covers this

        sigma = _realized_sigma_30min(ctx.market.recent_closes)
        atm_strike = _round_to_strike(close, 1.0)
        expiry = self._zero_dte_expiry(ctx)

        # Instrument: high vol → debit single-leg; low vol → credit spread
        # in the bounce direction.  Mirror of put_wall_bounce.
        if bounce == "bullish":
            if sigma > _VOL_DEBIT_SWITCH:
                action = ActionEnum.BUY_CALL_DEBIT
                legs = [Leg(expiry=expiry, strike=atm_strike, right="C", side="BUY", qty=1)]
            else:
                action = ActionEnum.SELL_PUT_SPREAD
                long_strike = atm_strike - _SPREAD_WIDTH_POINTS
                legs = [
                    Leg(expiry=expiry, strike=atm_strike, right="P", side="SELL", qty=1),
                    Leg(expiry=expiry, strike=long_strike, right="P", side="BUY", qty=1),
                ]
            entry_ref = flip * (1.0 + _REJECT_BUFFER_PCT)
            stop_ref = flip * (1.0 - _STOP_PCT)
        else:
            if sigma > _VOL_DEBIT_SWITCH:
                action = ActionEnum.BUY_PUT_DEBIT
                legs = [Leg(expiry=expiry, strike=atm_strike, right="P", side="BUY", qty=1)]
            else:
                action = ActionEnum.SELL_CALL_SPREAD
                long_strike = atm_strike + _SPREAD_WIDTH_POINTS
                legs = [
                    Leg(expiry=expiry, strike=atm_strike, right="C", side="SELL", qty=1),
                    Leg(expiry=expiry, strike=long_strike, right="C", side="BUY", qty=1),
                ]
            entry_ref = flip * (1.0 - _REJECT_BUFFER_PCT)
            stop_ref = flip * (1.0 + _STOP_PCT)

        target_ref, target_level_name = self._pick_target(
            bounce=bounce,
            close=close,
            call_wall=ctx.level("call_wall"),
            put_wall=ctx.level("put_wall"),
            max_gamma=ctx.level("max_gamma_strike"),
        )

        confidence = self.compute_confidence(ctx, bias=bounce)
        rationale = self._compose_rationale(
            bounce=bounce,
            flip=flip,
            close=close,
            sigma=sigma,
            target_name=target_level_name,
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=bounce,
            confidence=confidence,
            size_multiplier=0.75,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=entry_ref, trigger="at_touch"),
            target=Target(
                ref_price=target_ref,
                kind="level" if target_level_name else "premium_pct",
                level_name=target_level_name,
            ),
            stop=Stop(
                ref_price=stop_ref,
                kind="level",
                level_name="gamma_flip_failure",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "gamma_flip": flip,
                "bounce_direction": bounce,
                "call_wall": ctx.level("call_wall"),
                "put_wall": ctx.level("put_wall"),
                "max_gamma_strike": ctx.level("max_gamma_strike"),
                "realized_sigma_30min": round(sigma, 6),
                "advanced_signals_aligned": [
                    name
                    for name in (
                        "positioning_trap",
                        "dealer_delta_pressure",
                        "gex_gradient",
                    )
                    if (snap := ctx.signal(name))
                    and (snap.clamped_score * (1.0 if bounce == "bullish" else -1.0)) > 0
                ],
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        flip = ctx.market.gamma_flip
        close = ctx.close

        if flip is None:
            missing.append("gamma_flip unavailable")
        if close <= 0:
            missing.append("close price unavailable")

        if ctx.net_gex <= _NET_GEX_FLOOR:
            missing.append(
                f"net_gex {ctx.net_gex} <= floor {_NET_GEX_FLOOR:.0f} "
                "(need long-gamma backdrop for dealers to defend the flip)"
            )

        # MSI gamma_anchor.flip_distance_subscore must indicate price near flip.
        flip_subscore = self._flip_distance_subscore(ctx)
        if flip_subscore is None:
            missing.append("gamma_anchor.flip_distance_subscore unavailable")
        elif flip_subscore < _FLIP_DISTANCE_MIN:
            missing.append(
                f"flip_distance_subscore {flip_subscore:.2f} < {_FLIP_DISTANCE_MIN:.2f} "
                "(price not near gamma_flip)"
            )

        # Time gate: 9:35 ET (earlier than break/wall bounce patterns —
        # flip defense in the open is high-conviction).
        if ctx.et_time < _OPEN_GATE:
            missing.append(
                f"too early ({ctx.et_time} ET); needs >= {_OPEN_GATE.strftime('%H:%M')} ET"
            )

        # Bounce detection on trailing bars.
        if flip is not None and flip > 0:
            bounce = _detect_bounce(
                ctx.market.recent_closes,
                flip,
                lows=ctx.market.recent_lows or None,
                highs=ctx.market.recent_highs or None,
            )
            if bounce is None:
                missing.append(
                    "no flip tag-and-reject detected: last "
                    f"{_RECENT_BARS} bars vs prior {_PRIOR_MODE_BARS}-bar mode "
                    "did not show a touch followed by rejection past the flip"
                )

            # Hard veto: an active range-break overrides bounce logic.
            rbi = ctx.signal("range_break_imminence")
            rbi_label = rbi.context_values.get("label") if rbi else None
            if rbi_label == "Breakout Mode":
                missing.append("range_break_imminence in 'Breakout Mode' — trend overrides bounce")

            vol_x = ctx.signal("vol_expansion")
            if vol_x and vol_x.triggered:
                missing.append("vol_expansion triggered — break regime overrides bounce")

            # Flow corroboration (direction matched).
            if bounce is not None:
                tape = ctx.signal("tape_flow_bias")
                ofi = ctx.signal("order_flow_imbalance")
                sign = 1.0 if bounce == "bullish" else -1.0
                flow_ok = (tape and (tape.score * sign) >= _FLOW_THRESHOLD) or (
                    ofi and (ofi.score * sign) >= _FLOW_THRESHOLD
                )
                if not flow_ok:
                    missing.append(
                        f"no {bounce} flow signal: "
                        f"tape_flow_bias={tape.score if tape else None}, "
                        f"order_flow_imbalance={ofi.score if ofi else None}"
                    )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _flip_distance_subscore(ctx: PlaybookContext) -> Optional[float]:
        ga = ctx.msi_components.get("gamma_anchor") if ctx.msi_components else None
        if not isinstance(ga, dict):
            return None
        gactx = ga.get("context")
        if not isinstance(gactx, dict):
            return None
        val = gactx.get("flip_distance_subscore")
        if not isinstance(val, (int, float)):
            return None
        return float(val)

    @staticmethod
    def _pick_target(
        *,
        bounce: BounceDirection,
        close: float,
        call_wall: Optional[float],
        put_wall: Optional[float],
        max_gamma: Optional[float],
    ) -> tuple[Optional[float], Optional[str]]:
        """Nearest structural level in the bounce direction, or None."""
        if bounce == "bullish":
            candidates: list[tuple[float, str]] = []
            if call_wall is not None and call_wall > close:
                candidates.append((float(call_wall), "call_wall"))
            if max_gamma is not None and max_gamma > close:
                candidates.append((float(max_gamma), "max_gamma_strike"))
            if not candidates:
                return None, None
            level, name = min(candidates, key=lambda w: w[0] - close)
            return level, name
        else:
            candidates = []
            if put_wall is not None and put_wall < close:
                candidates.append((float(put_wall), "put_wall"))
            if max_gamma is not None and max_gamma < close:
                candidates.append((float(max_gamma), "max_gamma_strike"))
            if not candidates:
                return None, None
            level, name = max(candidates, key=lambda w: w[0] - close)
            return level, name

    @staticmethod
    def _zero_dte_expiry(ctx: PlaybookContext) -> str:
        return ctx.et_date.isoformat()

    @staticmethod
    def _compose_rationale(
        bounce: BounceDirection,
        flip: float,
        close: float,
        sigma: float,
        target_name: Optional[str],
    ) -> str:
        side_word = "above" if bounce == "bullish" else "below"
        instrument_word = "call-side bounce" if bounce == "bullish" else "put-side rejection"
        target_phrase = f"target {target_name}" if target_name else "target sigma-scaled"
        return (
            f"Price ${close:.2f} tagged gamma_flip ${flip:.2f} and rejected "
            f"back {side_word}; 30-min sigma {sigma * 100:.2f}%; "
            f"{target_phrase} -> {instrument_word} 0DTE."
        )


PATTERN: PatternBase = GammaFlipBouncePattern()
