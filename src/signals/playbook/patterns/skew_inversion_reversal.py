"""Pattern 3.2: ``skew_inversion_reversal`` — Fear Spike Fade.

When ``skew_delta`` is deeply negative (puts pricing in disproportionate
fear) but the underlying tape is *not* breaking down, the fear is
overpriced — a contrarian bullish reversal trade.

Per ``docs/playbook_catalog.md`` §7.3.2.

The 20-day skew aggregations now use the PR-12 history loader:
- Target intensity = |20-day-mean skew| / 100, replacing the prior
  current-magnitude approximation.
- A new-20-day-low stop predicate is computed from the same history and
  surfaced in the Card's context for downstream stop monitoring.

The MA-proximity gate still uses the mean of available 1-min
``recent_closes`` as a proxy for the 20-day MA.  Wiring daily closes
through is a separate (smaller) follow-up.
"""

from __future__ import annotations

import math
import os
from datetime import time, timedelta
from typing import Optional

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

_SKEW_THRESHOLD = float(os.getenv("PLAYBOOK_SIR_SKEW_THRESHOLD", "-50"))
_TAPE_MIN = float(os.getenv("PLAYBOOK_SIR_TAPE_MIN", "0"))
_VOLREG_MIN = float(os.getenv("PLAYBOOK_SIR_VOLREG_MIN", "0.3"))
_MA_PROXIMITY_PCT = float(os.getenv("PLAYBOOK_SIR_MA_PROXIMITY_PCT", "0.005"))  # 0.5%
_OTM_ATR_MULT = float(os.getenv("PLAYBOOK_SIR_OTM_ATR_MULT", "1.5"))
_DTE_DAYS = int(os.getenv("PLAYBOOK_SIR_DTE_DAYS", "5"))
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_SIR_MAX_HOLD_MIN", str(3 * 24 * 60)))
_DAILY_SIGMA_SCALAR = math.sqrt(390.0)


def _realized_sigma_1min(closes: list[float]) -> float:
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


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


def _ma_proxy(closes: list[float]) -> Optional[float]:
    """Mean of available 1-min closes as a 20-day MA proxy."""
    usable = [c for c in (closes or []) if c and c > 0]
    if len(usable) < 5:
        return None
    return sum(usable) / len(usable)


class SkewInversionReversalPattern(PatternBase):
    id = "skew_inversion_reversal"
    name = "Fear Spike Fade"
    tier = "swing"
    direction = "bullish"
    valid_regimes = ("chop_range", "controlled_trend")
    preferred_regime = "chop_range"
    pattern_base = 0.50

    confluence_signals_for = ("vanna_charm_flow", "positioning_trap")
    confluence_signals_against = ("dealer_delta_pressure",)

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        close = ctx.close
        skew = ctx.signal("skew_delta")  # _check_triggers ensures non-None
        sigma_1min = _realized_sigma_1min(ctx.market.recent_closes)
        atr_daily_proxy = sigma_1min * _DAILY_SIGMA_SCALAR

        otm_offset = max(1.0, _OTM_ATR_MULT * atr_daily_proxy * close)
        strike = _round_to_strike(close + otm_offset, 1.0)
        expiry = self._dte_expiry(ctx, _DTE_DAYS)
        legs = [Leg(expiry=expiry, strike=strike, right="C", side="BUY", qty=1)]

        # Target: |20-day-mean skew| / 100 × ATR_daily × close added to close.
        # Falls back to current magnitude when no history is loaded.
        mean_clamped = self._mean_clamped_skew(skew)
        if mean_clamped is None:
            skew_intensity = abs(skew.score) / 100.0
            intensity_source = "current_skew"
        else:
            skew_intensity = abs(mean_clamped)
            intensity_source = "history_20d_mean"
        target_offset = skew_intensity * atr_daily_proxy * close
        target_ref = close + target_offset

        # Whether the current snapshot is a new 20-day low — surfaced for
        # downstream stop monitoring (the actual exit happens in the
        # portfolio engine, this is just the predicate).
        new_20d_low = self._is_new_20d_low(skew)

        confidence = self.compute_confidence(ctx, bias="bullish")

        ma_proxy = _ma_proxy(ctx.market.recent_closes)
        rationale = (
            f"skew_delta {skew.score:+.0f} (puts overpriced) but tape not "
            f"breaking down; bullish fear-fade with {_DTE_DAYS}-DTE OTM call "
            f"at ${strike:.0f} (close+${otm_offset:.2f} ≈ {_OTM_ATR_MULT}× "
            f"ATR_daily); target ${target_ref:.2f}, exit on -40% premium "
            "or skew making a new 20-day low."
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=ActionEnum.BUY_CALL_DEBIT,
            pattern=self.id,
            tier=self.tier,
            direction=self.direction,
            confidence=confidence,
            size_multiplier=0.5,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_open_next"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name="skew_revert_target",
            ),
            stop=Stop(
                ref_price=None,
                kind="signal_event",
                level_name="skew_new_20d_low_or_-40pct_premium",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "skew_delta_score": skew.score,
                "tape_flow_bias_score": (s.score if (s := ctx.signal("tape_flow_bias")) else None),
                "volatility_regime_score": self._volatility_regime_score(ctx),
                "ma_proxy": round(ma_proxy, 4) if ma_proxy is not None else None,
                "atr_daily_proxy": round(atr_daily_proxy, 6),
                "otm_offset": round(otm_offset, 4),
                "strike": strike,
                "skew_intensity": round(skew_intensity, 4),
                "skew_intensity_source": intensity_source,
                "skew_is_new_20d_low": new_20d_low,
                "target_offset": round(target_offset, 4),
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []

        if ctx.close <= 0:
            missing.append("close price unavailable")

        skew = ctx.signal("skew_delta")
        if skew is None:
            missing.append("skew_delta signal unavailable")
        elif skew.score > _SKEW_THRESHOLD:
            missing.append(
                f"skew_delta score {skew.score:.1f} > {_SKEW_THRESHOLD:.0f} "
                "(fear not extreme enough)"
            )

        tape = ctx.signal("tape_flow_bias")
        if tape is None:
            missing.append("tape_flow_bias signal unavailable")
        elif tape.score < _TAPE_MIN:
            missing.append(
                f"tape_flow_bias score {tape.score:.1f} < {_TAPE_MIN:.0f} "
                "(tape actively bearish — wait, don't catch a falling knife)"
            )

        vol_score = self._volatility_regime_score(ctx)
        if vol_score is None:
            missing.append("volatility_regime MSI score unavailable")
        elif vol_score < _VOLREG_MIN:
            missing.append(
                f"volatility_regime score {vol_score:.2f} < {_VOLREG_MIN:.2f} "
                "(vol not elevated enough for compression)"
            )

        ma = _ma_proxy(ctx.market.recent_closes)
        if ma is None:
            missing.append("MA proxy unavailable (need >= 5 recent_closes)")
        elif ctx.close > 0:
            distance = abs(ctx.close - ma) / ctx.close
            if distance > _MA_PROXIMITY_PCT:
                missing.append(
                    f"close {distance * 100:.2f}% from MA proxy "
                    f"(needs <= {_MA_PROXIMITY_PCT * 100:.2f}%)"
                )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _mean_clamped_skew(snap) -> Optional[float]:
        """Mean clamped score over loaded history.  None when empty."""
        if not getattr(snap, "score_history", None):
            return None
        scores = [s for _ts, s in snap.score_history]
        if not scores:
            return None
        return sum(scores) / len(scores)

    @staticmethod
    def _is_new_20d_low(snap) -> bool:
        """True when the current clamped score is below the prior history min.

        Uses the loaded score_history; returns False when no history is
        available (no signal that the thesis has broken).
        """
        if not getattr(snap, "score_history", None):
            return False
        prior = [s for _ts, s in snap.score_history[:-1]]  # exclude latest
        if not prior:
            return False
        return snap.clamped_score < min(prior)

    @staticmethod
    def _volatility_regime_score(ctx: PlaybookContext) -> Optional[float]:
        comp = ctx.msi_components.get("volatility_regime") if ctx.msi_components else None
        if not isinstance(comp, dict):
            return None
        score = comp.get("score")
        if not isinstance(score, (int, float)):
            return None
        return float(score)

    @staticmethod
    def _dte_expiry(ctx: PlaybookContext, days: int) -> str:
        return (ctx.et_date + timedelta(days=days)).isoformat()


PATTERN: PatternBase = SkewInversionReversalPattern()
