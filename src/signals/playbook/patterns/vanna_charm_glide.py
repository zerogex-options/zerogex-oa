"""Pattern 3.3: ``vanna_charm_glide`` — End-of-Week Hedging Drift.

Vanna + charm pressure both pushing the same direction across multiple
days; dealer hedging unwind into Friday close amplifies the drift.
We hold a Friday-expiry ATM debit in the drift direction.

Per ``docs/playbook_catalog.md`` §7.3.3.

The 2-day-sustained, same-sign requirement on
``vanna_charm_flow.score`` is enforced via ``daily_signed_max`` from
the PR-12 history loader: at least ``_SUSTAINED_DAYS_MIN`` distinct
ET trading days within the loaded window must show a same-sign extreme
above ``_VCF_SCORE_MIN``.
"""

from __future__ import annotations

import math
import os
from datetime import date, datetime, time, timedelta
from typing import Literal, Optional

import pytz

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

_VCF_SCORE_MIN = float(os.getenv("PLAYBOOK_VCG_SCORE_MIN", "40"))
_SUSTAINED_DAYS_MIN = int(os.getenv("PLAYBOOK_VCG_SUSTAINED_DAYS_MIN", "2"))
# History entries are clamped to [-1, +1].  Threshold expressed on that scale.
_SUSTAINED_DAILY_THRESHOLD = float(os.getenv("PLAYBOOK_VCG_SUSTAINED_DAILY_THRESHOLD", "0.40"))
_TARGET_ATR_MULT = float(os.getenv("PLAYBOOK_VCG_TARGET_ATR_MULT", "2.0"))
_FRIDAY_CLOSE_BUFFER_HOURS = int(os.getenv("PLAYBOOK_VCG_FRIDAY_CLOSE_HOUR", "14"))
_DAILY_SIGMA_SCALAR = math.sqrt(390.0)
_ALLOWED_DAYS = ("Tue", "Wed", "Thu")
_ET = pytz.timezone("America/New_York")


DriftDirection = Literal["bullish", "bearish"]


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


def _next_friday(d: date) -> date:
    """Return the next calendar Friday (today if it's already Friday)."""
    days_until = (4 - d.weekday()) % 7  # Monday=0, Friday=4
    return d + timedelta(days=days_until)


class VannaCharmGlidePattern(PatternBase):
    id = "vanna_charm_glide"
    name = "End-of-Week Hedging Drift"
    tier = "swing"
    direction = "context_dependent"
    valid_regimes = ("controlled_trend", "chop_range")
    preferred_regime = "controlled_trend"
    pattern_base = 0.50

    confluence_signals_for = ("gex_gradient", "tape_flow_bias")
    # range_break_imminence "Breakout Mode" label opposes this drift trade
    # — handled inline because the check is on the label, not the score sign.
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        vcf = ctx.signal("vanna_charm_flow")
        # _check_triggers ensures vcf is non-None and |score| >= threshold.
        drift: DriftDirection = "bullish" if vcf.score > 0 else "bearish"

        close = ctx.close
        sigma_1min = _realized_sigma_1min(ctx.market.recent_closes)
        atr_daily = sigma_1min * _DAILY_SIGMA_SCALAR
        atr_dollars = atr_daily * close

        atm_strike = _round_to_strike(close, 1.0)
        friday = _next_friday(ctx.et_date)
        expiry = friday.isoformat()

        if drift == "bullish":
            action = ActionEnum.BUY_CALL_DEBIT
            right = "C"
            target_ref = close + _TARGET_ATR_MULT * atr_dollars
        else:
            action = ActionEnum.BUY_PUT_DEBIT
            right = "P"
            target_ref = close - _TARGET_ATR_MULT * atr_dollars

        legs = [Leg(expiry=expiry, strike=atm_strike, right=right, side="BUY", qty=1)]

        max_hold = self._minutes_until_friday_close(ctx)

        confidence = self.compute_confidence(ctx, bias=drift)
        # Inline against-check: range_break_imminence == "Breakout Mode".
        rbi = ctx.signal("range_break_imminence")
        if rbi and rbi.context_values.get("label") == "Breakout Mode":
            confidence = max(0.20, confidence - 0.10)

        rationale = (
            f"vanna_charm_flow {vcf.score:+.0f} ({drift}) on {ctx.day_of_week} → "
            f"Friday-expiry ATM {right} debit at ${atm_strike:.0f}; "
            f"target ${target_ref:.2f} (close ± {_TARGET_ATR_MULT:.1f}× "
            f"ATR_daily ≈ ${atr_dollars:.2f})."
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=action,
            pattern=self.id,
            tier=self.tier,
            direction=drift,
            confidence=confidence,
            size_multiplier=0.5,
            max_hold_minutes=max_hold,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_close"),
            target=Target(
                ref_price=round(target_ref, 4),
                kind="level",
                level_name="atr_glide_target",
            ),
            stop=Stop(
                ref_price=None,
                kind="signal_event",
                level_name="vcf_sign_flip_or_-50pct_premium",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "drift_direction": drift,
                "vanna_charm_flow_score": vcf.score,
                "day_of_week": ctx.day_of_week,
                "friday_expiry": expiry,
                "atr_daily": round(atr_daily, 6),
                "atr_dollars": round(atr_dollars, 4),
                "positioning_trap_score": (
                    s.score if (s := ctx.signal("positioning_trap")) else None
                ),
                "rbi_label": (rbi.context_values.get("label") if rbi else None),
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []

        if ctx.close <= 0:
            missing.append("close price unavailable")

        dow = ctx.day_of_week
        if dow not in _ALLOWED_DAYS:
            missing.append(
                f"day_of_week {dow!r} not in {list(_ALLOWED_DAYS)} "
                "(need glide runway into Friday)"
            )

        vcf = ctx.signal("vanna_charm_flow")
        if vcf is None:
            missing.append("vanna_charm_flow signal unavailable")
        elif abs(vcf.score) < _VCF_SCORE_MIN:
            missing.append(f"vanna_charm_flow |score| {abs(vcf.score):.1f} < {_VCF_SCORE_MIN:.0f}")
        else:
            # 2-day-sustained, same-sign requirement.  Falls back to "accept
            # current trigger" when no history is loaded.
            sustained_days = self._count_sustained_same_sign_days(vcf)
            if sustained_days is not None and sustained_days < _SUSTAINED_DAYS_MIN:
                missing.append(
                    f"vanna_charm_flow same-sign extreme on only {sustained_days} ET "
                    f"trading day(s) (need >= {_SUSTAINED_DAYS_MIN})"
                )

        # positioning_trap must align (same sign) with drift, or be near-zero.
        if vcf is not None and abs(vcf.score) >= _VCF_SCORE_MIN:
            ptrap = ctx.signal("positioning_trap")
            if ptrap is not None:
                drift_sign = 1.0 if vcf.score > 0 else -1.0
                ptrap_alignment = ptrap.score * drift_sign
                if ptrap_alignment < 0 and abs(ptrap.score) >= 20:
                    missing.append(
                        f"positioning_trap score {ptrap.score:+.0f} opposes drift "
                        f"{('bullish' if drift_sign > 0 else 'bearish')!r} "
                        "(crowd against the glide)"
                    )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _count_sustained_same_sign_days(snap) -> Optional[int]:
        """Days where vcf same-sign extreme exceeds the threshold.

        Uses ``daily_signed_max`` so direction is preserved.  Counts only
        the days whose extreme is on the same side as the current snapshot.
        Returns None when no history is loaded (fall back to current trigger).
        """
        if not getattr(snap, "score_history", None):
            return None
        current_sign = 1.0 if snap.score > 0 else -1.0
        signed_extremes = snap.daily_signed_max()
        return sum(
            1
            for _day, ext in signed_extremes
            if (ext * current_sign) > 0 and abs(ext) >= _SUSTAINED_DAILY_THRESHOLD
        )

    @staticmethod
    def _minutes_until_friday_close(ctx: PlaybookContext) -> int:
        """Minutes from the current ET timestamp to next Friday 14:00 ET."""
        ts = ctx.timestamp
        if ts.tzinfo is None:
            ts = pytz.UTC.localize(ts)
        et_now = ts.astimezone(_ET)
        friday = _next_friday(et_now.date())
        target = _ET.localize(datetime.combine(friday, time(_FRIDAY_CLOSE_BUFFER_HOURS, 0)))
        delta = target - et_now
        return max(1, int(delta.total_seconds() // 60))


PATTERN: PatternBase = VannaCharmGlidePattern()
