"""Pattern 2.1: ``pin_risk_premium_sell`` — Sell Premium into Overnight Pin.

When price sits tightly bracketed between max_pain and the nearest wall
in a long-gamma backdrop with low realized vol, dealer hedging tends to
pin overnight.  We sell defined-risk premium centered on max_pain.

The catalog labels this ``BUY_IRON_CONDOR`` but the thesis is "sell
defined-risk premium" — collecting credit by being short the inner
strikes and long the outer wings.  In this codebase's action enum
that's ``SELL_IRON_CONDOR`` (BUY = paid debit, SELL = received credit).

Per ``docs/playbook_catalog.md`` §7.2.1.
"""

from __future__ import annotations

import math
import os
from datetime import time
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

_NET_GEX_FLOOR = float(os.getenv("PLAYBOOK_PRPS_NET_GEX_FLOOR", "2.0e9"))
_MAX_PAIN_PROXIMITY_PCT = float(os.getenv("PLAYBOOK_PRPS_MAXPAIN_PROXIMITY_PCT", "0.0030"))
_REALIZED_SIGMA_CEILING = float(os.getenv("PLAYBOOK_PRPS_SIGMA_CEILING", "0.0012"))
_WING_SIGMA_MULT = float(os.getenv("PLAYBOOK_PRPS_WING_SIGMA_MULT", "2.0"))
_WING_MIN_POINTS = float(os.getenv("PLAYBOOK_PRPS_WING_MIN_POINTS", "3.0"))
_OUTER_WING_EXTRA = float(os.getenv("PLAYBOOK_PRPS_OUTER_WING_EXTRA", "5.0"))
_START_HOUR = int(os.getenv("PLAYBOOK_PRPS_START_HOUR_ET", "15"))
_START_MIN = int(os.getenv("PLAYBOOK_PRPS_START_MIN_ET", "30"))
# Hold from ~15:35 ET entry until next-day 14:00 ET ≈ 22.5 hours = 1350 min.
_MAX_HOLD_MIN = int(os.getenv("PLAYBOOK_PRPS_MAX_HOLD_MIN", "1350"))


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


def _round_to_strike(price: float, increment: float = 1.0) -> float:
    return round(price / increment) * increment


class PinRiskPremiumSellPattern(PatternBase):
    id = "pin_risk_premium_sell"
    name = "Sell Premium into Overnight Pin"
    tier = "1DTE"
    direction = "non_directional"
    valid_regimes = ("chop_range", "high_risk_reversal")
    preferred_regime = "chop_range"
    pattern_base = 0.50

    confluence_signals_for = ("gex_gradient",)
    # vol_expansion / 0dte_position_imbalance triggered would oppose a
    # pin trade — handled inline since the check is on triggered, not score.
    confluence_signals_against = ()

    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        miss = self._check_triggers(ctx)
        if miss:
            return None

        close = ctx.close
        max_pain = ctx.level("max_pain") or ctx.market.max_pain
        sigma = _realized_sigma_30min(ctx.market.recent_closes)

        center = _round_to_strike(float(max_pain), 1.0)
        wing_distance = max(_WING_MIN_POINTS, _WING_SIGMA_MULT * sigma * close)
        # Round wing distance to whole strike units so legs land on real strikes.
        wing_offset = max(1.0, _round_to_strike(wing_distance, 1.0))
        short_call_strike = center + wing_offset
        long_call_strike = short_call_strike + _OUTER_WING_EXTRA
        short_put_strike = center - wing_offset
        long_put_strike = short_put_strike - _OUTER_WING_EXTRA

        expiry = self._one_dte_expiry(ctx)
        # SELL_IRON_CONDOR: short the inner strikes, long the outer wings.
        legs = [
            Leg(expiry=expiry, strike=short_call_strike, right="C", side="SELL", qty=1),
            Leg(expiry=expiry, strike=long_call_strike, right="C", side="BUY", qty=1),
            Leg(expiry=expiry, strike=short_put_strike, right="P", side="SELL", qty=1),
            Leg(expiry=expiry, strike=long_put_strike, right="P", side="BUY", qty=1),
        ]

        # Compute confidence with non-directional bias.  The confluence helper
        # uses a sign-vs-bias product, but pin trades are direction-neutral —
        # collect aligned signals manually as positive contributions.
        confidence = self._compute_pin_confidence(ctx)

        rationale = (
            f"Long-gamma pin: net GEX ${ctx.net_gex / 1e9:.1f}B, "
            f"price ${close:.2f} within {_MAX_PAIN_PROXIMITY_PCT * 100:.2f}% of max_pain "
            f"${max_pain:.2f}, 30-min σ {sigma * 100:.3f}% < ceiling {_REALIZED_SIGMA_CEILING * 100:.2f}% "
            f"→ sell 1DTE iron condor with ±${wing_offset:.0f} wings."
        )

        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=ActionEnum.SELL_IRON_CONDOR,
            pattern=self.id,
            tier=self.tier,
            direction=self.direction,
            confidence=confidence,
            size_multiplier=0.5,
            max_hold_minutes=_MAX_HOLD_MIN,
            legs=legs,
            entry=Entry(ref_price=close, trigger="at_close"),
            target=Target(
                ref_price=center,
                kind="premium_pct",
                level_name="50pct_credit_captured",
            ),
            stop=Stop(
                ref_price=None,
                kind="signal_event",
                level_name="wing_breached_or_max_loss_x1.5",
            ),
            rationale=rationale,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "net_gex": ctx.net_gex,
                "max_pain": max_pain,
                "close": close,
                "wing_offset": wing_offset,
                "short_call_strike": short_call_strike,
                "long_call_strike": long_call_strike,
                "short_put_strike": short_put_strike,
                "long_put_strike": long_put_strike,
                "realized_sigma_30min": round(sigma, 6),
                "rbi_label": (
                    ctx.signal("range_break_imminence").context_values.get("label")
                    if ctx.signal("range_break_imminence")
                    else None
                ),
            },
        )

    # ------------------------------------------------------------------
    # Triggers
    # ------------------------------------------------------------------

    def _check_triggers(self, ctx: PlaybookContext) -> list[str]:
        missing: list[str] = []
        et = ctx.et_time
        if et < time(_START_HOUR, _START_MIN):
            missing.append(f"too early ({et} ET); needs >= {_START_HOUR:02d}:{_START_MIN:02d} ET")

        if ctx.close <= 0:
            missing.append("close price unavailable")

        if ctx.net_gex is None or ctx.net_gex <= _NET_GEX_FLOOR:
            missing.append(
                f"net_gex {ctx.net_gex} <= floor {_NET_GEX_FLOOR:.0f} (need long-gamma backdrop)"
            )

        max_pain = ctx.level("max_pain") or ctx.market.max_pain
        if max_pain is None:
            missing.append("max_pain level unavailable")
        elif ctx.close > 0:
            distance_pct = abs(ctx.close - max_pain) / ctx.close
            if distance_pct > _MAX_PAIN_PROXIMITY_PCT:
                missing.append(
                    f"price {distance_pct * 100:.2f}% from max_pain "
                    f"(needs <= {_MAX_PAIN_PROXIMITY_PCT * 100:.2f}%)"
                )

        sigma = _realized_sigma_30min(ctx.market.recent_closes)
        if sigma > _REALIZED_SIGMA_CEILING:
            missing.append(
                f"30-min σ {sigma * 100:.3f}% > ceiling {_REALIZED_SIGMA_CEILING * 100:.2f}% "
                "(too active to pin)"
            )

        rbi = ctx.signal("range_break_imminence")
        rbi_label = rbi.context_values.get("label") if rbi else None
        if rbi_label not in ("Range Fade", "Weak Range"):
            missing.append(
                f"range_break_imminence.label is {rbi_label!r} "
                "(needs 'Range Fade' or 'Weak Range' for pin thesis)"
            )

        # Active vol_expansion or 0dte_position_imbalance fights the pin.
        vol_x = ctx.signal("vol_expansion")
        if vol_x and vol_x.triggered:
            missing.append("vol_expansion triggered (breakout regime fights pin)")
        odpi = ctx.signal("0dte_position_imbalance")
        if odpi and odpi.triggered and abs(odpi.score) >= 30:
            missing.append(
                f"0dte_position_imbalance |score| {abs(odpi.score):.1f} too directional for pin"
            )

        return missing

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        return self._check_triggers(ctx)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _compute_pin_confidence(self, ctx: PlaybookContext) -> float:
        """Direction-neutral confidence: aligned signals add to multiplier
        regardless of sign; opposing-vol signals were already gated out."""
        from src.signals.playbook.types import clamp_confidence

        mult = 1.0
        # gamma_anchor.local_gamma_subscore <= -0.5 → strong pinning.
        ga = ctx.msi_components.get("gamma_anchor") if ctx.msi_components else None
        if isinstance(ga, dict):
            ga_ctx = ga.get("context") or {}
            local_gamma = ga_ctx.get("local_gamma_subscore")
            if isinstance(local_gamma, (int, float)) and local_gamma <= -0.5:
                mult += 0.05
        # gex_gradient near zero → balanced gamma.
        gex_grad = ctx.signal("gex_gradient")
        if gex_grad and abs(gex_grad.clamped_score) <= 0.20:
            mult += 0.05
        # volatility_regime score very negative → calm, supports pin.
        vol_reg = ctx.msi_components.get("volatility_regime") if ctx.msi_components else None
        if isinstance(vol_reg, dict):
            score = vol_reg.get("score")
            if isinstance(score, (int, float)) and score <= -0.4:
                mult += 0.05
        mult = max(0.7, min(1.4, mult))
        regime_fit = self._regime_fit(ctx.msi_regime)
        return clamp_confidence(self.pattern_base * mult * regime_fit)

    @staticmethod
    def _one_dte_expiry(ctx: PlaybookContext) -> str:
        """Next session expiry — naive +1 day, ignores weekends/holidays.

        Calendar-aware expiry routing is part of the broker integration
        (out of scope for the playbook); the leg's date is what we ask for
        and the broker will reject an invalid expiry.
        """
        from datetime import timedelta

        return (ctx.et_date + timedelta(days=1)).isoformat()


PATTERN: PatternBase = PinRiskPremiumSellPattern()
