"""Range Break Imminence Score — regime-switch detector.

Answers the question: *"Should I keep fading this range, or is the chop
about to resolve into a real directional move?"*

Fusion of four inputs, each weighted per the published scoring model:

    Skew Delta extreme .............. 30
    Dealer Delta Pressure ........... 25
    Trap Detection .................. 25
    Compression / Volatility Contraction 20
    ------------------------------------
                                    = 100

Output convention on :class:`AdvancedSignalResult`:

  * ``score`` ∈ [-1, 1] — signed break-imminence, negative = bearish
    break imminent, positive = bullish. Magnitude tracks the 0–100
    ``imminence`` scaled to unit range.
  * ``context['imminence']`` ∈ [0, 100] — absolute break-risk score
    matching the dashboard's intended display.
  * ``context['label']`` — one of ``Range Fade`` / ``Weak Range`` /
    ``Break Watch`` / ``Breakout Mode`` so the playbook flip is direct.
"""

from __future__ import annotations

import os
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.components.utils import realized_sigma
from src.signals.advanced.base import AdvancedSignalResult

# Component weights (must sum to 100).
_W_SKEW = 30.0
_W_DEALER = 25.0
_W_TRAP = 25.0
_W_COMPRESSION = 20.0

# Structural baseline spread for OTM-put-vs-OTM-call IV in equity indices.
_SKEW_BASELINE = float(os.getenv("SIGNAL_RBI_SKEW_BASELINE", "0.02"))
# Deviation beyond baseline that saturates the skew sub-score.
_SKEW_SATURATION = float(os.getenv("SIGNAL_RBI_SKEW_SATURATION", "0.04"))

# Dealer net delta magnitude (shares-equivalent) that saturates the score.
_DEALER_NORM = float(os.getenv("SIGNAL_RBI_DEALER_NORM", "3.0e8"))

# Flow-delta scale used by the trap sub-score (premium units per bar).
_FLOW_NORM = float(os.getenv("SIGNAL_RBI_FLOW_NORM", "250000"))

# Price must sit within this fraction of the range extreme to count as pinned.
_TRAP_PROXIMITY_FRACTION = float(os.getenv("SIGNAL_RBI_TRAP_PROX_FRAC", "0.25"))

# Range lookback window (bars) for trap-proximity math.
_RANGE_WINDOW = int(os.getenv("SIGNAL_RBI_RANGE_WINDOW", "20"))

# Compression: short-sigma / long-sigma ratio. <= FULL ⇒ 100% compressed,
# >= NONE ⇒ 0% (expanding or normal).
_COMPRESSION_FULL = float(os.getenv("SIGNAL_RBI_COMPRESSION_FULL", "0.5"))
_COMPRESSION_NONE = float(os.getenv("SIGNAL_RBI_COMPRESSION_NONE", "1.0"))

# Label thresholds per the published playbook map.
_LABEL_FADE_MAX = 40.0  # 0–39: Range Fade
_LABEL_WEAK_MAX = 65.0  # 40–64: Weak Range
_LABEL_WATCH_MAX = 80.0  # 65–79: Break Watch
# 80–100: Breakout Mode

# Trigger cutoff for the triggered flag (matches "Break Watch" onset).
_TRIGGER_IMMINENCE = _LABEL_WEAK_MAX


class RangeBreakImminenceSignal:
    """Composite regime-switch detector fused from four orthogonal inputs."""

    name = "range_break_imminence"

    def evaluate(self, ctx: MarketContext) -> AdvancedSignalResult:
        skew = self._skew_component(ctx)
        dealer = self._dealer_component(ctx)
        trap = self._trap_component(ctx)
        compression = self._compression_component(ctx)

        # Directional bias is a weighted average of the three directional
        # inputs (compression is directionless). Each signed value is in
        # [-1, 1] so bias stays in [-1, 1].
        directional_num = (
            skew["signed"] * _W_SKEW + dealer["signed"] * _W_DEALER + trap["signed"] * _W_TRAP
        )
        directional_den = _W_SKEW + _W_DEALER + _W_TRAP
        bias = directional_num / directional_den

        # Imminence magnitude is a weighted sum of absolute contributions
        # (each magnitude is 0–100, weights sum to 100 ⇒ result is 0–100).
        imminence = (
            skew["magnitude"] * _W_SKEW
            + dealer["magnitude"] * _W_DEALER
            + trap["magnitude"] * _W_TRAP
            + compression["magnitude"] * _W_COMPRESSION
        ) / 100.0
        imminence = max(0.0, min(100.0, imminence))

        # Use the continuous bias directly so the score grades smoothly
        # rather than snapping to ±(imminence/100) at any non-zero bias.
        direction = self._direction_flag(bias)
        score = max(-1.0, min(1.0, bias * (imminence / 100.0)))

        label, playbook = self._label_and_playbook(imminence, direction)
        signal = self._signal_label(score, imminence)

        return AdvancedSignalResult(
            name=self.name,
            score=score,
            context={
                "imminence": round(imminence, 2),
                "bias": round(bias, 4),
                "direction": (
                    "bullish" if direction > 0 else "bearish" if direction < 0 else "neutral"
                ),
                "label": label,
                "playbook": playbook,
                "triggered": imminence >= _TRIGGER_IMMINENCE,
                "signal": signal,
                "skew": skew,
                "dealer": dealer,
                "trap": trap,
                "compression": compression,
                "weights": {
                    "skew": _W_SKEW,
                    "dealer": _W_DEALER,
                    "trap": _W_TRAP,
                    "compression": _W_COMPRESSION,
                },
            },
        )

    # ------------------------------------------------------------------
    # Sub-scores — each returns {"signed": [-1, 1], "magnitude": [0, 100], ...}
    # ------------------------------------------------------------------

    def _skew_component(self, ctx: MarketContext) -> dict:
        skew = (ctx.extra or {}).get("skew") or {}
        put_iv = skew.get("otm_put_iv")
        call_iv = skew.get("otm_call_iv")
        base = {
            "signed": 0.0,
            "magnitude": 0.0,
            "spread": None,
            "deviation": None,
            "otm_put_iv": None,
            "otm_call_iv": None,
        }
        if put_iv is None or call_iv is None:
            return base
        try:
            spread = float(put_iv) - float(call_iv)
        except (TypeError, ValueError):
            return base

        deviation = spread - _SKEW_BASELINE
        normalized = max(-1.0, min(1.0, deviation / _SKEW_SATURATION))
        # Elevated put skew (positive deviation) = bearish bias.
        signed = -normalized
        magnitude = min(1.0, abs(deviation) / _SKEW_SATURATION) * 100.0
        return {
            "signed": signed,
            "magnitude": magnitude,
            "spread": round(spread, 6),
            "deviation": round(deviation, 6),
            "otm_put_iv": round(float(put_iv), 6),
            "otm_call_iv": round(float(call_iv), 6),
        }

    def _dealer_component(self, ctx: MarketContext) -> dict:
        dni = self._dealer_net_delta(ctx)
        if dni is None:
            return {"signed": 0.0, "magnitude": 0.0, "dealer_net_delta": None}
        normalized = max(-1.0, min(1.0, dni / _DEALER_NORM))
        # Dealers short delta (negative DNI) → must buy into strength → bullish.
        signed = -normalized
        magnitude = abs(normalized) * 100.0
        return {
            "signed": signed,
            "magnitude": magnitude,
            "dealer_net_delta": round(dni, 2),
        }

    def _trap_component(self, ctx: MarketContext) -> dict:
        extra = ctx.extra or {}
        closes = ctx.recent_closes or []
        if len(closes) < 5 or ctx.close <= 0:
            return {
                "signed": 0.0,
                "magnitude": 0.0,
                "side": "none",
                "reason": "insufficient_bars",
            }

        window = closes[-_RANGE_WINDOW:] if len(closes) > _RANGE_WINDOW else closes
        range_lo = min(window)
        range_hi = max(window)
        range_span = range_hi - range_lo
        if range_span <= 0:
            return {
                "signed": 0.0,
                "magnitude": 0.0,
                "side": "none",
                "reason": "no_range",
            }

        # 1.0 when price is pinned at the extreme, decaying to 0.0 once it
        # is more than TRAP_PROXIMITY_FRACTION away from that extreme.
        dist_from_low_frac = (ctx.close - range_lo) / range_span
        dist_from_high_frac = (range_hi - ctx.close) / range_span
        near_low = max(0.0, 1.0 - dist_from_low_frac / _TRAP_PROXIMITY_FRACTION)
        near_high = max(0.0, 1.0 - dist_from_high_frac / _TRAP_PROXIMITY_FRACTION)

        call_flow_delta = float(extra.get("call_flow_delta") or 0.0)
        put_flow_delta = float(extra.get("put_flow_delta") or 0.0)

        # Bearish trap: price pinned at range-low while put flow accelerates
        # and/or call flow decelerates — "support" is being baited.
        bearish = near_low * self._flow_pressure(put_flow_delta, -call_flow_delta)
        # Bullish trap: price pinned at range-high with call flow accelerating.
        bullish = near_high * self._flow_pressure(call_flow_delta, -put_flow_delta)

        if bearish > bullish and bearish > 0.0:
            signed = -min(1.0, bearish)
            magnitude = min(1.0, bearish) * 100.0
            side = "bearish_trap"
        elif bullish > 0.0:
            signed = min(1.0, bullish)
            magnitude = min(1.0, bullish) * 100.0
            side = "bullish_trap"
        else:
            signed = 0.0
            magnitude = 0.0
            side = "none"

        return {
            "signed": signed,
            "magnitude": magnitude,
            "side": side,
            "range_low": round(range_lo, 4),
            "range_high": round(range_hi, 4),
            "near_low_pct": round(near_low, 4),
            "near_high_pct": round(near_high, 4),
            "call_flow_delta": round(call_flow_delta, 2),
            "put_flow_delta": round(put_flow_delta, 2),
        }

    def _compression_component(self, ctx: MarketContext) -> dict:
        closes = ctx.recent_closes or []
        if len(closes) < 20:
            return {"magnitude": 0.0, "reason": "insufficient_bars"}
        short_sigma = realized_sigma(closes, window=10)
        long_sigma = realized_sigma(closes, window=60)
        if long_sigma <= 0:
            return {
                "magnitude": 0.0,
                "short_sigma": round(short_sigma, 6),
                "long_sigma": 0.0,
                "ratio": None,
            }
        ratio = short_sigma / long_sigma
        span = max(1e-9, _COMPRESSION_NONE - _COMPRESSION_FULL)
        compression = max(0.0, min(1.0, (_COMPRESSION_NONE - ratio) / span))
        return {
            "magnitude": compression * 100.0,
            "short_sigma": round(short_sigma, 6),
            "long_sigma": round(long_sigma, 6),
            "ratio": round(ratio, 4),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _flow_pressure(primary: float, secondary: float) -> float:
        total = max(0.0, primary) + max(0.0, secondary)
        return max(0.0, min(1.0, total / max(_FLOW_NORM, 1.0)))

    @staticmethod
    def _dealer_net_delta(ctx: MarketContext) -> Optional[float]:
        if ctx.dealer_net_delta:
            return float(ctx.dealer_net_delta)
        rows = (ctx.extra or {}).get("gex_by_strike") if ctx.extra else None
        if not rows or ctx.close <= 0:
            return None
        # Prefer delta-weighted OI when the analytics layer provided it.
        # Until that lands, fall back to the linear-distance delta proxy
        # used by DealerDeltaPressureComponent so the dealer sub-score
        # contributes something rather than being structurally zero.
        have_delta_oi = any(
            isinstance(r, dict) and ("call_delta_oi" in r or "put_delta_oi" in r) for r in rows
        )
        if have_delta_oi:
            total = 0.0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    call_d = float(row.get("call_delta_oi") or 0.0)
                    put_d = float(row.get("put_delta_oi") or 0.0)
                except (TypeError, ValueError):
                    continue
                total -= call_d + put_d
            return total

        total = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            strike = row.get("strike")
            if strike is None:
                continue
            try:
                strike_f = float(strike)
                call_oi_f = float(row.get("call_oi") or 0)
                put_oi_f = float(row.get("put_oi") or 0)
            except (TypeError, ValueError):
                continue
            distance_pct = (ctx.close - strike_f) / ctx.close
            call_delta = max(0.0, min(1.0, 0.5 - distance_pct * 10))
            put_delta = -max(0.0, min(1.0, 0.5 + distance_pct * 10))
            total -= (call_oi_f * call_delta + put_oi_f * put_delta) * 100
        return total

    @staticmethod
    def _direction_flag(bias: float) -> float:
        if bias > 1e-6:
            return 1.0
        if bias < -1e-6:
            return -1.0
        return 0.0

    @staticmethod
    def _label_and_playbook(imminence: float, direction: float) -> tuple[str, str]:
        if imminence < _LABEL_FADE_MAX:
            return (
                "Range Fade",
                "Fade range extremes. Buy low end, short high end, avoid mid-range.",
            )
        if imminence < _LABEL_WEAK_MAX:
            return (
                "Weak Range",
                "Still fade, reduce size. Take profits faster; do not hold fades too long.",
            )
        if imminence < _LABEL_WATCH_MAX:
            side = "lows" if direction < 0 else "highs" if direction > 0 else "extremes"
            return (
                "Break Watch",
                f"Stop blindly fading {side}. Only fade after failed breakouts/reclaims; "
                "start preparing continuation entries.",
            )
        if direction < 0:
            return (
                "Breakout Mode",
                "Follow the break. If range low breaks and holds, short bounce attempts "
                "instead of buying the dip.",
            )
        if direction > 0:
            return (
                "Breakout Mode",
                "Follow the break. If range high breaks and holds, long pullbacks instead "
                "of shorting the rip.",
            )
        return (
            "Breakout Mode",
            "Coil is loaded with no directional bias — wait for the first acceptance "
            "outside the range, then trade the retest.",
        )

    @staticmethod
    def _signal_label(score: float, imminence: float) -> str:
        if imminence < _TRIGGER_IMMINENCE:
            return "range_fade"
        if score < 0:
            return "bearish_break_imminent"
        if score > 0:
            return "bullish_break_imminent"
        return "break_watch_neutral"


# Backward-compat aliases to mirror the convention used by sibling signals.
RangeBreakImminenceComponent = RangeBreakImminenceSignal
RangeBreakImminenceSignalComponent = RangeBreakImminenceSignal
