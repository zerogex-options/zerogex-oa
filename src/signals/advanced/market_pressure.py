"""Market Pressure Index (MPI) — directional loading detector.

Answers: *"Is the market loaded to move, and which way will it break?"*

Distinct from neighboring signals:
  * Squeeze Setup is reactive — fires when momentum + flow + GEX align
    AFTER acceleration has already begun.
  * Range Break Imminence focuses on regime-switch likelihood inside
    range-bound chop (skew + dealer + trap + compression).
  * Vol Expansion measures GEX-driven readiness × current momentum.
  * Gamma/VWAP Confluence detects level-cluster magnetism.

MPI asks a forward-looking, pre-momentum question: how much DEALER
HEDGING DEMAND is loaded into the book right now, and which direction
will that hedging flow when any catalyst lights the fuse?

Four pillars combine multiplicatively (magnitude) and as a weighted
directional vector (sign):

    1. Gamma Compression (C)          — how tight is the spring?
       * Wall pinch       : 1 − (call_wall − put_wall) / spot
       * Flip proximity   : Cauchy decay on |spot − flip| / spot
       * Net-GEX regime multiplier (short-gamma amplifies hedging)
       → C ∈ [0, 1], directionless.

    2. Hedging Vector (H)             — which way will dealers hedge?
       * Vanna : ∂Δ/∂σ  → IV-crush bias (morning weight)
       * Charm : ∂Δ/∂t  → expiry-decay bias (afternoon weight)
       * Session-weighted blend (α·vanna + (1−α)·charm·charm_amp)
       * Dealer-net-delta gate (small DNI ⇒ muted hedging response)
       * Alignment bonus when vanna and charm point the same way
       → H_signed ∈ [-1, 1], H_magnitude ∈ [0, 1].

    3. Flow Asymmetry (F)             — is the catalyst loaded?
       * Premium skew : (Δcall_prem − Δput_prem) / Σ|Δprem|
       * Smart-money skew : (smart_call − smart_put) / Σ
       * Magnitude gate on total premium flux
       → F_signed ∈ [-1, 1], F_magnitude ∈ [0, 1].

    4. Vol Tension (T)                — is vol cheap & contracted?
       * (1 − iv_rank) : IV cheapness
       * short/long σ ratio : realized compression
       * T = √((1 − iv_rank) · vol_squeeze)
       → T ∈ [0, 1], modulates magnitude from 0.5× → 1.0×.

Dealer pressure (D) enters only on the directional side as
``−tanh(DNI / norm)`` (dealers long delta must sell into strength).

Final assembly:
    M (loading)  = 100 · C · H_mag · F_mag · (0.5 + 0.5·T)
    dir_raw      = w_H · H_signed + w_F · F_signed · F_mag + w_D · D_signed
    dir          = clamp(dir_raw · confidence_mult, -1, 1)
    score        = sign(dir) · √|dir| · (M / 100)

``confidence_mult`` ∈ [1 − bonus, 1 + bonus] rewards or penalizes
agreement between the three directional inputs (H, gated-F, D).  The
``√|dir|`` shape keeps moderate biases meaningful while still grading
smoothly through zero.

Output convention:
    ``score``  ∈ [-1, 1] — signed pressure; positive = bullish loading.
    ``context['loading']``  ∈ [0, 100] — directionless pressure magnitude.
    ``context['label']``    — Discharged / Building / Loaded / Critical.
    ``context['triggered']`` — True when loading ≥ 50 AND |dir| ≥ 0.2.
"""

from __future__ import annotations

import math
import os
from typing import Optional

from src.signals.components.base import MarketContext
from src.signals.components.utils import (
    SESSION_CLOSE_MIN_ET,
    SESSION_OPEN_MIN_ET,
    minute_of_day_et,
    realized_sigma,
)
from src.signals.advanced.base import (
    AdvancedSignalResult,
    flow_flux_norm,
)

# ---------------------------------------------------------------------------
# Tunables (env-overridable so production can recalibrate without a deploy)
# ---------------------------------------------------------------------------

# Wall pinch saturates to 0 (no compression) when call_wall − put_wall
# spans this fraction of spot.  A 2% wall spread on SPY is "normal".
_WALL_WIDE_PCT = float(os.getenv("SIGNAL_MP_WALL_WIDE_PCT", "0.02"))

# Flip proximity uses a Cauchy kernel: σ² / (σ² + d²) where
# d = |spot − flip| / spot.  At d = σ the score is 0.5; at d = 3σ it's
# ≈0.1.  σ defaults to 50bps — flip within 50bps of spot is "near".
_FLIP_PROX_SIGMA = float(os.getenv("SIGNAL_MP_FLIP_PROX_SIGMA", "0.005"))

# Net-GEX regime multiplier — short-gamma amplifies dealer hedging
# response, long-gamma dampens it.  Mapped from a saturating tanh of
# the scale-invariant net_gex / (S² · total_oi) ratio.
_REGIME_MIN_MULT = float(os.getenv("SIGNAL_MP_REGIME_MIN_MULT", "0.5"))
_REGIME_MID_MULT = float(os.getenv("SIGNAL_MP_REGIME_MID_MULT", "0.75"))
_REGIME_MAX_MULT = float(os.getenv("SIGNAL_MP_REGIME_MAX_MULT", "1.0"))
_REGIME_GEX_SCALE = float(os.getenv("SIGNAL_MP_REGIME_GEX_SCALE", "50.0"))

# Vanna and charm normalizers (fallbacks; per-symbol cache via
# ctx.extra['normalizers'] overrides when available).  Matches the
# scale used by ``src.signals.basic.vanna_charm_flow``.
_VANNA_NORM = float(os.getenv("SIGNAL_MP_VANNA_NORM", "1.5e8"))
_CHARM_NORM = float(os.getenv("SIGNAL_MP_CHARM_NORM", "1.0e10"))

# Charm amplification ramps from 1.0 to _CHARM_AMP_MAX after
# _CHARM_AMP_START fraction of session — charm flow accelerates into
# expiry.  Mirrors ``basic.vanna_charm_flow``.
_CHARM_AMP_START = 0.6
_CHARM_AMP_MAX = 1.5

# Session weight on vanna vs charm.  Open: α_OPEN (vanna dominant during
# IV crush); close: α_CLOSE (charm dominant during decay).
_ALPHA_VANNA_OPEN = float(os.getenv("SIGNAL_MP_ALPHA_VANNA_OPEN", "0.7"))
_ALPHA_VANNA_CLOSE = float(os.getenv("SIGNAL_MP_ALPHA_VANNA_CLOSE", "0.3"))

# Dealer-net-delta gate scale (shares-equivalent).
_DEALER_NORM = float(os.getenv("SIGNAL_MP_DEALER_NORM", "3.0e8"))
# Floor of the H-magnitude gate when DNI ≈ 0 — small dealers still
# hedge from intrinsic greeks even when net delta is flat.
_DEALER_GATE_FLOOR = float(os.getenv("SIGNAL_MP_DEALER_GATE_FLOOR", "0.4"))

# Alignment bonus when vanna and charm share sign (additional 20% by
# default); disagreement carves out 10%.
_VC_ALIGN_BONUS = float(os.getenv("SIGNAL_MP_VC_ALIGN_BONUS", "0.2"))
_VC_DISAGREE_PENALTY = float(os.getenv("SIGNAL_MP_VC_DISAGREE_PENALTY", "0.1"))

# Premium-skew vs smart-money-skew blend on the flow side.
_PREM_SKEW_WEIGHT = float(os.getenv("SIGNAL_MP_PREM_SKEW_WEIGHT", "0.6"))

# F directional input is gated to zero when its own magnitude is below
# this threshold (flow direction without volume is noise).
_FLOW_DIR_GATE = float(os.getenv("SIGNAL_MP_FLOW_DIR_GATE", "0.2"))

# Realized-vol compression ratio (short_sigma / long_sigma): values
# ≤ FULL count as fully squeezed, ≥ NONE = no squeeze.
_VOL_SQUEEZE_FULL = float(os.getenv("SIGNAL_MP_VOL_SQUEEZE_FULL", "0.5"))
_VOL_SQUEEZE_NONE = float(os.getenv("SIGNAL_MP_VOL_SQUEEZE_NONE", "1.0"))

# Tension contribution floor — missing IV / vol data degrades smoothly,
# not catastrophically.  Loading is multiplied by floor + (1−floor)·T.
_TENSION_FLOOR = float(os.getenv("SIGNAL_MP_TENSION_FLOOR", "0.5"))

# Directional weights — must sum to ~1.0.  Hedging gets the largest
# weight because it is the *mechanical* driver of imminent flow;
# Flow is the *catalyst* loader; Dealer is positional confirmation.
_W_H = float(os.getenv("SIGNAL_MP_W_HEDGING", "0.45"))
_W_F = float(os.getenv("SIGNAL_MP_W_FLOW", "0.40"))
_W_D = float(os.getenv("SIGNAL_MP_W_DEALER", "0.15"))

# Agreement confidence multiplier endpoints (1 ± bonus).
_AGREEMENT_BONUS = float(os.getenv("SIGNAL_MP_AGREEMENT_BONUS", "0.3"))

# Label thresholds on 0–100 loading magnitude.
_LABEL_BUILDING_MIN = 25.0
_LABEL_LOADED_MIN = 50.0
_LABEL_CRITICAL_MIN = 75.0

# Trigger thresholds — both loading and directional clarity must clear.
_TRIGGER_LOADING = 50.0
_TRIGGER_DIR = 0.2

# Below this absolute score we treat a loaded-but-directionless read as
# its own label rather than mis-classifying as bullish/bearish.
_LOADED_NEUTRAL_SCORE_EPS = 0.05


class MarketPressureSignal:
    """Composite "loaded to break" detector with explicit dealer-flow vector."""

    name = "market_pressure"

    def evaluate(self, ctx: MarketContext) -> AdvancedSignalResult:
        compression = self._compression(ctx)
        hedging = self._hedging_vector(ctx)
        flow = self._flow_asymmetry(ctx)
        tension = self._vol_tension(ctx)
        dealer = self._dealer_pressure(ctx)

        # Magnitude — multiplicative across the three "must-have"
        # pillars; tension modulates the result via a floor so missing
        # vol data degrades smoothly.
        m_core = compression["magnitude"] * hedging["magnitude"] * flow["magnitude"]
        m_tension = _TENSION_FLOOR + (1.0 - _TENSION_FLOOR) * tension["magnitude"]
        loading = max(0.0, min(100.0, m_core * m_tension * 100.0))

        # Direction — weighted sum of three signed inputs.  F is gated by
        # its own magnitude so direction without volume contributes
        # nothing.  Dealer signed is in [-1, 1] via tanh-saturated DNI.
        flow_signed_effective = flow["signed"] if flow["magnitude"] >= _FLOW_DIR_GATE else 0.0
        dir_raw = _W_H * hedging["signed"] + _W_F * flow_signed_effective + _W_D * dealer["signed"]

        confidence_mult = self._confidence(
            dir_raw,
            hedging["signed"],
            flow_signed_effective,
            dealer["signed"],
        )
        direction = max(-1.0, min(1.0, dir_raw * confidence_mult))

        # √|direction| shape keeps moderate biases meaningful while
        # still grading smoothly through zero; (loading/100) scales by
        # the multiplicative magnitude.
        score = (1.0 if direction >= 0 else -1.0) * math.sqrt(abs(direction)) * (loading / 100.0)
        score = max(-1.0, min(1.0, score))

        label, playbook = self._label_and_playbook(loading, direction)
        signal_label = self._signal_label(score, loading)

        return AdvancedSignalResult(
            name=self.name,
            score=score,
            context={
                "loading": round(loading, 2),
                "direction": round(direction, 4),
                "direction_sign": (
                    "bullish" if direction > 1e-6 else "bearish" if direction < -1e-6 else "neutral"
                ),
                "label": label,
                "playbook": playbook,
                "triggered": loading >= _TRIGGER_LOADING and abs(direction) >= _TRIGGER_DIR,
                "signal": signal_label,
                "confidence_mult": round(confidence_mult, 4),
                "compression": compression,
                "hedging": hedging,
                "flow": flow,
                "tension": tension,
                "dealer": dealer,
                "weights": {
                    "hedging": _W_H,
                    "flow": _W_F,
                    "dealer": _W_D,
                },
            },
        )

    # ------------------------------------------------------------------
    # Sub-components — each returns at minimum {"magnitude": [0, 1], ...};
    # directional contributors also return {"signed": [-1, 1]}.
    # ------------------------------------------------------------------

    @staticmethod
    def _compression(ctx: MarketContext) -> dict:
        spot = ctx.close
        net_gex = float(ctx.net_gex or 0.0)
        if spot <= 0:
            return {
                "magnitude": 0.0,
                "wall_pinch": None,
                "flip_proximity": None,
                "regime_mult": None,
                "net_gex_mult": None,
                "call_wall": None,
                "put_wall": None,
                "gamma_flip": None,
                "flip": None,
                "spot": None,
                "net_gex": net_gex,
                "reason": "no_spot",
            }

        extra = ctx.extra or {}
        call_wall = extra.get("call_wall")
        put_wall = extra.get("put_wall")
        flip = ctx.gamma_flip

        wall_pinch: Optional[float] = None
        if call_wall is not None and put_wall is not None and float(call_wall) > float(put_wall):
            gap_pct = (float(call_wall) - float(put_wall)) / spot
            wall_pinch = max(0.0, 1.0 - gap_pct / max(_WALL_WIDE_PCT, 1e-9))

        flip_prox: Optional[float] = None
        if flip is not None:
            d = abs(spot - float(flip)) / spot
            flip_prox = (_FLIP_PROX_SIGMA**2) / (_FLIP_PROX_SIGMA**2 + d**2)

        if wall_pinch is None and flip_prox is None:
            return {
                "magnitude": 0.0,
                "wall_pinch": None,
                "flip_proximity": None,
                "regime_mult": None,
                "net_gex_mult": None,
                "call_wall": call_wall,
                "put_wall": put_wall,
                "gamma_flip": flip,
                "flip": flip,
                "spot": spot,
                "net_gex": net_gex,
                "reason": "no_structure",
            }

        # Geometric mean when both inputs are informative (non-zero) — BOTH
        # must be high for real compression.  When one input is saturated
        # to zero (e.g. wide walls on QQQ at 4-5% spread), it carries no
        # information; fall back to the other input alone rather than
        # annihilating the whole pillar.
        wp_informative = wall_pinch is not None and wall_pinch > 0.0
        fp_informative = flip_prox is not None and flip_prox > 0.0
        if wp_informative and fp_informative:
            base = math.sqrt(wall_pinch * flip_prox)  # type: ignore[arg-type]
        elif wp_informative:
            base = wall_pinch  # type: ignore[assignment]
        elif fp_informative:
            base = flip_prox  # type: ignore[assignment]
        else:
            base = 0.0

        regime_mult = MarketPressureSignal._regime_multiplier(ctx)
        magnitude = max(0.0, min(1.0, base * regime_mult))

        return {
            "magnitude": round(magnitude, 4),
            "wall_pinch": round(wall_pinch, 4) if wall_pinch is not None else None,
            "flip_proximity": round(flip_prox, 4) if flip_prox is not None else None,
            "regime_mult": round(regime_mult, 4),
            # Alias for frontend / docstring shorthand.
            "net_gex_mult": round(regime_mult, 4),
            "call_wall": call_wall,
            "put_wall": put_wall,
            "gamma_flip": flip,
            "flip": flip,
            "spot": spot,
            "net_gex": net_gex,
        }

    @staticmethod
    def _regime_multiplier(ctx: MarketContext) -> float:
        """Map net-GEX regime to a [MIN, MAX] scaler.

        Uses the dimensionless ``net_gex / (S² · total_oi · 100 · 0.01)``
        ratio when ``total_oi`` is present (matches ``vol_expansion``'s
        scale-invariant path).  Falls back to ``tanh(net_gex / 1e9)`` so
        the multiplier still moves on older context payloads.
        """
        spot = ctx.close
        total_oi = ctx.total_oi
        net_gex = float(ctx.net_gex or 0.0)
        if spot and spot > 0 and total_oi and total_oi > 0:
            denom = spot * spot * total_oi * 100.0 * 0.01
            ratio = net_gex / denom
            scaled = max(-1.0, min(1.0, ratio * _REGIME_GEX_SCALE))
        else:
            scaled = max(-1.0, min(1.0, net_gex / 1.0e9))
        # ``scaled`` ∈ [-1, +1]: −1 = strongly short gamma (max mult),
        # +1 = strongly long gamma (min mult).  MID at 0.
        if scaled <= 0:
            return _REGIME_MID_MULT + (_REGIME_MAX_MULT - _REGIME_MID_MULT) * (-scaled)
        return _REGIME_MID_MULT + (_REGIME_MIN_MULT - _REGIME_MID_MULT) * scaled

    def _hedging_vector(self, ctx: MarketContext) -> dict:
        dni_raw = self._dealer_net_delta(ctx)
        agg = self._aggregate_vanna_charm(ctx)
        if agg is None:
            return {
                "signed": 0.0,
                "magnitude": 0.0,
                "vanna": None,
                "charm": None,
                "alpha_vanna": None,
                "session_alpha": None,
                "charm_amplification": None,
                "charm_amp": None,
                "alignment_mult": None,
                "alignment_bonus": None,
                "dealer_gate": None,
                "dealer_dni": round(dni_raw, 2) if dni_raw is not None else None,
                "reason": "no_greeks",
            }

        vanna_total, charm_total, source = agg
        norm_v, norm_c = self._field_norms(ctx)
        if norm_v <= 0 or norm_c <= 0:
            return {
                "signed": 0.0,
                "magnitude": 0.0,
                "vanna": round(vanna_total, 2),
                "charm": round(charm_total, 2),
                "alpha_vanna": None,
                "session_alpha": None,
                "charm_amplification": None,
                "charm_amp": None,
                "alignment_mult": None,
                "alignment_bonus": None,
                "dealer_gate": None,
                "dealer_dni": round(dni_raw, 2) if dni_raw is not None else None,
                "reason": "bad_norms",
            }

        charm_amp = self._charm_amplification(ctx)
        v_norm = max(-1.0, min(1.0, vanna_total / norm_v))
        c_norm = max(-1.0, min(1.0, (charm_total * charm_amp) / norm_c))

        alpha = self._vanna_charm_alpha(ctx)
        vector = alpha * v_norm + (1.0 - alpha) * c_norm

        if v_norm * c_norm > 0:
            align_mult = 1.0 + _VC_ALIGN_BONUS
        elif v_norm * c_norm < 0:
            align_mult = 1.0 - _VC_DISAGREE_PENALTY
        else:
            align_mult = 1.0

        dni_abs = abs(dni_raw or 0.0)
        dealer_gate = _DEALER_GATE_FLOOR + (1.0 - _DEALER_GATE_FLOOR) * min(
            1.0, dni_abs / max(_DEALER_NORM, 1.0)
        )

        magnitude_adj = max(0.0, min(1.0, abs(vector) * align_mult * dealer_gate))
        signed = (1.0 if vector >= 0 else -1.0) * magnitude_adj

        return {
            "signed": round(signed, 4),
            "magnitude": round(magnitude_adj, 4),
            "vanna": round(vanna_total, 2),
            "charm": round(charm_total, 2),
            "vanna_normalized": round(v_norm, 4),
            "charm_normalized": round(c_norm, 4),
            "alpha_vanna": round(alpha, 4),
            # Alias for frontend / docstring shorthand.
            "session_alpha": round(alpha, 4),
            "charm_amplification": round(charm_amp, 4),
            "charm_amp": round(charm_amp, 4),
            "alignment_mult": round(align_mult, 4),
            "alignment_bonus": round(align_mult, 4),
            "dealer_gate": round(dealer_gate, 4),
            # Signed DNI surfaced alongside the gate so the hedging card can
            # show "Dealer DNI" without cross-referencing the dealer dict.
            "dealer_dni": round(dni_raw, 2) if dni_raw is not None else None,
            "source": source,
        }

    @staticmethod
    def _vanna_charm_alpha(ctx: MarketContext) -> float:
        """Linear ramp from morning (_ALPHA_VANNA_OPEN) to close (_ALPHA_VANNA_CLOSE)."""
        minute = minute_of_day_et(ctx.timestamp)
        if minute is None:
            return 0.5 * (_ALPHA_VANNA_OPEN + _ALPHA_VANNA_CLOSE)
        if minute <= SESSION_OPEN_MIN_ET:
            return _ALPHA_VANNA_OPEN
        if minute >= SESSION_CLOSE_MIN_ET:
            return _ALPHA_VANNA_CLOSE
        span = max(1, SESSION_CLOSE_MIN_ET - SESSION_OPEN_MIN_ET)
        frac = (minute - SESSION_OPEN_MIN_ET) / span
        return _ALPHA_VANNA_OPEN + (_ALPHA_VANNA_CLOSE - _ALPHA_VANNA_OPEN) * frac

    @staticmethod
    def _charm_amplification(ctx: MarketContext) -> float:
        minute = minute_of_day_et(ctx.timestamp)
        if minute is None or minute <= SESSION_OPEN_MIN_ET:
            return 1.0
        if minute >= SESSION_CLOSE_MIN_ET:
            return _CHARM_AMP_MAX
        span = max(1, SESSION_CLOSE_MIN_ET - SESSION_OPEN_MIN_ET)
        frac = (minute - SESSION_OPEN_MIN_ET) / span
        if frac < _CHARM_AMP_START:
            return 1.0
        ramp = (frac - _CHARM_AMP_START) / max(1e-9, 1.0 - _CHARM_AMP_START)
        return 1.0 + (_CHARM_AMP_MAX - 1.0) * ramp

    @staticmethod
    def _aggregate_vanna_charm(ctx: MarketContext) -> Optional[tuple[float, float, str]]:
        rows = (ctx.extra or {}).get("gex_by_strike") or []
        if not rows:
            return None
        vanna_total = 0.0
        charm_total = 0.0
        saw_dealer = False
        saw_any = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                dv = row.get("dealer_vanna_exposure")
                dc = row.get("dealer_charm_exposure")
                if dv is not None:
                    vanna_total += float(dv)
                    saw_dealer = True
                    saw_any = True
                elif (v := row.get("vanna_exposure")) is not None:
                    vanna_total += -float(v)
                    saw_any = True
                if dc is not None:
                    charm_total += float(dc)
                    saw_dealer = True
                    saw_any = True
                elif (c := row.get("charm_exposure")) is not None:
                    charm_total += -float(c)
                    saw_any = True
            except (TypeError, ValueError):
                continue
        if not saw_any:
            return None
        source = "dealer_exposure" if saw_dealer else "market_exposure_negated"
        return vanna_total, charm_total, source

    @staticmethod
    def _field_norms(ctx: MarketContext) -> tuple[float, float]:
        extra = ctx.extra if isinstance(ctx.extra, dict) else {}
        normalizers = extra.get("normalizers") if isinstance(extra, dict) else None
        norm_v = _VANNA_NORM
        norm_c = _CHARM_NORM
        if isinstance(normalizers, dict):
            try:
                fv = float(normalizers.get("dealer_vanna_exposure"))  # type: ignore[arg-type]
                if fv > 0:
                    norm_v = fv
            except (TypeError, ValueError):
                pass
            try:
                fc = float(normalizers.get("dealer_charm_exposure"))  # type: ignore[arg-type]
                if fc > 0:
                    norm_c = fc
            except (TypeError, ValueError):
                pass
        return norm_v, norm_c

    @staticmethod
    def _flow_asymmetry(ctx: MarketContext) -> dict:
        extra = ctx.extra or {}
        call_fd = float(extra.get("call_flow_delta") or 0.0)
        put_fd = float(extra.get("put_flow_delta") or 0.0)
        flow_norm = flow_flux_norm(ctx)

        prem_total = abs(call_fd) + abs(put_fd)
        prem_skew = (call_fd - put_fd) / prem_total if prem_total > 0 else 0.0

        sm_call = float(ctx.smart_call or 0.0)
        sm_put = float(ctx.smart_put or 0.0)
        sm_total = sm_call + sm_put
        smart_skew = (sm_call - sm_put) / sm_total if sm_total > 0 else 0.0

        signed = _PREM_SKEW_WEIGHT * prem_skew + (1.0 - _PREM_SKEW_WEIGHT) * smart_skew
        signed = max(-1.0, min(1.0, signed))
        magnitude = min(1.0, prem_total / max(flow_norm, 1.0))

        # Reason field surfaces "no flow ingested for this symbol" vs.
        # "flow ingested but neutral" so the dashboard can distinguish a
        # genuinely balanced book from a missing-data condition.  Both
        # smart-money premium and acceleration deltas must be zero — a
        # truly balanced book usually has *some* premium flux on both
        # sides, so all-zero is a strong "no rows" indicator.
        no_flow = prem_total == 0.0 and sm_call == 0.0 and sm_put == 0.0
        reason: Optional[str] = "no_flow_data" if no_flow else None

        return {
            "signed": round(signed, 4),
            "magnitude": round(magnitude, 4),
            "premium_skew": round(prem_skew, 4),
            "smart_skew": round(smart_skew, 4),
            # Alias for frontend / docstring shorthand.
            "smart_money_skew": round(smart_skew, 4),
            "call_flow_delta": round(call_fd, 2),
            "put_flow_delta": round(put_fd, 2),
            "smart_call": round(sm_call, 2),
            "smart_put": round(sm_put, 2),
            "flow_norm_used": round(flow_norm, 2),
            # Total signed premium flux across calls and puts (alias for
            # |call_flow_delta| + |put_flow_delta|).  Frontends use this as
            # the raw "is anything happening?" gauge before the magnitude
            # is gated by flow_norm.
            "total_flux": round(prem_total, 2),
            "reason": reason,
        }

    @staticmethod
    def _vol_tension(ctx: MarketContext) -> dict:
        closes = ctx.recent_closes or []
        short_sigma = realized_sigma(closes, window=10) if len(closes) >= 10 else 0.0
        long_sigma = realized_sigma(closes, window=60) if len(closes) >= 20 else 0.0
        ratio: Optional[float] = (short_sigma / long_sigma) if long_sigma > 0 else None
        if ratio is None:
            vol_squeeze = 0.0
        else:
            span = max(1e-9, _VOL_SQUEEZE_NONE - _VOL_SQUEEZE_FULL)
            vol_squeeze = max(0.0, min(1.0, (_VOL_SQUEEZE_NONE - ratio) / span))

        iv_rank = ctx.iv_rank
        if iv_rank is None:
            magnitude = vol_squeeze * 0.5
            iv_cheapness: Optional[float] = None
        else:
            iv_cheapness = max(0.0, min(1.0, 1.0 - float(iv_rank)))
            magnitude = math.sqrt(iv_cheapness * vol_squeeze)

        return {
            "magnitude": round(magnitude, 4),
            "iv_rank": iv_rank,
            "iv_cheapness": round(iv_cheapness, 4) if iv_cheapness is not None else None,
            "short_sigma": round(short_sigma, 6) if short_sigma > 0 else 0.0,
            "long_sigma": round(long_sigma, 6) if long_sigma > 0 else 0.0,
            "vol_ratio": round(ratio, 4) if ratio is not None else None,
            "vol_squeeze": round(vol_squeeze, 4),
        }

    @classmethod
    def _dealer_pressure(cls, ctx: MarketContext) -> dict:
        dni = cls._dealer_net_delta(ctx)
        if dni is None:
            return {"signed": 0.0, "dealer_net_delta": None}
        # Dealers long delta (DNI > 0) ⇒ must sell into strength ⇒ bearish.
        signed = -math.tanh(dni / max(_DEALER_NORM, 1.0))
        return {
            "signed": round(signed, 4),
            "dealer_net_delta": round(dni, 2),
        }

    @staticmethod
    def _dealer_net_delta(ctx: MarketContext) -> Optional[float]:
        """Prefer the explicit field; otherwise estimate from gex_by_strike.

        Mirrors the fallback chain in
        :meth:`src.signals.advanced.range_break_imminence.RangeBreakImminenceSignal._dealer_net_delta`
        so the two signals see the same dealer-positioning picture.
        """
        if ctx.dealer_net_delta:
            return float(ctx.dealer_net_delta)
        rows = (ctx.extra or {}).get("gex_by_strike") if ctx.extra else None
        if not rows or ctx.close <= 0:
            return None
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
    def _confidence(dir_raw: float, *signed_inputs: float) -> float:
        """Reward agreement, penalize disagreement among directional inputs.

        Counts how many non-zero ``signed_inputs`` share the sign of
        ``dir_raw``; normalizes to [-1, +1] and maps to
        ``1 ± _AGREEMENT_BONUS``.  Returns 1.0 when fewer than two
        active inputs are present (no agreement to measure).
        """
        if abs(dir_raw) < 1e-9:
            return 1.0
        target_sign = 1.0 if dir_raw > 0 else -1.0
        active = [s for s in signed_inputs if abs(s) > 1e-9]
        if len(active) < 2:
            return 1.0
        net = sum(1.0 if (s * target_sign) > 0 else -1.0 for s in active)
        normalized = net / len(active)
        return 1.0 + _AGREEMENT_BONUS * normalized

    @staticmethod
    def _label_and_playbook(loading: float, direction: float) -> tuple[str, str]:
        if loading < _LABEL_BUILDING_MIN:
            return (
                "Discharged",
                "No actionable loading. Trade existing setups; ignore "
                "pressure-driven entries until the book reloads.",
            )
        if loading < _LABEL_LOADED_MIN:
            return (
                "Building",
                "Pressure accumulating but not yet actionable. Tighten stops on "
                "counter-pressure trades; prepare directional templates.",
            )
        side = "upside" if direction > 1e-6 else "downside" if direction < -1e-6 else "either side"
        if loading < _LABEL_CRITICAL_MIN:
            return (
                "Loaded",
                f"Significant pressure to the {side}. Stop fading; start "
                "scaling into continuation entries on first confirmation "
                "(VWAP reclaim, wall break, flow spike).",
            )
        return (
            "Critical",
            f"Coil at the limit. Expect violent resolution to the {side}. "
            "Take the directional trade with reduced size on stops; cut "
            "all counter-pressure exposure.",
        )

    @staticmethod
    def _signal_label(score: float, loading: float) -> str:
        if loading < _TRIGGER_LOADING:
            return "discharged"
        if abs(score) < _LOADED_NEUTRAL_SCORE_EPS:
            return "loaded_neutral"
        return "bullish_pressure" if score > 0 else "bearish_pressure"


MarketPressureComponent = MarketPressureSignal
MarketPressureIndexSignal = MarketPressureSignal
