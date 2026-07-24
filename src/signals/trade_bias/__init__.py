"""Trade Bias — backend Signals-Engine process.

Trade Bias is a *signed directional* read (which way, how convinced, and — from
Phase 3 — whether live price-action/flow/tape/momentum overrode the gamma-based
posture). It is distinct from the 0-100 Market State Index (``signal_scores``),
which is a directionless *state magnitude*.

Phase 1 ports the front-end ``computeBias()`` (frontend/core/tradeBias.ts) into
the engine verbatim and persists it to ``trade_bias_scores`` so the read gains
an API, history, and backtestability. Later phases fuse a first-class momentum
component and a generic swing bounce/reject detector into a graded override.
"""

from src.signals.trade_bias.bias import (
    BiasInput,
    BiasResult,
    ChecklistItem,
    WatchingSignal,
    compute_bias,
)
from src.signals.trade_bias.engine import TradeBiasEngine, TradeBiasSnapshot
from src.signals.trade_bias.fusion import (
    INTRADAY_PROFILE,
    SWING_PROFILE,
    ContinuousBias,
    FusedBias,
    TacticalRead,
    TenorProfile,
    compute_tactical,
    continuous_bias,
    fuse,
    profile_for,
)

__all__ = [
    "BiasInput",
    "BiasResult",
    "ChecklistItem",
    "WatchingSignal",
    "compute_bias",
    "TradeBiasEngine",
    "TradeBiasSnapshot",
    "TacticalRead",
    "FusedBias",
    "ContinuousBias",
    "TenorProfile",
    "SWING_PROFILE",
    "INTRADAY_PROFILE",
    "compute_tactical",
    "continuous_bias",
    "fuse",
    "profile_for",
]
