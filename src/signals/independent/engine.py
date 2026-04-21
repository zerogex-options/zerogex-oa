"""Independent signal orchestrator (signals excluded from composite score)."""
from __future__ import annotations

from src.signals.components.base import MarketContext
from src.signals.independent.base import IndependentSignalResult
from src.signals.independent.eod_pressure import EODPressureSignal
from src.signals.independent.gamma_vwap_confluence import GammaVWAPConfluenceSignal
from src.signals.independent.squeeze_setup import SqueezeSetupSignal
from src.signals.independent.trap_detection import TrapDetectionSignal
from src.signals.independent.vol_expansion import VolExpansionSignal
from src.signals.independent.zero_dte_position_imbalance import (
    ZeroDTEPositionImbalanceSignal,
)


class IndependentSignalEngine:
    """Generate side-channel signals persisted like components with weight=0."""

    def __init__(self) -> None:
        self._signals = (
            VolExpansionSignal(),
            EODPressureSignal(),
            SqueezeSetupSignal(),
            TrapDetectionSignal(),
            ZeroDTEPositionImbalanceSignal(),
            GammaVWAPConfluenceSignal(),
        )

    def evaluate(self, ctx: MarketContext) -> list[IndependentSignalResult]:
        return [signal.evaluate(ctx) for signal in self._signals]
