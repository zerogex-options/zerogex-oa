"""Advanced signal orchestrator (signals outside Market State Index)."""

from __future__ import annotations

from src.signals.components.base import MarketContext
from src.signals.components.spectrum import ensure_non_zero
from src.signals.advanced.base import AdvancedSignalResult
from src.signals.advanced.eod_pressure import EODPressureSignal
from src.signals.advanced.gamma_vwap_confluence import GammaVWAPConfluenceSignal
from src.signals.advanced.range_break_imminence import RangeBreakImminenceSignal
from src.signals.advanced.squeeze_setup import SqueezeSetupSignal
from src.signals.advanced.trap_detection import TrapDetectionSignal
from src.signals.advanced.vol_expansion import VolExpansionSignal
from src.signals.advanced.zero_dte_position_imbalance import (
    ZeroDTEPositionImbalanceSignal,
)


class AdvancedSignalEngine:
    """Generate side-channel signals persisted like components with weight=0."""

    def __init__(self) -> None:
        self._signals = (
            VolExpansionSignal(),
            EODPressureSignal(),
            SqueezeSetupSignal(),
            TrapDetectionSignal(),
            ZeroDTEPositionImbalanceSignal(),
            GammaVWAPConfluenceSignal(),
            RangeBreakImminenceSignal(),
        )

    def evaluate(self, ctx: MarketContext) -> list[AdvancedSignalResult]:
        results: list[AdvancedSignalResult] = []
        for signal in self._signals:
            result = signal.evaluate(ctx)
            # Replace abstain-zero scores with a regime-derived tilt so
            # every signal lands on a continuous spectrum.
            result.score = ensure_non_zero(result.score, ctx)
            results.append(result)
        return results
