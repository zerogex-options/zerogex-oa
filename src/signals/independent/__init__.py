"""Independent signals package.

Signals in this package are **not** part of the composite score. They are
evaluated and persisted separately and can independently trigger trade ideas.
"""

from src.signals.independent.base import IndependentSignalResult
from src.signals.independent.engine import IndependentSignalEngine
from src.signals.independent.squeeze_setup import SqueezeSetupSignal
from src.signals.independent.trap_detection import TrapDetectionSignal
from src.signals.independent.zero_dte_position_imbalance import (
    ZeroDTEPositionImbalanceSignal,
    ZeroDtePositionImbalanceSignal,
)
from src.signals.independent.gamma_vwap_confluence import (
    GammaVWAPConfluenceSignal,
    GammaVwapConfluenceSignal,
)
from src.signals.independent.vol_expansion import VolExpansionSignal
from src.signals.independent.eod_pressure import EODPressureSignal, EodPressureSignal

__all__ = [
    "IndependentSignalResult",
    "IndependentSignalEngine",
    "SqueezeSetupSignal",
    "TrapDetectionSignal",
    "ZeroDTEPositionImbalanceSignal",
    "ZeroDtePositionImbalanceSignal",
    "GammaVWAPConfluenceSignal",
    "GammaVwapConfluenceSignal",
    "VolExpansionSignal",
    "EODPressureSignal",
    "EodPressureSignal",
]
