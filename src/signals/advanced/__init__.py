"""Advanced signals package.

Signals in this package are **not** part of the Market State Index. They are
evaluated and persisted separately and can independently trigger trade ideas.
"""

from src.signals.advanced.base import AdvancedSignalResult, IndependentSignalResult
from src.signals.advanced.engine import AdvancedSignalEngine, IndependentSignalEngine
from src.signals.advanced.squeeze_setup import SqueezeSetupSignal
from src.signals.advanced.trap_detection import TrapDetectionSignal
from src.signals.advanced.zero_dte_position_imbalance import (
    ZeroDTEPositionImbalanceSignal,
    ZeroDtePositionImbalanceSignal,
)
from src.signals.advanced.gamma_vwap_confluence import (
    GammaVWAPConfluenceSignal,
    GammaVwapConfluenceSignal,
)
from src.signals.advanced.vol_expansion import VolExpansionSignal
from src.signals.advanced.eod_pressure import EODPressureSignal, EodPressureSignal

__all__ = [
    "AdvancedSignalResult",
    "AdvancedSignalEngine",
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
