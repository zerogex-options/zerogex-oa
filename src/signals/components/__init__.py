"""Primary Market State Index components.

Active (non-zero weight) components contributing to the 0-100 composite:
  - net_gex_sign           (16 pts)
  - gamma_anchor           (30 pts)  [Phase 2.1, blends three sub-signals]
  - put_call_ratio         (12 pts)
  - volatility_regime      (6  pts)
  - order_flow_imbalance   (19 pts)  [Phase 3.1; bumped +6 in Phase 2.1]
  - dealer_delta_pressure  (17 pts)  [Phase 3.1; bumped +5 in Phase 2.1]

The three former gamma-cluster classes (``FlipDistanceComponent``,
``LocalGammaComponent``, ``PriceVsMaxGammaComponent``) remain importable
because ``GammaAnchorComponent`` instantiates them internally as
delegates to compute its blended score.  They are NOT registered as
standalone MSI components anymore — their per-cycle subscores surface
in the API via ``gamma_anchor``'s nested ``context`` field.

Total active weight: 100 pts.  See ``ScoringEngine.COMPONENT_POINTS``
for the authoritative table.
"""

from src.signals.basic.dealer_delta_pressure import DealerDeltaPressureComponent
from src.signals.components.base import ComponentBase, MarketContext
from src.signals.components.flip_distance import FlipDistanceComponent
from src.signals.components.gamma_anchor import GammaAnchorComponent
from src.signals.components.local_gamma import LocalGammaComponent
from src.signals.components.net_gex_sign import NetGexSignComponent
from src.signals.components.order_flow_imbalance import OrderFlowImbalanceComponent
from src.signals.components.price_vs_max_gamma import PriceVsMaxGammaComponent
from src.signals.components.put_call_ratio_state import PutCallRatioStateComponent
from src.signals.components.volatility_regime import VolatilityRegimeComponent

__all__ = [
    "ComponentBase",
    "MarketContext",
    "NetGexSignComponent",
    "GammaAnchorComponent",
    "FlipDistanceComponent",
    "LocalGammaComponent",
    "PutCallRatioStateComponent",
    "PriceVsMaxGammaComponent",
    "VolatilityRegimeComponent",
    "OrderFlowImbalanceComponent",
    "DealerDeltaPressureComponent",
]
