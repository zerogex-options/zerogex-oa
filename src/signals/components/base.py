"""Base interface for all scoring components."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class MarketContext:
    """All market data available to scoring components each cycle."""

    timestamp: datetime
    underlying: str
    close: float
    net_gex: float
    gamma_flip: Optional[float]
    put_call_ratio: float
    max_pain: Optional[float]
    smart_call: float
    smart_put: float
    recent_closes: list[float]
    iv_rank: Optional[float]
    # Extended fields populated if available — components should handle None gracefully
    dealer_net_delta: float = 0.0
    vwap: Optional[float] = None
    vwap_deviation_pct: Optional[float] = None
    orb_status: Optional[str] = None
    extra: dict = field(default_factory=dict)  # escape hatch for future fields


class ComponentBase(ABC):
    """
    All scoring components must implement this interface.

    compute() must return a float in [-1.0, +1.0]:
      +1.0 = strongly bullish
      -1.0 = strongly bearish
       0.0 = neutral / insufficient data

    context_values() should return a dict of the key inputs used,
    for storage in signal_component_scores.context_values.
    """

    name: str  # must be defined on each subclass as a class attribute
    weight: float  # must be defined on each subclass as a class attribute

    @abstractmethod
    def compute(self, ctx: MarketContext) -> float: ...

    def context_values(self, ctx: MarketContext) -> dict:
        """Override to store relevant inputs alongside the score."""
        return {}
