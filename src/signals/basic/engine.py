"""Basic signal orchestrator.

Parallel to :class:`AdvancedSignalEngine`, but for continuous directional
scores that don't emit discrete triggered events. Each signal implements
the ``ComponentBase`` interface; the engine adapts ``compute()`` +
``context_values()`` into :class:`AdvancedSignalResult` so persistence and
read-side plumbing stay identical.
"""
from __future__ import annotations

from src.signals.components.base import ComponentBase, MarketContext
from src.signals.advanced.base import AdvancedSignalResult
from src.signals.basic.dealer_delta_pressure import DealerDeltaPressureComponent
from src.signals.basic.gex_gradient import GexGradientComponent
from src.signals.basic.positioning_trap import PositioningTrapComponent
from src.signals.basic.skew_delta import SkewDeltaComponent
from src.signals.basic.tape_flow_bias import TapeFlowBiasComponent
from src.signals.basic.vanna_charm_flow import VannaCharmFlowComponent


class BasicSignalEngine:
    """Evaluate the six continuous basic signals each cycle."""

    def __init__(self) -> None:
        self._signals: tuple[ComponentBase, ...] = (
            TapeFlowBiasComponent(),
            SkewDeltaComponent(),
            VannaCharmFlowComponent(),
            DealerDeltaPressureComponent(),
            GexGradientComponent(),
            PositioningTrapComponent(),
        )

    @property
    def signal_names(self) -> tuple[str, ...]:
        return tuple(s.name for s in self._signals)

    def evaluate(self, ctx: MarketContext) -> list[AdvancedSignalResult]:
        results: list[AdvancedSignalResult] = []
        for signal in self._signals:
            raw = signal.compute(ctx)
            score = max(-1.0, min(1.0, float(raw)))
            context = signal.context_values(ctx) or {}
            results.append(
                AdvancedSignalResult(name=signal.name, score=score, context=dict(context))
            )
        return results
