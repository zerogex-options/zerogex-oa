"""PatternBase: abstract interface every Playbook pattern implements.

A pattern is a self-contained decision rule. Given a ``PlaybookContext``,
its ``match()`` method either returns a fully-populated ``ActionCard`` or
``None`` if its trigger conditions aren't met.

Patterns are responsible for their own:
  * Trigger conditions
  * Instrument selection (BUY_PUT_DEBIT, SELL_CALL_SPREAD, etc.)
  * Entry / target / stop reference prices
  * Confidence computation (with confluence helpers exposed below)
  * Plain-English rationale

The PlaybookEngine (``engine.py``) handles regime gating, hysteresis,
conflict resolution, and STAND_DOWN — patterns should not implement
those concerns.

See ``docs/playbook_catalog.md`` §3 and §5.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Optional

from src.signals.playbook.context import PlaybookContext
from src.signals.playbook.types import ActionCard, clamp_confidence

# Adjacent-regime gradient for the regime_fit calculation.  Patterns
# declare a `preferred_regime` (single string) plus `valid_regimes`
# (iterable).  The engine awards 1.1x for exact match, 1.0x for
# adjacent, 0.8x for valid-but-two-steps-away.
REGIME_ORDER = ("trend_expansion", "controlled_trend", "chop_range", "high_risk_reversal")


def regime_distance(a: str, b: str) -> int:
    """Return |index difference| of two regime labels in REGIME_ORDER.

    Returns a large number if either label is unknown so the engine
    treats it as "not adjacent" rather than crashing.
    """
    try:
        return abs(REGIME_ORDER.index(a) - REGIME_ORDER.index(b))
    except ValueError:
        return 99


class PatternBase(ABC):
    """Every Playbook pattern subclasses this."""

    # Subclasses MUST set these as class attributes:
    id: str = ""  # snake_case unique id
    name: str = ""  # human-readable display name
    tier: str = ""  # "0DTE" | "1DTE" | "swing"
    direction: str = ""  # "bullish" | "bearish" | "non_directional" | "context_dependent"
    valid_regimes: tuple[str, ...] = ()  # subset of REGIME_ORDER
    preferred_regime: str = ""  # one of REGIME_ORDER (drives regime_fit boost)
    pattern_base: float = 0.50  # prior; replaced by historical hit rate in PR-3

    # Default dwell windows by tier (minutes) — the engine uses these for
    # hysteresis. Patterns can override.
    DWELL_BY_TIER: dict[str, int] = {"0DTE": 5, "1DTE": 15, "swing": 60}

    # Confluence sets — names of advanced+basic signals whose alignment
    # boosts (or penalizes) confidence.  Subclasses should populate.
    confluence_signals_for: tuple[str, ...] = ()
    confluence_signals_against: tuple[str, ...] = ()

    @abstractmethod
    def match(self, ctx: PlaybookContext) -> Optional[ActionCard]:
        """Return a populated ActionCard if triggered, else None."""

    # ------------------------------------------------------------------
    # Helpers patterns can use when computing confidence
    # ------------------------------------------------------------------

    def dwell_minutes(self) -> int:
        return self.DWELL_BY_TIER.get(self.tier, 5)

    def compute_confidence(
        self,
        ctx: PlaybookContext,
        *,
        bias: str,  # "bullish" | "bearish" — direction the pattern argues for
        extra_for: Iterable[str] = (),
        extra_against: Iterable[str] = (),
    ) -> float:
        """Apply the spec's confidence formula.

        ``confidence = pattern_base * confluence_multiplier * regime_fit``,
        clamped to [0.20, 0.95].

        Confluence: each aligned-for signal adds +0.05; each
        aligned-against subtracts -0.10.  ``bias`` decides which sign
        of a signal counts as alignment (e.g. for a bearish pattern,
        a signal with score < 0 is aligned-for).
        """
        base = self.pattern_base
        regime_fit = self._regime_fit(ctx.msi_regime)
        confluence = self._confluence_multiplier(
            ctx, bias=bias, extra_for=extra_for, extra_against=extra_against
        )
        return clamp_confidence(base * confluence * regime_fit)

    def _regime_fit(self, current_regime: Optional[str]) -> float:
        if not current_regime:
            return 1.0
        if current_regime == self.preferred_regime:
            return 1.1
        # Both must be in REGIME_ORDER for distance to be meaningful.
        d = regime_distance(self.preferred_regime, current_regime)
        if d == 1:
            return 1.0
        return 0.8

    def _confluence_multiplier(
        self,
        ctx: PlaybookContext,
        *,
        bias: str,
        extra_for: Iterable[str],
        extra_against: Iterable[str],
    ) -> float:
        mult = 1.0
        sign_for = 1.0 if bias == "bullish" else -1.0
        for signal_name in tuple(self.confluence_signals_for) + tuple(extra_for):
            snap = ctx.signal(signal_name)
            if snap and (snap.clamped_score * sign_for) > 0.0:
                mult += 0.05
        for signal_name in tuple(self.confluence_signals_against) + tuple(extra_against):
            snap = ctx.signal(signal_name)
            if snap and (snap.clamped_score * sign_for) < 0.0:
                mult -= 0.10
        return max(0.7, min(1.4, mult))

    # ------------------------------------------------------------------
    # Diagnostic: list missing trigger conditions for STAND_DOWN reporting
    # ------------------------------------------------------------------

    def explain_miss(self, ctx: PlaybookContext) -> list[str]:
        """Return human-readable list of unmet trigger conditions.

        Default returns empty list; patterns can override to provide
        useful STAND_DOWN diagnostics.
        """
        return []
