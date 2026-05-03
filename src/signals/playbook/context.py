"""PlaybookContext: enriched MarketContext for pattern evaluation.

Extends the existing ``MarketContext`` with the snapshots and live levels
patterns need to make decisions:

  * MSI snapshot (composite, regime, components)
  * Latest advanced + basic signal snapshots
  * Live structural levels (call_wall, put_wall, etc.)
  * Time-of-day flags
  * Open positions (for management cards)

See ``docs/playbook_catalog.md`` §3.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Any, Optional

import pytz

from src.signals.components.base import MarketContext

_ET = pytz.timezone("America/New_York")


@dataclass
class SignalSnapshot:
    """Compact view of one advanced or basic signal at the current cycle."""

    name: str
    score: float  # in [-100, +100]
    clamped_score: float  # in [-1, +1]
    triggered: bool = False
    signal: Optional[str] = None  # signal-specific label, e.g. "bearish_fade"
    direction: Optional[str] = None  # bullish | bearish | neutral
    context_values: dict[str, Any] = field(default_factory=dict)
    # Trailing time-series, oldest → newest, populated by context_builder for
    # signals that patterns query for multi-day aggregations (squeeze_setup,
    # vanna_charm_flow, skew_delta).  Empty for signals not in the
    # _HISTORY_LOAD_FOR set.
    score_history: list[tuple[datetime, float]] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Aggregation helpers used by patterns
    # ------------------------------------------------------------------

    def history_by_day(self) -> dict[str, list[float]]:
        """Group score_history into per-ET-date buckets of clamped scores."""
        out: dict[str, list[float]] = {}
        for ts, score in self.score_history:
            if ts.tzinfo is None:
                ts = pytz.UTC.localize(ts)
            day_key = ts.astimezone(_ET).date().isoformat()
            out.setdefault(day_key, []).append(float(score))
        return out

    def daily_max_abs(self) -> list[tuple[str, float]]:
        """Per-day max(|clamped_score|), oldest → newest."""
        buckets = self.history_by_day()
        return [(day, max(abs(s) for s in scores)) for day, scores in sorted(buckets.items())]

    def daily_signed_max(self) -> list[tuple[str, float]]:
        """Per-day signed score with the largest absolute value, oldest → newest.

        Useful for "sign-sustained" checks where direction matters.
        """
        buckets = self.history_by_day()
        out: list[tuple[str, float]] = []
        for day, scores in sorted(buckets.items()):
            extreme = max(scores, key=abs)
            out.append((day, float(extreme)))
        return out


@dataclass
class OpenPosition:
    """Compact view of an open position for management Card matching."""

    pattern_id: str  # which pattern opened it (if known)
    direction: str  # bullish | bearish | non_directional
    instrument: str  # ActionEnum value used to open
    legs: list[dict[str, Any]] = field(default_factory=list)
    opened_at: Optional[datetime] = None
    entry_premium: Optional[float] = None
    underlying: str = ""


@dataclass
class PlaybookContext:
    """Everything a pattern needs to make a decision.

    Wraps the existing ``MarketContext`` plus playbook-specific fields.
    Pattern code reads ``ctx.market`` for raw market data and ``ctx.<field>``
    for the enriched snapshots.
    """

    market: MarketContext
    msi_score: Optional[float] = None  # composite_score 0-100
    msi_regime: Optional[str] = (
        None  # trend_expansion | controlled_trend | chop_range | high_risk_reversal
    )
    msi_components: dict[str, Any] = field(default_factory=dict)
    advanced_signals: dict[str, SignalSnapshot] = field(default_factory=dict)
    basic_signals: dict[str, SignalSnapshot] = field(default_factory=dict)
    levels: dict[str, Optional[float]] = field(default_factory=dict)
    open_positions: list[OpenPosition] = field(default_factory=list)
    recently_emitted: dict[str, datetime] = field(
        default_factory=dict
    )  # pattern_id -> last emit time

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def underlying(self) -> str:
        return self.market.underlying

    @property
    def timestamp(self) -> datetime:
        return self.market.timestamp

    @property
    def close(self) -> float:
        return self.market.close

    @property
    def net_gex(self) -> float:
        return self.market.net_gex

    def advanced(self, name: str) -> Optional[SignalSnapshot]:
        return self.advanced_signals.get(name)

    def basic(self, name: str) -> Optional[SignalSnapshot]:
        return self.basic_signals.get(name)

    def signal(self, name: str) -> Optional[SignalSnapshot]:
        """Lookup across both advanced and basic signal namespaces."""
        return self.advanced_signals.get(name) or self.basic_signals.get(name)

    def level(self, name: str) -> Optional[float]:
        return self.levels.get(name)

    # ------------------------------------------------------------------
    # Time-of-day helpers (Eastern Time, regular session 09:30-16:00)
    # ------------------------------------------------------------------

    def _et_dt(self) -> datetime:
        ts = self.timestamp
        if ts.tzinfo is None:
            ts = pytz.UTC.localize(ts)
        return ts.astimezone(_ET)

    @property
    def et_time(self) -> time:
        return self._et_dt().time()

    @property
    def et_date(self):
        """Date in Eastern Time (used for 0DTE expiry resolution)."""
        return self._et_dt().date()

    @property
    def day_of_week(self) -> str:
        return self._et_dt().strftime("%a")  # "Mon", "Tue", ...

    @property
    def is_first_30min(self) -> bool:
        t = self.et_time
        return time(9, 30) <= t < time(10, 0)

    @property
    def is_last_30min(self) -> bool:
        t = self.et_time
        return time(15, 30) <= t < time(16, 0)

    @property
    def minutes_to_close(self) -> int:
        """Minutes from now until 16:00 ET. Negative if after the bell."""
        t = self.et_time
        close = time(16, 0)
        delta = (close.hour * 60 + close.minute) - (t.hour * 60 + t.minute)
        return delta

    def open_position_for(self, pattern_id: str) -> Optional[OpenPosition]:
        for pos in self.open_positions:
            if pos.pattern_id == pattern_id:
                return pos
        return None
