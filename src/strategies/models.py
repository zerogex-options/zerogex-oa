"""Domain types for the consolidated strategy catalog.

The catalog is the single source of truth for *what strategies exist*. These
types carry a strategy's identity, the market thesis behind it, which engines
can execute it, its default tuning, and the accumulated research evidence that
decides its stage.

Deliberately free of DB and engine imports: the catalog is plain data that the
TradeWorkz fleet, the backtester, and Pattern Insights all read. Measured
numbers live in ``playbook_pattern_stats``; what lives here is the *authored*
record — the screens we ran, with what window, and what we concluded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class Stage(str, Enum):
    """Where a strategy stands in the research pipeline.

    This describes *evidence*, not deployment. A strategy is provisionable into
    the live fleet only when it is ``VALIDATED`` **and** carries a bot binding;
    see :meth:`StrategyEntry.is_provisionable`. Keeping the two apart is what
    lets a strategy be actively refined (and backtested, and measured in
    Pattern Insights) without any paper capital riding on it.
    """

    #: In the catalog and under active refinement. No edge established yet.
    #: This is the default resting state — NOT a synonym for "failed".
    RESEARCH = "research"
    #: A screen shows promise but has not cleared the promotion gate.
    CANDIDATE = "candidate"
    #: Cleared the promotion gate (see policy.PROMOTION_*).
    VALIDATED = "validated"
    #: Replaced by a better implementation of the same thesis. Not a verdict
    #: on the thesis — the successor carries it forward.
    SUPERSEDED = "superseded"
    #: Exhausted under the retirement policy: deep history, repeated retuning,
    #: no edge found. The only terminal state, and the hardest to reach.
    RETIRED = "retired"


class Verdict(str, Enum):
    """What one screening run concluded."""

    #: Measured a real, repeatable edge.
    EDGE = "edge"
    #: Measured a decisive absence of edge on an adequate sample.
    NO_EDGE = "no_edge"
    #: Too few trades to conclude anything. NOT evidence of no edge.
    INSUFFICIENT = "insufficient"
    #: The entry gates were too tight for the available history to produce a
    #: sample — the mechanism was never actually tested. NOT evidence either.
    UNDERPOWERED = "underpowered"
    #: The run was invalidated by a harness or data bug found afterwards.
    INVALID = "invalid"


#: Verdicts that count as evidence *against* a strategy. An underpowered or
#: insufficient run says nothing about the thesis, so it never counts toward
#: retirement — this is the distinction that keeps a promising-but-thin
#: strategy (PF 2.28 on 5 trades) out of the retirement pile.
CONCLUSIVE_AGAINST: Tuple[Verdict, ...] = (Verdict.NO_EDGE,)


class Engine(str, Enum):
    """Which execution surface can run a strategy."""

    #: A TradeWorkz bot class — ticks live against a MarketSnapshot, and
    #: replays over history through the bot-replay backtest path.
    BOT = "bot"
    #: A playbook pattern — emits Action Cards live, which the backtester
    #: replays and the calibration feed measures.
    PATTERN = "pattern"


class Family(str, Enum):
    """Thesis grouping. Strategies in one family read the same market
    mechanism, so a family-wide failure is a real finding about the mechanism
    rather than about one implementation."""

    WALL = "wall"
    GAMMA_FLIP = "gamma_flip"
    PIN = "pin"
    FORCED_FLOW = "forced_flow"
    ORDER_FLOW = "order_flow"
    VOL_REGIME = "vol_regime"
    TREND = "trend"
    POSITIONING = "positioning"
    PROFILE = "profile"


FAMILY_LABELS: Dict[Family, str] = {
    Family.WALL: "Wall structure",
    Family.GAMMA_FLIP: "Gamma flip",
    Family.PIN: "Pin & max pain",
    Family.FORCED_FLOW: "Forced dealer flow",
    Family.ORDER_FLOW: "Aggressor order flow",
    Family.VOL_REGIME: "Volatility regime",
    Family.TREND: "Trend & momentum",
    Family.POSITIONING: "Positioning & skew",
    Family.PROFILE: "Gamma profile geometry",
}


@dataclass(frozen=True)
class ResearchRun:
    """One screening run against history — the catalog's unit of evidence.

    ``tuning_generation`` is how the catalog expresses "we tried tuning it":
    generation 0 is the strategy as first shipped, and each deliberate
    re-parameterisation bumps it. The retirement policy counts *distinct
    generations that concluded NO_EDGE*, so re-running the same parameters
    twenty times never accumulates toward retirement.
    """

    ran_on: date
    #: Calendar days of history the screen covered.
    window_days: int
    #: Round-trips the screen produced. Zero is meaningful (the gates never
    #: opened) and pairs with an UNDERPOWERED verdict.
    trades: int
    verdict: Verdict
    profit_factor: Optional[float] = None
    expectancy: Optional[float] = None
    win_rate: Optional[float] = None
    #: Which harness produced it, e.g. "tradeworkz-backtest",
    #: "playbook-calibration", "thesis-backtest".
    harness: str = "tradeworkz-backtest"
    tuning_generation: int = 0
    symbols: Tuple[str, ...] = ()
    notes: str = ""

    @property
    def is_conclusive_against(self) -> bool:
        return self.verdict in CONCLUSIVE_AGAINST


@dataclass(frozen=True)
class Retirement:
    """The audit record a retirement decision must carry.

    Constructing one does not retire anything — ``policy.can_retire`` still has
    to agree that the evidence clears the bar. This exists so a retirement is
    never a bare stage flip: it records what it rested on.
    """

    decided_on: date
    #: Depth of history the decision rests on, in calendar days.
    history_days: int
    #: Distinct tuning generations that concluded NO_EDGE.
    tuning_generations: int
    rationale: str


@dataclass(frozen=True)
class StrategyEntry:
    """One strategy in the catalog — the source of truth for all three surfaces.

    Identity is stable forever. ``id`` is the canonical catalog key; ``bot_id``
    and ``pattern_id`` are the *legacy* keys already written into
    ``tw_trades`` / ``tw_positions`` and ``signal_action_cards`` /
    ``playbook_pattern_stats`` respectively. Nothing is ever renamed — the
    catalog carries whatever ids history already wrote, and maps them onto one
    canonical entry.

    Where both engines exist, ``id`` follows the pattern id, because that is
    the id already customer-visible in Backtesting and Pattern Insights (two of
    the three surfaces).
    """

    id: str
    name: str
    family: Family
    tier: str  # "0DTE" | "1DTE" | "swing"
    direction_mode: str  # "bullish" | "bearish" | "context" | "neutral"
    tagline: str
    thesis: str
    stage: Stage

    # ── Engine bindings ────────────────────────────────────────────────
    #: TradeWorkz strategy class name (key into registry.STRATEGY_CLASSES).
    bot_class: Optional[str] = None
    #: Legacy ``tw_bots.id``. Defaults to ``id`` when a bot binding exists.
    bot_id_legacy: Optional[str] = None
    #: Playbook pattern id. Defaults to ``id`` when ``has_pattern`` is set.
    pattern_id_legacy: Optional[str] = None
    #: Whether a playbook pattern implements this strategy.
    has_pattern: bool = False

    # ── Tuning ─────────────────────────────────────────────────────────
    #: The general strategy's parameters — the catalog's shared defaults.
    params: Dict[str, Any] = field(default_factory=dict)
    #: Bot-specific overrides layered on top of ``params``. This is how a bot
    #: "pulls the general strategy and tunes it to its own spec" without
    #: forking the strategy.
    bot_params: Dict[str, Any] = field(default_factory=dict)

    # ── Lineage ────────────────────────────────────────────────────────
    #: Catalog ids this strategy replaces.
    supersedes: Tuple[str, ...] = ()
    #: Catalog id that replaced this one (set when stage is SUPERSEDED).
    superseded_by: Optional[str] = None

    # ── Evidence ───────────────────────────────────────────────────────
    research: Tuple[ResearchRun, ...] = ()
    retirement: Optional[Retirement] = None

    universe: str = "*"
    is_public: bool = True

    #: True when this strategy has ever been provisioned into ``tw_bots`` with
    #: a live sleeve, so a row (and possibly ``tw_trades`` history) exists for
    #: it in the database. Only these need force-disabling on provision when
    #: they are no longer live-eligible — a strategy that never shipped has no
    #: row to turn off. Purely a record of deployment history; it says nothing
    #: about the strategy's merit.
    provisioned_history: bool = False

    # ------------------------------------------------------------------
    # Identity resolution
    # ------------------------------------------------------------------

    @property
    def bot_id(self) -> Optional[str]:
        """Legacy ``tw_bots.id``, or None when no bot implements this."""
        if self.bot_class is None:
            return None
        return self.bot_id_legacy or self.id

    @property
    def pattern_id(self) -> Optional[str]:
        """Playbook pattern id, or None when no pattern implements this."""
        if not self.has_pattern:
            return None
        return self.pattern_id_legacy or self.id

    @property
    def engines(self) -> Tuple[Engine, ...]:
        out = []
        if self.bot_class is not None:
            out.append(Engine.BOT)
        if self.has_pattern:
            out.append(Engine.PATTERN)
        return tuple(out)

    @property
    def aliases(self) -> Tuple[str, ...]:
        """Every id this strategy is known by, canonical first.

        Used to fold historical rows keyed on a legacy id back onto the one
        catalog entry.
        """
        out = [self.id]
        for alt in (self.bot_id, self.pattern_id):
            if alt and alt not in out:
                out.append(alt)
        return tuple(out)

    # ------------------------------------------------------------------
    # Tuning
    # ------------------------------------------------------------------

    def effective_bot_params(self) -> Dict[str, Any]:
        """Catalog defaults with the bot's own overrides applied on top."""
        merged = dict(self.params)
        merged.update(self.bot_params)
        return merged

    # ------------------------------------------------------------------
    # Evidence accessors (the retirement policy reads these)
    # ------------------------------------------------------------------

    @property
    def backtestable(self) -> bool:
        """Whether any engine can generate entries for this over history."""
        return bool(self.engines)

    @property
    def is_provisionable(self) -> bool:
        """Whether this may be given a live capital sleeve.

        Requires validated evidence AND a bot to execute it. A validated
        pattern with no bot binding is a gap to close, not a live strategy.
        """
        return self.stage is Stage.VALIDATED and self.bot_class is not None

    @property
    def deepest_window_days(self) -> int:
        """Longest history window any screen has covered. 0 when never run."""
        return max((r.window_days for r in self.research), default=0)

    @property
    def total_screened_trades(self) -> int:
        return sum(r.trades for r in self.research)

    @property
    def conclusive_tuning_generations(self) -> Tuple[int, ...]:
        """Distinct tuning generations that concluded NO_EDGE, ascending."""
        return tuple(
            sorted({r.tuning_generation for r in self.research if r.is_conclusive_against})
        )

    @property
    def has_edge_evidence(self) -> bool:
        return any(r.verdict is Verdict.EDGE for r in self.research)

    @property
    def latest_run(self) -> Optional[ResearchRun]:
        if not self.research:
            return None
        return max(self.research, key=lambda r: r.ran_on)
