"""PlaybookEngine: orchestrate pattern matching → single Action Card.

Responsibilities (per ``docs/playbook_catalog.md`` §4):

  1. Discover registered patterns (built-in + custom dir).
  2. Call ``match()`` on each pattern with the PlaybookContext, but only in
     the regular session; outside it, emit a STAND_DOWN without running any.
  3. Apply gates: regime, one Card per idea, hysteresis, and the entry bar
     each pattern has earned on the symbol (``adaptive_gate``).
  4. Resolve conflicts: highest confidence wins, tier-priority tiebreak.
  5. Surface losing candidates as ``alternatives_considered``.
  6. Emit a structured STAND_DOWN Card when nothing survives.

The engine itself is sync — pattern evaluation is CPU-only.  Building the
PlaybookContext (which involves DB fetches) happens upstream in async
code.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import sys
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Optional

from src import config
from src.market_calendar import in_regular_session
from src.signals.playbook import adaptive_gate
from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import OpenPosition, PlaybookContext
from src.signals.playbook.types import (
    ActionCard,
    ActionEnum,
    Alternative,
    NearMiss,
    TIER_AFTER_CLOSE,
    TIER_END_OF_DAY,
    TIER_INTRADAY,
)

logger = logging.getLogger(__name__)


# The flat floor from spec §4.5. It is now the neutral bar the adaptive gate
# uses for a pattern with no graded record yet (PLAYBOOK_ADAPTIVE_NEUTRAL_BAR).
CONFIDENCE_FLOOR = 0.25
DEFAULT_CUSTOM_DIR = "~/.zerogex/playbook/custom"

_MARKET_CLOSED = (
    "Market closed: Cards are issued only in the regular session, 09:30 ET to the close."
)


class PlaybookEngine:
    """Discover patterns, run them, resolve to one ActionCard."""

    def __init__(self, patterns: Optional[list[PatternBase]] = None):
        if patterns is None:
            patterns = self._discover_builtin_patterns()
            patterns.extend(self._discover_custom_patterns())
        # De-duplicate by id; later registrations win (custom overrides builtin).
        seen: dict[str, PatternBase] = {}
        for p in patterns:
            if not p.id:
                logger.warning("Skipping pattern with empty id: %r", p)
                continue
            seen[p.id] = p
        self.patterns: list[PatternBase] = list(seen.values())
        logger.info(
            "PlaybookEngine loaded %d patterns: %s",
            len(self.patterns),
            [p.id for p in self.patterns],
        )

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    @staticmethod
    def _discover_builtin_patterns() -> list[PatternBase]:
        """Walk src/signals/playbook/patterns/ and load all PATTERN exports."""
        from src.signals.playbook import patterns as patterns_pkg

        out: list[PatternBase] = []
        pkg_path = Path(patterns_pkg.__file__).parent
        for py_file in sorted(pkg_path.glob("*.py")):
            if py_file.name.startswith("_"):
                continue
            mod_name = f"src.signals.playbook.patterns.{py_file.stem}"
            try:
                mod = importlib.import_module(mod_name)
            except Exception as exc:
                logger.exception("Failed to import builtin pattern %s: %s", mod_name, exc)
                continue
            pattern = getattr(mod, "PATTERN", None)
            if isinstance(pattern, PatternBase):
                out.append(pattern)
            else:
                logger.warning("Builtin pattern module %s has no PATTERN export", mod_name)
        return out

    @staticmethod
    def _discover_custom_patterns() -> list[PatternBase]:
        """Auto-load patterns from SIGNALS_PLAYBOOK_CUSTOM_DIR."""
        custom_dir = os.path.expanduser(
            os.getenv("SIGNALS_PLAYBOOK_CUSTOM_DIR", DEFAULT_CUSTOM_DIR)
        )
        path = Path(custom_dir)
        if not path.is_dir():
            return []
        out: list[PatternBase] = []
        for py_file in sorted(path.glob("*.py")):
            if py_file.name.startswith("_"):
                continue
            try:
                spec = importlib.util.spec_from_file_location(
                    f"playbook_custom_{py_file.stem}", py_file
                )
                if spec is None or spec.loader is None:
                    continue
                mod = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = mod
                spec.loader.exec_module(mod)
            except Exception as exc:
                logger.exception("Failed to import custom pattern %s: %s", py_file, exc)
                continue
            pattern = getattr(mod, "PATTERN", None)
            if isinstance(pattern, PatternBase):
                out.append(pattern)
            else:
                logger.warning("Custom pattern %s has no PATTERN export", py_file)
        return out

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(self, ctx: PlaybookContext, *, now: Optional[datetime] = None) -> ActionCard:
        """Run patterns through all gates and return one ActionCard."""
        card, _held_back = self.evaluate_with_held_back(ctx, now=now)
        return card

    def evaluate_with_held_back(
        self, ctx: PlaybookContext, *, now: Optional[datetime] = None
    ) -> tuple[ActionCard, list[tuple[ActionCard, str]]]:
        """``evaluate`` plus the ideas the entry bar held back.

        A held-back idea passed every other gate and would have been published
        under the old flat floor; only its pattern's record on this symbol
        kept it back. The cycle records these so the grader can grade them,
        which is how a paused pattern earns its way back.

        ``now`` is the wall clock. The live callers (the signal cycle and
        ``/api/signals/action``) pass it; replays and tests that evaluate a
        fixed moment leave it out and skip the clock check.
        """
        # Step 0: session gate. A Card is an instruction to trade options at
        # the prices printed on it, and those options trade only in the
        # regular session. The signal cycle runs 24x5, so without this gate
        # patterns fired on pre-market and after-hours prints: Card #11270
        # (QQQ, 2026-09-23) was a 0DTE put issued off the 04:00 ET print whose
        # 120-minute hold ended before the options opened. Returning before
        # any pattern runs also keeps such a cycle out of the table, since a
        # STAND_DOWN is never persisted.
        if not ctx.is_regular_session:
            return (
                self._outside_session(
                    ctx,
                    _MARKET_CLOSED,
                    session="closed",
                ),
                [],
            )
        # The bar says when the data is from; the clock says when the Card would
        # go out. After the close a cash index's newest bar stays at 15:59, and a
        # stalled feed freezes any symbol's, so the bar alone kept the session
        # "open": an NDX Card stamped 15:59 on 2026-09-24 went out after the close.
        if now is not None:
            clock_card = self._clock_gate(ctx, now)
            if clock_card is not None:
                return clock_card, []
        # A cash index's 09:30 bar is its stale opening print, near the prior
        # close, not a level anyone traded. NDX and SPX Cards fired off it at the
        # open quoted yesterday's price.
        if ctx.is_index_opening_bar:
            return (
                self._outside_session(
                    ctx,
                    "Index opening print: a cash index's 09:30 prints are stale until "
                    "its stocks open, so Cards start at 09:31 ET.",
                    session="index_open",
                ),
                [],
            )

        # Step 1: collect raw candidates.
        candidates: list[tuple[PatternBase, ActionCard]] = []
        miss_diagnostics: list[NearMiss] = []
        for pattern in self.patterns:
            try:
                card = pattern.match(ctx)
            except Exception as exc:
                logger.exception("Pattern %s.match raised: %s", pattern.id, exc)
                continue
            if card is None:
                missing = pattern.explain_miss(ctx)
                if missing:
                    miss_diagnostics.append(NearMiss(pattern=pattern.id, missing=missing))
                continue
            candidates.append((pattern, card))

        # Step 2: regime gate.
        regime = ctx.msi_regime
        if regime:
            after_regime: list[tuple[PatternBase, ActionCard]] = []
            for pattern, card in candidates:
                if not pattern.valid_regimes or regime in pattern.valid_regimes:
                    after_regime.append((pattern, card))
                else:
                    miss_diagnostics.append(
                        NearMiss(
                            pattern=pattern.id,
                            missing=[
                                f"current regime '{regime}' not in valid_regimes "
                                f"{list(pattern.valid_regimes)}"
                            ],
                        )
                    )
            candidates = after_regime

        # Step 3: position-state gate, which is also "one Card per idea".
        # ``open_positions`` holds each pattern's latest idea on this symbol
        # (see ideas.py), so a pattern whose last Card is still inside its
        # hold window does not issue another one.
        management_actions = {ActionEnum.TAKE_PROFIT, ActionEnum.TIGHTEN_STOP, ActionEnum.CLOSE}
        after_position: list[tuple[PatternBase, ActionCard]] = []
        for pattern, card in candidates:
            if card.action in management_actions:
                if ctx.open_position_for(pattern.id):
                    after_position.append((pattern, card))
                else:
                    miss_diagnostics.append(
                        NearMiss(
                            pattern=pattern.id,
                            missing=["management card requires an open position from this pattern"],
                        )
                    )
            else:
                existing = ctx.open_position_for(pattern.id)
                blocked = self._idea_still_live(ctx, existing, card)
                if blocked:
                    miss_diagnostics.append(NearMiss(pattern=pattern.id, missing=[blocked]))
                    continue
                after_position.append((pattern, card))
        candidates = after_position

        # Step 4: hysteresis. Ahead of the entry bar so an idea the bar holds
        # back is always a fresh one, never a re-trigger inside the dwell.
        candidates = self._apply_hysteresis(ctx, candidates, miss_diagnostics)

        # Step 5: the entry bar each pattern has earned on this symbol. A
        # pattern with no graded record gets the old flat floor; one that has
        # been losing here needs far more confidence or is paused; one that
        # has been winning is let through at lower confidence.
        held_back: list[tuple[ActionCard, str]] = []
        after_bar: list[tuple[PatternBase, ActionCard]] = []
        for pattern, card in candidates:
            verdict = adaptive_gate.assess(pattern.id, ctx.underlying, card.direction)
            if card.confidence >= verdict.bar:
                card.context = {**(card.context or {}), "track_record": verdict.summary()}
                after_bar.append((pattern, card))
                continue
            reason = verdict.miss_reason(card.confidence)
            miss_diagnostics.append(NearMiss(pattern=pattern.id, missing=[reason]))
            # Record it for grading only if the record, not the flat floor,
            # is what kept it back.
            if verdict.is_adaptive and card.confidence >= config.PLAYBOOK_ADAPTIVE_NEUTRAL_BAR:
                card.context = {**(card.context or {}), "track_record": verdict.summary()}
                held_back.append((card, reason))
        candidates = after_bar

        # Step 6: resolve.
        if not candidates:
            return self._stand_down(ctx, miss_diagnostics), held_back

        winner_pattern, winner_card = self._resolve_conflict(ctx, candidates)
        winner_card.alternatives_considered = [
            Alternative(
                pattern=p.id,
                reason=f"rejected: lower confidence ({c.confidence:.2f})",
            )
            for (p, c) in candidates
            if p.id != winner_pattern.id
        ]
        return winner_card, held_back

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _idea_still_live(
        ctx: PlaybookContext, existing: Optional[OpenPosition], card: ActionCard
    ) -> Optional[str]:
        """Why ``card`` repeats an idea that is still live, or None if it doesn't.

        The pattern's last idea holds the slot for its whole hold window: a
        second Card inside it would be the same trade, later and at a worse
        price. The one exception is a stopped-out idea, which frees the slot
        for a Card the other way (the move it bet on failed; a reversal is a
        new idea). A target hit does not free it: re-entering after the move
        played out is the chase this gate exists to stop.
        """
        if existing is None or existing.opened_at is None:
            return None
        hold = existing.max_hold_minutes or card.max_hold_minutes
        if not hold:
            return None
        age = (ctx.timestamp - existing.opened_at).total_seconds() / 60.0
        if age >= float(hold):
            return None
        if existing.status == "stop_hit" and existing.direction != card.direction:
            return None
        side = f"{existing.direction} " if existing.direction in ("bullish", "bearish") else ""
        if existing.status == "target_hit":
            state = "already reached its target"
        elif existing.status == "stop_hit":
            state = "was stopped out; no re-entry the same way"
        else:
            state = "is still live"
        return (
            f"one Card per idea: its {side}Card from {age:.0f}m ago {state} "
            f"({hold}m hold window)"
        )

    def _apply_hysteresis(
        self,
        ctx: PlaybookContext,
        candidates: list[tuple[PatternBase, ActionCard]],
        miss_diagnostics: list[NearMiss],
    ) -> list[tuple[PatternBase, ActionCard]]:
        out: list[tuple[PatternBase, ActionCard]] = []
        for pattern, card in candidates:
            last_emit = ctx.recently_emitted.get(pattern.id)
            if last_emit is None:
                out.append((pattern, card))
                continue
            dwell = pattern.dwell_minutes()
            elapsed = (ctx.timestamp - last_emit).total_seconds() / 60.0
            if elapsed < dwell:
                miss_diagnostics.append(
                    NearMiss(
                        pattern=pattern.id,
                        missing=[f"hysteresis: emitted {elapsed:.0f}m ago " f"(dwell {dwell}m)"],
                    )
                )
                continue
            out.append((pattern, card))
        return out

    def _resolve_conflict(
        self,
        ctx: PlaybookContext,
        candidates: list[tuple[PatternBase, ActionCard]],
    ) -> tuple[PatternBase, ActionCard]:
        """Highest confidence; ties broken by tier priority then alpha id."""
        tier_priority = self._current_tier_priority(ctx)

        def sort_key(item: tuple[PatternBase, ActionCard]):
            pattern, card = item
            tier_rank = tier_priority.index(pattern.tier) if pattern.tier in tier_priority else 99
            return (-card.confidence, tier_rank, pattern.id)

        candidates_sorted = sorted(candidates, key=sort_key)
        return candidates_sorted[0]

    @staticmethod
    def _current_tier_priority(ctx: PlaybookContext) -> tuple[str, ...]:
        et = ctx.et_time
        if et >= time(15, 55):
            return TIER_AFTER_CLOSE
        if et >= time(15, 30):
            return TIER_END_OF_DAY
        return TIER_INTRADAY

    # Hint terms that mark a NearMiss as "almost matched" — patterns that
    # reached a downstream gate (regime, position, hysteresis) rather than
    # failing in the initial trigger check.  We surface these first because
    # they're more informative for an operator reading STAND_DOWN.
    _GATE_MISS_HINTS = (
        "hysteresis",
        "valid_regimes",
        "open position",
        "hold window",
        "regime",
        "track record",
        "paused",
    )

    def _clock_gate(self, ctx: PlaybookContext, now: datetime) -> Optional[ActionCard]:
        """STAND_DOWN when the wall clock is outside the session or the newest
        bar is too old to quote; None when a Card may go out."""
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        if not in_regular_session(now):
            return self._outside_session(
                ctx,
                _MARKET_CLOSED,
                session="closed",
            )
        bar_ts = ctx.timestamp
        if bar_ts.tzinfo is None:
            bar_ts = bar_ts.replace(tzinfo=timezone.utc)
        age = (now - bar_ts).total_seconds()
        if age > config.PLAYBOOK_MAX_BAR_AGE_SECONDS:
            return self._outside_session(
                ctx,
                f"Stale data: the newest bar is {age / 60:.0f} min old, "
                "so no Card is issued off it.",
                session="stale",
            )
        return None

    def _outside_session(self, ctx: PlaybookContext, rationale: str, *, session: str) -> ActionCard:
        """STAND_DOWN for a cycle no Card may be issued in.

        No near-misses: no pattern ran, so none of them came close.
        """
        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=ActionEnum.STAND_DOWN,
            pattern="stand_down",
            tier="n/a",
            direction="non_directional",
            confidence=0.0,
            rationale=rationale,
            near_misses=[],
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
                "session": session,
            },
        )

    def _stand_down(self, ctx: PlaybookContext, miss_diagnostics: list[NearMiss]) -> ActionCard:
        # Sort gate-blocked misses (almost-matched) ahead of trigger-failure
        # misses; cap at 10 to keep the payload bounded.
        def _is_gate_miss(nm: NearMiss) -> bool:
            return any(hint in m for m in nm.missing for hint in self._GATE_MISS_HINTS)

        ordered = sorted(miss_diagnostics, key=lambda nm: (0 if _is_gate_miss(nm) else 1))
        capped = ordered[:10]
        # A pattern whose own Card is still live did find structure; saying
        # "no tradable structure" would contradict the Card it just issued.
        live = [nm.pattern for nm in capped if any("is still live" in m for m in nm.missing)]
        if not capped:
            rationale = "No tradable structure: no patterns produced a candidate this cycle."
        elif live:
            rationale = f"No new Card: the Card already issued by {', '.join(live)} is still live."
        else:
            patterns_named = ", ".join(m.pattern for m in capped)
            rationale = f"No tradable structure. Closest patterns: {patterns_named}."
        return ActionCard(
            underlying=ctx.underlying,
            timestamp=ctx.timestamp,
            action=ActionEnum.STAND_DOWN,
            pattern="stand_down",
            tier="n/a",
            direction="non_directional",
            confidence=0.0,
            rationale=rationale,
            near_misses=capped,
            context={
                "msi": ctx.msi_score,
                "regime": ctx.msi_regime,
            },
        )
