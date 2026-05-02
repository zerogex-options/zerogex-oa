"""PlaybookEngine: orchestrate pattern matching → single Action Card.

Responsibilities (per ``docs/playbook_catalog.md`` §4):

  1. Discover registered patterns (built-in + custom dir).
  2. Call ``match()`` on each pattern with the PlaybookContext.
  3. Apply gates: regime, position-state, confidence floor, hysteresis.
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
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Optional

from src.signals.playbook.base import PatternBase
from src.signals.playbook.context import PlaybookContext
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


CONFIDENCE_FLOOR = 0.25  # Cards below this are dropped (see spec §4.5).
DEFAULT_CUSTOM_DIR = "~/.zerogex/playbook/custom"


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

    def evaluate(self, ctx: PlaybookContext) -> ActionCard:
        """Run patterns through all gates and return one ActionCard."""
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

        # Step 3: position-state gate.
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
                # Entry card: drop if same pattern already has an open position
                # within its max_hold_minutes window.
                existing = ctx.open_position_for(pattern.id)
                if existing and existing.opened_at and card.max_hold_minutes:
                    age = (ctx.timestamp - existing.opened_at).total_seconds() / 60.0
                    if age < float(card.max_hold_minutes):
                        miss_diagnostics.append(
                            NearMiss(
                                pattern=pattern.id,
                                missing=[
                                    f"entry suppressed: pattern already open "
                                    f"({age:.0f}m of {card.max_hold_minutes}m hold window)"
                                ],
                            )
                        )
                        continue
                after_position.append((pattern, card))
        candidates = after_position

        # Step 4: confidence floor.
        candidates = [(p, c) for (p, c) in candidates if c.confidence >= CONFIDENCE_FLOOR]

        # Step 5: hysteresis.
        candidates = self._apply_hysteresis(ctx, candidates, miss_diagnostics)

        # Step 6: resolve.
        if not candidates:
            return self._stand_down(ctx, miss_diagnostics)

        winner_pattern, winner_card = self._resolve_conflict(ctx, candidates)
        winner_card.alternatives_considered = [
            Alternative(
                pattern=p.id,
                reason=f"rejected: lower confidence ({c.confidence:.2f})",
            )
            for (p, c) in candidates
            if p.id != winner_pattern.id
        ]
        return winner_card

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
    )

    def _stand_down(self, ctx: PlaybookContext, miss_diagnostics: list[NearMiss]) -> ActionCard:
        # Sort gate-blocked misses (almost-matched) ahead of trigger-failure
        # misses; cap at 10 to keep the payload bounded.
        def _is_gate_miss(nm: NearMiss) -> bool:
            return any(hint in m for m in nm.missing for hint in self._GATE_MISS_HINTS)

        ordered = sorted(miss_diagnostics, key=lambda nm: (0 if _is_gate_miss(nm) else 1))
        capped = ordered[:10]
        if not capped:
            rationale = "No tradable structure: no patterns produced a candidate this cycle."
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
