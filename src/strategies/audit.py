"""Strategy catalog audit — what is in the catalog, and what it is waiting on.

Run it: ``make strategy-catalog-audit`` (or
``python -m src.strategies.audit [--json] [--family wall] [--stage research]``).

Four things it exists to answer:

1. **Is the catalog internally consistent?** Every binding resolves, every id
   is unique, no strategy claims a stage its own evidence does not support.
2. **What can each of the three surfaces actually see?** Bot bindings,
   pattern bindings, and whether a strategy is backtestable at all.
3. **What is eligible for promotion?** A strategy whose research log already
   clears the gate but which is not yet marked VALIDATED is money on the
   table — and a VALIDATED strategy with no bot cannot trade, which is a gap
   worth naming loudly.
4. **What is eligible for retirement?** Under
   ``policy.RETIREMENT_MIN_HISTORY_DAYS`` this is expected to be *nothing* for
   a long time; the report shows how far short each strategy is so "not
   retirable" carries a sense of scale rather than reading as a rubber stamp.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Iterable, List, Optional

from src.strategies import (
    FAMILY_LABELS,
    RETIREMENT_MIN_HISTORY_DAYS,
    Family,
    Stage,
    StrategyEntry,
    all_strategies,
    can_promote,
    can_retire,
    retirement_shortfall,
    validate_stage,
)

_STAGE_MARK = {
    Stage.VALIDATED: "OK ",
    Stage.CANDIDATE: "~~ ",
    Stage.RESEARCH: "   ",
    Stage.SUPERSEDED: "-> ",
    Stage.RETIRED: "XX ",
}


def integrity_errors() -> List[str]:
    """Every way the catalog can be internally wrong, in one list.

    Also exercised by the catalog integrity test, so CI fails on a broken
    catalog rather than waiting for an operator to run the audit.
    """
    from src.tradeworkz.registry import NON_CATALOG_STRATEGY_CLASSES, STRATEGY_CLASSES

    errors: List[str] = []
    entries = all_strategies()

    # Stage vs evidence.
    errors.extend(e for e in (validate_stage(x) for x in entries) if e)

    # Bindings resolve.
    for entry in entries:
        if entry.bot_class is not None and entry.bot_class not in STRATEGY_CLASSES:
            errors.append(
                f"{entry.id}: bot_class {entry.bot_class!r} is not in " "registry.STRATEGY_CLASSES"
            )
    try:
        from src.signals.playbook.engine import PlaybookEngine

        known_patterns = {p.id for p in PlaybookEngine._discover_builtin_patterns()}
    except Exception as exc:  # pragma: no cover - discovery is best-effort here
        errors.append(f"pattern discovery failed: {exc}")
        known_patterns = set()
    if known_patterns:
        for entry in entries:
            if entry.pattern_id and entry.pattern_id not in known_patterns:
                errors.append(
                    f"{entry.id}: pattern_id {entry.pattern_id!r} matches no " "playbook pattern"
                )
        claimed = {e.pattern_id for e in entries if e.pattern_id}
        for orphan in sorted(known_patterns - claimed):
            errors.append(f"playbook pattern {orphan!r} is not in the catalog")

    # Every bot class is claimed exactly once (infrastructure classes exempt).
    claimed_bots: Dict[str, str] = {}
    for entry in entries:
        if entry.bot_class is None:
            continue
        prior = claimed_bots.get(entry.bot_class)
        if prior is not None:
            errors.append(
                f"bot_class {entry.bot_class!r} claimed by both {prior!r} and {entry.id!r}"
            )
        claimed_bots[entry.bot_class] = entry.id
    for name in STRATEGY_CLASSES:
        if name in NON_CATALOG_STRATEGY_CLASSES:
            continue
        if name not in claimed_bots:
            errors.append(f"bot class {name!r} is registered but not in the catalog")

    # Lineage points at real entries.
    ids = {e.id for e in entries}
    for entry in entries:
        for sid in entry.supersedes:
            if sid not in ids:
                errors.append(f"{entry.id}: supersedes unknown strategy {sid!r}")
        if entry.superseded_by and entry.superseded_by not in ids:
            errors.append(f"{entry.id}: superseded_by unknown strategy {entry.superseded_by!r}")
        if entry.superseded_by == entry.id:
            errors.append(f"{entry.id}: superseded_by itself")

    # Prose the UI will render.
    for entry in entries:
        if not entry.name or not entry.tagline or len(entry.thesis) < 40:
            errors.append(f"{entry.id}: missing name / tagline / thesis prose")
    return errors


def gaps() -> Dict[str, List[str]]:
    """Actionable holes in the catalog, each as a list of strategy ids."""
    entries = all_strategies()
    return {
        # Evidence clears the promotion gate but the stage does not say so.
        "promotable_not_marked": [
            e.id
            for e in entries
            if e.stage in (Stage.RESEARCH, Stage.CANDIDATE) and can_promote(e).allowed
        ],
        # Proven thesis, but no bot exists to trade it.
        "validated_without_bot": [
            e.id for e in entries if e.stage is Stage.VALIDATED and e.bot_class is None
        ],
        # Proven thesis, bot written — waiting on the bot's own screen before
        # it can take capital. This is the actionable queue for
        # `make tradeworkz-backtest --bots <id>`.
        "awaiting_bot_screen": [
            e.id
            for e in entries
            if e.stage is Stage.VALIDATED and e.bot_class is not None and not e.bot_validated
        ],
        # No engine at all — invisible to Backtesting and Insights.
        "no_engine": [e.id for e in entries if not e.engines],
        # Never screened. The queue the research loop should work through.
        "never_screened": [e.id for e in entries if not e.research],
        # Only ever screened on a sample too thin to conclude anything.
        "inconclusive_only": [
            e.id
            for e in entries
            if e.research and not e.has_edge_evidence and not e.conclusive_tuning_generations
        ],
        # Meets the retirement bar. Expected to be empty for years.
        "retirement_eligible": [e.id for e in entries if can_retire(e).allowed],
    }


def _rows(entries: Iterable[StrategyEntry]) -> List[Dict[str, Any]]:
    out = []
    for e in entries:
        retire = can_retire(e)
        latest = e.latest_run
        out.append(
            {
                "id": e.id,
                "name": e.name,
                "family": e.family.value,
                "tier": e.tier,
                "stage": e.stage.value,
                "engines": [x.value for x in e.engines],
                "bot_id": e.bot_id,
                "pattern_id": e.pattern_id,
                "provisionable": e.is_provisionable,
                "backtestable": e.backtestable,
                "runs": len(e.research),
                "deepest_window_days": e.deepest_window_days,
                "screened_trades": e.total_screened_trades,
                "conclusive_generations": len(e.conclusive_tuning_generations),
                "has_edge": e.has_edge_evidence,
                "latest_verdict": latest.verdict.value if latest else None,
                "latest_pf": latest.profit_factor if latest else None,
                "retirement_eligible": retire.allowed,
                "retirement_blockers": list(retire.blockers),
                "history_progress": round(retirement_shortfall(e), 4),
            }
        )
    return out


def _fmt_pf(v: Optional[float]) -> str:
    return f"{v:.2f}" if isinstance(v, (int, float)) else "  — "


def render(entries: List[StrategyEntry]) -> str:
    lines: List[str] = []
    lines.append("")
    lines.append("ZeroGEX strategy catalog")
    lines.append("=" * 108)
    lines.append(
        f"{'':3}{'strategy':<32} {'tier':<5} {'stage':<11} "
        f"{'engines':<14} {'runs':>4} {'deep':>5} {'trades':>7} {'PF':>6}  verdict"
    )
    lines.append("-" * 108)

    by_family: Dict[Family, List[StrategyEntry]] = {}
    for e in entries:
        by_family.setdefault(e.family, []).append(e)

    for family in Family:
        group = by_family.get(family)
        if not group:
            continue
        lines.append("")
        lines.append(f"  {FAMILY_LABELS[family].upper()}")
        for e in sorted(group, key=lambda x: x.name):
            latest = e.latest_run
            engines = "+".join(x.value for x in e.engines) or "none"
            pf = _fmt_pf(latest.profit_factor if latest else None)
            verdict = latest.verdict.value if latest else "never screened"
            lines.append(
                f"{_STAGE_MARK[e.stage]}{e.id:<32} {e.tier:<5} {e.stage.value:<11} "
                f"{engines:<14} {len(e.research):>4} {e.deepest_window_days:>5} "
                f"{e.total_screened_trades:>7} {pf:>6}  {verdict}"
            )

    g = gaps()
    lines.append("")
    lines.append("-" * 108)
    lines.append("Coverage")
    total = len(entries)
    lines.append(f"  strategies                 {total}")
    n_backtestable = sum(1 for e in entries if e.backtestable)
    lines.append(f"  backtestable               {n_backtestable}/{total}")
    lines.append(f"  bot binding                {sum(1 for e in entries if e.bot_class)}")
    lines.append(f"  pattern binding            {sum(1 for e in entries if e.has_pattern)}")
    lines.append(f"  live-eligible (funded)     {sum(1 for e in entries if e.is_provisionable)}")
    lines.append(f"  bot screened for edge      {sum(1 for e in entries if e.bot_validated)}")

    lines.append("")
    lines.append("Gaps")
    labels = {
        "promotable_not_marked": "evidence clears the gate, stage not updated",
        "validated_without_bot": "VALIDATED but no bot can trade it",
        "awaiting_bot_screen": "VALIDATED, bot written, awaiting its own screen",
        "no_engine": "no engine implements it",
        "never_screened": "never screened",
        "inconclusive_only": "only inconclusive screens so far",
        "retirement_eligible": "meets the retirement bar",
    }
    for key, label in labels.items():
        ids = g[key]
        lines.append(f"  {label:<44} {len(ids):>3}  {', '.join(ids) if ids else '—'}")

    lines.append("")
    lines.append("Retirement policy")
    lines.append(
        f"  Requires {RETIREMENT_MIN_HISTORY_DAYS}d "
        f"({RETIREMENT_MIN_HISTORY_DAYS / 365:.0f}y) of history behind the deepest screen, "
        "3 conclusive"
    )
    lines.append(
        "  tuning generations, 200+ screened trades, and no screen that ever found an edge."
    )
    deepest = max((e.deepest_window_days for e in entries), default=0)
    lines.append(
        f"  Deepest screen anywhere in the catalog: {deepest}d "
        f"({deepest / RETIREMENT_MIN_HISTORY_DAYS * 100:.1f}% of the bar)."
    )
    if not g["retirement_eligible"]:
        lines.append("  NOTHING is retirement-eligible. Reaching the bar needs the deep-history")
        lines.append("  backfill costed in docs/design/historical-options-data-vendors.md —")
        lines.append(
            "  option_chains is pruned at DATA_RETENTION_DAYS and the archive starts 2026."
        )

    errors = integrity_errors()
    lines.append("")
    if errors:
        lines.append(f"INTEGRITY: {len(errors)} problem(s)")
        for err in errors:
            lines.append(f"  ! {err}")
    else:
        lines.append("INTEGRITY: clean — every binding resolves, every stage is supported.")
    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Audit the ZeroGEX strategy catalog.")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("--family", help="restrict to one family (e.g. wall, order_flow)")
    parser.add_argument("--stage", help="restrict to one stage (e.g. research, candidate)")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when the catalog has integrity problems (for CI)",
    )
    args = parser.parse_args(argv)

    entries = list(all_strategies())
    if args.family:
        entries = [e for e in entries if e.family.value == args.family.strip().lower()]
    if args.stage:
        entries = [e for e in entries if e.stage.value == args.stage.strip().lower()]
    if not entries:
        print("no strategies match that filter", file=sys.stderr)
        return 2

    errors = integrity_errors()
    if args.json:
        print(
            json.dumps(
                {
                    "strategies": _rows(entries),
                    "gaps": gaps(),
                    "integrity_errors": errors,
                    "policy": {
                        "retirement_min_history_days": RETIREMENT_MIN_HISTORY_DAYS,
                    },
                },
                indent=2,
            )
        )
    else:
        print(render(entries))
    return 1 if (args.strict and errors) else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
