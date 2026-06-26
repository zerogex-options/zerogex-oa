"""Refresh playbook pattern calibration and report the resulting bases.

This is the write side of the empirical-base feedback loop and the job the
nightly ``zerogex-oa-pattern-calibration.timer`` runs. It:

  1. Runs the playbook backtest harness for each configured underlying, which
     measures per-pattern hit rate / ``proposed_base`` and persists it to
     ``playbook_pattern_stats`` (the same harness as
     ``python -m src.signals.playbook.backtest``, looped over underlyings).
  2. Loads the calibration store from those fresh stats and prints, for each
     pattern, the hand-set prior vs the calibrated base that the live engine
     WILL use (when ``SIGNALS_PATTERN_CALIBRATION_ENABLED=1``), including which
     pairs were gated out by sample size / freshness.

The live signals process picks up the new numbers automatically via
``calibration.maybe_refresh`` on its TTL — this job only refreshes the stored
measurements and gives operators a reviewable diff. Use ``--no-backtest`` to
report against existing stats without re-running the (heavier) backtest.

Usage:
    python -m src.tools.pattern_calibration_refresh
    python -m src.tools.pattern_calibration_refresh --underlyings SPY SPX --days 90
    python -m src.tools.pattern_calibration_refresh --no-backtest
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from src.config import (
    SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS,
    SIGNALS_PATTERN_CALIBRATION_MIN_SAMPLES,
    SIGNALS_PATTERN_CALIBRATION_SOURCE,
    SIGNALS_UNDERLYINGS,
)
from src.database.connection import db_connection
from src.backtesting import calibration_feed
from src.signals.playbook import backtest as playbook_backtest
from src.signals.playbook import calibration as pattern_calibration

logger = logging.getLogger(__name__)


def _default_underlyings() -> list[str]:
    return [s.strip().upper() for s in (SIGNALS_UNDERLYINGS or "SPY").split(",") if s.strip()]


def _priors() -> dict[str, float]:
    """Hand-set ``pattern_base`` per pattern id, for the report's baseline column."""
    try:
        from src.signals.playbook.engine import PlaybookEngine

        patterns = PlaybookEngine._discover_builtin_patterns()
        return {p.id: float(getattr(p, "pattern_base", 0.5)) for p in patterns}
    except Exception:  # pragma: no cover
        logger.warning("could not discover pattern priors", exc_info=True)
        return {}


def _format_report(priors: dict[str, float], store: pattern_calibration.CalibrationStore) -> str:
    lines = ["", "Pattern calibration (prior → calibrated, * = trusted pair present):"]
    ids = sorted(set(priors) | {p for p, _ in store.by_pair} | set(store.by_pattern))
    for pid in ids:
        prior = priors.get(pid)
        wide = store.by_pattern.get(pid)
        pairs = {u: b for (p, u), b in store.by_pair.items() if p == pid}
        prior_s = f"{prior:.3f}" if prior is not None else "  ?  "
        if wide is None:
            lines.append(f"  {pid:<32} {prior_s} → (kept prior; insufficient/stale data)")
        else:
            pair_s = ", ".join(f"{u}:{b:.3f}" for u, b in sorted(pairs.items()))
            lines.append(f"  {pid:<32} {prior_s} → {wide:.3f} * [{pair_s}]")
    return "\n".join(lines)


def _compare_report(
    priors: dict[str, float],
    touch_rows: list,
    pnl_rows: list,
    *,
    min_samples: int,
) -> str:
    """Side-by-side of the two measurement sources per (pattern, underlying).

    Each row prints the prior, the raw measured base + resolved count for each
    source ('—' if absent, marked '·' if below the sample gate so it would fall
    back to the prior), Δ = option_pnl − underlying_touch where both exist, and
    the value the live engine would actually use under ``source=auto`` (the
    gated + clamped base, tagged by which source won: P=option_pnl, T=touch,
    w=pattern-wide fallback, or 'prior' when nothing trustworthy exists).
    """
    def _index(rows):
        # rows: (pattern, underlying, window_end, n_resolved, proposed_base)
        out = {}
        for pattern, underlying, _we, n, base in rows:
            out[(pattern, (underlying or "").upper())] = (base, int(n or 0))
        return out

    touch = _index(touch_rows)
    pnl = _index(pnl_rows)

    # What 'auto' would resolve to: the gated + clamped, P&L-preferred store.
    touch_store = pattern_calibration.build_store_from_rows(
        touch_rows, source="underlying_touch"
    )
    pnl_store = pattern_calibration.build_store_from_rows(pnl_rows, source="option_pnl")

    def _auto_cell(pattern, key) -> str:
        if key in pnl_store.by_pair:
            return f"{pnl_store.by_pair[key]:.3f} P"
        if key in touch_store.by_pair:
            return f"{touch_store.by_pair[key]:.3f} T"
        wide = pnl_store.by_pattern.get(pattern, touch_store.by_pattern.get(pattern))
        if wide is not None:
            return f"{wide:.3f} w"
        return "prior"

    def _cell(entry) -> str:
        if entry is None or entry[0] is None:
            return f"{'—':>13}"
        base, n = entry
        gate = " " if n >= min_samples else "·"
        return f"{base:.3f}{gate}({n:>6})"

    keys = sorted(set(touch) | set(pnl))
    lines = [
        "",
        "Calibration sources — underlying_touch vs option_pnl "
        f"(· = below {min_samples}-sample gate):",
        f"  {'pattern':<30}{'undl':<6}{'prior':>7}  "
        f"{'touch (n)':>13}  {'option_pnl (n)':>14}  {'Δ(pnl−touch)':>12}  "
        f"{'auto→':>11}",
    ]
    for pattern, undl in keys:
        key = (pattern, undl)
        prior = priors.get(pattern)
        t = touch.get(key)
        p = pnl.get(key)
        prior_s = f"{prior:.3f}" if prior is not None else "  ?  "
        delta_s = ""
        if t and p and t[0] is not None and p[0] is not None:
            delta_s = f"{p[0] - t[0]:+.3f}"
        lines.append(
            f"  {pattern:<30}{undl:<6}{prior_s:>7}  "
            f"{_cell(t):>13}  {_cell(p):>14}  {delta_s:>12}  "
            f"{_auto_cell(pattern, key):>11}"
        )
    return "\n".join(lines)


def _explain_report(result, *, pattern: str, underlying: str, limit: int = 40) -> str:
    """Per-trade drill-in for one pattern's realized-P&L run.

    Prints each trade (entry/exit premium, hold, P&L, outcome) and — the key
    diagnostic — the outcome distribution with how many trades in EACH outcome
    were actually profitable. A pattern whose ``target_hit`` trades are mostly
    unprofitable is a confirmed theta trap (right on direction, wrong on premium).
    """
    trades = list(result.trades)
    n = len(trades)
    head = [f"\n{pattern} @ {underlying}: {n} option_pnl trade(s)"]
    if n == 0:
        head.append(
            "  (no priced entries — e.g. a premium-seller skipped by the "
            "defined-risk guard)"
        )
        return "\n".join(head)

    head.append(
        f"  {'#':>3}  {'entered':<16}{'hold':>6}  {'contract':<10}"
        f"{'entry':>8}{'exit':>8}{'net_pnl':>11}{'ret%':>8}  outcome"
    )
    for t in trades[:limit]:
        entered = t.entered_at.strftime("%m-%d %H:%M") if t.entered_at else "—"
        strike = f"{t.strike:g}{(t.option_type or '').upper()}" if t.strike else "—"
        ret = f"{t.return_pct:+.1f}" if t.return_pct is not None else "—"
        exitp = f"{t.exit_premium:.2f}" if t.exit_premium is not None else "—"
        head.append(
            f"  {t.seq:>3}  {entered:<16}{(t.hold_minutes or 0):>6}  {strike:<10}"
            f"{t.entry_premium:>8.2f}{exitp:>8}{t.net_pnl:>11.0f}{ret:>8}  {t.outcome}"
        )
    if n > limit:
        head.append(f"  … {n - limit} more")

    # Outcome distribution with profitability — the theta-trap tell.
    dist: dict[str, list[int]] = {}
    wins = 0
    for t in trades:
        slot = dist.setdefault(t.outcome, [0, 0])
        slot[0] += 1
        if t.net_pnl > 0:
            slot[1] += 1
            wins += 1
    head.append(f"\n  realized win rate: {wins}/{n} = {wins / n:.1%}")
    head.append("  by outcome (count, profitable):")
    for outcome, (cnt, prof) in sorted(dist.items(), key=lambda kv: -kv[1][0]):
        head.append(f"    {outcome:<14} {cnt:>4}  profitable {prof:>4} ({prof / cnt:.0%})")
    return "\n".join(head)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Refresh + report playbook pattern calibration")
    parser.add_argument("--underlyings", nargs="*", default=None)
    parser.add_argument(
        "--days", type=int, default=SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS,
        help="history window each backtest scans",
    )
    parser.add_argument(
        "--no-backtest", action="store_true",
        help="report against existing stats without re-running the backtest",
    )
    parser.add_argument(
        "--no-touch", action="store_true",
        help="skip the underlying-touch harness (refresh only the P&L feed)",
    )
    parser.add_argument(
        "--pnl", dest="pnl", action="store_true", default=None,
        help="also refresh the realized option-P&L feed (option_pnl source)",
    )
    parser.add_argument(
        "--no-pnl", dest="pnl", action="store_false",
        help="skip the realized option-P&L feed",
    )
    parser.add_argument(
        "--compare", action="store_true",
        help="print a side-by-side of both sources from existing stats and exit",
    )
    parser.add_argument(
        "--explain", metavar="PATTERN", default=None,
        help="drill into one pattern: run its option_pnl trades and dump them",
    )
    args = parser.parse_args(argv)

    underlyings = args.underlyings if args.underlyings else _default_underlyings()

    if args.explain:
        from src.backtesting import calibration_feed as feed

        with db_connection() as conn:
            for u in underlyings:
                result = feed.explain_trades(
                    conn, underlying=u, pattern=args.explain, days=args.days
                )
                print(_explain_report(result, pattern=args.explain, underlying=u))
        return 0

    if args.compare:
        with db_connection() as conn:
            touch_rows = pattern_calibration._load_rows(conn, "underlying_touch")
            pnl_rows = pattern_calibration._load_rows(conn, "option_pnl")
        print(
            _compare_report(
                _priors(), touch_rows, pnl_rows,
                min_samples=SIGNALS_PATTERN_CALIBRATION_MIN_SAMPLES,
            )
        )
        return 0

    # Default: run the P&L feed whenever the live engine would consult it.
    run_pnl = args.pnl
    if run_pnl is None:
        run_pnl = SIGNALS_PATTERN_CALIBRATION_SOURCE in ("option_pnl", "auto")

    with db_connection() as conn:
        if not args.no_backtest:
            for u in underlyings:
                if not args.no_touch:
                    logger.info("refreshing underlying_touch stats: %s over %d days", u, args.days)
                    try:
                        playbook_backtest.run(underlying=u, days=args.days, conn=conn, write=True)
                    except Exception:  # noqa: BLE001 - one underlying must not abort the rest
                        logger.exception("touch backtest failed for %s; continuing", u)
                if run_pnl:
                    logger.info("refreshing option_pnl stats: %s over %d days", u, args.days)
                    try:
                        calibration_feed.run(underlying=u, days=args.days, conn=conn, write=True)
                    except Exception:  # noqa: BLE001 - one underlying must not abort the rest
                        logger.exception("P&L calibration failed for %s; continuing", u)
        store = pattern_calibration.load_store(conn)

    print(f"\nActive calibration source: {SIGNALS_PATTERN_CALIBRATION_SOURCE}")
    print(_format_report(_priors(), store))
    logger.info(
        "calibration store (source=%s): %d trusted pairs, %d pattern-wide bases",
        SIGNALS_PATTERN_CALIBRATION_SOURCE, len(store.by_pair), len(store.by_pattern),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
