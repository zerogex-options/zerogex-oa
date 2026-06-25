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
    args = parser.parse_args(argv)

    underlyings = args.underlyings if args.underlyings else _default_underlyings()

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
