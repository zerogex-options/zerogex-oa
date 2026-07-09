"""Backtest report: what the corrected dealer-gamma regime WOULD have scored.

Context — the units bug (fixed in the dealer-gamma regime change): the
forecast writer fed the 0-100 Market State Index into a classifier that
expected a signed -1..+1 value, so EVERY committed ``daily_forecast.regime``
came out ``long_gamma`` and ``short_gamma`` was unreachable.  The 16:05
receipt then graded ``regime_correct`` against that pinned label, so the
public rolling ``regime_correct_rate`` measured little more than the base
rate of chop days.

This tool recomputes the regime each historical row WOULD have carried under
the corrected classifier and re-grades it against the SAME recorded actuals,
then reports committed-vs-recomputed hit-rates per symbol.

READ-ONLY.  It runs SELECTs and a rollback and changes NOTHING — the
committed forecasts are an immutable, tamper-evident public commitment
(``regime`` is protected by the ``enforce_daily_forecast_immutability``
trigger and folded into ``content_hash``); they are not, and must not be,
rewritten.  This is an estimate of the new logic's historical skill, not a
correction of the record.

TWO CAVEATS THAT LIMIT PRECISION (read before trusting the numbers):
  * Historical rows captured the signal surface at 07:00 ET — BEFORE the
    ~07:30 options reset — so the ``gamma_flip`` is the prior session's, and
    for SPX the ``open_spot`` is the frozen prior cash close.  The recompute
    is therefore a rough directional estimate, not ground truth.
  * ``net_gex`` was never stored on the row, so the recompute uses the
    classifier's spot-vs-gamma-flip FALLBACK path, not the primary
    net-GEX-sign path the live writer now uses.

The trustworthy regime accuracy accrues from the fix forward; that is what
the (fix-date-gated) live ``regime_correct_rate`` reports.

Usage:
    python -m src.tools.regime_regrade_report
    python -m src.tools.regime_regrade_report --symbol SPY
    python -m src.tools.regime_regrade_report --json /tmp/regime_backtest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from src.database.connection import db_connection
from src.jobs.forecast_range_model import REGIME_THRESHOLD_FALLBACK, _classify_regime

logger = logging.getLogger("zerogex.regime_regrade_report")


def _regrade(regime: Optional[str], open_spot: float, actual_close: float,
             threshold: float) -> Optional[bool]:
    """Grade a regime label against the realized close move — mirrors
    ``update_daily_forecast_receipt`` exactly so committed and recomputed
    grades are comparable.  Returns None for transition/None (never wrong)."""
    if open_spot <= 0:
        return None
    move_pct = abs(actual_close - open_spot) / open_spot
    if regime == "long_gamma":
        return move_pct <= threshold
    if regime == "short_gamma":
        return move_pct > threshold
    return None  # transition / unresolved — neutral, not graded


@dataclass
class SymbolStats:
    symbol: str
    n_rows: int = 0
    # Committed (as published).
    committed_scored: int = 0
    committed_wins: int = 0
    committed_regime_counts: dict[str, int] = field(default_factory=dict)
    # Recomputed under the corrected classifier (spot-vs-flip fallback).
    recomputed_scored: int = 0
    recomputed_wins: int = 0
    recomputed_regime_counts: dict[str, int] = field(default_factory=dict)
    # How many rows' label changed.
    label_flips: int = 0

    def _rate(self, wins: int, scored: int) -> Optional[float]:
        return round(wins / scored, 4) if scored else None

    @property
    def committed_rate(self) -> Optional[float]:
        return self._rate(self.committed_wins, self.committed_scored)

    @property
    def recomputed_rate(self) -> Optional[float]:
        return self._rate(self.recomputed_wins, self.recomputed_scored)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "n_rows": self.n_rows,
            "committed_regime_correct_rate": self.committed_rate,
            "committed_scored": self.committed_scored,
            "committed_regime_counts": self.committed_regime_counts,
            "recomputed_regime_correct_rate": self.recomputed_rate,
            "recomputed_scored": self.recomputed_scored,
            "recomputed_regime_counts": self.recomputed_regime_counts,
            "label_flips": self.label_flips,
        }


_QUERY = """
    SELECT symbol, date, open_spot, gamma_flip, regime,
           regime_move_threshold, actual_close, regime_correct
    FROM daily_forecast
    WHERE receipt_ts IS NOT NULL
      AND actual_close IS NOT NULL
      AND open_spot IS NOT NULL
      AND (%(symbol)s IS NULL OR symbol = %(symbol)s)
    ORDER BY symbol, date
"""


def analyze(conn, symbol: Optional[str]) -> dict[str, SymbolStats]:
    """Run the read-only backtest and return per-symbol stats."""
    with conn.cursor() as cur:
        cur.execute(_QUERY, {"symbol": symbol.upper() if symbol else None})
        rows = cur.fetchall()

    stats: dict[str, SymbolStats] = {}
    for row in rows:
        (sym, _day, open_spot, gamma_flip, committed_regime,
         threshold, actual_close, committed_correct) = row
        s = stats.setdefault(sym, SymbolStats(symbol=sym))
        s.n_rows += 1
        open_spot = float(open_spot)
        actual_close = float(actual_close)
        thr = float(threshold) if threshold is not None else REGIME_THRESHOLD_FALLBACK

        # Committed side (as published/graded).
        s.committed_regime_counts[committed_regime] = (
            s.committed_regime_counts.get(committed_regime, 0) + 1
        )
        if committed_correct is not None:
            s.committed_scored += 1
            s.committed_wins += 1 if committed_correct else 0

        # Recomputed under the corrected classifier — net_gex not stored, so
        # this exercises the spot-vs-gamma-flip fallback path.
        recomputed_regime = _classify_regime(
            None,
            open_spot,
            float(gamma_flip) if gamma_flip is not None else None,
            surface_fresh=True,
        )
        s.recomputed_regime_counts[recomputed_regime] = (
            s.recomputed_regime_counts.get(recomputed_regime, 0) + 1
        )
        if recomputed_regime != committed_regime:
            s.label_flips += 1
        recomputed_correct = _regrade(recomputed_regime, open_spot, actual_close, thr)
        if recomputed_correct is not None:
            s.recomputed_scored += 1
            s.recomputed_wins += 1 if recomputed_correct else 0

    return stats


def _fmt_rate(rate: Optional[float]) -> str:
    return "—" if rate is None else f"{rate * 100:5.1f}%"


def _format_report(stats: dict[str, SymbolStats]) -> str:
    lines = [
        "=" * 78,
        "REGIME REGRADE BACKTEST — committed vs. corrected-classifier (READ-ONLY)",
        "=" * 78,
        "",
        "Estimate only. Historical inputs were captured at 07:00 ET (pre options",
        "reset); net_gex was not stored, so this uses the spot-vs-gamma-flip",
        "fallback. The committed forecasts are immutable and UNCHANGED.",
        "",
    ]
    if not stats:
        lines.append("No receipted forecast rows found.")
        return "\n".join(lines)

    for sym in sorted(stats):
        s = stats[sym]
        lines.append(f"── {sym} ── ({s.n_rows} receipted rows)")
        lines.append(
            f"   committed  regime_correct_rate: {_fmt_rate(s.committed_rate)} "
            f"(n={s.committed_scored})  labels={dict(sorted(s.committed_regime_counts.items()))}"
        )
        lines.append(
            f"   recomputed regime_correct_rate: {_fmt_rate(s.recomputed_rate)} "
            f"(n={s.recomputed_scored})  labels={dict(sorted(s.recomputed_regime_counts.items()))}"
        )
        lines.append(f"   label flips (committed → recomputed): {s.label_flips}/{s.n_rows}")
        lines.append("")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backtest the corrected regime against stored history (read-only)."
    )
    parser.add_argument("--symbol", default=None, help="Restrict to one symbol (default: all).")
    parser.add_argument("--json", dest="json_path", default=None,
                        help="Also write the per-symbol summary as JSON to this path.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger.info("Regime regrade backtest (symbol=%s) — READ-ONLY", args.symbol or "ALL")

    with db_connection() as conn:
        stats = analyze(conn, args.symbol)
        # Defensive: this tool never writes — make the read-only contract
        # explicit even if a future edit adds a statement.
        conn.rollback()

    print(_format_report(stats))

    if args.json_path:
        payload = {sym: s.as_dict() for sym, s in sorted(stats.items())}
        with open(args.json_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        logger.info("Wrote JSON summary to %s", args.json_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
