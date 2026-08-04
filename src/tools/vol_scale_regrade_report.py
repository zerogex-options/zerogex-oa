"""Backtest report: what the expected-vol call WOULD have scored under the
corrected range-vs-σ grading scale.

Context — the scale bug (fixed alongside this tool): the receipt graded the
day's realized RANGE (high−low) as a fraction of the 1-σ VIX-implied move and
bucketed on edges centered at 1.0.  But for a driftless diffusion the expected
daily range is √(8/π)·σ ≈ 1.6σ, so a statistically ORDINARY day landed at
ratio ≈ 1.6 and was stamped "expansion".  As half of every two-cell scorecard,
that dragged nearly every day to MIXED/MISSED and made the public replay look
worthless.  The fix grades realized range against an EXPECTED range
(√(8/π)·implied, per-symbol calibrated) so an ordinary day re-centers at ≈1.0.

This tool re-grades each historical row's committed expected-vol call against
the SAME recorded OHLC under three denominators, per symbol:

  * OLD    — range ÷ (1·implied):            the original buggy scale.
  * FIXED  — range ÷ (√(8/π)·implied):        the pure Parkinson re-centering.
  * CALIB  — range ÷ (√(8/π)·basis·implied):  FIXED with the per-symbol basis
             the nightly calibrator converges to (median realized range),
             which absorbs the variance-risk premium.

The committed morning call (``expected_vol_state``) is UNCHANGED — its compute
path never moved, only the realized-side denominator did — so holding it fixed
and re-grading the realized bucket is the honest apples-to-apples comparison
(the same shape as ``regime_regrade_report``).

It ALSO recomputes the v1.5 REWORKED prediction (persistence anchor + gamma
tilt) causally — for each row the anchor is the median of only the PRIOR days'
realized ratios, exactly what the live writer sees each morning — and reports
its accuracy against the committed call and the majority-realized-bucket
baseline the vol call must beat.  Pre-v1.5 rows didn't snapshot ``net_gex``, so
the gamma tilt is off for them and the recompute mainly exercises the
persistence anchor (the primary driver) — a floor on the reworked model's real
skill, not a ceiling.  The per-symbol median ATR÷implied is printed too, as the
implied_move sanity check.

READ-ONLY.  It runs SELECTs and a rollback and changes NOTHING — the committed
forecasts are an immutable, tamper-evident public commitment (folded into
``content_hash`` and protected by the ``enforce_daily_forecast_immutability``
trigger); they are not, and must not be, rewritten.  This is an estimate of the
corrected scale's historical skill, not a correction of the record.

CAVEAT: the CALIB basis here is computed over the WHOLE sampled window (a
hindsight median), whereas the live calibrator learns it causally from a
trailing window.  Treat CALIB as the asymptote the live number drifts toward,
not a day-by-day replay.

Usage:
    python -m src.tools.vol_scale_regrade_report
    python -m src.tools.vol_scale_regrade_report --symbol SPY
    python -m src.tools.vol_scale_regrade_report --json /tmp/vol_scale_backtest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from src.database.connection import db_connection
from src.jobs.forecast_range_model import (
    RANGE_OVER_SIGMA,
    _classify_vol_state,
    grade_realized_claims,
    predict_expected_vol,
    robust_persistence_anchor,
)

logger = logging.getLogger("zerogex.vol_scale_regrade_report")

# Match the live calibrator's clamp (forecast_calibrate.BOUNDS) so the CALIB
# basis in this report can't stray past what the cron would actually apply.
VOL_BASIS_BOUNDS = (0.45, 1.40)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


@dataclass
class _Row:
    open_spot: float
    actual_low: float
    actual_high: float
    implied_move: float
    expected_vol_state: Optional[str]
    committed_correct: Optional[bool]
    # Features for recomputing the v1.5 reworked prediction (from the immutable
    # forecast_inputs snapshot; absent on pre-v1.5 rows → treated as None).
    atr_5d: Optional[float] = None
    net_gex: Optional[float] = None
    local_gex: Optional[float] = None
    vix_z_score_20d: Optional[float] = None
    flip_distance: Optional[float] = None


@dataclass
class SymbolStats:
    symbol: str
    n_rows: int = 0
    n_gradeable: int = 0  # rows with implied_move + actuals + a committed call
    calib_basis: Optional[float] = None
    # Correct-counts under each denominator (over gradeable rows).
    old_wins: int = 0
    fixed_wins: int = 0
    calib_wins: int = 0
    # Realized-bucket distributions (over rows with implied_move + actuals).
    old_buckets: dict[str, int] = field(default_factory=dict)
    fixed_buckets: dict[str, int] = field(default_factory=dict)
    calib_buckets: dict[str, int] = field(default_factory=dict)
    # Committed stored verdict rate (mix of old/new depending on grade time) —
    # shown for reference against the deterministic recomputes.
    committed_scored: int = 0
    committed_wins: int = 0
    # v1.5 REWORKED prediction (persistence anchor + gamma tilt), recomputed
    # causally and graded on the calibrated scale — the "does it beat baseline?"
    # number. And the majority-realized-bucket baseline it must clear.
    reworked_scored: int = 0
    reworked_wins: int = 0
    # ATR÷implied diagnostic (implied_move sanity) — median over rows.
    atr_over_implied: list[float] = field(default_factory=list)

    def _rate(self, wins: int, scored: int) -> Optional[float]:
        return round(wins / scored, 4) if scored else None

    @property
    def reworked_rate(self) -> Optional[float]:
        return self._rate(self.reworked_wins, self.reworked_scored)

    @property
    def baseline_rate(self) -> Optional[float]:
        """Majority realized bucket share on the calibrated scale — the strawman
        the vol call must beat."""
        total = sum(self.calib_buckets.values())
        return round(max(self.calib_buckets.values()) / total, 4) if total else None

    @property
    def median_atr_over_implied(self) -> Optional[float]:
        return round(statistics.median(self.atr_over_implied), 3) if self.atr_over_implied else None

    @property
    def old_rate(self) -> Optional[float]:
        return self._rate(self.old_wins, self.n_gradeable)

    @property
    def fixed_rate(self) -> Optional[float]:
        return self._rate(self.fixed_wins, self.n_gradeable)

    @property
    def calib_rate(self) -> Optional[float]:
        return self._rate(self.calib_wins, self.n_gradeable)

    @property
    def committed_rate(self) -> Optional[float]:
        return self._rate(self.committed_wins, self.committed_scored)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "n_rows": self.n_rows,
            "n_gradeable": self.n_gradeable,
            "calib_basis": self.calib_basis,
            "committed_vol_correct_rate": self.committed_rate,
            "committed_scored": self.committed_scored,
            "old_scale_vol_correct_rate": self.old_rate,
            "fixed_scale_vol_correct_rate": self.fixed_rate,
            "calibrated_scale_vol_correct_rate": self.calib_rate,
            "reworked_vol_correct_rate": self.reworked_rate,
            "reworked_scored": self.reworked_scored,
            "majority_bucket_baseline": self.baseline_rate,
            "median_atr_over_implied": self.median_atr_over_implied,
            "realized_bucket_counts_old": dict(sorted(self.old_buckets.items())),
            "realized_bucket_counts_fixed": dict(sorted(self.fixed_buckets.items())),
            "realized_bucket_counts_calibrated": dict(sorted(self.calib_buckets.items())),
        }


_QUERY = """
    SELECT symbol, date, open_spot, actual_low, actual_high,
           implied_move, expected_vol_state, vol_state_correct, forecast_inputs
    FROM daily_forecast
    WHERE receipt_ts IS NOT NULL
      AND actual_low IS NOT NULL
      AND actual_high IS NOT NULL
      AND (%(symbol)s IS NULL OR symbol = %(symbol)s)
    ORDER BY symbol, date
"""


def _fi_get(fi: Any, key: str) -> Optional[float]:
    """Pull a numeric feature from the forecast_inputs JSONB blob (dict or JSON
    string), tolerating missing keys / pre-v1.5 rows."""
    if isinstance(fi, str):
        try:
            fi = json.loads(fi)
        except (json.JSONDecodeError, TypeError):
            return None
    if not isinstance(fi, dict) or fi.get(key) is None:
        return None
    try:
        return float(fi[key])
    except (TypeError, ValueError):
        return None


def _regrade_ratio(row: _Row, range_over_sigma: float) -> tuple[Optional[float], Optional[str], Optional[bool]]:
    """Re-grade one row's vol call under a given range/σ denominator.

    Reuses the production grader so the band edges + rounding stay identical to
    what the live receipt writes. Returns (realized_ratio, realized_bucket,
    correct-or-None)."""
    g = grade_realized_claims(
        open_spot=row.open_spot,
        actual_low=row.actual_low,
        actual_high=row.actual_high,
        implied_move=row.implied_move,
        expected_vol_state=row.expected_vol_state,
        call_wall=None,
        put_wall=None,
        gamma_flip=None,
        level_touch_probs=None,
        flip_cross_prob=None,
        range_over_sigma=range_over_sigma,
    )
    ratio = g["realized_vol_ratio"]
    bucket = _classify_vol_state(ratio) if ratio is not None else None
    return ratio, bucket, g["vol_state_correct"]


def analyze(conn, symbol: Optional[str]) -> dict[str, SymbolStats]:
    """Run the read-only backtest and return per-symbol stats."""
    with conn.cursor() as cur:
        cur.execute(_QUERY, {"symbol": symbol.upper() if symbol else None})
        db_rows = cur.fetchall()

    # Bucket the raw rows per symbol first so we can derive each symbol's CALIB
    # basis (median realized range ÷ Parkinson expected range) before grading.
    per_symbol: dict[str, list[_Row]] = {}
    for (sym, _day, open_spot, actual_low, actual_high,
         implied_move, expected_vol_state, committed_correct, fi) in db_rows:
        per_symbol.setdefault(sym, []).append(_Row(
            open_spot=float(open_spot) if open_spot is not None else 0.0,
            actual_low=float(actual_low),
            actual_high=float(actual_high),
            implied_move=float(implied_move) if implied_move is not None else 0.0,
            expected_vol_state=expected_vol_state,
            committed_correct=committed_correct,
            atr_5d=_fi_get(fi, "atr_5d"),
            net_gex=_fi_get(fi, "net_gex"),
            local_gex=_fi_get(fi, "local_gex"),
            vix_z_score_20d=_fi_get(fi, "vix_z_score_20d"),
            flip_distance=_fi_get(fi, "flip_distance"),
        ))

    stats: dict[str, SymbolStats] = {}
    for sym, rows in per_symbol.items():
        s = SymbolStats(symbol=sym)
        s.n_rows = len(rows)

        # CALIB basis: median of (range ÷ (√(8/π)·implied)) over rows with a
        # usable implied move — the divisor that maps the median day to ≈1.0.
        raws = [
            (r.actual_high - r.actual_low) / (RANGE_OVER_SIGMA * r.implied_move)
            for r in rows if r.implied_move > 0
        ]
        basis = _clamp(statistics.median(raws), *VOL_BASIS_BOUNDS) if raws else 1.0
        s.calib_basis = round(basis, 4)

        # Rows are chronological (ORDER BY symbol, date), so we can build the
        # causal persistence anchor from PRIOR realized ratios as we walk them —
        # exactly what the live writer sees each morning.
        prior_ratios: list[float] = []
        for r in rows:
            if r.committed_correct is not None:
                s.committed_scored += 1
                s.committed_wins += 1 if r.committed_correct else 0
            if r.implied_move <= 0:
                continue  # not vol-gradeable without an implied move

            cal_ratio, cal_b, cal_ok = _regrade_ratio(r, RANGE_OVER_SIGMA * basis)
            _, old_b, old_ok = _regrade_ratio(r, 1.0)
            _, fix_b, fix_ok = _regrade_ratio(r, RANGE_OVER_SIGMA)

            for bucket, dist in ((old_b, s.old_buckets), (fix_b, s.fixed_buckets),
                                 (cal_b, s.calib_buckets)):
                if bucket is not None:
                    dist[bucket] = dist.get(bucket, 0) + 1

            if r.atr_5d is not None and r.atr_5d > 0:
                s.atr_over_implied.append(r.atr_5d / r.implied_move)

            # v1.5 reworked prediction, recomputed causally from PRIOR history +
            # this row's committed features, then graded on the calibrated scale.
            anchor = robust_persistence_anchor(prior_ratios)
            rw_state, _rw_ratio, _msg = predict_expected_vol(
                persistence_anchor=anchor,
                atr_5d=r.atr_5d,
                implied_move=r.implied_move,
                vol_basis=basis,
                net_gex=r.net_gex,
                gex_surface_fresh=True,
                local_gex=r.local_gex,
                vix_z_score_20d=r.vix_z_score_20d,
                flip_distance=r.flip_distance,
            )
            if cal_b is not None:
                s.reworked_scored += 1
                s.reworked_wins += 1 if rw_state == cal_b else 0

            # Committed prediction graded three ways (needs a committed call).
            if r.expected_vol_state is not None:
                s.n_gradeable += 1
                s.old_wins += 1 if old_ok else 0
                s.fixed_wins += 1 if fix_ok else 0
                s.calib_wins += 1 if cal_ok else 0

            if cal_ratio is not None:
                prior_ratios.append(cal_ratio)  # feed forward for the next day

        stats[sym] = s
    return stats


def _fmt_rate(rate: Optional[float]) -> str:
    return "—" if rate is None else f"{rate * 100:5.1f}%"


def _format_report(stats: dict[str, SymbolStats]) -> str:
    lines = [
        "=" * 78,
        "VOL-SCALE REGRADE + REWORK BACKTEST (READ-ONLY)",
        "=" * 78,
        "",
        "All rates are on the CALIBRATED scale. The headline is whether the vol",
        "call beats its baseline (always guess the majority realized bucket):",
        "  committed = the shipped v1.4 call · REWORKED = v1.5 (persistence anchor",
        "  + gamma tilt), recomputed causally · baseline = majority-bucket strawman.",
        "The scale line (OLD→FIXED→CALIB) shows the denominator fix in isolation.",
        "ATR/implied is the implied_move sanity check (healthy ~1.1–1.7).",
        "Committed forecasts are immutable and UNCHANGED.",
        "",
    ]
    if not stats:
        lines.append("No receipted forecast rows found.")
        return "\n".join(lines)

    def _verdict(rate: Optional[float], base: Optional[float]) -> str:
        if rate is None or base is None:
            return ""
        if rate > base:
            return f"  ✓ beats baseline (+{(rate - base) * 100:.1f}pt)"
        if rate == base:
            return "  = baseline"
        return f"  ✗ below baseline ({(rate - base) * 100:.1f}pt)"

    for sym in sorted(stats):
        s = stats[sym]
        lines.append(f"── {sym} ── ({s.n_rows} receipted rows, {s.n_gradeable} vol-gradeable)")
        lines.append(
            f"   basis={s.calib_basis}  ATR/implied(med)={s.median_atr_over_implied}"
            + ("  ⚠ implied_move looks mis-scaled"
               if s.median_atr_over_implied is not None and s.median_atr_over_implied < 0.8
               else "")
        )
        lines.append(f"   VOL CALL (calibrated scale, n={s.reworked_scored}):")
        lines.append(f"     committed v1.4 : {_fmt_rate(s.calib_rate)}")
        lines.append(
            f"     REWORKED v1.5  : {_fmt_rate(s.reworked_rate)}"
            f"{_verdict(s.reworked_rate, s.baseline_rate)}"
        )
        lines.append(f"     baseline       : {_fmt_rate(s.baseline_rate)} (majority bucket)")
        lines.append(
            f"   scale check (committed call): OLD {_fmt_rate(s.old_rate)}"
            f" → FIXED {_fmt_rate(s.fixed_rate)} → CALIB {_fmt_rate(s.calib_rate)}"
        )
        lines.append(
            f"   realized buckets: OLD {dict(sorted(s.old_buckets.items()))}"
            f" → CALIB {dict(sorted(s.calib_buckets.items()))}"
        )
        lines.append("")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backtest the corrected vol-grade scale against stored history (read-only)."
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
    logger.info("Vol-scale regrade backtest (symbol=%s) — READ-ONLY", args.symbol or "ALL")

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
