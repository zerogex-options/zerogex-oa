"""Replay a blank gamma-flip cycle and name the gate that rejected it.

:mod:`src.tools.gamma_flip_blackout_forensics` establishes, from
retention-exempt rows, that the blank cycles of 2026-08 and 2026-09 were
``gate_declined``: ``gamma_flip_raw`` was present on essentially every blank
row, so the spot-shift profile HAD a zero crossing and something between that
crossing and the published value refused it.  That narrows the cause to this
codebase.  It does not say WHICH of the gates did it, and there are four, each
added to fix a real pathology:

* the **interior** margin (the 2026-05-19 QQQ flips pinned to the grid edge),
* the **structural** floor (noise-floor sign changes in a collapsed profile),
* the **actionable-distance** ceiling (the 2026-05-20 SPX flip that walked off
  the bottom of the chart),
* and the **DTE ramp**, which is upstream of all three -- it reshapes the
  profile before any gate sees it, which is why ``gamma_flip_raw``, computed
  without it, can carry a crossing when the published flip does not.

This tool rebuilds the exact chain the engine saw at a past instant, re-runs
the real resolver on it, and then re-runs it once per gate with that ONE gate
relaxed.  Whichever relaxation publishes a flip is the gate that declined.

**Why this can still be done for August.**  ``option_chains`` carries 90 days,
so the chain is recoverable well past the point where the journal that
explained it has rotated.  The snapshot is rebuilt through the engine's own
``_run_snapshot_query`` and its own AM-settlement filter rather than a
hand-written SELECT, so the contracts fed to the resolver are the contracts the
resolver actually had -- a replay that reconstructs the input differently from
production answers a question nobody asked.

**What a relaxation result is and is not.**  A gate that publishes when
relaxed is the gate that rejected the crossing.  It is NOT automatically the
gate that is wrong: every one of them exists because an ungated flip did
visible damage once.  The output is evidence for a calibration decision, not
the decision.  In particular ``all gates off`` is a control -- it should
publish whenever ``gamma_flip_raw`` did, and if it does not, the profile
itself differs from the raw one and the cause is upstream in the DTE ramp.

READ-ONLY.  Runs SELECTs and a rollback; writes nothing.

Usage:
    python -m src.tools.gamma_flip_gate_replay --symbol NDX --session 2026-09-17
    python -m src.tools.gamma_flip_gate_replay --symbol SPX --at "2026-08-06 11:00"
    python -m src.tools.gamma_flip_gate_replay --symbol NDX --session 2026-09-17 --samples 5

Exit codes:
    0 -- replay ran and a report was produced.
    1 -- nothing to replay (no blank rows in the session, or no stored chain).
    2 -- database connection or query error.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import signal
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

from src.database.connection import db_connection
from src.market_calendar import is_am_settled_contract
from src.symbols import get_canonical_symbol
from src.tools.gamma_flip_resolution_healthcheck import session_window

logger = logging.getLogger("zerogex.gamma_flip_gate_replay")

ET = pytz.timezone("America/New_York")

#: The engine's own snapshot boundaries, mirrored so the replay rebuilds the
#: same contract set: AM-settled same-day expirations are dropped once the
#: 09:30 SOQ has happened, and the minimum expiration rolls forward at 16:15.
_OPEN = datetime.strptime("09:30", "%H:%M").time()
_CLOSE = datetime.strptime("16:15", "%H:%M").time()

#: Each entry is (label, {module attribute: relaxed value}).  Relaxing means
#: setting the knob to the value that makes that gate unconditionally pass, so
#: a flip appearing under exactly one entry identifies the gate with no
#: further arithmetic.  ``DTE ramp off`` is not a gate -- it changes the
#: profile itself -- and is listed last but one for that reason.
RELAXATIONS: Sequence[Tuple[str, Dict[str, Any]]] = (
    ("interior margin off", {"GAMMA_PROFILE_INTERIOR_MARGIN": 0.0}),
    ("structural floor off", {"GAMMA_PROFILE_STRUCTURAL_MIN_FRAC": 0.0}),
    ("distance ceiling off", {"GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT": 1.0}),
    (
        "active-strike filter loosened",
        {"GAMMA_PROFILE_STRUCTURAL_ACTIVE_DISTANCE_PCT": 0.10},
    ),
    ("DTE ramp off", {"GAMMA_PROFILE_DTE_WEIGHTING": False}),
    (
        "all gates off (control)",
        {
            "GAMMA_PROFILE_INTERIOR_MARGIN": 0.0,
            "GAMMA_PROFILE_STRUCTURAL_MIN_FRAC": 0.0,
            "GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT": 1.0,
        },
    ),
)


@dataclass(frozen=True)
class ReplayResult:
    """One cycle, resolved under production settings and under each relaxation."""

    symbol: str
    timestamp: datetime
    spot: float
    contracts: int
    baseline_flip: Optional[float]
    baseline_span: float
    stored_flip: Optional[float]
    stored_raw: Optional[float]
    relaxed: Sequence[Tuple[str, Optional[float]]]
    dte_sweep: Sequence[Tuple[float, Optional[float]]]
    production_dte_ref: float
    diagnostics: Dict[str, Any]

    @property
    def culprits(self) -> List[str]:
        """Relaxations that published, excluding the control."""
        return [
            label
            for label, flip in self.relaxed
            if flip is not None and not label.endswith("(control)")
        ]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.astimezone(ET).isoformat(),
            "spot": self.spot,
            "contracts": self.contracts,
            "baseline_flip": self.baseline_flip,
            "baseline_span": self.baseline_span,
            "stored_flip": self.stored_flip,
            "stored_raw": self.stored_raw,
            "relaxed": {label: flip for label, flip in self.relaxed},
            "dte_sweep": {str(ref): flip for ref, flip in self.dte_sweep},
            "production_dte_ref": self.production_dte_ref,
            "culprits": self.culprits,
            "diagnostics": self.diagnostics,
        }


@contextlib.contextmanager
def _preserve_signal_handlers():
    """Constructing an AnalyticsEngine installs its own SIGINT/SIGTERM handlers.

    Harmless in the engine process and wrong in a CLI, where Ctrl-C should end
    the replay rather than start a graceful shutdown of a loop that is not
    running.  Save and restore around construction.
    """
    saved = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            saved[sig] = signal.getsignal(sig)
        except (ValueError, OSError):  # pragma: no cover - platform dependent
            pass
    try:
        yield
    finally:
        for sig, handler in saved.items():
            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, handler)


@contextlib.contextmanager
def _patched(module, overrides: Dict[str, Any]):
    """Temporarily rebind module-level knobs.

    The gate constants are imported into ``main_engine``'s namespace and read
    as globals at call time, so rebinding them on the module is exactly what
    the corresponding environment variable would have done at import -- without
    re-importing the world once per relaxation.
    """
    previous = {name: getattr(module, name) for name in overrides}
    try:
        for name, value in overrides.items():
            setattr(module, name, value)
        yield
    finally:
        for name, value in previous.items():
            setattr(module, name, value)


def blank_timestamps(
    cursor, symbol: str, session_date: date, samples: int
) -> List[Tuple[datetime, Optional[float]]]:
    """Cycles in this session whose flip was NULL, spread across the blackout.

    Evenly spaced rather than the first N: a blank run that starts at the open
    and a blank run that starts at noon can have different causes, and sampling
    only the head of the session would never show it.
    """
    start, end = session_window(session_date)
    cursor.execute(
        """
        SELECT timestamp, gamma_flip_raw
        FROM gex_summary
        WHERE underlying = %(symbol)s
          AND timestamp >= %(start)s
          AND timestamp <= %(end)s
          AND gamma_flip_point IS NULL
        ORDER BY timestamp
        """,
        {"symbol": symbol, "start": start, "end": end},
    )
    rows = list(cursor.fetchall())
    if not rows or samples <= 0:
        return []
    if len(rows) <= samples:
        return rows
    step = len(rows) / float(samples)
    return [rows[int(i * step)] for i in range(samples)]


def stored_row(cursor, symbol: str, timestamp: datetime) -> Tuple[Optional[float], Optional[float]]:
    """What the engine actually persisted for this cycle, for cross-checking."""
    cursor.execute(
        """
        SELECT gamma_flip_point, gamma_flip_raw
        FROM gex_summary
        WHERE underlying = %(symbol)s AND timestamp = %(ts)s
        """,
        {"symbol": symbol, "ts": timestamp},
    )
    row = cursor.fetchone()
    return (row[0], row[1]) if row else (None, None)


def load_chain(engine, cursor, timestamp: datetime) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    """Rebuild the contracts and spot the engine saw at ``timestamp``.

    Uses the engine's own snapshot query and its own AM-settlement filter, so
    the resolver is handed the same input it was handed live.  Reads
    ``option_chains`` (90-day retention) rather than the ``option_chains_latest``
    cache, which only ever holds the present.
    """
    from src.config import _getenv_int

    cursor.execute(
        """
        SELECT close
        FROM underlying_quotes
        WHERE symbol = %(symbol)s AND timestamp <= %(ts)s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        {"symbol": engine.db_symbol, "ts": timestamp},
    )
    row = cursor.fetchone()
    spot = float(row[0]) if row and row[0] is not None else None
    if spot is None:
        return [], None

    ts_et = timestamp.astimezone(ET)
    min_expiration = ts_et.date() - timedelta(days=1) if ts_et.time() < _CLOSE else ts_et.date()
    rows = engine._run_snapshot_query(
        cursor,
        timestamp,
        engine.snapshot_lookback_hours,
        min_expiration,
        _getenv_int("ANALYTICS_SNAPSHOT_MAX_ROWS", 50000, min=1),
    )

    options = [
        {
            "option_symbol": r[0],
            "strike": float(r[1]),
            "expiration": r[2],
            "option_type": r[3],
            "last": float(r[4]) if r[4] else 0.0,
            "bid": float(r[5]) if r[5] else 0.0,
            "ask": float(r[6]) if r[6] else 0.0,
            "volume": int(r[7]) if r[7] else 0,
            "open_interest": int(r[8]) if r[8] else 0,
            "delta": float(r[9]) if r[9] else 0.0,
            "gamma": float(r[10]) if r[10] else 0.0,
            "theta": float(r[11]) if r[11] else 0.0,
            "vega": float(r[12]) if r[12] else 0.0,
            "implied_volatility": float(r[13]) if r[13] else None,
        }
        for r in rows
    ]

    today_et = ts_et.date()
    if ts_et.time() >= _OPEN:
        options = [
            o
            for o in options
            if not (
                o["expiration"] == today_et
                and is_am_settled_contract(engine.db_symbol, o["option_symbol"], o["expiration"])
            )
        ]
    return options, spot


def replay_cycle(
    engine,
    module,
    symbol: str,
    timestamp: datetime,
    options: List[Dict[str, Any]],
    spot: float,
    stored: Tuple[Optional[float], Optional[float]],
    dte_refs: Sequence[float] = (),
) -> ReplayResult:
    """Resolve once as production would, then once per relaxed gate."""
    profile, flip, span = engine._resolve_gamma_flip(options, spot, timestamp)

    relaxed: List[Tuple[str, Optional[float]]] = []
    for label, overrides in RELAXATIONS:
        with _patched(module, overrides):
            _p, relaxed_flip, _s = engine._resolve_gamma_flip(options, spot, timestamp)
        relaxed.append((label, relaxed_flip))

    # The DTE reference is per-ENGINE, not a module constant, so the sweep
    # patches the instance.  Sweeping it is not the same as the "DTE ramp off"
    # relaxation: off answers "is the ramp responsible", the sweep answers
    # "what horizon would publish", which is the number that goes in .env.
    dte_sweep: List[Tuple[float, Optional[float]]] = []
    for ref in dte_refs:
        with _patched(engine, {"dte_ref_days": float(ref)}):
            _p, swept, _s = engine._resolve_gamma_flip(options, spot, timestamp)
        dte_sweep.append((float(ref), swept))

    diagnostics: Dict[str, Any] = {}
    if flip is None:
        diagnostics = engine._gamma_flip_unresolved_diagnostics(options, profile, spot, timestamp)

    return ReplayResult(
        symbol=symbol,
        timestamp=timestamp,
        spot=spot,
        contracts=len(options),
        baseline_flip=flip,
        baseline_span=span,
        stored_flip=stored[0],
        stored_raw=stored[1],
        relaxed=tuple(relaxed),
        dte_sweep=tuple(dte_sweep),
        production_dte_ref=float(engine.dte_ref_days),
        diagnostics=diagnostics,
    )


def recommended_dte_ref(blank: Sequence[ReplayResult]) -> Optional[float]:
    """The largest swept DTE reference that publishes in EVERY blank cycle.

    Largest, not smallest.  A shorter reference weakens the ramp, and the ramp
    is there to stop a same-day wall pinning a multi-day regime level; the
    value worth deploying is the longest horizon that still resolves the chain
    in front of it, not the one that flattens the ramp hardest.

    ``None`` when the cycles were not swept, or when no single value publishes
    in all of them -- in which case the chain needs more than a new constant.
    """
    swept = [r for r in blank if r.dte_sweep]
    if not swept or len(swept) != len(blank):
        return None
    publishing = [{ref for ref, flip in r.dte_sweep if flip is not None} for r in swept]
    common = set.intersection(*publishing) if publishing else set()
    return max(common) if common else None


def _fmt(value: Optional[float]) -> str:
    return "-" if value is None else f"{value:,.2f}"


def _g(diagnostics: Dict[str, Any], key: str, spec: str = ".3g") -> str:
    """A diagnostic field, or ``-``.

    The diagnostic dict is built from whatever the chain supported, so a key
    can be absent or None on exactly the degraded cycles this tool is pointed
    at.  Formatting must not be the thing that fails there.
    """
    value = diagnostics.get(key)
    return "-" if value is None else format(float(value), spec)


def _pct(diagnostics: Dict[str, Any], key: str) -> str:
    value = diagnostics.get(key)
    return "-" if value is None else f"{float(value) * 100:.0f}%"


def format_report(results: Sequence[ReplayResult]) -> List[str]:
    """One block per replayed cycle, then the gate that explains all of them."""
    if not results:
        return ["nothing to replay: no blank cycles found in the requested window"]

    lines: List[str] = []
    for r in results:
        lines.append("")
        lines.append(
            f"{r.symbol} {r.timestamp.astimezone(ET):%Y-%m-%d %H:%M:%S ET}  "
            f"spot={_fmt(r.spot)}  contracts={r.contracts}"
        )
        lines.append(
            f"  stored:   flip={_fmt(r.stored_flip)}  raw={_fmt(r.stored_raw)}   "
            f"replayed: flip={_fmt(r.baseline_flip)}  span={r.baseline_span:g}"
        )
        if r.baseline_flip is not None and r.stored_flip is None:
            lines.append(
                "  NOTE: the replay PUBLISHED where production did not. The chain "
                "or the configuration has changed since; this cycle cannot explain "
                "the original blank."
            )
        for label, flip in r.relaxed:
            mark = "PUBLISHES" if flip is not None else "still blank"
            lines.append(f"    {label:<32} {mark:<12} {_fmt(flip)}")
        if r.dte_sweep:
            lines.append("    DTE reference sweep (days -> published flip):")
            for ref, flip in r.dte_sweep:
                mark = "PUBLISHES" if flip is not None else "still blank"
                lines.append(f"      ref={ref:<5g} {mark:<12} {_fmt(flip)}")
        if r.diagnostics:
            d = r.diagnostics
            lines.append(
                f"  usable={d.get('usable_total')} "
                f"(calls={d.get('usable_calls')} puts={d.get('usable_puts')})  "
                f"profile pos/neg/zero={d.get('profile_pos_pts')}/"
                f"{d.get('profile_neg_pts')}/{d.get('profile_zero_pts')}"
            )
            lines.append(
                f"  peak={_g(d, 'profile_peak')} median={_g(d, 'profile_median')} "
                f"reference={_g(d, 'profile_reference')} "
                f"floor={_g(d, 'structural_floor')}"
            )
            lines.append(
                f"  iv p50={_g(d, 'iv_p50', '.3f')} p90={_g(d, 'iv_p90', '.3f')} "
                f"at_default={_pct(d, 'iv_at_default_share')}  "
                f"oi_share 0dte={_pct(d, 'oi_share_0dte')} "
                f"weighted 0dte={_pct(d, 'weighted_oi_share_0dte')}"
            )

    blank = [r for r in results if r.baseline_flip is None]
    lines.append("")
    if not blank:
        lines.append(
            "Every replayed cycle publishes today. The blank rows cannot be "
            "reproduced from the stored chain, so the cause is not in the "
            "resolver as it stands now."
        )
        return lines

    shared = set(blank[0].culprits)
    for r in blank[1:]:
        shared &= set(r.culprits)
    if shared:
        lines.append(
            f"{len(blank)} of {len(results)} replayed cycles reproduce blank. "
            f"Relaxing {' or '.join(sorted(shared))} publishes a flip in every "
            "one of them."
        )
        recommended = recommended_dte_ref(blank)
        if recommended is not None:
            lines.append(
                f"The LARGEST DTE reference that publishes in all of them is "
                f"{recommended:g} days (production runs "
                f"{blank[0].production_dte_ref:g}). Largest rather than "
                "smallest: the ramp exists to hold near-dated out of a "
                "multi-day level, so take the longest horizon that still "
                "resolves rather than the one that weakens it most."
            )
    else:
        lines.append(
            f"{len(blank)} of {len(results)} replayed cycles reproduce blank, but "
            "no single relaxation publishes in all of them -- the cycles have "
            "different causes. Read the per-cycle blocks above."
        )
    return lines


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--symbol", required=True, help="Underlying to replay.")
    parser.add_argument("--session", default=None, help="Session date (YYYY-MM-DD).")
    parser.add_argument("--at", default=None, help='Exact ET cycle, "YYYY-MM-DD HH:MM[:SS]".')
    parser.add_argument(
        "--samples",
        type=int,
        default=3,
        help="Blank cycles to replay from --session, spread across it (default 3).",
    )
    parser.add_argument(
        "--dte-ref-days",
        default=None,
        help=(
            "Comma-separated horizon-occupancy references to sweep, in days "
            "(e.g. 0.5,1,2,3,5). Answers what GAMMA_PROFILE_DTE_REF_DAYS_<SYMBOL> "
            "would have to be for this chain to publish."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.session and not args.at:
        parser.error("one of --session or --at is required")

    symbol = get_canonical_symbol(args.symbol)
    dte_refs = (
        [float(v) for v in str(args.dte_ref_days).split(",") if v.strip()]
        if args.dte_ref_days
        else []
    )

    from src.analytics import main_engine as module
    from src.analytics.main_engine import AnalyticsEngine

    with _preserve_signal_handlers():
        engine = AnalyticsEngine(underlying=symbol)

    results: List[ReplayResult] = []
    try:
        with db_connection() as conn:
            cursor = conn.cursor()

            if args.at:
                naive = datetime.fromisoformat(args.at)
                targets = [(ET.localize(naive), None)]
            else:
                targets = blank_timestamps(
                    cursor, symbol, date.fromisoformat(args.session), args.samples
                )

            for timestamp, _raw in targets:
                options, spot = load_chain(engine, cursor, timestamp)
                if not options or spot is None:
                    logger.warning(
                        "no stored chain for %s at %s -- option_chains retention is "
                        "90 days, so this cycle may be past it",
                        symbol,
                        timestamp.astimezone(ET),
                    )
                    continue
                results.append(
                    replay_cycle(
                        engine,
                        module,
                        symbol,
                        timestamp,
                        options,
                        spot,
                        stored_row(cursor, symbol, timestamp),
                        dte_refs,
                    )
                )
            conn.rollback()
    except Exception as exc:  # noqa: BLE001 - a diagnostic reports, it does not raise
        logger.error("gamma flip gate replay failed: %s", exc, exc_info=True)
        return 2

    if args.json:
        print(json.dumps({"cycles": [r.as_dict() for r in results]}, indent=2, default=str))
    else:
        for line in format_report(results):
            print(line)

    return 0 if results else 1


if __name__ == "__main__":
    sys.exit(main())
