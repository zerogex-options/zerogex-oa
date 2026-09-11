"""Explain one historical Pin Strike — the full per-candidate breakdown.

``gex_summary`` persists only the ANSWER (``pin_strike`` / ``pin_score`` /
``pin_confidence`` / ``pin_strike_reason``).  The per-candidate curve behind it
is explicitly not persisted (see :class:`src.analytics.pin_strike.PinStrikeResult`
— "Diagnostics (not persisted)"), so when a member asks *why* a strike scored
the way it did, there is no row to point at.

That question arrives often enough to deserve a tool.  ``pin_confidence`` is a
SHARE-OF-FIELD measure — the winner's score over the sum of every positive
candidate's score — so a strike carrying several times its neighbors' gamma can
still land under the 33% "Moderate" bucket: the Gaussian kernel spreads that
peak across the neighboring strikes, and each of those neighbors is itself a
candidate in the denominator.  Reading the persisted confidence alone cannot
show that; the candidate table can, at a glance.

This tool RECOMPUTES the pin for a historical minute and prints that table.
It does not re-derive the model: it loads the chain snapshot exactly the way
:meth:`AnalyticsEngine._get_snapshot` does and calls
:meth:`AnalyticsEngine._calculate_pin_strike` — the same production path, the
same canonical BSM gamma, the same ``PIN_STRIKE_*`` config.  Because the inputs
are reconstructed rather than replayed, the recompute is then CHECKED against
the persisted answer and any drift is reported loudly: an unverified
recomputation is not evidence, and must never be sent to a member as though it
were.

Retention note: the recompute needs per-contract open interest, which lives in
``option_chains`` (~``DATA_RETENTION_DAYS``, default 90d).  ``option_chains_archive``
carries no ``open_interest`` column at all, so once a session ages out of the
live table the breakdown is gone for good and only the persisted answer remains.

READ-ONLY (SELECT only — safe to run any time, including mid-session).

Usage:
    python -m src.tools.pin_strike_explain --underlying NDX --at "2026-09-03 14:30"
    python -m src.tools.pin_strike_explain --underlying SPX --at "2026-09-03 15:55" --top 40
    python -m src.tools.pin_strike_explain --underlying NDX --at "2026-09-03 14:30" --near 29500

``--at`` is ET (the timezone the member's screenshot is in); pass ``--utc`` to
read it as UTC instead.  The nearest persisted analytics frame at or before that
instant is used, so the output lines up with a row that actually shipped.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.analytics.main_engine import AnalyticsEngine
from src.database.connection import db_connection
from src.market_calendar import ET, is_spx_am_settled_expiration
from src.utils.logging import get_logger

logger = get_logger(__name__)

# The engine's own snapshot window / row cap defaults. Mirrored rather than
# imported because they are instance attributes resolved from env at
# construction; the engine instance built below is the source of truth and
# these only document the fallback.
_DEFAULT_ROW_CAP = 50000


# --------------------------------------------------------------------------- #
# Loading — deliberately the engine's own SQL and filters.
# --------------------------------------------------------------------------- #
def _resolve_frame(cursor, underlying: str, at: datetime) -> Optional[Dict[str, Any]]:
    """The persisted analytics frame at or before ``at``, with its pin answer.

    Anchoring to a real ``gex_summary`` row (rather than the caller's raw
    instant) is what makes the recompute checkable: the timestamp used for the
    chain snapshot is then the same one the engine wrote against.
    """
    cursor.execute(
        """
        SELECT timestamp, pin_strike, pin_score, pin_confidence, pin_strike_reason
        FROM gex_summary
        WHERE underlying = %s
          AND timestamp <= %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (underlying, at),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return {
        "timestamp": row[0],
        "pin_strike": float(row[1]) if row[1] is not None else None,
        "pin_score": float(row[2]) if row[2] is not None else None,
        "pin_confidence": float(row[3]) if row[3] is not None else None,
        "pin_strike_reason": row[4],
    }


def _resolve_spot(cursor, underlying: str, at: datetime) -> Optional[float]:
    """Spot the same way the replay frame resolves it — newest quote at/before."""
    cursor.execute(
        """
        SELECT uq.close
        FROM underlying_quotes uq
        WHERE uq.symbol = %s
          AND uq.timestamp <= %s
        ORDER BY uq.timestamp DESC
        LIMIT 1
        """,
        (underlying, at),
    )
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _load_options(
    cursor, engine: AnalyticsEngine, timestamp: datetime, row_cap: int
) -> List[Dict[str, Any]]:
    """The engine's latest-per-contract snapshot, rebuilt for a past minute.

    Runs ``AnalyticsEngine._SNAPSHOT_QUERY`` verbatim with the same parameter
    contract (underlying, timestamp, lookback_start, min_expiration, row_cap)
    and applies the same post-query AM-settled SPX drop, so the contract set
    handed to the model matches what the cycle saw. ``option_chains_latest`` is
    deliberately NOT consulted: it holds only the current snapshot and cannot
    answer for a past minute.
    """
    ts_et = timestamp.astimezone(ET)
    lookback_start = timestamp - timedelta(hours=engine.snapshot_lookback_hours)
    min_expiration = (
        ts_et.date() - timedelta(days=1) if ts_et.time() < dt_time(16, 15) else ts_et.date()
    )

    cursor.execute(
        AnalyticsEngine._SNAPSHOT_QUERY,
        (engine.db_symbol, timestamp, lookback_start, min_expiration, row_cap),
    )
    rows = cursor.fetchall()
    if len(rows) >= row_cap:
        logger.warning(
            "Snapshot hit the row cap (%d) — the candidate set may be incomplete.", row_cap
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

    # Same AM-settled SPX drop the engine applies after 09:30 ET (SPXW is
    # PM-settled and shares the underlying, so it is not filtered).
    today_et = ts_et.date()
    if ts_et.time() >= dt_time(9, 30):
        options = [
            o
            for o in options
            if not (
                o["expiration"] == today_et
                and not (o["option_symbol"] or "").upper().startswith("SPXW")
                and is_spx_am_settled_expiration(engine.db_symbol, o["expiration"])
            )
        ]
    return options


# --------------------------------------------------------------------------- #
# Reporting.
# --------------------------------------------------------------------------- #
def _fmt_money(v: float) -> str:
    """Dollar gamma in the units the dashboard and the member's own figures use."""
    a = abs(v)
    for unit, div in (("b", 1e9), ("m", 1e6), ("k", 1e3)):
        if a >= div:
            return f"{v / div:,.1f}{unit}"
    return f"{v:,.1f}"


def _bucket(confidence: Optional[float]) -> str:
    """The UI's strength bucket (core/pinStrike.ts thresholds)."""
    if confidence is None:
        return "—"
    if confidence >= 0.50:
        return "Strong"
    if confidence >= 0.33:
        return "Moderate"
    return "Weak"


def _check_against_persisted(result, frame: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Compare the recompute to the row that actually shipped.

    Tolerances are loose on the scores and tight on the strike: the strike is a
    discrete choice and must match exactly or the recompute is describing a
    different pin, while ``pin_score`` carries float/NUMERIC round-tripping and
    is only expected to agree to a fraction of a percent.
    """
    notes: List[str] = []
    ok = True

    if (result.pin_strike is None) != (frame["pin_strike"] is None):
        ok = False
        notes.append(
            f"  ! active-state differs: recomputed "
            f"{'a pin' if result.pin_strike is not None else 'NO pin'}, "
            f"stored {'a pin' if frame['pin_strike'] is not None else 'NO pin'}"
        )
    elif result.pin_strike is not None and frame["pin_strike"] is not None:
        if abs(result.pin_strike - frame["pin_strike"]) > 1e-6:
            ok = False
            notes.append(
                f"  ! strike differs: recomputed {result.pin_strike:g}, "
                f"stored {frame['pin_strike']:g}"
            )
        for label, got, want in (
            ("score", result.pin_score, frame["pin_score"]),
            ("confidence", result.pin_confidence, frame["pin_confidence"]),
        ):
            if got is None or want is None:
                continue
            denom = max(abs(want), 1e-12)
            if abs(got - want) / denom > 0.005:
                ok = False
                notes.append(f"  ! {label} differs: recomputed {got:.6g}, stored {want:.6g}")

    if result.pin_strike is None and result.reason != frame["pin_strike_reason"]:
        ok = False
        notes.append(
            f"  ! reason differs: recomputed {result.reason}, stored {frame['pin_strike_reason']}"
        )
    return ok, notes


def _report(
    *,
    underlying: str,
    frame: Dict[str, Any],
    spot: float,
    result,
    top: int,
    near: Optional[float],
    contract_count: int,
) -> bool:
    ts_et = frame["timestamp"].astimezone(ET)
    print()
    print("=" * 78)
    print(f"  Pin Strike breakdown — {underlying} @ {ts_et:%Y-%m-%d %H:%M:%S} ET")
    print("=" * 78)
    print(f"  spot                 {spot:,.2f}")
    print(f"  0DTE contracts used  {contract_count:,}")
    print(f"  expiration           {result.expiration}")
    print(
        f"  ATM IV (sigma)       {result.sigma:.4f}" if result.sigma else "  ATM IV (sigma)       —"
    )
    if result.tau:
        # Minutes is the unit anyone reading this actually thinks in; the raw
        # year-fraction is kept beside it because that is what the model used.
        print(
            f"  time to close (tau)  {result.tau:.6f} yr "
            f"(~{result.tau * 365 * 24 * 60:.0f} min)"
        )
    else:
        print("  time to close (tau)  —")
    print(
        f"  kernel bandwidth     {result.bandwidth:.2f} pts"
        if result.bandwidth
        else "  kernel bandwidth     —"
    )
    print(f"  candidates scored    {len(result.candidates):,}")

    if not result.is_active:
        print()
        print(f"  NO ACTIVE PIN — reason: {result.reason}")
        ok, notes = _check_against_persisted(result, frame)
        print()
        print("  Recompute vs the persisted row:")
        print("\n".join(notes) if notes else "    matches the stored answer.")
        return ok

    positives = [c for c in result.candidates if c.pin_score > 0.0]
    total = sum(c.pin_score for c in positives) or 1.0

    print()
    print(
        f"  WINNER  {result.pin_strike:g}   "
        f"score {_fmt_money(result.pin_score or 0.0)}   "
        f"confidence {(result.pin_confidence or 0.0) * 100:.1f}%  "
        f"({_bucket(result.pin_confidence)})"
    )
    print(
        f"          restoring gamma {_fmt_money(result.restoring_gex or 0.0)}  ×  "
        f"reachability {result.reachability:.4f}"
    )
    print()
    print(
        "  Confidence is the winner's SHARE of the column below, not its size. "
        "A peak\n  spread across its neighbors by the kernel splits that share "
        "with them."
    )
    print()

    ranked = sorted(positives, key=lambda c: c.pin_score, reverse=True)
    shown = ranked if near is not None else ranked[:top]
    if near is not None:
        # Everything within a few bandwidths of the strike the member asked
        # about, in strike order — the shape of the neighborhood is the point.
        span = (result.bandwidth or 0.0) * 3 or 1.0
        shown = sorted(
            (c for c in positives if abs(c.strike - near) <= span), key=lambda c: c.strike
        )
        print(f"  Strikes within ±{span:g} pts of {near:g} (strike order):")
    else:
        print(f"  Top {len(shown)} of {len(positives)} scoring candidates:")

    print()
    print(
        f"  {'strike':>10}  {'local GEX':>12}  {'restoring':>12}  "
        f"{'reach':>7}  {'score':>12}  {'share':>7}"
    )
    print(f"  {'-' * 10}  {'-' * 12}  {'-' * 12}  {'-' * 7}  {'-' * 12}  {'-' * 7}")
    winner = result.pin_strike
    for c in shown:
        mark = " <" if winner is not None and abs(c.strike - winner) < 1e-9 else ""
        print(
            f"  {c.strike:>10g}  {_fmt_money(c.local_gex):>12}  "
            f"{_fmt_money(c.restoring_gex):>12}  {c.reachability:>7.4f}  "
            f"{_fmt_money(c.pin_score):>12}  {c.pin_score / total * 100:>6.1f}%{mark}"
        )

    if near is not None and not shown:
        print(f"  (no scoring candidate within ±{span:g} pts of {near:g})")

    shown_share = sum(c.pin_score for c in shown) / total * 100
    print()
    print(
        f"  Rows shown account for {shown_share:.1f}% of the field; "
        f"{len(positives)} candidates carry positive score in total."
    )

    ok, notes = _check_against_persisted(result, frame)
    print()
    print("  Recompute vs the persisted row:")
    if notes:
        print("\n".join(notes))
        print()
        print("  DRIFT — do not quote these numbers to a member. The reconstructed")
        print("  inputs no longer reproduce what shipped (late-arriving quote")
        print("  revisions and config changes since the session are the usual causes).")
    else:
        print(
            f"    matches — strike {frame['pin_strike']:g}, "
            f"score {frame['pin_score']:.6g}, "
            f"confidence {frame['pin_confidence']:.4f}."
        )
    return ok


# --------------------------------------------------------------------------- #
# Entry point.
# --------------------------------------------------------------------------- #
def _parse_at(raw: str, as_utc: bool) -> datetime:
    """Parse the caller's timestamp into an aware datetime.

    ``ET`` is a pytz zone, so the ET branch must go through ``localize`` —
    ``replace(tzinfo=ET)`` would silently attach the zone's 1883 LMT offset
    (-04:56) and shift the lookup by four minutes.
    """
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            naive = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        return naive.replace(tzinfo=timezone.utc) if as_utc else ET.localize(naive)
    raise argparse.ArgumentTypeError(
        f"unrecognized timestamp {raw!r} — use 'YYYY-MM-DD HH:MM' (ET unless --utc)"
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Explain one historical Pin Strike: the full per-candidate breakdown.",
    )
    parser.add_argument("--underlying", required=True, help="Symbol, e.g. NDX / SPX / SPY / QQQ")
    parser.add_argument(
        "--at", required=True, help="Timestamp, 'YYYY-MM-DD HH:MM' (ET unless --utc)"
    )
    parser.add_argument("--utc", action="store_true", help="Read --at as UTC rather than ET")
    parser.add_argument(
        "--top", type=int, default=25, help="Rows in the candidate table (default 25)"
    )
    parser.add_argument(
        "--near",
        type=float,
        default=None,
        help="Show the neighborhood around this strike in strike order, instead of the top-N",
    )
    parser.add_argument(
        "--row-cap",
        type=int,
        default=_DEFAULT_ROW_CAP,
        help=f"Snapshot row cap (default {_DEFAULT_ROW_CAP})",
    )
    parser.add_argument("--verbose", action="store_true", help="Engine logging at INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    at = _parse_at(args.at, args.utc)
    engine = AnalyticsEngine(args.underlying)

    with db_connection() as conn:
        with conn.cursor() as cursor:
            frame = _resolve_frame(cursor, engine.db_symbol, at)
            if frame is None:
                print(
                    f"No analytics frame for {engine.db_symbol} at or before "
                    f"{at:%Y-%m-%d %H:%M %Z}.",
                    file=sys.stderr,
                )
                return 2

            spot = _resolve_spot(cursor, engine.db_symbol, frame["timestamp"])
            if spot is None or spot <= 0:
                print(
                    f"No underlying quote for {engine.db_symbol} at or before "
                    f"{frame['timestamp']}; cannot recompute.",
                    file=sys.stderr,
                )
                return 2

            options = _load_options(cursor, engine, frame["timestamp"], args.row_cap)

    if not options:
        print(
            f"No option_chains rows for {engine.db_symbol} in the snapshot window "
            f"ending {frame['timestamp']}. Past DATA_RETENTION_DAYS the chain is "
            f"gone (option_chains_archive carries no open_interest), so only the "
            f"persisted answer survives for this session.",
            file=sys.stderr,
        )
        return 2

    same_day = [o for o in options if o["expiration"] == frame["timestamp"].astimezone(ET).date()]
    result = engine._calculate_pin_strike(options, spot, frame["timestamp"])

    ok = _report(
        underlying=engine.db_symbol,
        frame=frame,
        spot=spot,
        result=result,
        top=args.top,
        near=args.near,
        contract_count=len(same_day),
    )
    print()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
