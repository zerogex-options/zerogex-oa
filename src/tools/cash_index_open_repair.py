"""Repair the cash-index session-open phantom in ``underlying_quotes``.

A cash index (SPX, NDX, RUT, DJX) is computed from its constituents, and at
09:30:00 ET almost none have opened yet — so the index's opening print is the
stale opening-rotation value (≈ the prior close) and only "catches up" to the
real level over the first seconds as constituents open. TradeStation reports
that stale value as the 09:30 bar's ``Open``, and folds it into the bar's OHLC,
so it also lands in the ``high`` (gap down, where the stale value is the highest)
or the ``low`` (gap up). Stored verbatim it paints a phantom full-range candle
from the stale value to the real level instead of the true opening gap.

Detection is self-contained — no prior close needed. For a cash index the 09:30
open is *always* the stale rotation value (never a real traded print at
09:30:00), so when it equals an extreme (``open == high`` on a gap down,
``open == low`` on a gap up) and the bar gapped (``open != close``), that extreme
IS the stale value and the bar is a phantom. When the open sits strictly inside
``(low, high)`` the overnight move was small and there is no visible phantom, so
the bar is left alone. Because the open is never a real print, this never
mis-flags a legitimately-formed bar.

Reconstruction — the ``close`` (last print) and the extreme that is NOT the stale
open are real. Rebuild from those, anchored on ``close``:

    new_open = close
    real_high = high if high != open else close
    real_low  = low  if low  != open else close
    new_high = max(close, real_high, real_low)
    new_low  = min(close, real_high, real_low)

The true first print and the masked extreme are not recoverable from a completed
bar, so ``close`` is the honest proxy — it clears the phantom and keeps the real,
non-stale extreme (the real low on a gap down, the real high on a gap up).
Idempotent: after the fix ``open == close``, which the ``open <> close`` guard
excludes on any re-run.

The live ingester now runs :func:`repair_one_session_open` on each session's
open bar the moment it is complete, so new data is correct as stored. This module
is the shared source of that logic and also provides a one-time CLI to clean up
rows that were stored before the ingester fix (``make cash-index-open-repair``).

Dry-run by default: without ``--execute`` the CLI is read-only and only reports
what WOULD change.

Verify against a live database — the pure reconstruction and SQL-contract logic
is unit-tested (``tests/test_cash_index_open_repair.py``), but the DB path needs
a real connection to exercise.

Usage::

    python -m src.tools.cash_index_open_repair              # dry-run, all history
    python -m src.tools.cash_index_open_repair --execute    # apply
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.symbols import get_index_volume_proxies, is_cash_index

logger = logging.getLogger(__name__)

# The reconstruction, as a SQL fragment over a row's own OHLC. Every RHS in an
# UPDATE ... SET is evaluated against the pre-update row, so the ``open`` inside
# these CASEs is always the old (stale) open even though we also set open=close.
# The stale open equals whichever extreme is the phantom, so "extreme <> open"
# keeps the real extreme and substitutes close for the phantom one.
_NEW_HIGH_EXPR = (
    "GREATEST(close, CASE WHEN high <> open THEN high ELSE close END, "
    "CASE WHEN low <> open THEN low ELSE close END)"
)
_NEW_LOW_EXPR = (
    "LEAST(close, CASE WHEN high <> open THEN high ELSE close END, "
    "CASE WHEN low <> open THEN low ELSE close END)"
)

# The phantom signature, applied to a cash-index 09:30 ET bar: the stale open is
# an extreme of its own bar, and the bar gapped. ``open <> close`` also makes the
# fix idempotent (after it, open == close). ``timestamp`` is TIMESTAMPTZ, so
# ``timezone('America/New_York', timestamp)`` is the DST-correct ET wall clock.
_PHANTOM_WHERE = "(open = high OR open = low) AND open <> close"

# ---- Batch CLI path (one-time cleanup of already-stored history) ----
# Named params are psycopg2-adapted: a Python list becomes a Postgres array for
# ``= ANY(...)``; ``None`` start/end disable that date bound.
_TARGETS_CTE = f"""
    WITH targets AS (
        SELECT
            symbol,
            timestamp,
            open  AS old_open,
            high  AS old_high,
            low   AS old_low,
            close,
            close AS new_open,
            {_NEW_HIGH_EXPR} AS new_high,
            {_NEW_LOW_EXPR}  AS new_low
        FROM underlying_quotes
        WHERE symbol = ANY(%(symbols)s)
          AND (timezone('America/New_York', timestamp))::time = TIME '09:30:00'
          AND {_PHANTOM_WHERE}
          AND (%(start)s::date IS NULL
               OR (timezone('America/New_York', timestamp))::date >= %(start)s::date)
          AND (%(end)s::date IS NULL
               OR (timezone('America/New_York', timestamp))::date <= %(end)s::date)
    )
"""

_SELECT_CANDIDATES_SQL = _TARGETS_CTE + """
    SELECT symbol, timestamp, old_open, old_high, old_low, close,
           new_open, new_high, new_low
    FROM targets
    ORDER BY symbol, timestamp
"""

_UPDATE_SQL = _TARGETS_CTE + """
    UPDATE underlying_quotes u
    SET open = t.new_open,
        high = t.new_high,
        low  = t.new_low,
        updated_at = NOW()
    FROM targets t
    WHERE u.symbol = t.symbol AND u.timestamp = t.timestamp
"""

_CANDIDATE_COLS = (
    "symbol",
    "timestamp",
    "old_open",
    "old_high",
    "old_low",
    "close",
    "new_open",
    "new_high",
    "new_low",
)

# ---- Single-bar path (the live ingester, once per session on the open bar) ----
_SINGLE_BAR_REPAIR_SQL = f"""
    UPDATE underlying_quotes
    SET open = close,
        high = {_NEW_HIGH_EXPR},
        low  = {_NEW_LOW_EXPR},
        updated_at = NOW()
    WHERE symbol = %(symbol)s
      AND timestamp = %(ts)s
      AND {_PHANTOM_WHERE}
"""


def reconstruct_session_open(
    open_: Any, high: Any, low: Any, close: Any
) -> Optional[Tuple[Any, Any, Any]]:
    """Return the rebuilt ``(open, high, low)`` for a phantom bar, or ``None``.

    The Python mirror of the SQL reconstruction (single source of the rule).
    Returns ``None`` when the bar is not a phantom — the open is not an extreme,
    or the bar did not gap — so it is safe to call on any bar. Works with float
    or Decimal.
    """
    if open_ == close or (open_ != high and open_ != low):
        return None
    real_high = high if high != open_ else close
    real_low = low if low != open_ else close
    new_high = max(close, real_high, real_low)
    new_low = min(close, real_high, real_low)
    return close, new_high, new_low


def repair_one_session_open(conn, symbol: str, open_ts: datetime) -> int:
    """Repair the single 09:30 ET open bar at ``open_ts`` for ``symbol``.

    Runs the phantom UPDATE against an existing (psycopg2) connection and returns
    the number of rows changed (0 if the bar is absent or not a phantom). Does
    not commit — the caller owns the transaction. Used by the live ingester the
    moment a session's open bar is complete.
    """
    cur = conn.cursor()
    cur.execute(_SINGLE_BAR_REPAIR_SQL, {"symbol": symbol, "ts": open_ts})
    return cur.rowcount


def default_cash_index_symbols() -> List[str]:
    """The cash indexes to repair by default (SPX, NDX, RUT, DJX), sorted.

    The volume-proxy map is the codebase's single source of truth for "this
    symbol is a cash index" (see ``src.symbols.is_cash_index``).
    """
    return sorted(get_index_volume_proxies().keys())


def resolve_symbols(raw: Optional[str]) -> List[str]:
    """Parse the ``--symbols`` argument into a list of cash-index symbols.

    Accepts comma- and/or whitespace-separated symbols. Empty/None yields the
    full default cash-index set. Non-index symbols are dropped with a warning —
    the phantom only affects cash indexes, and refusing to touch anything else
    keeps the repair from masking unrelated data issues.
    """
    if not raw or not raw.strip():
        return default_cash_index_symbols()
    out: List[str] = []
    seen = set()
    for token in re.split(r"[,\s]+", raw.strip()):
        sym = token.strip().upper()
        if not sym or sym in seen:
            continue
        if is_cash_index(sym):
            seen.add(sym)
            out.append(sym)
        else:
            logger.warning("Skipping %s: not a cash index (phantom only affects cash indexes)", sym)
    return out


def repair(
    symbols: List[str],
    start: Optional[date] = None,
    end: Optional[date] = None,
    *,
    dry_run: bool = True,
    conn_factory: Optional[Callable[[], Any]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Find (and, unless ``dry_run``, repair) phantom session-open bars.

    Returns ``(candidates, updated)`` where ``candidates`` is the list of phantom
    bars found (dicts with old/new OHLC) and ``updated`` is the number of rows
    the UPDATE actually changed (0 in dry-run mode). The UPDATE is skipped
    entirely when there is nothing to change, so a clean database is never
    written to.
    """
    if conn_factory is None:
        from src.database import db_connection

        conn_factory = db_connection

    params = {"symbols": list(symbols), "start": start, "end": end}
    with conn_factory() as conn:
        cur = conn.cursor()
        cur.execute(_SELECT_CANDIDATES_SQL, params)
        candidates = [dict(zip(_CANDIDATE_COLS, row)) for row in cur.fetchall()]

        updated = 0
        if not dry_run and candidates:
            cur.execute(_UPDATE_SQL, params)
            updated = cur.rowcount
    return candidates, updated


def _counts_by_symbol(candidates: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in candidates:
        counts[row["symbol"]] = counts.get(row["symbol"], 0) + 1
    return counts


def _report(
    candidates: List[Dict[str, Any]], updated: int, *, dry_run: bool, as_json: bool
) -> None:
    counts = _counts_by_symbol(candidates)
    if as_json:
        print(
            json.dumps(
                {
                    "dry_run": dry_run,
                    "found": len(candidates),
                    "updated": updated,
                    "by_symbol": counts,
                },
                default=str,
            )
        )
        return

    verb = "Would repair" if dry_run else "Repaired"
    if not candidates:
        logger.info("No phantom session-open bars found — nothing to repair.")
        return
    logger.info("%s %d phantom session-open bar(s): %s", verb, len(candidates), counts)
    # A small before/after sample so the operator can eyeball the reconstruction.
    for row in candidates[:10]:
        logger.info(
            "  %s %s  O/H/L %s/%s/%s -> %s/%s/%s  (close %s)",
            row["symbol"],
            row["timestamp"],
            row["old_open"],
            row["old_high"],
            row["old_low"],
            row["new_open"],
            row["new_high"],
            row["new_low"],
            row["close"],
        )
    if len(candidates) > 10:
        logger.info("  ... and %d more", len(candidates) - 10)
    if dry_run:
        logger.info("Dry-run only — re-run with --execute (or CONFIRM=yes) to apply.")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "One-time cleanup of the cash-index 09:30 ET session-open phantom "
            "(stale opening-rotation value) in underlying_quotes."
        )
    )
    parser.add_argument(
        "--symbols",
        help=(
            "Comma/space-separated cash indexes (default: all — SPX, NDX, RUT, DJX). "
            "Non-index symbols are ignored."
        ),
    )
    parser.add_argument("--start", help="Inclusive ET start date YYYY-MM-DD (default: all history)")
    parser.add_argument("--end", help="Inclusive ET end date YYYY-MM-DD (default: all history)")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the UPDATE. Without this the tool is read-only (dry-run).",
    )
    parser.add_argument("--json", action="store_true", help="Emit a JSON summary instead of text.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    symbols = resolve_symbols(args.symbols)
    if not symbols:
        logger.error("No cash-index symbols to repair after filtering — nothing to do.")
        return 2

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    if start and end and end < start:
        logger.error("--end (%s) is before --start (%s)", end, start)
        return 2

    dry_run = not args.execute
    try:
        candidates, updated = repair(symbols, start, end, dry_run=dry_run)
    except Exception:
        logger.error("Repair failed", exc_info=True)
        return 1

    _report(candidates, updated, dry_run=dry_run, as_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
