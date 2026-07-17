"""One-shot repair for the cash-index session-open phantom in ``underlying_quotes``.

A cash index (SPX, NDX, RUT, DJX) has no transactional tape of its own, so
TradeStation carries the prior session's close forward and stamps the 09:30 ET
open bar's ``Open`` with that prior close until the constituents actually open.
Stored verbatim, that painted a phantom full-range candle from the prior close
down to the real level instead of the true opening gap.

The live ingester and the backfill tool now write the first genuine print as
the open going forward (see ``src/ingestion/main_engine.py`` and
``src/tools/underlying_backfill.py``). This tool repairs bars that were ALREADY
stored with the phantom — the upsert's ``COALESCE(open)`` deliberately preserves
an existing non-null open, so re-ingesting or re-backfilling does NOT overwrite
them; a direct UPDATE is the only way to fix already-stored rows.

Phantom signature (false-positive-free): the carried-forward prior close lands
OUTSIDE the 09:30 bar's own ``[low, high]`` range on any day the index gapped —
a genuine print can never be outside the range it helped form. So a 09:30 ET
cash-index bar with ``open > high OR open < low`` is a phantom that still needs
repair; a bar already carrying a real first print (live-fixed or backfill-
proxied) has ``open`` within ``[low, high]`` and is left untouched. That also
makes this tool idempotent — a second run finds nothing to change.

The repaired open is the bar's own ``close`` (the index level at the end of the
opening minute): a completed bar can't recover the true first print, so this is
the closest honest proxy and is guaranteed within ``[low, high]``. It removes
the phantom body and lets the gap show; on a no-gap day (open already within
range) nothing is touched, which is correct — there is no phantom to remove.

Dry-run by default: without ``--execute`` the tool is read-only and only reports
what WOULD change. The Makefile wrapper (``make cash-index-open-repair``) gates
``--execute`` behind ``CONFIRM=yes``.

Verify against a live database — the pure predicate/SQL-contract logic is
unit-tested (``tests/test_cash_index_open_repair.py``), but the DB path needs a
real connection to exercise.

Usage::

    python -m src.tools.cash_index_open_repair --start 2026-07-01 --end 2026-07-17
    python -m src.tools.cash_index_open_repair --execute        # apply
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.symbols import get_index_volume_proxies, is_cash_index

logger = logging.getLogger(__name__)

# One predicate, shared by the dry-run SELECT and the UPDATE so they can never
# drift. Named params (``%(symbols)s`` etc.) are psycopg2-adapted: a Python list
# becomes a Postgres array for ``= ANY(...)``; ``None`` start/end disable the
# date bound. ``timestamp`` is TIMESTAMPTZ, so ``timezone('America/New_York',
# timestamp)`` yields the ET wall-clock instant (DST-correct) to match 09:30 ET
# and to bound by ET session date.
_PHANTOM_PREDICATE = """
    symbol = ANY(%(symbols)s)
    AND (open > high OR open < low)
    AND (timezone('America/New_York', timestamp))::time = TIME '09:30:00'
    AND (%(start)s::date IS NULL
         OR (timezone('America/New_York', timestamp))::date >= %(start)s::date)
    AND (%(end)s::date IS NULL
         OR (timezone('America/New_York', timestamp))::date <= %(end)s::date)
"""

_SELECT_CANDIDATES_SQL = f"""
    SELECT symbol, timestamp, open, high, low, close
    FROM underlying_quotes
    WHERE {_PHANTOM_PREDICATE}
    ORDER BY symbol, timestamp
"""

_UPDATE_SQL = f"""
    UPDATE underlying_quotes
    SET open = close, updated_at = NOW()
    WHERE {_PHANTOM_PREDICATE}
"""

_CANDIDATE_COLS = ("symbol", "timestamp", "open", "high", "low", "close")


def is_phantom_session_open(open_: Any, high: Any, low: Any) -> bool:
    """True iff ``open_`` lies outside the bar's own ``[low, high]`` range.

    The Python mirror of the SQL predicate's core test. A real print is always
    within the range it helped form, so an out-of-range open is the
    carried-forward prior close (the phantom). Works with float or Decimal.
    """
    return open_ > high or open_ < low


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

    Returns ``(candidates, updated)`` where ``candidates`` is the list of
    phantom rows found (as dicts keyed by ``symbol, timestamp, open, high, low,
    close``) and ``updated`` is the number of rows the UPDATE actually changed
    (0 in dry-run mode). The UPDATE is skipped entirely when there is nothing to
    change, so a clean database is never written to.
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
    # A small sample so the operator can eyeball the before/after.
    for row in candidates[:10]:
        logger.info(
            "  %s %s  open %s -> %s  (outside [%s, %s])",
            row["symbol"],
            row["timestamp"],
            row["open"],
            row["close"],
            row["low"],
            row["high"],
        )
    if len(candidates) > 10:
        logger.info("  ... and %d more", len(candidates) - 10)
    if dry_run:
        logger.info("Dry-run only — re-run with --execute (or CONFIRM=yes) to apply.")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Repair the cash-index 09:30 ET session-open phantom open "
            "(carried-forward prior close) in underlying_quotes."
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
