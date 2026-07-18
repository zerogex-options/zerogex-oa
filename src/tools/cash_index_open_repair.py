"""One-shot repair for the cash-index session-open phantom in ``underlying_quotes``.

A cash index (SPX, NDX, RUT, DJX) has no transactional tape of its own, so
TradeStation carries the prior session's close forward and stamps the 09:30 ET
open bar's ``Open`` with that prior close until the constituents actually open.
That carried-forward price does not just corrupt the open — TradeStation folds
it into the bar's OHLC, so it also lands in the ``high`` (on a day the index
gapped down, where the prior close is the highest value) or the ``low`` (gap
up). Stored verbatim, it paints a phantom full-range candle from the prior close
to the real level instead of the true opening gap.

Detection — the false-positive-free signature is that the 09:30 open EXACTLY
equals the prior session's close (``underlying_quotes`` has no pre-market rows
for a cash index, so "the bar immediately before 09:30" is the prior session's
last print). A genuine index open never equals the prior close to the penny by
chance, so this never touches a legitimately-formed bar — unlike an
``open == high`` test, which also matches a real bar that opened at its high.

Reconstruction — only ``close`` (the last print) and the extreme that is NOT the
carried-forward price are real. Rebuild from those, anchored on ``close``:

    new_open = close
    real_high = high if high != prior_close else close
    real_low  = low  if low  != prior_close else close
    new_high = max(close, real_high, real_low)
    new_low  = min(close, real_high, real_low)

The true first print and the masked extreme are not recoverable from a completed
bar, so ``close`` is the honest proxy — it removes the phantom and preserves the
real, non-phantom extreme (the real low on a gap down, the real high on a gap
up). Idempotent: after the fix ``open == close``, so the ``open <> close`` guard
excludes the row on any re-run.

The live ingester stores TradeStation's bar verbatim (faithful capture); this
tool owns the correction and runs both as a one-shot over history and, via the
``zerogex-oa-cash-index-open-repair`` timer, a few minutes after each open.

Dry-run by default: without ``--execute`` the tool is read-only and only reports
what WOULD change. The Makefile wrapper (``make cash-index-open-repair``) gates
``--execute`` behind ``CONFIRM=yes``.

Verify against a live database — the pure reconstruction and SQL-contract logic
is unit-tested (``tests/test_cash_index_open_repair.py``), but the DB path needs
a real connection to exercise.

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

# The 09:30 ET open bars in scope, with the prior session's close attached and
# the reconstructed OHLC computed. Shared by the dry-run SELECT and the UPDATE so
# detection + reconstruction can never drift between preview and apply.
#
# ``timestamp`` is TIMESTAMPTZ, so ``timezone('America/New_York', timestamp)``
# yields the ET wall-clock instant (DST-correct) for the 09:30 match and the ET
# date bounds. The LATERAL picks the most recent row strictly before the bar —
# for a cash index (no pre-market rows) that is the prior session's last print,
# i.e. the value TradeStation carried forward. Named params are psycopg2-adapted:
# a Python list becomes a Postgres array for ``= ANY(...)``; ``None`` start/end
# disable that bound.
_TARGETS_CTE = """
    WITH open_bars AS (
        SELECT symbol, timestamp
        FROM underlying_quotes
        WHERE symbol = ANY(%(symbols)s)
          AND (timezone('America/New_York', timestamp))::time = TIME '09:30:00'
          AND (%(start)s::date IS NULL
               OR (timezone('America/New_York', timestamp))::date >= %(start)s::date)
          AND (%(end)s::date IS NULL
               OR (timezone('America/New_York', timestamp))::date <= %(end)s::date)
    ),
    targets AS (
        SELECT
            uq.symbol,
            uq.timestamp,
            uq.open  AS old_open,
            uq.high  AS old_high,
            uq.low   AS old_low,
            uq.close,
            pc.prior_close,
            uq.close AS new_open,
            GREATEST(
                uq.close,
                CASE WHEN uq.high <> pc.prior_close THEN uq.high ELSE uq.close END,
                CASE WHEN uq.low  <> pc.prior_close THEN uq.low  ELSE uq.close END
            ) AS new_high,
            LEAST(
                uq.close,
                CASE WHEN uq.high <> pc.prior_close THEN uq.high ELSE uq.close END,
                CASE WHEN uq.low  <> pc.prior_close THEN uq.low  ELSE uq.close END
            ) AS new_low
        FROM open_bars ob
        JOIN underlying_quotes uq
          ON uq.symbol = ob.symbol AND uq.timestamp = ob.timestamp
        CROSS JOIN LATERAL (
            SELECT p.close AS prior_close
            FROM underlying_quotes p
            WHERE p.symbol = ob.symbol AND p.timestamp < ob.timestamp
            ORDER BY p.timestamp DESC
            LIMIT 1
        ) pc
        WHERE uq.open = pc.prior_close   -- TradeStation carried the prior close forward
          AND uq.open <> uq.close        -- and it isn't a flat no-op bar
    )
"""

_SELECT_CANDIDATES_SQL = _TARGETS_CTE + """
    SELECT symbol, timestamp, old_open, old_high, old_low, close,
           prior_close, new_open, new_high, new_low
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
    "prior_close",
    "new_open",
    "new_high",
    "new_low",
)


def reconstruct_session_open(
    open_: Any, high: Any, low: Any, close: Any, prior_close: Any
) -> Optional[Tuple[Any, Any, Any]]:
    """Return the rebuilt ``(open, high, low)`` for a phantom bar, or ``None``.

    The Python mirror of the SQL reconstruction (single source of the rule).
    Returns ``None`` when the bar is not a carry-forward phantom — i.e. the open
    does not equal the prior close, or the bar is a flat no-op — so it is safe to
    call on any bar. Works with float or Decimal.
    """
    if open_ != prior_close or open_ == close:
        return None
    real_high = high if high != prior_close else close
    real_low = low if low != prior_close else close
    new_high = max(close, real_high, real_low)
    new_low = min(close, real_high, real_low)
    return close, new_high, new_low


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
    phantom bars found (dicts with old/new OHLC and the prior close) and
    ``updated`` is the number of rows the UPDATE actually changed (0 in dry-run
    mode). The UPDATE is skipped entirely when there is nothing to change, so a
    clean database is never written to.
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
            "  %s %s  prior_close=%s  O/H/L %s/%s/%s -> %s/%s/%s  (close %s)",
            row["symbol"],
            row["timestamp"],
            row["prior_close"],
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
            "Repair the cash-index 09:30 ET session-open phantom "
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
