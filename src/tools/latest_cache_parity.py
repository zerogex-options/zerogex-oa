"""Does the ``option_chains_latest`` cache give the same snapshot as history?

Run this before turning on ``ANALYTICS_USE_LATEST_CACHE``.  For each symbol it
reads the analytics snapshot BOTH ways inside one ``REPEATABLE READ`` read-only
transaction, so the two reads see the same committed data:

  * history: ``AnalyticsEngine._SNAPSHOT_QUERY``, the DISTINCT ON over
    ``option_chains`` that the flag replaces;
  * cache: ``AnalyticsEngine._SNAPSHOT_QUERY_CACHE`` over ``option_chains_latest``;

with the engine's own anchor (the newest ``option_chains`` row), lookback,
16:15 ET expiration roll-off, row cap and AM-settled drop.  It then compares

  * contracts present in one path and not the other;
  * per contract, everything GEX is built from: open interest, volume, gamma
    and implied volatility (OI and volume also per strike, summed over
    expirations and both types);
  * GEX, through the engine's own ``_calculate_gex_by_strike`` and
    ``_calculate_gex_summary``: call wall, put wall, gamma flip, net GEX at
    spot, total net GEX, max-gamma strike and max pain.

One difference is expected and reported apart: a contract whose newest history
row has no gamma (ingestion's Greeks gate).  History then falls back to an
older gamma-bearing row, or drops the contract, while the cache keeps its
newest row with the last good Greeks, so the two can differ there by design.

A sample passes when no contract differs for any other reason.  The GEX
readings are shown side by side and do not fail a sample on their own: the
inputs are compared contract by contract, so GEX can only move through a
contract already counted, and a Greeks gap moving it is the by-design case
above.  (On 2026-11-16 MSFT failed on total net GEX alone, every differing
contract a Greeks gap.)

READ-ONLY (SELECT only).  Each sample costs one history snapshot query per
symbol, the same query the analytics engine runs every cycle.

Usage (on the box, repo root, venv active):

    python -m src.tools.latest_cache_parity                  # ANALYTICS_UNDERLYINGS, once
    python -m src.tools.latest_cache_parity SPY SPX          # just these
    python -m src.tools.latest_cache_parity --every 600 --until 16:10   # through the session
    python -m src.tools.latest_cache_parity --every 600 --until 16:10 --json > parity.jsonl

Exit status: 0 when every sample passed, 1 when any failed, 2 on a DB error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence

from src.analytics.main_engine import AnalyticsEngine
from src.config import _getenv_int, _getenv_str
from src.database import db_connection
from src.market_calendar import ET, is_settled_am_contract
from src.symbols import parse_underlyings

# How far apart two GEX readings must be before the report calls them different.
# Informational only (see the module docstring); with identical inputs the
# readings are identical.
FLIP_TOLERANCE_PCT_OF_SPOT = 0.001  # 0.1% of spot
NET_GEX_TOLERANCE_REL = 0.01  # 1% of the larger magnitude

_NEWEST_HISTORY_ROW_SQL = """
    SELECT DISTINCT ON (option_symbol) option_symbol, gamma IS NULL
    FROM option_chains
    WHERE option_symbol = ANY(%s)
      AND timestamp <= %s
      AND timestamp >= %s
    ORDER BY option_symbol, timestamp DESC
"""

_GEX_FIELDS = (
    "call_wall",
    "put_wall",
    "gamma_flip_point",
    "net_gex_at_spot",
    "total_net_gex",
    "max_gamma_strike",
    "max_pain",
)


def _rows_to_options(rows: Iterable[Sequence[Any]], db_symbol: str, ts_et: datetime) -> List[dict]:
    """The snapshot rows as the option dicts ``_get_snapshot`` builds, minus the
    AM-settled contracts it drops.  The same mapping is applied to both paths,
    so it cannot itself create a difference between them."""
    options = []
    for row in rows:
        if is_settled_am_contract(db_symbol, row[0], row[2], ts_et):
            continue
        options.append(
            {
                "option_symbol": row[0],
                "strike": float(row[1]),
                "expiration": row[2],
                "option_type": row[3],
                "last": float(row[4]) if row[4] else 0.0,
                "bid": float(row[5]) if row[5] else 0.0,
                "ask": float(row[6]) if row[6] else 0.0,
                "volume": int(row[7]) if row[7] else 0,
                "open_interest": int(row[8]) if row[8] else 0,
                "delta": float(row[9]) if row[9] else 0.0,
                "gamma": float(row[10]) if row[10] else 0.0,
                "theta": float(row[11]) if row[11] else 0.0,
                "vega": float(row[12]) if row[12] else 0.0,
                "implied_volatility": float(row[13]) if row[13] else None,
            }
        )
    return options


def _by_strike(options: List[dict]) -> Dict[float, Dict[str, int]]:
    agg: Dict[float, Dict[str, int]] = defaultdict(lambda: {"open_interest": 0, "volume": 0})
    for opt in options:
        agg[opt["strike"]]["open_interest"] += opt["open_interest"]
        agg[opt["strike"]]["volume"] += opt["volume"]
    return agg


def _gex(engine: AnalyticsEngine, options: List[dict], spot: float, anchor: datetime) -> dict:
    """Walls, flip and net GEX the way the cycle computes them, with fresh
    walls (no published incumbent) so both paths are judged on the same terms."""
    if not options:
        return {}
    by_strike = engine._calculate_gex_by_strike(options, spot, anchor)
    if not by_strike:
        return {}
    summary = engine._calculate_gex_summary(by_strike, options, spot, anchor) or {}
    return {field: summary.get(field) for field in _GEX_FIELDS}


def _gex_agrees(field: str, hist: Any, cache: Any, spot: float) -> bool:
    if hist is None or cache is None:
        return hist is None and cache is None
    if field == "gamma_flip_point":
        return abs(float(hist) - float(cache)) <= spot * FLIP_TOLERANCE_PCT_OF_SPOT
    if field in ("net_gex_at_spot", "total_net_gex"):
        scale = max(abs(float(hist)), abs(float(cache)))
        return scale == 0 or abs(float(hist) - float(cache)) <= scale * NET_GEX_TOLERANCE_REL
    return float(hist) == float(cache)


def compare_snapshot(
    cursor,
    engine: AnalyticsEngine,
    *,
    gex_engines: Optional[Sequence[AnalyticsEngine]] = None,
    row_cap: Optional[int] = None,
) -> Dict[str, Any]:
    """Compare the two snapshot paths for ``engine``'s symbol on ``cursor``.

    Run it inside one transaction (``main`` uses REPEATABLE READ, READ ONLY) so
    both reads see the same data.  ``gex_engines`` is a (history, cache) pair
    for the GEX math, so neither path's computation can leave state behind for
    the other; by default two fresh engines for the symbol.
    """
    db_symbol = engine.db_symbol
    if row_cap is None:
        row_cap = _getenv_int("ANALYTICS_SNAPSHOT_MAX_ROWS", 50000, min=1)

    cursor.execute(
        """
        SELECT timestamp
        FROM option_chains
        WHERE underlying = %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (db_symbol,),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return {"symbol": db_symbol, "status": "no_data"}
    anchor = row[0]

    cursor.execute(
        """
        SELECT close
        FROM underlying_quotes
        WHERE symbol = %s
          AND timestamp <= %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (db_symbol, anchor),
    )
    row = cursor.fetchone()
    spot = float(row[0]) if row and row[0] is not None else None

    ts_et = anchor.astimezone(ET)
    min_expiration = (
        ts_et.date() - timedelta(days=1) if ts_et.time() < dt_time(16, 15) else ts_et.date()
    )
    lookback_hours = engine.snapshot_lookback_hours
    lookback_start = anchor - timedelta(hours=lookback_hours)

    hist_rows = engine._run_snapshot_query(cursor, anchor, lookback_hours, min_expiration, row_cap)
    cache_rows = engine._run_snapshot_query_from_cache(
        cursor, anchor, lookback_hours, min_expiration, row_cap
    )
    hist = {o["option_symbol"]: o for o in _rows_to_options(hist_rows, db_symbol, ts_et)}
    cache = {o["option_symbol"]: o for o in _rows_to_options(cache_rows, db_symbol, ts_et)}

    oi_differs = sorted(
        s
        for s in hist.keys() & cache.keys()
        if hist[s]["open_interest"] != cache[s]["open_interest"]
    )
    volume_differs = sorted(
        s for s in hist.keys() & cache.keys() if hist[s]["volume"] != cache[s]["volume"]
    )
    greeks_differ = sorted(
        s
        for s in hist.keys() & cache.keys()
        if hist[s]["gamma"] != cache[s]["gamma"]
        or hist[s]["implied_volatility"] != cache[s]["implied_volatility"]
    )
    only_history = sorted(hist.keys() - cache.keys())
    only_cache = sorted(cache.keys() - hist.keys())

    # Contracts whose newest history row has no gamma explain themselves; see
    # the module docstring.  One indexed lookup, only for contracts that differ.
    suspects = sorted(
        set(oi_differs)
        | set(volume_differs)
        | set(greeks_differ)
        | set(only_history)
        | set(only_cache)
    )
    greeks_gap: set = set()
    if suspects:
        cursor.execute(_NEWEST_HISTORY_ROW_SQL, (suspects, anchor, lookback_start))
        greeks_gap = {sym for sym, gamma_is_null in cursor.fetchall() if gamma_is_null}
    unexplained = [s for s in suspects if s not in greeks_gap]

    hist_strikes = _by_strike(list(hist.values()))
    cache_strikes = _by_strike(list(cache.values()))
    strikes_differ = {
        field: sum(
            1
            for k in hist_strikes.keys() | cache_strikes.keys()
            if hist_strikes.get(k, {}).get(field, 0) != cache_strikes.get(k, {}).get(field, 0)
        )
        for field in ("open_interest", "volume")
    }

    gex_hist: Dict[str, Any] = {}
    gex_cache: Dict[str, Any] = {}
    gex_differs: List[str] = []
    if spot and spot > 0:
        if gex_engines is None:
            gex_engines = (AnalyticsEngine(engine.underlying), AnalyticsEngine(engine.underlying))
        gex_hist = _gex(gex_engines[0], list(hist.values()), spot, anchor)
        gex_cache = _gex(gex_engines[1], list(cache.values()), spot, anchor)
        gex_differs = [
            f for f in _GEX_FIELDS if not _gex_agrees(f, gex_hist.get(f), gex_cache.get(f), spot)
        ]

    passed = not unexplained and bool(hist)
    return {
        "symbol": db_symbol,
        "status": "pass" if passed else ("empty" if not hist else "fail"),
        "anchor": anchor.isoformat(),
        "spot": spot,
        "contracts_history": len(hist),
        "contracts_cache": len(cache),
        "only_history": len(only_history),
        "only_cache": len(only_cache),
        "oi_differs": len(oi_differs),
        "volume_differs": len(volume_differs),
        "greeks_differ": len(greeks_differ),
        "greeks_gap": len(greeks_gap),
        "unexplained": len(unexplained),
        "unexplained_examples": unexplained[:5],
        "strikes_oi_differ": strikes_differ["open_interest"],
        "strikes_volume_differ": strikes_differ["volume"],
        "gex_history": gex_hist,
        "gex_cache": gex_cache,
        "gex_differs": gex_differs,
    }


def sample(engines: Sequence[AnalyticsEngine]) -> List[Dict[str, Any]]:
    """One comparison per symbol, each in its own consistent read-only snapshot."""
    results = []
    for engine in engines:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            results.append(compare_snapshot(cursor, engine))
            conn.rollback()
    return results


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and abs(value) >= 1e6:
        return f"{value / 1e9:,.2f}b" if abs(value) >= 1e9 else f"{value / 1e6:,.1f}m"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def _line(result: Dict[str, Any]) -> str:
    if result["status"] == "no_data":
        return f"{result['symbol']:<6} no option_chains rows"
    anchor_et = datetime.fromisoformat(result["anchor"]).astimezone(ET)
    gh, gc = result["gex_history"], result["gex_cache"]
    gex = "  ".join(
        f"{label} {_fmt(gh.get(f))}/{_fmt(gc.get(f))}"
        for label, f in (
            ("callwall", "call_wall"),
            ("putwall", "put_wall"),
            ("flip", "gamma_flip_point"),
            ("netGEX@spot", "net_gex_at_spot"),
        )
    )
    return (
        f"{result['symbol']:<6} {anchor_et:%Y-%m-%d %H:%M} ET  "
        f"contracts {result['contracts_history']}/{result['contracts_cache']}  "
        f"OI-diff {result['oi_differs']}  vol-diff {result['volume_differs']}  "
        f"greeks-diff {result['greeks_differ']}  "
        f"one-side {result['only_history']}+{result['only_cache']}  "
        f"greeks-gap {result['greeks_gap']}  unexplained {result['unexplained']}  "
        f"{gex}  [{result['status'].upper()}]"
        + (f"  GEX differs: {','.join(result['gex_differs'])}" if result["gex_differs"] else "")
        + (
            f"  e.g. {','.join(result['unexplained_examples'])}"
            if result["unexplained_examples"]
            else ""
        )
    )


def main(argv: Optional[List[str]] = None) -> int:
    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Compare the option_chains_latest snapshot with the history snapshot.",
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Symbols to check (default: ANALYTICS_UNDERLYINGS, else ANALYTICS_UNDERLYING)",
    )
    parser.add_argument(
        "--every", type=float, default=0, help="Repeat every N seconds (default: one sample)"
    )
    parser.add_argument(
        "--until", default=None, help="With --every, stop at this ET clock time, e.g. 16:10"
    )
    parser.add_argument("--json", action="store_true", help="One JSON object per line")
    parser.add_argument("--verbose", action="store_true", help="Engine logging at INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.ERROR,
        format="%(levelname)s %(name)s: %(message)s",
    )
    raw = ",".join(args.symbols) or _getenv_str(
        "ANALYTICS_UNDERLYINGS", _getenv_str("ANALYTICS_UNDERLYING", "SPY")
    )
    engines = [AnalyticsEngine(underlying=s) for s in parse_underlyings(raw)]
    if not engines:
        print("No valid symbols.", file=sys.stderr)
        return 2
    stop_at = dt_time.fromisoformat(args.until) if args.until else None

    totals: Dict[str, Dict[str, int]] = defaultdict(lambda: {"pass": 0, "fail": 0, "other": 0})
    while True:
        try:
            results = sample(engines)
        except Exception as e:
            print(f"DB error: {e}", file=sys.stderr)
            return 2
        for result in results:
            bucket = result["status"] if result["status"] in ("pass", "fail") else "other"
            totals[result["symbol"]][bucket] += 1
            print(json.dumps(result, default=str) if args.json else _line(result), flush=True)
        if args.every <= 0:
            break
        if stop_at is not None and datetime.now(ET).time() >= stop_at:
            break
        time.sleep(args.every)

    if not args.json:
        print()
        for symbol, t in totals.items():
            print(f"{symbol:<6} {t['pass']} passed, {t['fail']} failed, {t['other']} empty")
    failed = sum(t["fail"] for t in totals.values())
    print(
        "PARITY: " + ("FAIL" if failed else "PASS") + f" ({failed} failed samples)",
        file=sys.stderr,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
