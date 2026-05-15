"""Read-only ops CLI that routes Makefile inspection targets through the
canonical API query layer (``src.api.database.DatabaseManager``).

C2 follow-up: several ``make`` targets embedded hand-rolled SQL that
duplicated query-layer logic (e.g. ``flow-buying-pressure`` was a
byte-for-byte copy of ``DatabaseManager.get_flow_buying_pressure``'s
CTE; ``max-pain-*`` reproduced ``get_max_pain_current``'s snapshot read;
``gex-summary`` hand-selected ``gex_summary`` instead of the canonical
wall-fallback reader).  When the query layer changed, the Makefile
silently diverged.  Routing the highest-divergence-risk targets through
this CLI keeps the SQL in exactly one place.

This is intentionally read-only: it only calls ``get_*`` methods and
prints a table.  It does NOT replace ``make db-tail-*`` (raw table
tailing) — those are deliberately schema-direct.

Usage:
    python -m src.tools.db_query_cli gex-summary [SYMBOL]
    python -m src.tools.db_query_cli flow-buying-pressure [SYMBOL] [LIMIT]
    python -m src.tools.db_query_cli max-pain-current [SYMBOL]
    python -m src.tools.db_query_cli max-pain-expirations [SYMBOL]
    python -m src.tools.db_query_cli max-pain-strikes [SYMBOL] [LIMIT]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any, List, Sequence
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager

ET = ZoneInfo("America/New_York")


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _et(ts) -> str:
    try:
        return ts.astimezone(ET).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _print_table(headers: Sequence[str], rows: List[Sequence[Any]]) -> None:
    data = [list(map(str, headers))] + [[str(c) for c in r] for r in rows]
    widths = [max(len(r[i]) for r in data) for i in range(len(headers))]

    def line(vals):
        return " " + " | ".join(str(vals[i]).ljust(widths[i]) for i in range(len(widths)))

    print(line(data[0]))
    print("-" + "-+-".join("-" * w for w in widths))
    for r in data[1:]:
        print(line(r))
    print(f"({len(rows)} rows)")


async def _gex_summary(db: DatabaseManager, symbol: str, _limit: int) -> None:
    s = await db.get_latest_gex_summary(symbol)
    if not s:
        print(f"(no GEX summary for {symbol})")
        return
    headers = [
        "symbol",
        "time_et",
        "spot",
        "net_gex",
        "gamma_flip",
        "pcr",
        "max_pain",
        "call_wall",
        "put_wall",
    ]
    row = [
        s.get("symbol"),
        _et(s.get("timestamp")),
        f'{_f(s.get("spot_price")):,.2f}',
        f'{_f(s.get("net_gex")):,.0f}',
        (f'{_f(s.get("gamma_flip")):,.2f}' if s.get("gamma_flip") is not None else "N/A"),
        f'{_f(s.get("put_call_ratio")):.2f}',
        (f'{_f(s.get("max_pain")):,.2f}' if s.get("max_pain") is not None else "N/A"),
        (f'{_f(s.get("call_wall")):,.2f}' if s.get("call_wall") is not None else "N/A"),
        (f'{_f(s.get("put_wall")):,.2f}' if s.get("put_wall") is not None else "N/A"),
    ]
    _print_table(headers, [row])


async def _flow_buying_pressure(db: DatabaseManager, symbol: str, limit: int) -> None:
    rows = await db.get_flow_buying_pressure(symbol, limit)
    headers = ["time_et", "symbol", "price", "volume", "buy_pct", "period_buy_pct", "chg", "bias"]
    table = [
        [
            _et(r.get("timestamp")),
            r.get("symbol"),
            f'{_f(r.get("price")):,.2f}',
            f'{int(_f(r.get("volume"))):,}',
            f'{_f(r.get("buy_pct")):.2f}',
            f'{_f(r.get("period_buy_pct")):.2f}',
            f'{_f(r.get("price_chg")):+.2f}',
            r.get("momentum"),
        ]
        for r in rows
    ]
    _print_table(headers, table)


async def _max_pain_current(db: DatabaseManager, symbol: str, _limit: int) -> None:
    mp = await db.get_max_pain_current(symbol)
    if not mp:
        print(f"(no max-pain snapshot for {symbol})")
        return
    headers = ["symbol", "source_ts_et", "underlying", "max_pain", "difference", "num_expirations"]
    row = [
        mp.get("symbol"),
        _et(mp.get("timestamp")),
        f'{_f(mp.get("underlying_price")):,.2f}',
        f'{_f(mp.get("max_pain")):,.2f}',
        f'{_f(mp.get("difference")):+,.2f}',
        len(mp.get("expirations") or []),
    ]
    _print_table(headers, [row])


async def _max_pain_expirations(db: DatabaseManager, symbol: str, _limit: int) -> None:
    mp = await db.get_max_pain_current(symbol)
    if not mp:
        print(f"(no max-pain snapshot for {symbol})")
        return
    headers = ["expiration", "max_pain", "diff_from_underlying", "num_strikes"]
    table = [
        [
            e.get("expiration"),
            f'{_f(e.get("max_pain")):,.2f}',
            f'{_f(e.get("difference_from_underlying")):+,.2f}',
            len(e.get("strikes") or []),
        ]
        for e in (mp.get("expirations") or [])
    ]
    _print_table(headers, table)


async def _max_pain_strikes(db: DatabaseManager, symbol: str, limit: int) -> None:
    mp = await db.get_max_pain_current(symbol)
    if not mp:
        print(f"(no max-pain snapshot for {symbol})")
        return
    # get_max_pain_current returns expirations ordered ascending, so the
    # first entry is the nearest expiration (matches the Makefile target).
    expirations = mp.get("expirations") or []
    if not expirations:
        print(f"(no expirations in max-pain snapshot for {symbol})")
        return
    nearest = expirations[0]
    strikes = sorted(
        nearest.get("strikes") or [],
        key=lambda s: _f(s.get("settlement_price")),
    )[:limit]
    headers = ["settlement_price", "call_notional", "put_notional", "total_notional"]
    table = [
        [
            f'{_f(s.get("settlement_price")):,.2f}',
            f'{_f(s.get("call_notional")):,.0f}',
            f'{_f(s.get("put_notional")):,.0f}',
            f'{_f(s.get("total_notional")):,.0f}',
        ]
        for s in strikes
    ]
    print(f"Nearest expiration: {nearest.get('expiration')}")
    _print_table(headers, table)


_COMMANDS = {
    "gex-summary": _gex_summary,
    "flow-buying-pressure": _flow_buying_pressure,
    "max-pain-current": _max_pain_current,
    "max-pain-expirations": _max_pain_expirations,
    "max-pain-strikes": _max_pain_strikes,
}


async def _run(command: str, symbol: str, limit: int) -> None:
    db = DatabaseManager()
    await db.connect()
    try:
        await _COMMANDS[command](db, symbol.upper(), limit)
    finally:
        await db.disconnect()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("command", choices=sorted(_COMMANDS))
    parser.add_argument("symbol", nargs="?", default="SPY")
    parser.add_argument("limit", nargs="?", type=int, default=20)
    args = parser.parse_args(argv)
    asyncio.run(_run(args.command, args.symbol, args.limit))
    return 0


if __name__ == "__main__":
    sys.exit(main())
