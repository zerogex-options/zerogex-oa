import argparse
import asyncio
import traceback
from typing import Awaitable, Callable

from src.api.database import DatabaseManager


CheckFn = Callable[[DatabaseManager], Awaitable[object]]


def _build_checks(symbol: str) -> list[tuple[str, CheckFn]]:
    return [
        ("check_health", lambda db: db.check_health()),
        ("get_latest_quote", lambda db: db.get_latest_quote(symbol)),
        ("get_session_closes", lambda db: db.get_session_closes(symbol)),
        ("get_historical_quotes", lambda db: db.get_historical_quotes(symbol, window_units=5, timeframe="1min")),
        ("get_gex_summary", lambda db: db.get_gex_summary(symbol)),
        ("get_gex_by_strike", lambda db: db.get_gex_by_strike(symbol, limit=10)),
        ("get_gex_heatmap", lambda db: db.get_gex_heatmap(symbol, timeframe="5min", window_units=10)),
        ("get_flow_by_type", lambda db: db.get_flow_by_type(symbol, "current")),
        ("get_flow_by_strike", lambda db: db.get_flow_by_strike(symbol, "current", 10)),
        ("get_flow_by_expiration", lambda db: db.get_flow_by_expiration(symbol, "current", 10)),
        ("get_buying_pressure", lambda db: db.get_buying_pressure(symbol, "current")),
        ("get_smart_money_flow", lambda db: db.get_smart_money_flow(symbol, "current", 10)),
        ("get_max_pain_current", lambda db: db.get_max_pain_current(symbol, strike_limit=50)),
    ]


async def _run(symbol: str) -> int:
    db = DatabaseManager()
    await db.connect()
    failures = 0

    try:
        for name, fn in _build_checks(symbol):
            try:
                result = await fn(db)
                size = len(result) if isinstance(result, list) else ("ok" if result else "empty")
                print(f"[PASS] {name}: {size}")
            except Exception as exc:
                failures += 1
                print(f"[FAIL] {name}: {exc.__class__.__name__}: {exc}")
                traceback.print_exc()
    finally:
        await db.disconnect()

    print(f"\nCompleted {len(_build_checks(symbol))} checks with {failures} failure(s).")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run API Engine DB smoke checks to validate query compatibility after schema changes."
    )
    parser.add_argument("--symbol", default="SPY", help="Underlying symbol to test (default: SPY)")
    args = parser.parse_args()
    return asyncio.run(_run(args.symbol.upper()))


if __name__ == "__main__":
    raise SystemExit(main())
