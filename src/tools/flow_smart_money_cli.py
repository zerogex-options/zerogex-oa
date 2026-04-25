import asyncio
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager

HEADERS = [
    "time",
    "contract",
    "strike",
    "expiration",
    "dte",
    "option_type",
    "flow",
    "notional",
    "price",
    "score",
    "notional_class",
    "size_class",
]


def _to_float(value) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _fmt_row(row: dict) -> list[str]:
    ts_et = row["timestamp"].astimezone(ZoneInfo("America/New_York")).strftime("%H:%M")
    flow = int(row["flow"])
    notional_val = _to_float(row["notional"])
    price = (notional_val / (flow * 100)) if flow else 0.0
    return [
        ts_et,
        str(row["contract"])[:15],
        f'{_to_float(row["strike"]):.4f}',
        str(row["expiration"]),
        str(row["dte"]),
        str(row["option_type"]),
        f"{flow}",
        f"{notional_val:,.0f}",
        f"{price:.2f}",
        f'{_to_float(row["score"]):.0f}',
        str(row["notional_class"]).replace("$", ""),
        str(row["size_class"]),
    ]


def _print_table(rows: list[list[str]]) -> None:
    data = [HEADERS] + rows
    widths = [max(len(str(r[i])) for r in data) for i in range(len(HEADERS))]

    def line(vals):
        return " " + " | ".join(str(vals[i]).ljust(widths[i]) for i in range(len(widths)))

    print(line(HEADERS))
    print("-" + "-+-".join("-" * w for w in widths))
    for row in rows:
        print(line(row))
    print(f"({len(rows)} rows)")


async def _run() -> None:
    db = DatabaseManager()
    await db.connect()
    try:
        rows = await db.get_smart_money_flow("SPY", "current", 20)
    finally:
        await db.disconnect()

    _print_table([_fmt_row(row) for row in rows])


if __name__ == "__main__":
    asyncio.run(_run())
