"""Regression: the 4:05 PM receipt grades against what the morning committed.

``update_daily_forecast_receipt`` reads the morning row with an explicit column
list, then grades with ``row.get("pin_tolerance")``,
``row.get("regime_move_threshold")`` and ``row.get("raw_*")``. asyncpg's
``Record.get`` returns None for a column that was not SELECTed, so when the
list left those five out every receipt fell back silently: pins graded at the
$1.00 default instead of the committed (SPX ~$10-20) tolerance, regime calls
at the legacy 0.5% instead of the committed VIX-scaled threshold, and both raw
verdicts written as NULL.

The fake connection below answers the SELECT with ONLY the columns the SQL
names, the way asyncpg does, so any column the grader reads but the query
forgets shows up here as a wrong verdict.
"""

from __future__ import annotations

import asyncio
import re
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

_MORNING_ROW = {
    "projected_low": 6650.0,
    "projected_high": 6750.0,
    "pin_strike": 6700.0,
    "regime": "long_gamma",
    "open_spot": 6690.0,
    "receipt_ts": None,
    "pin_tolerance": 10.06,
    "regime_move_threshold": 0.012,
    "raw_projected_low": 6640.0,
    "raw_projected_high": 6760.0,
    "raw_pin_strike": 6705.0,
    "implied_move": None,
    "expected_vol_state": None,
    "flip_cross_prob": None,
    "level_touch_probs": None,
    "gamma_flip": None,
    "call_wall": None,
    "put_wall": None,
    "forecast_inputs": None,
}


def _selected_columns(sql: str) -> list[str]:
    body = sql[sql.index("SELECT") + len("SELECT") : sql.index("FROM")]
    return [c.strip() for c in body.split(",") if c.strip()]


class _Conn:
    def __init__(self):
        self.update_args: tuple | None = None

    async def fetchrow(self, sql, *args):
        if sql.lstrip().upper().startswith("SELECT"):
            cols = _selected_columns(sql)
            # Only what the query names, like an asyncpg Record.
            return {c: _MORNING_ROW[c] for c in cols if c in _MORNING_ROW}
        assert re.match(r"\s*UPDATE daily_forecast", sql)
        self.update_args = args
        return {"ok": True}


def _grade(actual_low: float, actual_high: float, actual_close: float) -> tuple:
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api.database import DatabaseManager

    db = DatabaseManager()
    conn = _Conn()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    result = asyncio.run(
        db.update_daily_forecast_receipt(
            "SPX",
            date(2026, 11, 13),
            datetime(2026, 11, 13, 21, 5, tzinfo=timezone.utc),
            actual_low,
            actual_high,
            actual_close,
        )
    )
    assert result is not None
    assert conn.update_args is not None
    return conn.update_args


def test_the_select_names_every_column_the_grader_reads():
    import inspect

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api.database import DatabaseManager

    source = inspect.getsource(DatabaseManager.update_daily_forecast_receipt)
    select_sql = source[source.index("SELECT") : source.index("FROM daily_forecast")]
    selected = set(_selected_columns(select_sql + "FROM"))
    read = set(re.findall(r'row(?:\.get\(|\[)"([a-z_]+)"', source))
    read |= set(re.findall(r'_rf\("([a-z_]+)"\)', source))
    assert read, "no row reads found; the test's patterns are stale"
    assert read <= selected, f"read but never selected: {sorted(read - selected)}"


def test_a_close_inside_the_committed_tolerance_is_a_pin_hit():
    # $6 from a 6700 pin, committed tolerance $10.06. The $1 fallback said miss.
    args = _grade(6660.0, 6720.0, 6706.0)
    pin_hit = args[7]
    raw_pin_hit = args[10]
    assert pin_hit is True
    # Raw pin 6705 is $1 away: graded at the same committed tolerance.
    assert raw_pin_hit is True


def test_the_raw_band_is_graded_not_written_as_null():
    args = _grade(6660.0, 6720.0, 6706.0)
    raw_range_respected = args[9]
    assert raw_range_respected is True
    # A low under the raw band's 6640 floor fails it.
    args = _grade(6630.0, 6720.0, 6706.0)
    assert args[9] is False


def test_the_regime_call_uses_the_committed_threshold():
    # long_gamma with a committed 1.2% threshold: a 0.9% close move is still
    # "chop". The legacy 0.5% fallback graded it wrong.
    close = 6690.0 * 1.009
    args = _grade(6660.0, close + 1, close)
    regime_correct = args[8]
    assert regime_correct is True
