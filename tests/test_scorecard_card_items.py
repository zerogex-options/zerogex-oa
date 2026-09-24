"""The daily scorecard lists every Action Card of the day, not just the first.

The public scorecard used to show a count and link only the day's first card,
so the rest of a day's calls could be found only by guessing ids. The query
already reads every card for the window; these tests pin that it returns them,
oldest first, with the fields the page's list renders.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from src.api.queries import signals as signals_mod
from src.api.queries.signals import SignalsQueriesMixin

_START = datetime(2026, 9, 23, 4, 0, tzinfo=timezone.utc)  # ET midnight (EDT)
_END = datetime(2026, 9, 24, 4, 0, tzinfo=timezone.utc)


def _row(card_id: int, minutes_after_start: int, action: str = "BUY_PUT_DEBIT") -> dict:
    return {
        "id": card_id,
        "timestamp": _START + timedelta(minutes=minutes_after_start),
        "pattern": "max_pain_gravitation",
        "action": action,
        "tier": "0DTE",
        "direction": "bearish",
        "confidence": 0.29,
    }


class _FakeConn:
    def __init__(self, card_rows: list[dict]) -> None:
        self.card_rows = card_rows
        self.sql: list[str] = []

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        self.sql.append(sql)
        return self.card_rows if "FROM signal_action_cards" in sql else []

    async def fetchrow(self, sql: str, *args: Any) -> None:
        return None


class _Stub(SignalsQueriesMixin):
    def __init__(self, card_rows: list[dict]) -> None:
        self.conn = _FakeConn(card_rows)

    @asynccontextmanager
    async def _acquire_connection(self):
        yield self.conn


def _scorecard(card_rows: list[dict]) -> tuple[dict, _FakeConn]:
    stub = _Stub(card_rows)
    coro = stub.get_daily_scorecard(
        symbol="QQQ", start_utc=_START, end_utc=_END, signal_names=[], horizon_minutes=60
    )
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro), stub.conn
    finally:
        loop.close()


def test_every_card_of_the_day_is_listed_oldest_first():
    rows = [_row(11270, 240), _row(11275, 575, "SELL_CALL_SPREAD"), _row(11290, 700)]
    out, conn = _scorecard(rows)

    cards = out["cards"]
    assert cards["total"] == 3
    assert cards["first_card_id"] == 11270
    assert [c["id"] for c in cards["items"]] == [11270, 11275, 11290]
    first = cards["items"][0]
    assert first == {
        "id": 11270,
        "timestamp": datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc),
        "pattern": "max_pain_gravitation",
        "action": "BUY_PUT_DEBIT",
        "tier": "0DTE",
        "direction": "bearish",
        "confidence": 0.29,
    }
    # Oldest first is the SQL's job; the list keeps whatever order it returns.
    card_sql = next(s for s in conn.sql if "FROM signal_action_cards" in s)
    assert "ORDER BY timestamp ASC, id ASC" in card_sql
    assert "action <> 'STAND_DOWN'" in card_sql


def test_quiet_day_has_an_empty_list_not_a_missing_key():
    out, _ = _scorecard([])
    assert out["cards"]["total"] == 0
    assert out["cards"]["items"] == []


def test_list_is_capped_but_the_total_is_exact(monkeypatch):
    monkeypatch.setattr(signals_mod, "SCORECARD_CARD_ITEMS_CAP", 2)
    rows = [_row(100 + i, 300 + i) for i in range(5)]
    out, _ = _scorecard(rows)
    assert out["cards"]["total"] == 5
    assert [c["id"] for c in out["cards"]["items"]] == [100, 101]
