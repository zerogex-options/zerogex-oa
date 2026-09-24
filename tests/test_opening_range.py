"""The 09:30-10:00 ET opening range, end to end.

``src/opening_range.py`` is the one definition every consumer reads. These
lock in the window (complete at 10:00 ET, only on the session's own day), the
bar floor, and each consumer's wiring: the live signal cycle (read once per
session, not per 1Hz cycle), the live Playbook context the
``opening_range_break`` pattern reads, the ``/api/signals/action`` path, the
TradeWorkz snapshot, and the ``OpeningRangeHunter`` bot.

Before this, the pattern and the bot each compared the close against the
extreme of a window that included the close's own bar, and neither could ever
fire. ``test_live_cycle_context_fires_the_pattern`` is the regression that
matters: it drives the pattern through the same context the live cycle builds.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.opening_range import (
    MIN_BARS,
    OPENING_RANGE_SQL,
    opening_range_from_row,
    opening_range_window,
)

# 2026-09-23 is a Wednesday on EDT (UTC-4): 09:30 ET == 13:30 UTC.
OPEN_UTC = datetime(2026, 9, 23, 13, 30, tzinfo=timezone.utc)
OR_END_UTC = OPEN_UTC + timedelta(minutes=30)  # 10:00 ET


# ----------------------------------------------------------------------
# The window
# ----------------------------------------------------------------------


def test_window_is_unavailable_while_the_range_is_forming():
    assert opening_range_window(OR_END_UTC - timedelta(seconds=1)) is None
    assert opening_range_window(OPEN_UTC) is None


def test_window_is_09_30_to_10_00_et_once_complete():
    assert opening_range_window(OR_END_UTC) == (OPEN_UTC, OR_END_UTC)
    assert opening_range_window(OR_END_UTC + timedelta(hours=6)) == (OPEN_UTC, OR_END_UTC)


def test_window_follows_the_clock_across_dst():
    # 2026-12-02 is a Wednesday on EST (UTC-5): 09:30 ET == 14:30 UTC.
    start = datetime(2026, 12, 2, 14, 30, tzinfo=timezone.utc)
    assert opening_range_window(start + timedelta(minutes=45)) == (
        start,
        start + timedelta(minutes=30),
    )


def test_next_mornings_premarket_never_reads_yesterdays_range():
    # 07:00 ET on Thursday belongs to Wednesday's cash session, but it is
    # not Wednesday any more.
    thursday_premarket = datetime(2026, 9, 24, 11, 0, tzinfo=timezone.utc)
    assert opening_range_window(thursday_premarket) is None


def test_weekend_has_no_window():
    saturday_midday = datetime(2026, 9, 26, 16, 0, tzinfo=timezone.utc)
    assert opening_range_window(saturday_midday) is None


def test_naive_timestamps_are_utc():
    assert opening_range_window(OR_END_UTC.replace(tzinfo=None)) == (OPEN_UTC, OR_END_UTC)


# ----------------------------------------------------------------------
# The bar floor
# ----------------------------------------------------------------------


def test_row_with_enough_bars_is_the_range():
    assert opening_range_from_row((Decimal("773.40"), Decimal("770.85"), 30)) == (
        773.40,
        770.85,
    )


@pytest.mark.parametrize(
    "row",
    [
        None,
        (),
        (None, None, 0),  # no bars at all
        (773.4, 770.85, MIN_BARS - 1),  # a feed gap at the open
        (770.0, 771.0, 30),  # inverted -- never trust it
    ],
)
def test_row_without_a_trustworthy_range_is_none(row):
    assert opening_range_from_row(row) is None


# ----------------------------------------------------------------------
# Live signal cycle: read once per session, not once per 1Hz cycle
# ----------------------------------------------------------------------


class _Cursor:
    def __init__(self, row):
        self.row = row
        self.calls: list = []

    def execute(self, sql, params=None):
        if isinstance(self.row, Exception):
            raise self.row
        self.calls.append((sql, params))

    def fetchone(self):
        return self.row


def _engine():
    from src.signals.unified_signal_engine import UnifiedSignalEngine

    eng = UnifiedSignalEngine.__new__(UnifiedSignalEngine)
    eng.db_symbol = "SPY"
    eng._opening_range_cache = None
    return eng


def test_engine_does_not_query_before_10_00():
    eng, cur = _engine(), _Cursor((773.4, 770.85, 30))
    assert eng._fetch_opening_range(cur, None, OR_END_UTC - timedelta(minutes=1)) is None
    assert cur.calls == []


def test_engine_reads_the_window_then_caches_it_for_the_session():
    eng, cur = _engine(), _Cursor((773.4, 770.85, 30))

    # 10:00 bar: read (the 09:59 bar may still take a last update).
    assert eng._fetch_opening_range(cur, None, OR_END_UTC) == (773.4, 770.85)
    assert cur.calls == [(OPENING_RANGE_SQL, ("SPY", OPEN_UTC, OR_END_UTC))]
    # Same bar again (the 1Hz cycle): no second read.
    eng._fetch_opening_range(cur, None, OR_END_UTC)
    assert len(cur.calls) == 1
    # 10:01 bar: one more read, and that one is final.
    eng._fetch_opening_range(cur, None, OR_END_UTC + timedelta(minutes=1))
    assert len(cur.calls) == 2
    for minute in range(2, 60):
        eng._fetch_opening_range(cur, None, OR_END_UTC + timedelta(minutes=minute))
    assert len(cur.calls) == 2


def test_engine_retries_an_incomplete_range_once_per_bar():
    eng, cur = _engine(), _Cursor((773.4, 770.85, MIN_BARS - 1))
    t = OR_END_UTC + timedelta(minutes=5)
    assert eng._fetch_opening_range(cur, None, t) is None
    assert eng._fetch_opening_range(cur, None, t) is None
    assert len(cur.calls) == 1
    # A backfill fills the hole; the next bar picks it up.
    cur.row = (773.4, 770.85, 30)
    assert eng._fetch_opening_range(cur, None, t + timedelta(minutes=1)) == (773.4, 770.85)


def test_engine_read_failure_is_none_and_resets_the_transaction():
    rolled_back: list = []
    conn = SimpleNamespace(rollback=lambda: rolled_back.append(True))
    eng, cur = _engine(), _Cursor(RuntimeError("boom"))
    assert eng._fetch_opening_range(cur, conn, OR_END_UTC + timedelta(minutes=5)) is None
    assert rolled_back == [True]


def test_engine_starts_fresh_the_next_session():
    eng, cur = _engine(), _Cursor((773.4, 770.85, 30))
    eng._fetch_opening_range(cur, None, OR_END_UTC + timedelta(minutes=5))
    thursday = OR_END_UTC + timedelta(days=1, minutes=5)
    cur.row = (770.0, 766.0, 30)
    assert eng._fetch_opening_range(cur, None, thursday) == (770.0, 766.0)


def test_live_fetch_publishes_the_range():
    """The whole live fetch, answered by query text, carries the range out."""
    from unittest.mock import MagicMock, patch

    import src.signals.unified_signal_engine as use

    with patch.object(use, "get_canonical_symbol", return_value="SPY"):
        eng = use.UnifiedSignalEngine("SPY")

    now = OR_END_UTC + timedelta(minutes=10)  # 10:10 ET
    market_row = (now, 769.78, now, -1.0e9, 769.0, 0.001, 5.0e8, 0.05, 1.1, 770.0, 10_000, 10_000)
    state = {"sql": ""}
    first = {"done": False}

    def _execute(sql, params=None):
        state["sql"] = sql

    def _fetchone():
        if not first["done"]:
            first["done"] = True
            return market_row
        return (773.4, 770.85, 30) if state["sql"] == use.OPENING_RANGE_SQL else None

    cursor = MagicMock()
    cursor.execute.side_effect = _execute
    cursor.fetchone.side_effect = _fetchone
    cursor.fetchall.return_value = []
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cursor

    ctx = eng._fetch_market_context(conn=conn)
    assert ctx["opening_range_high"] == 773.4
    assert ctx["opening_range_low"] == 770.85
    market = eng._build_market_context(ctx)
    assert market.extra["opening_range_high"] == 773.4
    assert market.extra["opening_range_low"] == 770.85


def _live_ctx_dict(ts, close, opening_range):
    closes = [close + 0.1 * (25 - i) for i in range(26)]
    return {
        "timestamp": ts,
        "close": close,
        "net_gex": -1.0e9,
        "gamma_flip": 769.0,
        "put_call_ratio": 1.1,
        "max_pain": None,
        "smart_call": 0.0,
        "smart_put": 0.0,
        "recent_closes": closes,
        "recent_lows": [c - 0.05 for c in closes],
        "recent_highs": [c + 0.05 for c in closes],
        "opening_range_high": opening_range[0] if opening_range else None,
        "opening_range_low": opening_range[1] if opening_range else None,
    }


def test_live_cycle_context_fires_the_pattern():
    """The regression: through the same context the live cycle builds, a
    close under the opening-range low now produces a put card."""
    from src.signals.playbook.cycle import build_context_from_cycle
    from src.signals.playbook.patterns.opening_range_break import PATTERN as ORB

    ts = OR_END_UTC + timedelta(minutes=10)  # 10:10 ET
    market = _engine()._build_market_context(_live_ctx_dict(ts, 769.78, (773.4, 770.85)))
    score = SimpleNamespace(composite_score=42.3, direction="controlled_trend", components={})
    ctx = build_context_from_cycle(
        market_context=market, score=score, advanced_results=[], basic_results=[]
    )
    assert ctx.level("opening_range_high") == 773.4
    assert ctx.level("opening_range_low") == 770.85

    card = ORB.match(ctx)
    assert card is not None
    assert card.direction == "bearish"
    assert card.stop.ref_price == 773.4


def test_live_cycle_context_without_a_range_stands_down():
    from src.signals.playbook.cycle import build_context_from_cycle
    from src.signals.playbook.patterns.opening_range_break import PATTERN as ORB

    ts = OR_END_UTC + timedelta(minutes=10)
    market = _engine()._build_market_context(_live_ctx_dict(ts, 769.78, None))
    score = SimpleNamespace(composite_score=42.3, direction="controlled_trend", components={})
    ctx = build_context_from_cycle(
        market_context=market, score=score, advanced_results=[], basic_results=[]
    )
    assert ORB.match(ctx) is None


# ----------------------------------------------------------------------
# /api/signals/action path
# ----------------------------------------------------------------------


class _FakeAsyncConn:
    def __init__(self, row, seen):
        self._row, self._seen = row, seen

    async def fetchrow(self, *args):
        self._seen.append(args)
        return self._row


class _FakeAcquire:
    def __init__(self, row, seen):
        self._row, self._seen = row, seen

    async def __aenter__(self):
        return _FakeAsyncConn(self._row, self._seen)

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_api_reads_the_same_window(monkeypatch):
    from src.api.database import DatabaseManager
    from src.opening_range import OPENING_RANGE_SQL_ASYNC

    db, seen = DatabaseManager(), []
    monkeypatch.setattr(db, "_acquire_connection", lambda: _FakeAcquire((773.4, 770.85, 30), seen))
    got = await db.get_opening_range("SPY", OR_END_UTC + timedelta(minutes=10))
    assert got == (773.4, 770.85)
    assert seen == [(OPENING_RANGE_SQL_ASYNC, "SPY", OPEN_UTC, OR_END_UTC)]


@pytest.mark.asyncio
async def test_api_is_none_before_10_00_and_on_error(monkeypatch):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    def _boom():
        raise RuntimeError("pool down")

    monkeypatch.setattr(db, "_acquire_connection", _boom)
    assert await db.get_opening_range("SPY", OR_END_UTC - timedelta(minutes=1)) is None
    assert await db.get_opening_range("SPY", OR_END_UTC + timedelta(minutes=10)) is None


def test_api_context_puts_the_range_in_levels():
    from src.signals.playbook.context_builder import _build_market_context, _extract_levels

    market = _build_market_context(
        underlying="SPY",
        timestamp=OR_END_UTC + timedelta(minutes=10),
        score_row={"components": {}},
        advanced={},
        opening_range=(773.4, 770.85),
    )
    levels = _extract_levels(market.extra, {})
    assert levels["opening_range_high"] == 773.4
    assert levels["opening_range_low"] == 770.85


# ----------------------------------------------------------------------
# TradeWorkz snapshot and the OpeningRangeHunter bot
# ----------------------------------------------------------------------


class _SavepointCursor:
    def __init__(self, row):
        self.row = row
        self.calls: list = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if sql == OPENING_RANGE_SQL and isinstance(self.row, Exception):
            raise self.row

    def fetchone(self):
        return self.row


def test_snapshot_reads_the_window_inside_a_savepoint():
    from src.tradeworkz.context import _fetch_opening_range

    cur = _SavepointCursor((773.4, 770.85, 30))
    conn = SimpleNamespace(cursor=lambda: cur)
    assert _fetch_opening_range(conn, "SPY", OR_END_UTC + timedelta(minutes=10)) == (
        773.4,
        770.85,
    )
    sqls = [c[0] for c in cur.calls]
    assert sqls == [
        "SAVEPOINT tw_opening_range",
        OPENING_RANGE_SQL,
        "RELEASE SAVEPOINT tw_opening_range",
    ]
    # The read never reaches past 10:00 ET, whatever instant a backtest rebuilds.
    assert cur.calls[1][1] == ("SPY", OPEN_UTC, OR_END_UTC)


def test_snapshot_read_failure_rolls_back_to_its_savepoint():
    from src.tradeworkz.context import _fetch_opening_range

    cur = _SavepointCursor(RuntimeError("boom"))
    conn = SimpleNamespace(cursor=lambda: cur)
    assert _fetch_opening_range(conn, "SPY", OR_END_UTC + timedelta(minutes=10)) == (None, None)
    assert cur.calls[-1][0] == "ROLLBACK TO SAVEPOINT tw_opening_range"


def test_snapshot_skips_the_read_before_10_00():
    from src.tradeworkz.context import _fetch_opening_range

    cur = _SavepointCursor((773.4, 770.85, 30))
    conn = SimpleNamespace(cursor=lambda: cur)
    assert _fetch_opening_range(conn, "SPY", OR_END_UTC - timedelta(minutes=5)) == (None, None)
    assert cur.calls == []


def _hunter():
    from src.tradeworkz.bots.opening_range_hunter import OpeningRangeHunter
    from src.tradeworkz.models import BotSpec

    spec = BotSpec(
        id="opening_range_hunter",
        display_name="Opening Range Hunter",
        strategy_class="OpeningRangeHunter",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params={"break_buffer_pct": 0.0005, "max_hold_minutes": 90},
    )
    return OpeningRangeHunter(spec, ml_state=None)


def _snap(**over):
    from src.tradeworkz.context import MarketSnapshot

    base = dict(
        underlying="SPY",
        timestamp=OR_END_UTC + timedelta(minutes=10),  # 10:10 ET
        spot=769.78,
        net_gex=-1.0e9,
        put_wall=768.0,
        call_wall=775.0,
        # The trailing 24 hours always contain the bar spot came from.
        session_high=774.6,
        session_low=769.70,
        opening_range_high=773.4,
        opening_range_low=770.85,
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_bot_trades_the_opening_range_breakdown():
    sig = _hunter().open_criteria(_snap())
    assert sig is not None
    assert sig.direction == "bearish"
    assert sig.strategy_type == "BUY_PUT_DEBIT"
    assert sig.stop_price == 770.85
    assert sig.components_at_entry["opening_range_low"] == 770.85


def test_bot_trades_the_opening_range_breakout():
    sig = _hunter().open_criteria(_snap(spot=774.2, session_high=774.3))
    assert sig is not None
    assert sig.direction == "bullish"
    assert sig.stop_price == 773.4


def test_bot_stands_down_inside_the_range_or_without_one():
    assert _hunter().open_criteria(_snap(spot=772.0)) is None
    assert _hunter().open_criteria(_snap(opening_range_high=None, opening_range_low=None)) is None


def test_bot_waits_for_10_00():
    assert _hunter().open_criteria(_snap(timestamp=OR_END_UTC - timedelta(minutes=5))) is None
