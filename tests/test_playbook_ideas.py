"""Each pattern's latest idea (for one Card per idea), held-back idea writes,
and their wiring into the sync signal cycle."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from src import config
from src.signals.components.base import MarketContext
from src.signals.playbook import adaptive_gate, ideas
from src.signals.playbook.cycle import evaluate_and_persist
from src.signals.playbook.engine import PlaybookEngine
from src.signals.playbook.types import ActionEnum

TS = datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(ideas, "_table_missing_until", 0.0)
    monkeypatch.setattr(ideas, "_last_write_warning", 0.0)
    monkeypatch.setattr(config, "PLAYBOOK_ONE_CARD_PER_IDEA", True)
    monkeypatch.setattr(config, "PLAYBOOK_ADAPTIVE_GATE_ENABLED", True)
    adaptive_gate.set_active_store(None)
    yield
    adaptive_gate.set_active_store(None)


class _MissingTable(Exception):
    pgcode = "42P01"


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows: list = []

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        for needle, outcome in self.conn.script:
            if needle in sql:
                if isinstance(outcome, Exception):
                    raise outcome
                self._rows = outcome
                return
        self._rows = []

    def fetchall(self):
        return self._rows


@dataclass
class _Conn:
    script: list = field(default_factory=list)  # [(sql substring, rows | exception)]
    executed: list = field(default_factory=list)
    commits: int = 0
    rollbacks: int = 0

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def sql_containing(self, needle):
        return [s for s, _ in self.executed if needle in s]


# ----------------------------------------------------------------------
# Loading
# ----------------------------------------------------------------------


def test_rows_become_open_positions():
    rows = [
        ("p", "bullish", "BUY_CALL_DEBIT", TS, "90", None),
        {
            "pattern": "q",
            "direction": "bearish",
            "action": "BUY_PUT_DEBIT",
            "issued_at": "2026-05-01T18:00:00Z",
            "max_hold": 120.0,
            "outcome": "stop_hit",
        },
        ("r", "bullish", "BUY_CALL_DEBIT", None, "90", None),  # no timestamp: skipped
    ]
    out = ideas.open_positions_from_rows(rows, "SPY")
    assert [(o.pattern_id, o.max_hold_minutes, o.status) for o in out] == [
        ("p", 90, "pending"),
        ("q", 120, "stop_hit"),
    ]
    assert out[1].opened_at == datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)


def test_loader_reads_published_and_held_back_ideas():
    conn = _Conn(script=[("UNION ALL", [("p", "bullish", "BUY_CALL_DEBIT", TS, "90", None)])])
    out = ideas.load_open_ideas_sync(conn, "SPY")
    assert [o.pattern_id for o in out] == ["p"]
    sql = conn.sql_containing("UNION ALL")[0]
    assert "playbook_card_outcomes" in sql and "card_id IS NULL" in sql


def test_loader_falls_back_to_published_cards_when_the_table_is_missing():
    conn = _Conn(
        script=[
            ("UNION ALL", _MissingTable("relation does not exist")),
            ("FROM signal_action_cards", [("p", "bullish", "BUY_CALL_DEBIT", TS, "90", None)]),
        ]
    )
    out = ideas.load_open_ideas_sync(conn, "SPY")
    assert [o.pattern_id for o in out] == ["p"]
    assert conn.rollbacks == 1  # the aborted transaction is cleared first
    # And it stops trying the missing table for a while.
    conn.executed.clear()
    ideas.load_open_ideas_sync(conn, "SPY")
    assert conn.sql_containing("UNION ALL") == []


def test_loader_off_switch(monkeypatch):
    monkeypatch.setattr(config, "PLAYBOOK_ONE_CARD_PER_IDEA", False)
    conn = _Conn()
    assert ideas.load_open_ideas_sync(conn, "SPY") == []
    assert conn.executed == []


def test_loader_never_raises():
    conn = _Conn(script=[("SELECT", RuntimeError("db down"))])
    assert ideas.load_open_ideas_sync(conn, "SPY") == []


# ----------------------------------------------------------------------
# Held-back idea writes
# ----------------------------------------------------------------------


def _card_dict(**kw) -> dict[str, Any]:
    card = {
        "underlying": "SPY",
        "timestamp": TS,
        "action": "BUY_PUT_DEBIT",
        "pattern": "call_wall_fade",
        "tier": "0DTE",
        "direction": "bearish",
        "confidence": 0.5,
        "max_hold_minutes": 90,
        "entry": {"ref_price": 678.4, "trigger": "at_touch"},
        "target": {"ref_price": 675.0, "kind": "level"},
        "stop": {"ref_price": 680.0, "kind": "premium_pct"},
    }
    card.update(kw)
    return card


def test_held_back_idea_is_written_with_its_levels():
    conn = _Conn()
    ideas.insert_held_back_idea_sync(conn, _card_dict(), "paused: record")
    ((sql, params),) = [
        (s, p) for s, p in conn.executed if "INSERT INTO playbook_card_outcomes" in s
    ]
    assert "ON CONFLICT DO NOTHING" in sql
    assert params[0:2] == ("SPY", "call_wall_fade")
    assert params[7] == "paused: record"
    # entry, trigger, target, stop, hold. call_wall_fade labels its stop
    # premium_pct but prints the wall price; it is graded as that price.
    assert params[8:13] == (678.4, "at_touch", 675.0, 680.0, 90)
    assert conn.commits == 1


def test_a_printed_stop_price_is_read_as_a_price_whatever_its_label():
    levels = ideas.idea_levels(_card_dict())
    assert levels["stop_price"] == 680.0
    # A premium-sized number is not an underlying price.
    card = _card_dict(stop={"ref_price": 1.35, "kind": "premium_pct"})
    assert ideas.idea_levels(card)["stop_price"] is None
    # No price at all (signal-event exits).
    card = _card_dict(stop={"ref_price": None, "kind": "signal_event"})
    assert ideas.idea_levels(card)["stop_price"] is None


def test_held_back_write_backs_off_when_the_table_is_missing():
    conn = _Conn(script=[("INSERT", _MissingTable("missing"))])
    ideas.insert_held_back_idea_sync(conn, _card_dict(), "r")
    assert conn.rollbacks == 1
    conn.executed.clear()
    ideas.insert_held_back_idea_sync(conn, _card_dict(), "r")
    assert conn.executed == []


# ----------------------------------------------------------------------
# Cycle wiring
# ----------------------------------------------------------------------


def _market_ctx() -> MarketContext:
    return MarketContext(
        timestamp=TS,
        underlying="SPY",
        close=678.4,
        net_gex=7.1e9,
        gamma_flip=676.5,
        put_call_ratio=0.36,
        max_pain=675.0,
        smart_call=-765000.0,
        smart_put=-134000.0,
        recent_closes=[],
        iv_rank=None,
        extra={"vix_level": 16.7, "call_wall": 678.0, "put_wall": 674.0},
    )


def _result(name, score, context=None):
    return SimpleNamespace(name=name, score=float(score), context=context or {})


_ADVANCED = [_result("trap_detection", -0.35, {"signal": "bearish_fade", "triggered": True})]
_BASIC = [_result("tape_flow_bias", -0.50), _result("positioning_trap", -0.30)]
_SCORE = SimpleNamespace(composite_score=0.0, direction="high_risk_reversal", components={})


def _engine():
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CWF

    return PlaybookEngine(patterns=[CWF])


def test_cycle_blocks_a_repeat_of_a_live_idea():
    conn = _Conn(
        script=[("UNION ALL", [("call_wall_fade", "bearish", "BUY_PUT_DEBIT", TS, "90", None)])]
    )
    card = evaluate_and_persist(
        engine=_engine(),
        market_context=_market_ctx(),
        score=_SCORE,
        advanced_results=_ADVANCED,
        basic_results=_BASIC,
        conn=conn,
    )
    assert card.action == ActionEnum.STAND_DOWN
    assert conn.sql_containing("INSERT INTO signal_action_cards") == []


def test_cycle_records_an_idea_the_bar_held_back():
    adaptive_gate.set_active_store(
        adaptive_gate.build_store([("call_wall_fade", "SPY", "bearish", 40, 40.0, 10, 30, -20.0)])
    )
    conn = _Conn()
    card = evaluate_and_persist(
        engine=_engine(),
        market_context=_market_ctx(),
        score=_SCORE,
        advanced_results=_ADVANCED,
        basic_results=_BASIC,
        conn=conn,
    )
    assert card.action == ActionEnum.STAND_DOWN
    assert conn.sql_containing("INSERT INTO signal_action_cards") == []
    assert len(conn.sql_containing("INSERT INTO playbook_card_outcomes")) == 1


def test_cycle_publishes_as_before_without_a_record():
    conn = _Conn()
    card = evaluate_and_persist(
        engine=_engine(),
        market_context=_market_ctx(),
        score=_SCORE,
        advanced_results=_ADVANCED,
        basic_results=_BASIC,
        conn=conn,
    )
    assert card.pattern == "call_wall_fade"
    assert len(conn.sql_containing("INSERT INTO signal_action_cards")) == 1
    assert conn.sql_containing("INSERT INTO playbook_card_outcomes") == []
